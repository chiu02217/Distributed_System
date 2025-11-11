from concurrent import futures
import time
import grpc
import glob
import os
import sys
import uuid
from queue import Queue
import threading
from src.afs_client.afs_client import AFSClient
from src.common.grpc.auto_generated import file_operation_message_pb2 as afs_messages
from src.common.grpc.auto_generated import file_operation_service_pb2_grpc as afs_service
from src.common.grpc.auto_generated import coordinator_message_pb2 as coordinator_messages
from src.common.grpc.auto_generated import coordinator_service_pb2_grpc as coordinator_service 
from src.common.grpc.auto_generated import snapshot_message_pb2 as snapshot_messages
from src.common.grpc.auto_generated import snapshot_service_pb2_grpc as snapshot_service
from src.afs_coordinator.snapshot.coordinator_snapshot import CoordinatorSnapshotHandler
from src.common.config_loader import CONFIG

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def safe_call(func, max_retries=3, delay=1, *args, **kwargs):
    """
    Safely call a gRPC function with retries on failure.
     
    Args:
        func: The gRPC function to call.
        max_retries: Maximum number of retries.
        delay: Delay between retries in seconds.
        *args, **kwargs: Arguments to pass to the gRPC function.
    """
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except grpc.RpcError as e:
            if attempt < max_retries - 1:
                print(f"[safe_call] gRPC error on attempt {attempt + 1}/{max_retries}: {e}")
                time.sleep(delay)
            else:
                print(f"[safe_call] Failed after {max_retries} attempts: {e}")
                return None
        except Exception as e:
            print(f"[safe_call] Unexpected error: {e}")
            import traceback
            traceback.print_exc()
            return None
    return None
        
class CoordinatorServicer(coordinator_service.CoordinatorServiceServicer, snapshot_service.SnapshotServiceServicer):
    def __init__(self):
        self.task_queue = Queue() 
        
        self.assigned_tasks = {} 
        
        self.all_primes = set()
        
        # Locks for thread safety
        self.task_lock = threading.Lock()
        self.processing_lock = threading.Lock()
        self.primes_lock = threading.Lock()
        self.worker_regis_lock = threading.Lock()
        self.heartbeat_lock = threading.Lock()
        self.submission_lock = threading.Lock()
        self.submitted_requests = set() 
        
        # snapshot related
        self.workers = {}  # {worker_id: worker_address}
        self.worker_stubs = {}  # {worker_id: grpc stub}
        self.is_finished = False
        
        # task management
        self.total_tasks = 0
        self.completed_tasks = 0
        
        # higher cache size for grpc
        grpc_options = [
            ('grpc.max_receive_message_length', 100 * 1024 * 1024),
            ('grpc.max_send_message_length', 100 * 1024 * 1024)  
        ]

        # AFS connection setup
        afs_server_address = CONFIG.afs.server_address
        relative_cache_path = CONFIG.afs.afs_temp_path
        base_cache_dir = os.path.normpath(os.path.join(PROJECT_ROOT, relative_cache_path))
        coordinator_cache_dir = os.path.join(base_cache_dir, "coordinator")

        afs_channel = grpc.insecure_channel(afs_server_address, options=grpc_options)
        self.afs_client = AFSClient(channel=afs_channel, cache_dir=coordinator_cache_dir)

        print(f"[Coordinator] Connecting to AFS at {afs_server_address}...")
        
        # snapshot manager
        self.snapshot_manager = CoordinatorSnapshotHandler(self)
        
        # check for existing snapshots and recover state if found
        recovered = self.snapshot_manager.coor_recover_from_snapshot()
        if not recovered:
            print("[Coordinator] No snapshot found. Starting fresh.")
            self._load_tasks_from_afs()
        
        # handle heartbeat logs
        self.last_heartbeat = {}
        self.timeout_threshold = 10  # seconds
        threading.Thread(target=self._monitor_heartbeats, daemon=True).start()
        
        print(f"[Coordinator] Loaded tasks from AFS.")

        # start snapshot thread
        self.snapshot_manager.start_snapshot_thread()

    # load files(tasks) from afs
    # using new safe_call
    def _load_tasks_from_afs(self):
        todo_tasks = safe_call(self.afs_client.list_files, 
                              max_retries=5, 
                              delay=2,
                              path="inputs")

        if todo_tasks is None:
            print("[Coordinator] No tasks or error loading files from AFS after retries.")

        for task in todo_tasks:
            # Filter files with 'input_dataset_' prefix
            # may use enum in the future
            if task.startswith("input_dataset_"):
                self.task_queue.put(task)
                self.total_tasks += 1
        
        print(f"[Coordinator] Successfully loaded {len(todo_tasks)} tasks from AFS")

    def GetTask(self, request, context):
        """
        Handle worker's task request.
        
        Logic:
        1. If worker_id is in processing_tasks, it means the worker is restarted -> resume signal
        2. Otherwise, pop a new task from task_queue and assign it to the worker.
        3. If no tasks are left, return has_task=False.
        """
        response = coordinator_messages.GetTaskResponse()
        worker_id = request.worker_id
       
        # logic 1: resume task if worker is restarting
        with self.processing_lock:
            if worker_id in self.assigned_tasks:
                filename = self.assigned_tasks[worker_id]
                response.filename = filename
                response.has_task = True
                response.is_resume = True
                print(f"[Coordinator] Resuming task: {filename} for worker {worker_id}")
                return response
        
        # logic 2: pop a new task
        with self.task_lock:
            # logic 3: empty queue management
            if self.task_queue.empty():
                response.has_task = False
                print(f"[Coordinator] No tasks left to assign to worker {worker_id}.")
                return response
            
            filename = self.task_queue.get()
            response.has_task = True
            response.filename = filename
            self.assigned_tasks[request.worker_id] = filename

            print(f"[Coordinator] Assigned task: {response.filename} to worker {worker_id}.")
            return response
        
        return response

    def SubmitResult(self, request: coordinator_messages.SubmitResultRequest, context):
        """
        Handle worker's result submission.
        Logic:
        1. Update the set of all primes with the primes received from the worker.
        2. Remove the task from assigned_tasks.
        """
        self.snapshot_manager.process_result_from_worker(request)
        # logic 1: update primes set
        with self.primes_lock:
            self.all_primes.update(request.primes)

        with self.task_lock:
            self.assigned_tasks.pop(request.worker_id, None)
            print(f"[Coordinator] Received results for task {request.filename} from worker {request.worker_id}. Total unique primes so far: {len(self.all_primes)}")
            
            if self.task_queue.empty() and not self.assigned_tasks:
                self.is_finished = True
                print("[Coordinator] All tasks completed. Saving results...")
                self._save_results()

        return coordinator_messages.SubmitResultResponse(success=True)
   
    # snapshot related
    def RegisterWorkerId(self, request: snapshot_messages.RegisterWorkerIdRequest, context):
        worker_id = request.worker_id
        worker_address = request.worker_address
        
        with self.worker_regis_lock:
            if worker_id not in self.workers:
                print(f"[Coordinator] register new Worker: {worker_id} @ {worker_address}")
                channel = grpc.insecure_channel(worker_address)
                stub = snapshot_service.SnapshotServiceStub(channel)

                self.workers[worker_id] = worker_address
                self.worker_stubs[worker_id] = stub
            else:
                print(f"[Coordinator] Worker {worker_id} re-registered.")

        return snapshot_messages.RegisterWorkerIdResponse(success=True)

    def _save_results(self):
        filename = 'primes.txt'
        handle = None
        
        try:
            handle = safe_call(
                self.afs_client.create_file, 
                3, 1, 
                filename
            )
            
            if handle is None:
                print(f"[Coordinator] Error creating file {filename} in AFS.")
                return
            
            all_data = "\n".join(str(prime) for prime in sorted(self.all_primes))

            write_success = safe_call(
                self.afs_client.write_file,
                5, 2,
                handle,
                all_data
            )
            
            if not write_success:
                print(f"[Coordinator] Error writing to file {filename} in AFS.")
                return
            
            print(f"[Coordinator] Saved {len(self.all_primes)} unique primes to {filename} in AFS.")
            
        except Exception as e:
            print(f"[Coordinator] Exception while saving results to AFS: {e}")
        
        finally:
            if handle is not None:
                safe_call(
                    self.afs_client.close_file,
                    5, 2,
                    handle
                )
                print(f"[Coordinator] Closed file {filename} in AFS.")

    def Heartbeat(self, request: coordinator_messages.HeartbeatRequest, context):
        worker_id = request.worker_id
        self.last_heartbeat[worker_id] = time.time()
        print(f"[Coordinator] Heartbeat received from {worker_id}")
        return coordinator_messages.HeartbeatResponse(acknowledged=True) 
    
    def _monitor_heartbeats(self):
        while not self.is_finished:
            now = time.time()
            for worker_id, last_seen in list(self.last_heartbeat.items()):
                if now - last_seen > self.timeout_threshold:
                    print(f"[Coordinator] Worker {worker_id} timed out")
            time.sleep(2)

def run_coordinator_server():
    port = CONFIG.coordinator.port
    coordinator_servicer = CoordinatorServicer()
    coordinator_server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    coordinator_service.add_CoordinatorServiceServicer_to_server(
        coordinator_servicer, coordinator_server
    )
    snapshot_service.add_SnapshotServiceServicer_to_server(
        coordinator_servicer, coordinator_server
    )
    coordinator_server.add_insecure_port(f'[::]:{port}')
    coordinator_server.start()
    print(f"[Coordinator] Server started on port {port}")
    
    try:
        while not coordinator_servicer.is_finished:
            time.sleep(1)
        print("[Coordinator] All tasks processed. Press Ctrl+C to stop the server.")
        coordinator_server.wait_for_termination()
    except KeyboardInterrupt:
        print("[Coordinator] Shutting down server...")
        if not coordinator_servicer.is_finished:
            coordinator_servicer._save_results()
            print("[Coordinator] Not all tasks were completed before shutdown.")
        coordinator_server.stop(0)
    
if __name__ == '__main__':
    run_coordinator_server()
