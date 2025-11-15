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

# help gRPC calls with multi-retries
# used for AFS interactions
# can call diffrent functions with *args and **kwargs
def safe_call(func, max_retries=3, delay=1, *args, **kwargs):
    """
    Safely call a gRPC function with retries on failure.
    """
    # core retry loop
    for attempt in range(max_retries):
        try:
            # attempt the gRPC call
            return func(*args, **kwargs)

        # error handling and logging
        except grpc.RpcError as e:
            # log and retry
            if attempt >= max_retries - 1:
                # after max retries, still failed and return
                print(f"[safe_call] Failed after {max_retries} attempts: {e}")
                return None
            else:
                # continue retry
                print(f"[safe_call] gRPC error on attempt {attempt + 1}/{max_retries}: {e}")
                # short delay before retry
                time.sleep(delay)
        except Exception as e:
            print(f"[safe_call] Unexpected error: {e}") # error handling
            import traceback # at present (only for debug)
            traceback.print_exc()
            return None
    return None

# Coordinator gRPC Servicer
# handles task management, snapshot, heartbeat
class CoordinatorServicer(coordinator_service.CoordinatorServiceServicer, snapshot_service.SnapshotServiceServicer):
    def __init__(self):
        # task management lock
        self.task_lock = threading.Lock()
        self.processing_lock = threading.Lock()
        self.primes_lock = threading.Lock()
        # snapshot lock
        self.worker_regis_lock = threading.Lock()
        # heartbeat lock
        self.heartbeat_lock = threading.Lock()
        # submission lock
        self.submission_lock = threading.Lock()
        self.submitted_requests = set() 
        
        # task management parameters
        self.task_queue = Queue() 
        self.assigned_tasks = {} 
        self.all_primes = set() # unique primes
        self.total_tasks = 0
        self.completed_tasks = 0
        
        # snapshot related proverties
        self.workers = {}  # {worker_id: worker_address}
        self.worker_stubs = {}  # {worker_id: grpc stub}
        self.is_finished = False
        
        # higher cache size for grpc (100MB)
        grpc_options = [
            ('grpc.max_receive_message_length', 100 * 1024 * 1024),
            ('grpc.max_send_message_length', 100 * 1024 * 1024)  
        ]

        # AFS connection setup
        # loaded from CONFIG file
        afs_server_address = CONFIG.afs.server_address
        relative_cache_path = CONFIG.afs.afs_temp_path
        base_cache_dir = os.path.normpath(os.path.join(PROJECT_ROOT, relative_cache_path))
        coordinator_cache_dir = os.path.join(base_cache_dir, "coordinator")

        # access to the AFS client (with some local functionalities)
        afs_channel = grpc.insecure_channel(afs_server_address, options=grpc_options)
        self.afs_client = AFSClient(channel=afs_channel, cache_dir=coordinator_cache_dir)
        print(f"[Coordinator] Connecting to AFS at {afs_server_address}...")
        
        # initialize snapshot handler
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
    # using safe_call to call with ListFiles() at afs
    def _load_tasks_from_afs(self):
        todo_tasks = safe_call(self.afs_client.list_files, 
                              max_retries=5, 
                              delay=2,
                              file_store_path="inputs")

        # error handling
        # if no tasks loaded, log and return
        if todo_tasks is None:
            print("[Coordinator] No tasks or error loading files from AFS after retries.")
            return

        # enqueue tasks
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
            # check if worker has an assigned task? if so, resume it
            if worker_id in self.assigned_tasks:
                file_name = self.assigned_tasks[worker_id]
                response.filename = file_name
                response.has_task = True
                response.is_resume = True
                print(f"[Coordinator] Resuming task: {file_name} for worker {worker_id}")
                return response
        
        # logic 2: pop a new task
        with self.task_lock:
            # logic 3: empty queue management
            if self.task_queue.empty():
                response.has_task = False
                print(f"[Coordinator] No tasks left to assign to worker {worker_id}.")
                return response
            
            # assign new task
            file_name = self.task_queue.get()
            response.has_task = True
            response.filename = file_name
            self.assigned_tasks[request.worker_id] = file_name

            print(f"[Coordinator] Assigned task: {response.filename} to worker {worker_id}.")
            return response
        
        return response
    # Worker submit task's result
    def SubmitResult(self, request: coordinator_messages.SubmitResultRequest, context):
        self.snapshot_manager.process_result_from_worker(request)
        # logic 1: update primes set
        with self.primes_lock:
            self.all_primes.update(request.primes)

        with self.task_lock:
            self.assigned_tasks.pop(request.worker_id, None)
            print(f"[Coordinator] Received results for task {request.file_name} from worker {request.worker_id}. Total unique primes so far: {len(self.all_primes)}")
            
            if self.task_queue.empty() and not self.assigned_tasks:
                self.is_finished = True
                print("[Coordinator] All tasks completed. Saving results...")
                self._save_results()

        return coordinator_messages.SubmitResultResponse(success=True)
   
    # snapshot related
    def RegisterWorkerId(self, request: snapshot_messages.RegisterWorkerIdRequest, context):
        worker_id = request.worker_id
        worker_address = request.worker_address
        
        # register worker to coordinator 
        # for snapshot storage
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

    # integrate all results from multi-workers
    # save only one file ('primes.txt') to afs
    def _save_results(self):
        # standard process: create -> write -> close
        file_name = 'primes.txt'
        handle = None
        
        # create file in afs
        try:
            # using safe_call to call with CreateFile() at afs
            handle = safe_call(self.afs_client.create_file, 3, 1, file_name)
            
            # error handling
            if handle is None:
                print(f"[Coordinator] Error creating file {file_name} in AFS.")
                return
            
            
            # prepare data to write
            prime_strings = []
            # sort primes
            sorted_primes = sorted(self.all_primes)
            for prime in sorted_primes:
                prime_strings.append(str(prime))
            
            # store
            all_data = "\n".join(prime_strings)
            # using safe_call to call with WriteFile() at afs
            write_success = safe_call(
                self.afs_client.write_file,
                5, 2,
                handle,
                all_data,
            )
            
            # error handling
            if not write_success:
                print(f"[Coordinator] Error writing to file {file_name} in AFS.")
                return
            
            # final log
            print(f"[Coordinator] Saved {len(self.all_primes)} unique primes to {file_name} in AFS.")
            
        except Exception as e:
            # error
            print(f"[Coordinator] Exception while saving results to AFS: {e}")
        
        finally:
            # close file
            if handle is not None:
                # using safe_call to call with CloseFile() at afs
                safe_call(
                    self.afs_client.close_file,
                    5, 2,
                    handle
                )
                print(f"[Coordinator] Closed file {file_name} in AFS.")

    # monitor workers' heartbeats
    def Heartbeat(self, request: coordinator_messages.HeartbeatRequest, context):
        worker_id = request.worker_id
        # update each worker's last heartbeat(t)
        self.last_heartbeat[worker_id] = time.time()
        print(f"[Coordinator] Heartbeat received from {worker_id}")
        return coordinator_messages.HeartbeatResponse(acknowledged=True) 
    
    # continuous monitoring thread
    def _monitor_heartbeats(self):
        while not self.is_finished:
            now = time.time()
            # check for timed-out workers
            # give a buffer time, if worker return back in time then continue
            for worker_id, last_seen in list(self.last_heartbeat.items()):
                if now - last_seen > self.timeout_threshold:
                    print(f"[Coordinator] Worker {worker_id} timed out")
            time.sleep(2)

def run_coordinator_server():
    # start gRPC server for coordinator
    port = CONFIG.coordinator.port
    coordinator_servicer = CoordinatorServicer()
    coordinator_server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    coordinator_service.add_CoordinatorServiceServicer_to_server(
        coordinator_servicer, coordinator_server
    )
    
    # start snapshot service (also is a server)
    snapshot_service.add_SnapshotServiceServicer_to_server(
        coordinator_servicer, coordinator_server
    )
    
    # start coordinator
    coordinator_server.add_insecure_port(f'[::]:{port}')
    coordinator_server.start()
    print(f"[Coordinator] Server started on port {port}")
    
    try:
        # close the system
        while not coordinator_servicer.is_finished:
            time.sleep(1)
        print("[Coordinator] All tasks processed. Press Ctrl+C to stop the server.")
        coordinator_server.wait_for_termination()
    # handling shutdown
    except KeyboardInterrupt:
        # just shutting down
        print("[Coordinator] Shutting down server...")
        if not coordinator_servicer.is_finished:
            # snapshot save
            coordinator_servicer._save_results() # before shutdown
            print("[Coordinator] Warning! not all tasks were completed before shutdown.")
        coordinator_server.stop(0)
    
if __name__ == '__main__':
    # start coordinator
    run_coordinator_server()
