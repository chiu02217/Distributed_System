from concurrent import futures
import time
import grpc
import glob
import os
import sys
from queue import Queue
import threading
from src.common.grpc.auto_generated import coordinator_message_pb2 as coordinator_messages
from src.common.grpc.auto_generated import coordinator_service_pb2_grpc as coordinator_service
from src.common.grpc.auto_generated import snapshot_service_pb2_grpc as snapshot_service
from src.common.grpc.auto_generated import snapshot_message_pb2 as snapshot_messages
from src.afs.coordinator.snapshot.coordinator_snapshot import CoordinatorSnapshotHandler
from src.afs.afs_client.afs_client import AFSClient
from src.common.config_loader import CONFIG

class CoordinatorServicer(coordinator_service.CoordinatorServiceServicer, snapshot_service.SnapshotServiceServicer):
    def __init__(self):
        self.task_queue = Queue()
        self.all_primes = set() # set store unique primes
        self.task_lock = threading.Lock()
        self.primes_lock = threading.Lock()
        afs_server_address = CONFIG.afs.server_address
        self.assigned_tasks = {}   # 记录每个 worker 当前正在处理的任务

        # { "worker_id": "address" }
        self.workers = {}  
        # { "worker_id": <gRPC_Stub> }
        self.worker_stubs = {}
        self.worker_regis_lock = threading.Lock()
        
        
        
        print(f"[Coordinator] Connecting to AFS at {afs_server_address}...")
        self.afs_client = AFSClient(server_address=afs_server_address)
        
        # task management
        self.total_tasks = 0
        self.completed_tasks = 0
        self.is_finished = False
        
        # load tasks from AFS system
        self._load_tasks_from_afs()
        print(f"[Coordinator] Loaded {self.total_tasks} tasks from AFS.")

        # snapshot manager
        self.snapshot_manager = CoordinatorSnapshotHandler(self)
        self.snapshot_manager.start_snapshot_thread()

    # Handle the GetTask gRPC request. 
    def GetTask(self, request, context):

        response = coordinator_messages.GetTaskResponse()
        with self.task_lock:
            if self.task_queue.empty():
                response.has_task = False
                return response
            
            response.has_task = True
            response.filename = self.task_queue.get()
            response.remaining = self.task_queue.qsize()
            self.assigned_tasks[request.worker_id] = response.filename

            print(f"[Coordinator] Assigned task: {response.filename}, Remaining tasks: {response.remaining}")
            return response
        


    # Handle the SubmitResult gRPC request.
    def SubmitResult(self, request: coordinator_messages.SubmitResultRequest, context):

        with self.primes_lock:
            self.all_primes.update(request.primes)

        with self.task_lock:
            self.completed_tasks += 1
            self.assigned_tasks.pop(request.worker_id, None)

            print(f"[Coordinator] Completed tasks: {self.completed_tasks}/{self.total_tasks}")
            
            if self.completed_tasks == self.total_tasks and not self.is_finished:
                self.is_finished = True
                print("[Coordinator] All tasks completed. Saving results...")
                self._save_results()

        return coordinator_messages.SubmitResultResponse(success=True)
    
    # Handle the RegisterWorker gRPC request.
    def RegisterWorkerId(self, request: snapshot_messages.RegisterWorkerIdRequest, context):
        worker_id = request.worker_id
        worker_address = request.worker_address
        
        with self.worker_regis_lock:
            if worker_id not in self.workers:
                print(f"[Coordinator] register new Worker: {worker_id} @ {worker_address}")
                # Worker gRPC STUB
                channel = grpc.insecure_channel(worker_address)
                stub = snapshot_service.SnapshotServiceStub(channel)

                # store Worker info
                self.workers[worker_id] = worker_address
                self.worker_stubs[worker_id] = stub
            else:
                print(f"[Coordinator] Worker {worker_id} re-registered.")
                # (您可能還需要更新 channel 和 stub)

        return snapshot_messages.RegisterWorkerIdResponse(success=True)

    # Called when a worker fails or times out (detected by heartbeat thread).
    # Reassigns the unfinished task back to the queue.
    def reassign_task(self, worker_id):
        with self.task_lock:
            if worker_id in self.assigned_tasks:
                lost_task = self.assigned_tasks.pop(worker_id)
                self.task_queue.put(lost_task)
                print(f"[Coordinator] Reassigned task {lost_task} from failed worker {worker_id}")
            else:
                print(f"[Coordinator] No active task found for worker {worker_id}")

        
    # save results to output file
    def _save_results(self):
        file_path = 'primes.txt'
        handle = self.afs_client.create_file(file_path)
        
        if handle is None:
            print(f"[Coordinator] Error creating file {file_path} in AFS.")
            return
        try:
            for prime in sorted(self.all_primes):
                self.afs_client.write_file(handle, str(prime))
            
            self.afs_client.close_file(handle)
            print(f"[Coordinator] Saved {len(self.all_primes)} unique primes to {file_path}")
        except Exception as e:
            print(f"[Coordinator] Error saving results to AFS: {e}")
            
    # load tasks from AFS into the task queue
    def _load_tasks_from_afs(self):
        try:
            filenames = self.afs_client.list_files()
            if not filenames:
                print("[Coordinator] No files found in AFS.")
                return
            
            for filename in filenames:
                self.task_queue.put(filename)
                self.total_tasks += 1
                print(f"[Coordinator] Loaded task: {filename}")
        except Exception as e:
            print(f"[Coordinator] Error loading tasks from AFS: {e}")    
        
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
        
        if not coordinator_servicer.is_finished and coordinator_servicer.completed_tasks > 0:
            coordinator_servicer._save_results()
            print("[Coordinator] Not all tasks were completed before shutdown.")
        coordinator_server.stop(0)
    
if __name__ == '__main__':
    run_coordinator_server()