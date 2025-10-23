from concurrent import futures
import time
import grpc
import glob
import os
import sys
from queue import Queue
import threading
from src.common.grpc.auto_generated import coordinator_message_pb2 as messages
from src.common.grpc.auto_generated import coordinator_service_pb2_grpc as service
from src.afs.client.afs_client import AFSClient

class CoordinatorServicer(service.CoordinatorServiceServicer):
    def __init__(self, afs_server_address):
        self.task_queue = Queue()
        self.all_primes = set() # set store unique primes
        self.queue_lock = threading.Lock()
        self.result_lock = threading.Lock()
        
        print(f"[Coordinator] Connecting to AFS at {afs_server_address}...")
        self.afs_client = AFSClient(server_address=afs_server_address)
        
        # task management
        self.total_tasks = 0
        self.completed_tasks = 0
        self.is_finished = False
        
        # load tasks from AFS system
        self._load_tasks_from_afs()
        
        print(f"[Coordinator] Loaded {self.total_tasks} tasks from AFS.")
        
    def GetTask(self, request, context):
        """
        Handle the GetTask gRPC request.
        """
        response = messages.GetTaskResponse()
        with self.queue_lock:
            if self.task_queue.empty():
                response.has_task = False
                return response
            
            response.has_task = True
            response.filename = self.task_queue.get()
            response.remaining = self.task_queue.qsize()
            
            print(f"[Coordinator] Assigned task: {response.filename}, Remaining tasks: {response.remaining}")
            return response
        
    def SubmitResult(self, request, context):
        """
        Handle the SubmitResult gRPC request.
        """
        with self.result_lock:
            self.all_primes.update(request.primes)
            self.completed_tasks += 1
            
            print(f"[Coordinator] Completed tasks: {self.completed_tasks}/{self.total_tasks}")
            
            if self.completed_tasks == self.total_tasks and not self.is_finished:
                self.is_finished = True
                print("[Coordinator] All tasks completed. Saving results...")
                self._save_results()

        return messages.SubmitResultResponse(success=True) 
        
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
        
def run_server(afs_server_address, port):
    coordinator_servicer = CoordinatorServicer(afs_server_address)
    coordinator_server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    service.add_CoordinatorServiceServicer_to_server(
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
        print("\n[Coordinator] Shutting down server...")
        
        if not coordinator_servicer.is_finished and coordinator_servicer.completed_tasks > 0:
            coordinator_servicer._save_results()
            print("[Coordinator] Not all tasks were completed before shutdown.")
        coordinator_server.stop(0)
    
if __name__ == '__main__':
    run_server(
        afs_server_address = sys.argv[1] if len(sys.argv) > 1 else 'localhost:8000', 
        port = int(sys.argv[2]) if len(sys.argv) > 2 else 9000
    )
