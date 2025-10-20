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
    def __init__(self, input_dir, afs_server_address):
        self.task_queue = Queue()
        # set store (unique) primes
        self.all_primes = set()
        self.queue_lock = threading.Lock()
        self.result_lock = threading.Lock()
        self.afs_client = AFSClient(server_address=afs_server_address)
        
        for f in sorted (glob.glob(os.path.join(input_dir, 'input_dataset_*.txt'))):
            filename = os.path.basename(f)
            self.task_queue.put(filename)
        
        print(f"[Coordinator] Loaded {self.task_queue.qsize()} tasks from {input_dir}")
        
    # allocate task to worker( not finished yet )
    def get_task(self, request, context):
        pass
    # collect results from worker ( not finished yet )
    def submit_result(self, request, context):
        pass
    # save results to output file ( not finished yet )
    def save_results(self):
        pass

    # 這個應該不需要? 下面run server 就是啟動伺服器(?
    #def serve_coordinator(input_dir, afs_server_address, port=9000):
    
    def run_server(input_dir, afs_server_address, port=9000):
        coordinator_server = CoordinatorServicer(input_dir, afs_server_address)
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
        service.add_CoordinatorServiceServicer_to_server(
            coordinator_server, server
        )
        server.add_insecure_port(f'[::]:{port}')
        server.start()
        print(f"[Coordinator] Server started on port {port}")
        try:
            server.wait_for_termination()
            # 可以考慮做完任務後自動關閉伺服器
            # while not server.all_tasks_done():
            #     time.sleep(1)
        except KeyboardInterrupt:
            print("[Coordinator] Shutting down server...")
            server.stop(0)
    
if __name__ == '__main__':
    input_dir = sys.argv[1] if len(sys.argv) > 1 else './data/server_storage/input'
    afs_server_address = sys.argv[2] if len(sys.argv) > 2 else 'localhost:8000'
    port = int(sys.argv[3]) if len(sys.argv) > 3 else 9000

    CoordinatorServicer.run_server(input_dir, afs_server_address, port)
