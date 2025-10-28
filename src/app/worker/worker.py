import grpc
import sys
from src.app.worker.prime_algo import PrimeAlgorithm
from src.afs.client.afs_client import AFSClient
from src.app.worker.worker_interface import IWorker
from src.common.grpc.auto_generated import coordinator_message_pb2 as messages
from src.common.grpc.auto_generated import coordinator_service_pb2_grpc as service
from src.common.grpc.auto_generated import coordinator_service_pb2
import threading
import time

# prime number searching algo
    
class Worker(IWorker):
    def __init__(self, worker_id, coordinator_address, file_server_address, cache_dir):
        self.worker_id = worker_id
        self.coordinator_channel = grpc.insecure_channel(coordinator_address)
        self.coordinator_stub = service.CoordinatorServiceStub(self.coordinator_channel)
        self.afs_client = AFSClient(server_address=file_server_address, cache_dir=cache_dir)

        # start heartbeat thread
        self.stop_heartbeat = threading.Event()
        threading.Thread(target=self._send_heartbeat, daemon=True).start()

    def run_task(self):
        print(f"[Worker {self.worker_id}] Started!")
        # request new task from coordinator
        while True:
            try: 
                task = self.coordinator_stub.GetTask(
                    messages.GetTaskRequest(worker_id=self.worker_id)
                )
            except grpc.RpcError as e:
                print(f"[Worker {self.worker_id}] gRPC error while getting task: {e}")
                break
            
            if not task.has_task:
                print(f"[Worker {self.worker_id}] No more tasks available. Exiting.")
                #stop heartbeat after tasks complete
                self.stop_heartbeat.set()
                break
            
            print(f"[Worker {self.worker_id}] Received task: {task.filename}")
            
            success = self._process_task(task.filename)


    # sends heartbeat every three seconds
    def _send_heartbeat(self):
        while not self.stop_heartbeat.is_set():
            try:
                self.coordinator_stub.Heartbeat(
                    coordinator_service_pb2.HeartbeatRequest(worker_id=self.worker_id)
                )
                print(f"[Worker {self.worker_id}] Sent heartbeat")
            except grpc.RpcError as e:
                print(f"[Worker {self.worker_id}] Heartbeat failed: {e}")
            time.sleep(3)
            


    def _process_task(self, filename):
        file_handle = None
        
        try:
            file_handle = self.afs_client.open_file(filename)
            
            if file_handle is None:
                print(f"[Worker {self.worker_id}] Error: Could not open file {filename}")
                return False

            print(f"[Worker {self.worker_id}] Processing file: {filename}")

            prime_numbers = set()
            line_counter = 0
            
            while True:
                number_str = self.afs_client.read_file(file_handle)
                
                if number_str is None:
                    break
                
                line_counter += 1
                
                try:
                    n = int(number_str)
                    if PrimeAlgorithm.is_prime(n):
                        prime_numbers.add(n)
                except ValueError:
                    print(f"[Worker {self.worker_id}] Skipping invalid line: {number_str}")
                    continue
                
            try:
                self.coordinator_stub.SubmitResult(
                    messages.SubmitResultRequest(
                        worker_id=self.worker_id,
                        filename=filename,
                        primes=list(prime_numbers)
                    )
                )
                print(f"[Worker {self.worker_id}] Submitted {len(prime_numbers)} primes from {filename}")
                return True
            except grpc.RpcError as e:
                print(f"[Worker {self.worker_id}] gRPC error while submitting results: {e}")
                return False
                
        finally:
            if file_handle is not None:
                self.afs_client.close_file(file_handle)
                print(f"[Worker {self.worker_id}] Closed file {filename}")

if __name__ == '__main__':
    worker = Worker(
        sys.argv[1] if len(sys.argv) > 1 else 'worker-1',
        sys.argv[2] if len(sys.argv) > 2 else 'localhost:50000',
        sys.argv[3] if len(sys.argv) > 3 else 'localhost:8000',
        sys.argv[4] if len(sys.argv) > 4 else '/tmp/afs'
    )
    worker.run_task()