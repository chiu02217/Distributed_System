import grpc
import sys
from src.afs.client.afs_client import AFSClient
from src.common.grpc.auto_generated import coordinator_message_pb2 as messages
from src.common.grpc.auto_generated import coordinator_service_pb2_grpc as service

def is_prime(n):
    
class Worker:
    def __init__(self, worker_id, coordinator_address, file_server_address, cache_dir):
        self.worker_id = worker_id
        self.coordinator_channel = grpc.insecure_channel(coordinator_address)
        self.coordinator_stub = service.CoordinatorServiceStub(self.coordinator_channel)
        self.afs_client = AFSClient(server_address=file_server_address, cache_dir=cache_dir)
        
    def run(self):
        print(f"[Worker {self.worker_id}] Started!")
        
        while True:
            task = self.coordinator_stub.GetTask(
                messages.GetTaskRequest(worker_id=self.worker_id)
            )
            if not task.has_task:
                break
            
            print(f"[Worker {self.worker_id}] Received task: {task.filename}")
            
            lc_path = self.afs_client.open(task.input_filename)
            with open(self.afs_client.get_local_path(lc_path), 'r') as f:
                numbers = [int(line.strip()) for line in f]
            self.afs_client.close(lc_path)
            
            primes = [n for n in numbers if is_prime(n)]
            
            self.coordinator_stub.SubmitResult(
                msg.SubmitResultRequest(
                    worker_id=self.worker_id,
                    primes=primes
                )
            )

            print(f"[Worker {self.worker_id}] Found {len(primes)} primes")
        
        print(f"[Worker {self.worker_id}] No more tasks. Exiting.")

if __name__ == '__main__':
    worker = Worker(
        sys.argv[1] if len(sys.argv) > 1 else 'worker-1',
        sys.argv[2] if len(sys.argv) > 2 else 'localhost:9000',
        sys.argv[3] if len(sys.argv) > 3 else 'localhost:8000',
        sys.argv[4] if len(sys.argv) > 4 else '/tmp/afs'
    )
    worker.run()