import grpc
import sys
from src.app.worker.prime_algo import PrimeAlgorithm
from src.afs.client.afs_client import AFSClient
from src.app.worker.worker_interface import IWorker
from src.common.grpc.auto_generated import coordinator_message_pb2 as messages
from src.common.grpc.auto_generated import coordinator_service_pb2_grpc as service

# prime number searching algo
    
class Worker(IWorker):
    def __init__(self, worker_id, coordinator_address, file_server_address, cache_dir):
        self.worker_id = worker_id
        self.coordinator_channel = grpc.insecure_channel(coordinator_address)
        self.coordinator_stub = service.CoordinatorServiceStub(self.coordinator_channel)
        self.afs_client = AFSClient(server_address=file_server_address, cache_dir=cache_dir)
        
    def run_task(self):
        print(f"[Worker {self.worker_id}] Started!")
        # request new task from coordinator
        while True:
            task = self.coordinator_stub.GetTask(
                messages.GetTaskRequest(worker_id=self.worker_id)
            )
            if not task.filename:
                break
            
            print(f"[Worker {self.worker_id}] Received task: {task.filename}")
            
            file = self.afs_client.open(task.filename) 
            # This will cause memorty issue if the file is too large
            # with open(self.afs_client.get_local_path(local_path), 'r') as f:
            #     numbers = [int(line.strip()) for line in f]
            # self.afs_client.close(local_path)
            if file is None:
                print(f"[Worker {self.worker_id}] Error: Could not open file {task.filename}")
                # skip this task
                continue

            prime_numbers = set()
            # read numbers from file and check for primes
            while True:
                # 之前設計的read_file func, every time only read one line
                number_str = self.afs_client.read_file(file)
                # meaning it is the end of file
                if number_str is None:
                    break 
                try:
                    n = int(number_str)
                    if PrimeAlgorithm.is_prime(n):
                        prime_numbers.add(n)
                except ValueError:
                    print(f"[Worker {self.worker_id}] Skipping invalid line: {number_str}")
            
            self.coordinator_stub.SubmitResult(
                messages.SubmitResultRequest(
                    worker_id=self.worker_id,
                    primes=prime_numbers,
                    filename=task.filename
                )
            )

            print(f"[Worker {self.worker_id}] Found {len(prime_numbers)} primes")
        
        print(f"[Worker {self.worker_id}] No more tasks. Exiting.")

if __name__ == '__main__':
    worker = Worker(
        sys.argv[1] if len(sys.argv) > 1 else 'worker-1',
        sys.argv[2] if len(sys.argv) > 2 else 'localhost:9000',
        sys.argv[3] if len(sys.argv) > 3 else 'localhost:8000',
        sys.argv[4] if len(sys.argv) > 4 else '/tmp/afs'
    )
    worker.run_task()