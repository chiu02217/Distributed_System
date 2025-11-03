import grpc
import sys
import threading  
import json       
import time       
from src.common.prime_algo import PrimeAlgorithm
from src.afs.afs_client.afs_client import AFSClient
from src.app.worker.worker_client.i_worker_client import IWorker
from src.app.worker.snapshot.worker_snapshot import WorkerSnapshotHandler
from src.common.grpc.auto_generated import coordinator_message_pb2 as coordinator_messages
from src.common.grpc.auto_generated import coordinator_service_pb2_grpc as coordinator_service
from src.common.grpc.auto_generated import snapshot_message_pb2 as snapshot_messages
from src.common.grpc.auto_generated import snapshot_service_pb2_grpc as snapshot_service
from src.common.config_loader import CONFIG
from src.app.worker.worker_server import worker_server

    
class Worker(IWorker):
    def __init__(self, worker_id, worker_port):
        self.worker_id = worker_id
        self.worker_port = worker_port 
        
        coordinator_address = CONFIG.coordinator.coordinator_address
        file_server_address = CONFIG.afs.server_address
        # every worker own their cache dir
        cache_dir = f"{CONFIG.afs.afs_temp_path}/{worker_id}" 
        # debug
        print(f"[Worker {self.worker_id}] Coordinator: {coordinator_address}, AFS: {file_server_address}")
        common_channel = grpc.insecure_channel(coordinator_address)
        self.coordinator_stub = coordinator_service.CoordinatorServiceStub(common_channel)
        self.snapshot_stub = snapshot_service.SnapshotServiceStub(common_channel)
        self.afs_client = AFSClient(server_address=file_server_address, cache_dir=cache_dir)
        # snapshot need
        self.state_lock = threading.Lock() 
        self.current_snapshot_id = 0
        # current task 
        self.current_task_filename = None
        # line number in current task 
        self.current_task_line = 0     
        self.worker_snapshot_handler = WorkerSnapshotHandler(self)
        # worker server
        self.grpc_server = worker_server.run_worker_server(
            trigger_snapshot_callback=self.worker_snapshot_handler.handle_snapshot
        )
    
    def run_task(self):
        print(f"[Worker {self.worker_id}] Started!")
        # first register itself Id to coordinator
        try:
            # tell Coordinator how to call back
            register_req = snapshot_messages.RegisterWorkerIdRequest(
                worker_id=self.worker_id,
                worker_address=f"localhost:{self.worker_port}"
            )
            self.snapshot_stub.RegisterWorkerId(register_req)
            print(f"[Worker {self.worker_id}] register to coordinator success.")
        except grpc.RpcError as e:
            print(f"[Worker {self.worker_id}] register to coordinator error:  {e}")
            self.grpc_server.stop(0)
            return
        # request new task from coordinator
        while True:
            try: 
                task: coordinator_messages.GetTaskResponse = self.coordinator_stub.GetTask(
                    coordinator_messages.GetTaskRequest(worker_id=self.worker_id)
                )
            except grpc.RpcError as e:
                print(f"[Worker {self.worker_id}] gRPC error while getting task: {e}")
                break

            if task.snapshot_id > 0:
                self.worker_snapshot_handler.handle_snapshot(task.snapshot_id)
            
            if not task.has_task:
                print(f"[Worker {self.worker_id}] No more tasks available. Exiting.")
                break
            
            print(f"[Worker {self.worker_id}] Received task: {task.filename}")
            # for snapshot
            with self.state_lock:
                self.current_task_filename = task.filename
                self.current_task_line = 0
            
            # what to do when not successful or successful(asked by danny)
            success = self._process_task(task.filename)
            # clear current task state
            with self.state_lock:
                self.current_task_filename = None
                self.current_task_line = 0
        self.grpc_server.stop(0)

            
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
                # update snapshot current line number
                with self.state_lock:
                    self.current_task_line += 1
                
                line_counter += 1
                
                try:
                    n = int(number_str)
                    if PrimeAlgorithm.is_prime(n):
                        prime_numbers.add(n)
                except ValueError:
                    print(f"[Worker {self.worker_id}] Skipping invalid line: {number_str}")
                    continue
            # in order to avoid simultaneous write issue
            snapshot_id_to_report = 0
            with self.state_lock:
                snapshot_id_to_report = self.current_snapshot_id

            try:
                self.coordinator_stub.SubmitResult(
                    coordinator_messages.SubmitResultRequest(
                        worker_id=self.worker_id,
                        filename=filename,
                        primes=list(prime_numbers),
                        snapshot_id=snapshot_id_to_report
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
    worker_id = sys.argv[1] if len(sys.argv) > 1 else 'worker-1'
    # worker ports
    default_port = CONFIG.worker.worker_snapshot_port
    try:
        worker_num = int(worker_id.split('-')[-1])
        worker_port = default_port + (worker_num - 1)
    except:
        worker_port = default_port 
    # debug
    print(f"Coordinator address: {CONFIG.coordinator.coordinator_address}")
    print(f"AFS Server address: {CONFIG.afs.server_address}")
    print(f"Worker port: {worker_port}")
    
    worker = Worker(
        worker_id=worker_id,
        worker_port=worker_port,
    )
    # give client their service port
    worker.run_task()