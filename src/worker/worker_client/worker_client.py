import grpc
import sys
import threading  
import time       
import os
import uuid
from src.common.prime_algo import PrimeAlgorithm
from src.worker.worker_client.i_worker_client import IWorker
from src.worker.snapshot.worker_snapshot import WorkerSnapshotHandler
from src.common.grpc.auto_generated import file_operation_message_pb2 as afs_messages
from src.common.grpc.auto_generated import file_operation_service_pb2_grpc as afs_service
from src.common.grpc.auto_generated import coordinator_message_pb2 as coordinator_messages
from src.common.grpc.auto_generated import coordinator_service_pb2_grpc as coordinator_service
from src.common.grpc.auto_generated import snapshot_message_pb2 as snapshot_messages
from src.common.grpc.auto_generated import snapshot_service_pb2_grpc as snapshot_service
from src.common.config_loader import CONFIG
from src.worker.worker_server import worker_server

    
class Worker(IWorker):
    def __init__(self, worker_id, worker_port):
        self.worker_id = worker_id
        self.worker_port = worker_port 
        
        # Connect to Coordinator
        coordinator_address = CONFIG.coordinator.coordinator_address
        print(f"[Worker {self.worker_id}] Connecting to Coordinator at {coordinator_address}...")
        coordinator_channel = grpc.insecure_channel(coordinator_address)
        self.coordinator_stub = coordinator_service.CoordinatorServiceStub(coordinator_channel)

        # Connect to AFS
        file_server_address = CONFIG.afs.server_address
        print(f"[Worker {self.worker_id}] Connecting to AFS at {file_server_address}...")
        file_channel = grpc.insecure_channel(file_server_address)
        self.afs_stub = afs_service.FileOperationServiceStub(file_channel)
        
        # Local cache directory for this worker
        self.cache_dir = f"{CONFIG.afs.afs_temp_path}/{worker_id}" 
        
        # Snapshot related
        common_channel = grpc.insecure_channel(coordinator_address)
        self.snapshot_stub = snapshot_service.SnapshotServiceStub(common_channel)

        self.state_lock = threading.Lock() 
        self.current_snapshot_id = 0
        
        self.current_task_filename = None
        self.current_task_line = 0     
        self.worker_snapshot_handler = WorkerSnapshotHandler(self)
        
        # Heartbeat control
        self.stop_heartbeat = threading.Event()
        threading.Thread(target=self._send_heartbeat, daemon=True).start()
        
        # snapshot server
        self.grpc_server = worker_server.run_worker_server(
            trigger_snapshot_callback=self.worker_snapshot_handler.handle_snapshot, port=self.worker_port
        )
    
    def run_task(self):
        """
        Handle main task processing loop.
        Logic:
        1. Request task from Coordinator by sending worker_id.
        2. Check the file stored in snapshot if same as current task, if yes, continue working with current snapshot task.
        3. Otherwise, pull new task file and process to find prime numbers.
        """
        print(f"[Worker {self.worker_id}] Started!")
        # first register itself Id to coordinator - keeps trying until success
        while True:
            try:
                # tell Coordinator how to call back
                register_req = snapshot_messages.RegisterWorkerIdRequest(
                    worker_id=self.worker_id,
                    worker_address=f"localhost:{self.worker_port}"
                )
                self.snapshot_stub.RegisterWorkerId(register_req)
                print(f"[Worker {self.worker_id}] register to coordinator success.")
                break
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
                # stop heartbeat after tasks complete
                self.stop_heartbeat.set()
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

            
    def _send_heartbeat(self):
        while not self.stop_heartbeat.is_set():
            try:
                self.coordinator_stub.Heartbeat(
                    coordinator_messages.HeartbeatRequest(worker_id=self.worker_id)
                )
                print(f"[Worker {self.worker_id}] Sent heartbeat")
            except grpc.RpcError as e:
                print(f"[Worker {self.worker_id}] Heartbeat failed: {e}")
            time.sleep(3)

    def _process_task(self, filename):
        """
        Call AFS to open and read the file, process to find prime numbers.
        """
        os.makedirs(self.cache_dir, exist_ok=True)
        cache_file_path = os.path.join(self.cache_dir, filename)
        
        file_handle = None
        prime_numbers = set()
        try:
            # open file from AFS
            request = afs_messages.OpenFileRequest(
                filename=filename,
                request_id=f"{self.worker_id}-{uuid.uuid4()}"
            )
            response = self.afs_stub.OpenFile(request)

            if response.error:
                print(f"[Worker {self.worker_id}] Error opening file: {response.error}")
                return False

            file_handle = response.handle

            if file_handle is None:
                print(f"[Worker {self.worker_id}] Error: Could not open file {filename}")
                return False
            
            # read file content from AFS
            read_request = afs_messages.ReadFileRequest(handle=file_handle)
            read_response = self.afs_stub.ReadFile(read_request)

            if read_response.error:
                print(f"[Worker {self.worker_id}] Error reading file: {read_response.error}")
                return False

            content = read_response.content.decode('utf-8').splitlines()
            
            # cache file locally
            with open(cache_file_path, 'w') as cache_file:
                cache_file.write("\n".join(content))
            print(f"[Worker {self.worker_id}] Cached file {filename} locally.")
            
            # process prime numbers
            line_counter = 0
            
            for line in content:
                # update snapshot current line number
                with self.state_lock:
                    self.current_task_line += 1
                    
                try:
                    n = int(line.strip())
                    if PrimeAlgorithm.is_prime(n):
                        prime_numbers.add(n)
                except ValueError:
                    print(f"[Worker {self.worker_id}] Skipping invalid line: {line.strip()}")
                    continue
                line_counter += 1
            
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
                try:
                    close_request = afs_messages.CloseFileRequest(
                        handle=file_handle,
                        modified=False,
                        request_id=f"{self.worker_id}-close-{filename}-{uuid.uuid4()}"
                    )
                    close_response = self.afs_stub.CloseFile(close_request)
                    
                    if close_response.error:
                        print(f"[Worker {self.worker_id}] Error closing file: {close_response.error}")
                    else:
                        print(f"[Worker {self.worker_id}] Closed file {filename}")
                except grpc.RpcError as e:
                    print(f"[Worker {self.worker_id}] gRPC error while closing file: {e}")
                    
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
