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
from src.common.grpc.auto_generated import coordinator_service_pb2

    
class Worker(IWorker):
    def __init__(self, worker_id, worker_port):
        self.worker_id = worker_id
        self.worker_port = worker_port 
        
        coordinator_address = CONFIG.coordinator.coordinator_address
        print(f"[Worker {self.worker_id}] Connecting to Coordinator at {coordinator_address}...")
        coordinator_channel = grpc.insecure_channel(coordinator_address)
        self.coordinator_stub = coordinator_service.CoordinatorServiceStub(coordinator_channel)

        base_afs_port = CONFIG.afs.port
        self.afs_addresses = [
            f'localhost:{base_afs_port}',
            f'localhost:{base_afs_port + 1}',
            f'localhost:{base_afs_port + 2}',
        ]
        print(f"[Worker {self.worker_id}] Connecting to AFS Raft cluster...")
        self.afs_stub = self._connect_to_primary()
        if not self.afs_stub:
            print(f"[Worker {self.worker_id}] Failed to connect to AFS cluster!")
            sys.exit(1)
        
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
            trigger_snapshot_callback=self.worker_snapshot_handler.handle_snapshot
        )
   
    def _connect_to_primary(self, max_retries=5):
        for retry in range(max_retries):
            for addr in self.afs_addresses:
                try:
                    channel = grpc.insecure_channel(addr)
                    stub = afs_service.FileOperationServiceStub(channel)

                    request = afs_messages.ListFilesRequest()
                    response = stub.ListFiles(request, timeout=2)

                    if not response.error:
                        print(f"[Worker {self.worker_id}] Connected to AFS at {addr}")
                        self.current_afs_address = addr
                        return stub
                    else:
                        print(f"[Worker {self.worker_id}] {addr} returned error: {response.error}")

                except Exception as e:
                    print(f"[Worker {self.worker_id}] Failed to connect to {addr}: {e}")
                    continue
            if retry < max_retries -1:
                print(f"[Worker {self.worker_id}] No Primary node found, retrying in 2s...")
                time.sleep(2)

        return None

    def _call_afs_with_retry(self, operation_name, request_func, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = request_func(self.afs_stub)
                if hasattr(response, 'error') and response.error:
                    if "not the primary" in response.error:
                        print(f"[Worker {self.worker_id}] {operation_name}: Need to reconnect to primary server")
                        new_stub = self._connect_to_primary()
                        if new_stub:
                            self.afs_stub = new_stub
                            print(f"[Worker {self.worker_id}] Reconnected, retrying {operation_name}...")
                            continue
                        else:
                            print(f"[Worker {self.worker_id}] Failed to reconnect")
                            return None
                    return response
            except grpc.RpcError as e:
                print(f"f[Worker {self.worker_id}] gRPC error in {operation_name} (attempt {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    print(f"[Worker {self.worker_id}] Trying to reconnect...")
                    new_stub = self._connect_to_afs()
                    if new_stub:
                        self.afs_stub = new_stub
                        time.sleep(0.5)
                        continue
                    else:
                        print(f"[Worker {self.worker_id}] Reconnection failed")
                        return None

            except Exception as e:
                print(f"[Worker {self.worker_id}] Unexpected error in {operation_name}: {e}")
                return None
        print(f"[Worker {self.worker_id}] {operation_name} failed after {max_retries} attempts")
        return None

    def run_task(self):
        """
        Handle main task processing loop.
        Logic:
        1. Request task from Coordinator by sending worker_id.
        2. Check the file stored in snapshot if same as current task, if yes, continue working with current snapshot task.
        3. Otherwise, pull new task file and process to find prime numbers.
        """
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
        
        while True:
            try:
                task_request = coordinator_messages.GetTaskRequest(worker_id=self.worker_id)
                task_response = self.coordinator_stub.GetTask(task_request)
                task = task_response
            except grpc.RpcError as e:
                print(f"[Worker {self.worker_id}] gRPC error while getting task: {e}")
                break

            if task.snapshot_id > 0:
                self.worker_snapshot_handler.handle_snapshot(task.snapshot_id)
            
            if not task.has_task:
                print(f"[Worker {self.worker_id}] No more tasks available. Exiting.")
                self.stop_heartbeat.set()
                break
            
            print(f"[Worker {self.worker_id}] Received task: {task.filename}")
            # for snapshot
            with self.state_lock:
                self.current_task_filename = task.filename
                self.current_task_line = 0
            
            success = self._process_task(task.filename)
            if not success:
                print(f"[Worker {self.worker_id}] Task {task.filename} failed, will retry later")
                time.sleep(5)

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
            request_id = f"{self.worker_id}-open-{uuid.uuid4()}"
            open_request = afs_messages.OpenFileRequest(
                filename=filename,
                request_id=request_id
            )
            open_response = self._call_afs_with_retry(
                "OpenFile",
                lambda stub: stub.OpenFile(open_request)
            )

            if not open_response:
                print(f"[Worker {self.worker_id}] Error opening file: no response")
                return False
            if open_response.error:
                print(f"[Worker {self.worker_id}] Erroing opening file: {open_response.error}")
                return False

            file_handle = open_response.handle
            print(f"[Worker {self.worker_id}] File opened with handle: {file_handle}")
            
            read_request = afs_messages.ReadFileRequest(handle=file_handle)
            read_response = self._call_afs_with_retry(
                "ReadFile",
                lambda stub: stub.ReadFile(read_request)
            )
            
            if not read_response:
                print(f"[Worker {self.worker_id}] Error reading file: no response")
                return False
            if read_response.error:
                print(f"[Worker {self.worker_id}] Error reading file: {read_response.error}")
                return False

            content = read_response.content.decode('utf-8').splitlines()
            print(f"[Worker {self.worker_id}] Read {len(content)} lines")

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
                    close_request = afs_messages.CloseFileRequest(
                        handle=file_handle,
                        modified=False,
                        request_id=f"{self.worker_id}-close-{filename}-{uuid.uuid4()}"
                    )
                    close_response = self._call_afs_with_retry(
                        "CloseFile",
                        lambda stub: stub.CloseFile(close_request)
                    )
                    
                    if close_response.error:
                        print(f"[Worker {self.worker_id}] Error closing file: {close_response.error}")
                    else:
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
