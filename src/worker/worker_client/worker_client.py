import grpc
import sys
import threading  
import time       
import os
import json
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
from src.afs_client.afs_client import AFSClient

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    
class Worker(IWorker):
    # Init worker
    # worker_id = unique worker identifier
    # worker_port = port for worker's snapshot server
    def __init__(self, worker_id, worker_port):
        self.worker_id = worker_id
        self.worker_port = worker_port 
        
        # grpc cache size limited by 100MB
        grpc_options = [
            ('grpc.max_receive_message_length', 100 * 1024 * 1024),
            ('grpc.max_send_message_length', 100 * 1024 * 1024)
        ]
        
        # Current primes storage, set means no duplicate
        self.current_primes = set()
        
        # Connect to the task management server (coordinator)
        coordinator_address = CONFIG.coordinator.coordinator_address
        
        # absolute path for cache_dir (used for file caching)
        # when worker readin file from afs, it will cache in this dir
        relative_cache_path = CONFIG.afs.afs_temp_path
        base_cache_dir = os.path.normpath(os.path.join(PROJECT_ROOT, relative_cache_path))
        print(f"[{self.worker_id}] Connecting to Coordinator at {coordinator_address}...")
        
        # Setup coordinator connection (port num: 50001)
        coordinator_channel = grpc.insecure_channel(coordinator_address, options=grpc_options)
        self.coordinator_stub = coordinator_service.CoordinatorServiceStub(coordinator_channel)
        self.snapshot_stub = snapshot_service.SnapshotServiceStub(coordinator_channel)

        # Connect to AFS
        base_afs_port = CONFIG.afs.port

        # in the command line, set SINGLE_MODE=true to run single server afs system
        single_mode = os.getenv("SINGLE_MODE", "false").lower() == "true"
        if single_mode:
            # SINGLE_MODE: 
            # - file server address is fixed
            # - worker only interact with single afs server without raft
            file_server_address = CONFIG.afs.server_address
            cache_dir = os.path.join(base_cache_dir, worker_id)
            print(f"[{self.worker_id}] Running in SINGLE MODE")
            print(f"[{self.worker_id}] Coordinator: {coordinator_address}, AFS: {file_server_address}")
            
            # register normal afs client
            # afs_client used for implementing some local call functions (open/read/write...)
            afs_channel = grpc.insecure_channel(file_server_address, options=grpc_options)
            self.afs_client = AFSClient(cache_dir=cache_dir, channel=afs_channel) 
            self.afs_stub = None  # Not used in single mode
            self.use_afs_client = True
        else:
            # Raft cluster mode: use direct stub with failover
            # servers are mounted in the raft cluster, with checking the primary node and backup nodes.
            # worker will only if connect to the primary node (the leader) then do file operations
            # if the primary server crushed, raft cluster will elect a new primary server.
            self.afs_addresses = [
                f'localhost:{base_afs_port}',
                f'localhost:{base_afs_port + 1}',
                f'localhost:{base_afs_port + 2}',
            ]
            print(f"[{self.worker_id}] Connecting to AFS Raft cluster...")
            
            # connect to the Leader node
            self.afs_stub = self._connect_to_primary()
            
            # if failed to connect to any afs server, exit directly
            if not self.afs_stub:
                print(f"[{self.worker_id}] Failed to connect to any server, quiting...")
                sys.exit(1)
            
            self.cache_dir = os.path.join(base_cache_dir, worker_id)
            os.makedirs(self.cache_dir, exist_ok=True)
            
            # raft mode do not use afs client
            self.afs_client = None
            self.use_afs_client = False
        
        # Snapshot related
        self.state_lock = threading.Lock() 
        self.current_snapshot_id = 0
        self.current_task_filename = None
        self.current_task_line = 0     
        self.worker_snapshot_handler = WorkerSnapshotHandler(self)
        
        # Heartbeat control
        # Heartbeat: responsible for monitoring the liveness of the worker with handling task reassignment when worker crashes
        self.stop_heartbeat = threading.Event()
        threading.Thread(target=self._send_heartbeat, daemon=True).start()
        
        # snapshot server
        self.grpc_server = worker_server.run_worker_server(
            trigger_snapshot_callback=self.worker_snapshot_handler.handle_snapshot,
            port=self.worker_port,
        )
    # Connect to primary server in Raft cluster
    def _connect_to_primary(self, max_retries=5):
        # try connection for 5 times (default max_retries=5)
        for retry in range(max_retries):
            # for each server address, just try to connect and the raft cluster will allocate the primary server to reponse
            for addr in self.afs_addresses:
                try:
                    channel = grpc.insecure_channel(addr)
                    stub = afs_service.FileOperationServiceStub(channel)

                    request = afs_messages.ListFilesRequest(file_path="inputs")
                    response: afs_messages.ListFilesResponse = stub.ListFiles(request, timeout=3)
                    # I think it is better to set response.error as boolean, error message is str(Danny)
                    # if not find, kepp searching
                    if response.error:
                        print(f"[{self.worker_id}] {addr} returned error: {response.error}")
                        continue 

                    # found the Leader node (primary server)
                    print(f"[{self.worker_id}] Connected to AFS at {addr}")
                    self.current_afs_address = addr
                    return stub

                except Exception as e:
                    print(f"[{self.worker_id}] Failed to connect to {addr}: {e}")
                    continue
            if retry < max_retries - 1:
                print(f"[{self.worker_id}] No Primary node found, retrying...")
                time.sleep(2)

        return None

    # Call AFS with automatic retry and failover
    # used in file operations (which are similar to afs client functions, but with direct stub calls)
    def _call_afs_with_retry(self, operation_name, request_func, max_retries=3):
        # try to call for max_retries times
        for attempt in range(max_retries):
            try:
                response = request_func(self.afs_stub)
                # I think it is better to set response.error as boolean, error message is str(Danny)
                # if primary server changed, try to reconnect
                if hasattr(response, 'error') and response.error:
                    if "not the primary" in response.error:
                        print(f"[{self.worker_id}] {operation_name}: waiting to reconnect to primary server")
                        new_stub = self._connect_to_primary()
                        if not new_stub:
                            print(f"[{self.worker_id}] failed to reconnect")
                            return None
                        else:
                            self.afs_stub = new_stub
                            print(f"[{self.worker_id}] Reconnected, retrying {operation_name}...")
                            continue
                    return response
                return response
            except grpc.RpcError as e:
                # handle grpc errors and try to reconnect to primary node
                print(f"[{self.worker_id}] gRPC error in {operation_name} (attempt {attempt+1}/{max_retries}): {e}")
                
                if attempt < max_retries - 1:
                    print(f"[{self.worker_id}] Trying to reconnect...")
                    
                    new_stub = self._connect_to_primary()
                    if new_stub:
                        self.afs_stub = new_stub
                        time.sleep(0.5)
                        continue
                    else:
                        print(f"[{self.worker_id}] Reconnection failed")
                        return None

            except Exception as e:
                print(f"[{self.worker_id}] Unexpected error in {operation_name}: {e}")
                return None
            
        # cannot succeed after max retries
        print(f"[{self.worker_id}] {operation_name} failed after {max_retries} attempts")
        return None

    # Load worker snapshot to recover from crash
    def _load_worker_state_from_snapshot(self):
        print(f"[{self.worker_id}] is checking whether there is snapshot for worker...")
        latest_snapshot_file = f"snapshot_worker_{self.worker_id}.json"
        
        try:
            # If there is the raft cluster mode, we do not support snapshot recovery at present
            if not self.use_afs_client:
                print(f"[{self.worker_id}] Snapshot recovery not yet implemented for Raft Cluster mode")
                return 

            worker_snapshot_file = self.afs_client.open_file(latest_snapshot_file)
            # no snapshot found
            if worker_snapshot_file is None:
                print(f"[{self.worker_id}] No snapshot '{latest_snapshot_file}' found. Starting from begin")
                return 
            # found snapshot
            # remember to check the privilage for snapshot directory! (work on Windows but failed in DICE)
            print(f"[{self.worker_id}] Found snapshot '{latest_snapshot_file}'. Loading...")
            state_json_data = self.afs_client.read_json_file(worker_snapshot_file)
            self.afs_client.close_file(worker_snapshot_file)
            
            # if file is empty
            if not state_json_data or len(state_json_data) == 0:
                print(f"[{self.worker_id}] Snapshot file was empty.")
                return
            
            state_data = state_json_data.decode('utf-8')
            state = json.loads(state_data)
            
            # get the previous state
            # - if current_task_filename is None: 
            #   means no task assigned before crash
            # - else:
            #   continue from the previous task line
            with self.state_lock:
                self.current_task_filename = state.get("current_task_filename")
                self.current_task_line = state.get("current_task_line", 0)
                self.current_primes = set(state.get("temp_primes", []))
            
            print(f"[{self.worker_id}] successfully recovered from snapshot!")
            if self.current_task_filename:
                print(f" continue from line {self.current_task_line}.")

        except Exception as e:
            print(f"[{self.worker_id}] load snapshot failed: {e}.")
        finally:
            # finally task
            print(f"[{self.worker_id}] Updating or Creating snapshot file '{latest_snapshot_file}'...")
            self.worker_snapshot_handler.save_current_progress()

    # Handle main task processing loop.
    # Process:
    # 1. Request task from Coordinator by sending worker_id.
    # 2. Check the file stored in snapshot if same as current task (if yes, continue working with current snapshot task).
    # Otherwise, pull new task file and process to find prime numbers.
    def run_task(self):
        print(f"[{self.worker_id}] Started!")
        
        # evety time start to run, check whether there is worker snapshot or not
        self._load_worker_state_from_snapshot()
        
        # Register to coordinator with retry
        registered_successfully_or_not = False
        while not registered_successfully_or_not:
            try:
                register_req = snapshot_messages.RegisterWorkerIdRequest(
                    worker_id=self.worker_id,
                    worker_address=f"localhost:{self.worker_port}"
                )
                self.snapshot_stub.RegisterWorkerId(register_req)
                registered_successfully_or_not = True
                print(f"[{self.worker_id}] register to coordinator success.")
            except grpc.RpcError as e:
                print(f"[{self.worker_id}] register to coordinator error: {e}")
                time.sleep(2)
        
        # Request new task from coordinator
        while True:
            # Grpc use try catch when awkward might happen
            try:
                task_req = coordinator_messages.GetTaskRequest(worker_id=self.worker_id)
                task_res = self.coordinator_stub.GetTask(task_req)
                
            except grpc.RpcError as e:
                print(f"[{self.worker_id}] gRPC error while getting task: {e}")
                continue
            
            # has_task is also stored in coordinator's snapshot. 
            # For second time test, remember to clear the snapshot cache and then work on
            if not task_res.has_task:
                print(f"[{self.worker_id}] No more tasks available. Worker Leaving")
                self.stop_heartbeat.set()
                break
            
            print(f"[{self.worker_id}] Received task: {task_res.filename}")
            
            # Update state for current task and store it to snapshot
            # if task was assigned, then just continue to work
            with self.state_lock:
                if self.current_task_filename != task_res.filename:
                    self.current_task_filename = task_res.filename
                    self.current_task_line = 0
                    self.current_primes.clear()
            
            # start the main implementation -- searching prime numbers
            process_success = self._process_task(task_res.filename)
            
            # error handling: if failed, retry after 5s
            if not process_success:
                print(f"[{self.worker_id}] Task {task_res.filename} failed, retry in 5s")
                # every 5s
                time.sleep(5)
                
            # last task has been successfully processed
            # which means uploading results to coordinator successfully
            with self.state_lock:
                self.current_task_filename = None
                self.current_task_line = 0
                
        # if all tasks are processed, stop the iteration
        self.grpc_server.stop(0)
    
    # heartbeat msg will be sent every 3 seconds
    # for coordinator to decide whether the worker is alive or not
    # if it is not alive for N seconds, coordinator will just resassign the task to another worker or return it to the waiting queue
    def _send_heartbeat(self):
        # stop_heartbeat will only be set when all tasks are finished
        while not self.stop_heartbeat.is_set():
            try:
                self.coordinator_stub.Heartbeat(
                    coordinator_messages.HeartbeatRequest(worker_id=self.worker_id)
                )
                print(f"[{self.worker_id}] send heartbeat to coordinator...")
            except grpc.RpcError as e:
                print(f"[{self.worker_id}] Heartbeat failed: {e}")
            # Heartbeat Interval: send every 3s
            time.sleep(3)
      
    # Main Implementation:      
    # Read file and process prime numbers, support snapshot recovery
    def _process_task(self, filename):
        file_handle = None

        try:
            # check if it is single node mode:
            # use_afs_client => SINGLE_MODE
            if self.use_afs_client:
                file_handle = self.afs_client.open_file(filename)
                
                # error handling
                if file_handle is None:
                    print(f"[{self.worker_id}] Error: Could not open file {filename}")
                    return False

                # open file success
                print(f"[{self.worker_id}] Processing file: {filename}")

                # check if need to recover from snapshot
                # if the snapshot file contains history for current task process
                # count the lines that have been processed and skip them
                lines_to_skip = 0
                with self.state_lock:
                    if self.current_task_filename == filename: # current file has snapshot
                        if self.current_task_line > 0: # previous processing line is greater than 0
                            lines_to_skip = self.current_task_line
                            print(f"[{self.worker_id}] recover from snapshot, skip {lines_to_skip} lines...")

                # Load and process file line by line
                while True:
                    number_str = self.afs_client.read_file(file_handle)
                    
                    # error handling
                    if number_str == "SERVER_ERROR":
                        print(f"[Worker {self.worker_id}] Error: Could not read file {filename}")
                        return False

                    # error handling
                    if number_str is None:
                        break # skip blank lines

                    # update snapshot for current processing file ==> line num
                    with self.state_lock:
                        # just move to next line
                        self.current_task_line += 1

                    # skip lines that already processed
                    if self.current_task_line <= lines_to_skip:
                        continue 

                    # prime searching algorithm
                    # main implementation is in src/common/prime_algo.py
                    try:
                        n = int(number_str)
                        if PrimeAlgorithm.is_prime(n):
                            self.current_primes.add(n)
                    
                    # error handling
                    except ValueError:
                        print(f"[{self.worker_id}] Skipping invalid line: {number_str}")
                        continue
                    
                    # save snapshot every 500 lines (handling large files)
                    if self.current_task_line > 0 and self.current_task_line % 500 == 0:
                        print(f"[{self.worker_id}] is storing local line {self.current_task_line}")
                        self.worker_snapshot_handler.save_current_progress()
                    
                    # small delay for testing snapshot
                    time.sleep(0.01)
                    
                    
            # Raft Cluster Mode (3-sever afs)
            else:
                # handle safe_call for the gRPC functions
                # each gRPC functions will be called with RETRY and failover mechanism
                request_id = f"{self.worker_id}-open-{filename}-{uuid.uuid4()}"  # REQUEST_ID is unique for one processing iteration
                
                # gRPC application i) call OpenFile for [max_retries] times with same request_id
                open_request = afs_messages.OpenFileRequest(
                    filename=filename,
                    request_id=request_id
                )
                open_response = self._call_afs_with_retry(
                    "OpenFile",
                    lambda stub: stub.OpenFile(open_request) # specific expression
                )
                
                # error handling => no response (maybe all servers are down / network error)
                if not open_response:
                    print(f"[{self.worker_id}] Error opening file: no response")
                    return False
                
                # error handling => not found or other errors
                if open_response.error:
                    print(f"[{self.worker_id}] Error opening file: {open_response.error}")
                    return False

                # OpenFile successfully called
                file_handle = open_response.handle
                print(f"[{self.worker_id}] File opened with handle: {file_handle}")
                
                # gRPC application ii) call ReadFile for [max_retries] times with same request_id
                read_request = afs_messages.ReadFileRequest(handle=file_handle)
                read_response:afs_messages.ReadFileResponse = self._call_afs_with_retry(
                    "ReadFile",
                    lambda stub: stub.ReadFile(read_request)
                )
                
                # error handling
                if not read_response:
                    print(f"[{self.worker_id}] Error reading file: no response")
                    return False
                
                # error handling
                if read_response.error:
                    print(f"[{self.worker_id}] Error reading file: {read_response.error}")
                    return False

                # ReadFile successfully called
                content = read_response.content.decode('utf-8').splitlines()
                print(f"[{self.worker_id}] Read {len(content)} lines")

                # Cache the file (download from afs) locally
                cache_file_path = os.path.join(self.cache_dir, filename)
                with open(cache_file_path, 'w') as cache_file:
                    cache_file.write("\n".join(content))
                print(f"[{self.worker_id}] Cached file {filename} locally.")
                
                # for snapshot recovery
                lines_to_skip = 0;
                with self.state_lock:
                    if self.current_task_filename == filename and self.current_task_line > 0:
                        lines_to_skip = self.current_task_line
                        print(f"[{self.worker_id}] recover from snapshot, skip {lines_to_skip} lines")

                # Process prime numbers (similar to SINGLE_NODE mode)
                line_counter = 0
                for line in content:
                    # update line counter
                    line_counter += 1
                    
                    # skip lines that already processed
                    if line_counter <= lines_to_skip:
                        continue
                    
                    # clean line
                    line = line.strip()
                    
                    # skip blank lines
                    if not line:
                        continue
                    
                    # update snapshot for current processing file ==> line num
                    with self.state_lock:
                        self.current_task_line = line_counter
                    
                    # prime searching algorithm
                    try:
                        n = int(line)
                        if PrimeAlgorithm.is_prime(n):
                            self.current_primes.add(n)
                            
                    # error handling
                    except ValueError:
                        print(f"[{self.worker_id}] Skipping invalid line: {line}")
                        continue
                    
                    time.sleep(0.01)

            # save current snapshot 
            with self.state_lock:
                snapshot_id_to_report = self.current_snapshot_id

            # Submit results to coordinator, coordinator will integrate the results and then send to afs server
            try:
                # [No Fault Tolerance here!] 
                # no safe_call for SubmitResult because it is an interaction between coordinator and worker, not between raft and workers
                # if failed, need to retry the whole task
                # SubmitResult is also a gRPC function, for distributed prime searching system
                self.coordinator_stub.SubmitResult(
                    coordinator_messages.SubmitResultRequest(
                        worker_id=self.worker_id,
                        filename=filename,
                        primes=list(self.current_primes),
                        snapshot_id=snapshot_id_to_report
                    )
                )
                # snapshot will also be sent to coordinator for recording
                # snapshot file in worker side | snapshotid in coordinator side
                
                # after the response: successful submission
                print(f"[{self.worker_id}] Submitted {len(self.current_primes)} primes from {filename}")
                return True
            except grpc.RpcError as e:
                print(f"[{self.worker_id}] gRPC error while submitting results: {e}")
                return False

        finally:
            
            # Close file handle
            # ensure the file hasn't leaked
            if file_handle is not None:
                
                # single node mode
                if self.use_afs_client:
                    self.afs_client.close_file(file_handle)
                    print(f"[{self.worker_id}] Closed file {filename}")
                    
                # raft cluster mode
                else:
                    # gRPC application iii) call CloseFile for [max_retries] times with same request_id
                    close_request = afs_messages.CloseFileRequest(
                        handle=file_handle,
                        modified=False,
                        request_id=f"{self.worker_id}-close-{filename}-{uuid.uuid4()}"
                    )
                    close_response = self._call_afs_with_retry(
                        "CloseFile",
                        lambda stub: stub.CloseFile(close_request)
                    )
                    
                    # error handling
                    if close_response:
                        if close_response.error:
                            print(f"[{self.worker_id}] Error closing file: {close_response.error}")
                        else:
                            # successful close
                            print(f"[{self.worker_id}] Closed file {filename}")
                    else:
                        print(f"[{self.worker_id}] Error closing file: no response")

if __name__ == '__main__':
    # get worker id from command line args
    worker_id = sys.argv[1] if len(sys.argv) > 1 else 'worker-1'
    
    # snapshot server port for worker
    default_port = CONFIG.worker.worker_snapshot_port
    
    try:
        worker_num = int(worker_id.split('-')[-1])  # "worker-1" -> 1, "1" -> 1
        worker_port = default_port + (worker_num - 1)  # each worker has different snapshot server
    except:
        worker_port = default_port # if parsing fails, just use default port
    
    # print the config info for checking
    print(f"Coordinator address: {CONFIG.coordinator.coordinator_address}")
    print(f"AFS Server address: {CONFIG.afs.server_address}")
    print(f"Worker port: {worker_port}")
    
    worker = Worker(
        worker_id=worker_id,
        worker_port=worker_port,
    )

    # start work
    worker.run_task()
