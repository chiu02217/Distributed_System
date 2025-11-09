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
    def __init__(self, worker_id, worker_port):
        self.worker_id = worker_id
        self.worker_port = worker_port 
        # grpc cache size 100MB
        grpc_options = [
            ('grpc.max_receive_message_length', 100 * 1024 * 1024),
            ('grpc.max_send_message_length', 100 * 1024 * 1024)
        ]
        # 目前儲存的質數結果
        self.current_primes = set()
        # Connect to Coordinator
        coordinator_address = CONFIG.coordinator.coordinator_address
        # abs path for cache dir
        relative_cache_path = CONFIG.afs.afs_temp_path
        base_cache_dir = os.path.normpath(os.path.join(PROJECT_ROOT, relative_cache_path))
        print(f"[Worker {self.worker_id}] Connecting to Coordinator at {coordinator_address}...")
        coordinator_channel = grpc.insecure_channel(coordinator_address)
        self.coordinator_stub = coordinator_service.CoordinatorServiceStub(coordinator_channel)

        # Connect to AFS
        file_server_address = CONFIG.afs.server_address
        # every worker own their cache dir
        #cache_dir = f"{CONFIG.afs.afs_temp_path}/{worker_id}" 
        cache_dir = os.path.join(base_cache_dir, worker_id)
        # debug
        print(f"[Worker {self.worker_id}] Coordinator: {coordinator_address}, AFS: {file_server_address}")
        common_channel = grpc.insecure_channel(coordinator_address, options=grpc_options)
        afs_channel = grpc.insecure_channel(file_server_address, options=grpc_options)
        self.coordinator_stub = coordinator_service.CoordinatorServiceStub(common_channel)
        self.snapshot_stub = snapshot_service.SnapshotServiceStub(common_channel)
        self.afs_client = AFSClient(cache_dir=cache_dir, channel=afs_channel)
        # snapshot need
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
            trigger_snapshot_callback=self.worker_snapshot_handler.handle_snapshot,
            port=self.worker_port,
        )
    
    # load  worker snapshot to recover worker from carsh
    def _load_worker_state_from_snapshot(self):

        print(f"[Worker {self.worker_id}] is checking whether there is snapshot for worker...")
        latest_snapshot_file = f"snapshot_worker_{self.worker_id}.json"
        try:
            #latest_snapshot_file = self.afs_client.find_latest_worker_snapshot(latest_snapshot_file)
            handle = self.afs_client.open_file(latest_snapshot_file)
            if handle is not None:
                # --- 檔案存在，才讀取 ---
                print(f"[Worker {self.worker_id}] Found snapshot '{latest_snapshot_file}'. Loading...")
                state_data_bytes = self.afs_client.read_json_file(handle)
                self.afs_client.close_file(handle)
                
                if state_data_bytes and len(state_data_bytes) > 0:
                    state_data_str = state_data_bytes.decode('utf-8')
                    state = json.loads(state_data_str)
                    
                    # 重新載入 worker 狀態
                    with self.state_lock:
                        self.current_snapshot_id = state.get("current_snapshot_id", 0)
                        self.current_task_filename = state.get("current_task_filename")
                        self.current_task_line = state.get("current_task_line", 0)
                        self.current_primes = set(state.get("current_primes", []))
                    
                    print(f"[Worker {self.worker_id}] successfully recover from snapshot!")
                    if self.current_task_filename:
                        print(f" continue from {self.current_task_line} line。")
                else:
                    print(f"[Worker {self.worker_id}] Snapshot file was empty.")
            else:
                 # --- 檔案不存在 ---
                 print(f"[Worker {self.worker_id}] No fixed snapshot '{latest_snapshot_file}' found. Starting fresh.")

        except Exception as e:
            print(f"[Worker {self.worker_id}] load snapshot failed: {e}. Begin with the start.")

        # 3. 滿足「一啟動就建立」的需求
        # 無論是載入成功、失敗還是全新啟動，
        # 都在啟動時儲存一次「當前狀態」到那個固定檔案
        # - 如果是全新啟動，這會建立一個初始的空快照
        # - 如果是載入成功，這會用剛載入的狀態「重新儲存」一次 (確保檔案最新)
        print(f"[Worker {self.worker_id}] Saving/Updating snapshot file '{latest_snapshot_file}'...")
        self.worker_snapshot_handler.save_current_progress()

    def run_task(self):
        """
        Handle main task processing loop.
        Logic:
        1. Request task from Coordinator by sending worker_id.
        2. Check the file stored in snapshot if same as current task, if yes, continue working with current snapshot task.
        3. Otherwise, pull new task file and process to find prime numbers.
        """
        print(f"[Worker {self.worker_id}] Started!")
        self._load_worker_state_from_snapshot()
        # register to coordinator
        registered_successfully = False
        # first register itself Id to coordinator
        while not registered_successfully:
             try:
                register_req = snapshot_messages.RegisterWorkerIdRequest(
                    worker_id=self.worker_id,
                    worker_address=f"localhost:{self.worker_port}"
                )
                self.snapshot_stub.RegisterWorkerId(register_req)
                registered_successfully = True
                print(f"[Worker {self.worker_id}] register to coordinator success.")
             except grpc.RpcError as e:
                print(f"[Worker {self.worker_id}] register to coordinator error:  {e}")
                # retry
                time.sleep(2)
        # request new task from coordinator
        while True:
            try: 
                task: coordinator_messages.GetTaskResponse = self.coordinator_stub.GetTask(
                    coordinator_messages.GetTaskRequest(worker_id=self.worker_id)
                )
            except grpc.RpcError as e:
                print(f"[Worker {self.worker_id}] gRPC error while getting task: {e}")
                continue
            
            if not task.has_task:
                print(f"[Worker {self.worker_id}] No more tasks available. Exiting.")
                # stop heartbeat after tasks complete
                self.stop_heartbeat.set()
                break
            
            print(f"[Worker {self.worker_id}] Received task: {task.filename}")
            # for snapshot
            with self.state_lock:
                if self.current_task_filename != task.filename:
                    self.current_task_filename = task.filename
                    self.current_task_line = 0
                    self.current_primes.clear()
            
            if task.snapshot_id > 0:
                self.worker_snapshot_handler.handle_snapshot(task.snapshot_id)
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

    # read a line each time from AFS
    # support snapshot recovery
    def _process_task(self, filename):
        file_handle = None

        try:
            file_handle = self.afs_client.open_file(filename)

            if file_handle is None:
                print(f"[Worker {self.worker_id}] Error: Could not open file {filename}")
                return False

            print(f"[Worker {self.worker_id}] Processing file: {filename}")

            # check if need to recover from snapshot
            lines_to_skip = 0
            with self.state_lock:
                if self.current_task_filename == filename and self.current_task_line > 0:
                    lines_to_skip = self.current_task_line
                    print(f"[Worker {self.worker_id}] recover from snapshot，skip {lines_to_skip} lines...")

            # load and process file line by line
            while True:
                number_str = self.afs_client.read_file(file_handle)

                if number_str is None:
                    break 
                # update snapshot current line number
                with self.state_lock:
                    self.current_task_line += 1

                # skip lines if recovering from snapshot
                if self.current_task_line <= lines_to_skip:
                    continue 

                try:
                    n = int(number_str)
                    if PrimeAlgorithm.is_prime(n):
                        self.current_primes.add(n)
                except ValueError:
                    print(f"[Worker {self.worker_id}] Skipping invalid line: {number_str}")
                    continue

                if self.current_task_line > 0 and self.current_task_line % 500 == 0:
                    print(f"[Worker {self.worker_id}] is storing loacl line {self.current_task_line})")
                    self.worker_snapshot_handler.save_current_progress()
                # delay for testing snapshot
                time.sleep(0.01)

            with self.state_lock:
                snapshot_id_to_report = self.current_snapshot_id

            try:
                self.coordinator_stub.SubmitResult(
                    coordinator_messages.SubmitResultRequest(
                        worker_id=self.worker_id,
                        filename=filename,
                        primes=list(self.current_primes),
                        snapshot_id=snapshot_id_to_report
                    )
                )
                print(f"[Worker {self.worker_id}] Submitted {len(self.current_primes)} primes from {filename}")
                return True
            except grpc.RpcError as e:
                print(f"[Worker {self.worker_id}] gRPC error while submitting results: {e}")
                return False

        finally:
            # 5. 最後，關閉 AFSClient 的 handle
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
