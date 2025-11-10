from concurrent import futures
import time
import grpc
import os
import sys
import threading
from src.common.grpc.auto_generated import file_operation_message_pb2 as messages
from src.common.grpc.auto_generated import file_operation_service_pb2_grpc as service
from src.common.config_loader import CONFIG
from src.common.storage.simple_storage import SimpleStorage
from src.common.storage.raft_storage import RaftStorage

# for test relative path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# service implementation, use the file_operation_service_pb2_grpc.py file
class FileOperationServiceServicer(service.FileOperationServiceServicer):
    def __init__(self, input_dir, output_dir, snapshot_dir, raft_storage):
        # Initialize directories
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.snapshot_dir = snapshot_dir
        
        # Raft storage
        self.raft = raft_storage
        
        # Initialize file handle management
        self.next_handle = 1
        self.handle_lock = threading.Lock()        
        print(f"[AFS Server] initialized: input_dir={self.input_dir}, output_dir={self.output_dir}, snapshot_dir={self.snapshot_dir}")

    def _is_primary(self):
        return self.raft._isLeader()

    def _wait_ready(self, timeout=10):
        start = time.time()
        while time.time() - start < timeout:
            if self.raft.isReady():
                return True
            time.sleep(0.1)
        return False

    def _get_file_path(self, filename: str) -> str:
        """
        Get the file path based on the filename.
        """
        if filename.startswith("input_dataset_"):
            return os.path.join(self.input_dir, filename)
        elif filename.startswith("snapshot_"):
            return os.path.join(self.snapshot_dir, filename)
        elif filename.startswith("primes.txt"):
            return os.path.join(self.output_dir, filename)
        else:
            # Default case - put in output directory
            return os.path.join(self.output_dir, filename)

    def _get_request_id(self, request):
        return getattr(request, 'request_id', None)
    
    # non-idempotent function
    def OpenFile(self, request: messages.OpenFileRequest, context: grpc.ServicerContext):
        response = messages.OpenFileResponse()
       
        if not self._wait_ready():
            response.error = "The server cluster hasn't been ready, please try again later."
            print(f"[AFS Server] Error: {response.error}")
            return response
        
        if not self._is_primary():
            response.error = "Current node is not the primary server. Please connect to the primary server."
            print(f"[AFS Server] Error: {response.error}")
            return response

        request_id = self._get_request_id(request)
        if request_id and self.raft.is_request_executed(request_id):
            cached_response = self.raft.get_cached_response(request_id)
            response.handle = cached_response.get('handle', 0)
            response.error = cached_response.get('error', '')
            print(f"[AFS Server] The request_id({request_id}) has been executed before, with handle {response.handle}")
            return response

        try:
            file_path = self._get_file_path(request.filename)
            
            with self.handle_lock:
                handle = self.next_handle
                self.next_handle += 1
            
            self.raft.set(f"handle_{handle}", {
                'filename': request.filename,
                'path': file_path,
            })
            response.handle = handle
            print(f"[AFS Server] Opened file: {request.filename} with handle {handle}")
            
            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'handle': handle,
                    'error': ''
                })

        except Exception as e:
            response.error = str(e)
            print(f"[AFS Server] Error opening file: {e}")
            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'handle': 0,
                    'error': str(e)
                })

        return response

    # idempotent function
    def ReadFile(self, request: messages.ReadFileRequest, context: grpc.ServicerContext):
        response = messages.ReadFileResponse()
        
        if not self._is_primary():
            response.error = "Current node is not the primary server. Please connect to the primary server."
            print(f"[AFS Server] Error: {response.error}") 
            return response

        try:
            handle_info = None
            for _ in range(5):
                handle_info = self.raft.get(f"handle_{request.handle}")
                if handle_info:
                    break
                time.sleep(0.2)
            if not handle_info:
                response.error = f"Invalid handle: {request.handle}"
                return response
            
            with open(handle_info['path'], 'rb') as f:
                response.content = f.read()
            
            print(f"[AFS Server] Read file: {handle_info['filename']}")

        except Exception as e:
            response.error = str(e)
            print(f"[AFS Server] Error reading file: {e}")
        return response

    # non-idempotent function
    def CreateFile(self, request, context):
        response = messages.CreateFileResponse()

        if not self._wait_ready():
            response.error = "The server cluster hasn't been ready, please try again later."
            print(f"[AFS Server] Error: {response.error}")
            return response
        
        if not self._is_primary():
            response.error = "Current node is not the primary server. Please connect to the primary server."
            print(f"[AFS Server] Error: {response.error}") 
            return response
      
        request_id = self._get_request_id(request)
        if request_id and self.raft.is_request_executed(request_id):
            cached_response = self.raft.get_cached_response(request_id)
            response.handle = cached_response.get('handle', 0)
            response.error = cached_response.get('error', '')
            print(f"[AFS Server] The request_id({request_id}) has been executed before, with handle {response.handle}")
            return response
        
        try:
            file_path = self._get_file_path(request.filename)
            with open(file_path, 'wb') as f: 
                pass
            
            with self.handle_lock:
                handle = self.next_handle
                self.next_handle += 1
            self.raft.set(f"handle_{handle}", {
                'filename': request.filename,
                'path': file_path,
            })           
            response.handle = handle
            print(f"[AFS Server] Created file: {request.filename} with handle {handle}")

            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'handle': handle,
                    'error': ''
                })

        except Exception as e:
            response.error = str(e)
            print(f"[AFS Server] Error creating file: {e}")
            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'handle': 0,
                    'error': str(e)
                })

        return response

    # non-idempotent function
    def WriteFile(self, request: messages.WriteFileRequest, context: grpc.ServicerContext):
        response = messages.WriteFileResponse()
        
        if not self._is_primary():
            response.error = "Current node is not the primary server. Please connect to the primary server."
            print(f"[AFS Server] Error: {response.error}") 
            return response

        request_id = self._get_request_id(request)
        if request_id and self.raft.is_request_executed(request_id):
            cached_response = self.raft.get_cached_response(request_id)
            response.success = cached_response.get('success', False)
            response.error = cached_response.get('error', '')
            print(f"[AFS Server] The request_id({request_id}) has been executed before")
            return response
        
        try:
            handle_info = self.raft.get(f"handle_{request.handle}")
            if not handle_info:
                response.error = f"Invalid handle: {request.handle}"
                print(f"[AFS Server] {response.error}")

                if request_id:
                    self.raft.mark_request_executed(request_id, {
                        'success': False,
                        'error': response.error
                    })
                return response
            
            with open(handle_info['path'], 'wb') as f:
                f.write(request.content)

            response.success = True
            print(f"[AFS Server] Wrote to file: {handle_info['filename']}")

            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'success': True,
                    'error': ''
                })

        except Exception as e:
            response.error = str(e)
            print(f"[AFS Server] Error writing file: {e}")
            if request_id:
                self.raft.mark_request_executed(request_id, {
                   'success': False,
                   'error': str(e)
                })

        return response

    # non-idempotent function
    def CloseFile(self, request: messages.CloseFileRequest, context: grpc.ServicerContext):
        response = messages.CloseFileResponse()
        
        if not self._is_primary():
            response.error = "Current node is not the primary server. Please connect to the primary server."
            print(f"[AFS Server] Error: {response.error}") 
            return response

        request_id = self._get_request_id(request)
        if request_id and self.raft.is_request_executed(request_id):
            cached_response = self.raft.get_cached_response(request_id)
            response.success = cached_response.get('success', False)
            response.error = cached_response.get('error', '')
            print(f"[AFS Server] The request_id({request_id}) has been executed before")
            return response
        
        try:
            handle_info = self.raft.get(f"handle_{request.handle}")
            if not handle_info:
                response.error = f"Invalid handle: {request.handle}"
                if request_id:
                    self.raft.mark_request_executed(request_id, {
                        'success': False,
                        'error': response.error
                    })
                return response
            
            if request.modified and request.content:
                with open(handle_info['path'], 'wb') as f:
                    f.write(request.content)
                print(f"[AFS Server] Updated file: {handle_info['filename']}")
           
            self.raft.delete(f"handle_{request.handle}")

            response.success = True
            print(f"[AFS Server] Closed file {handle_info['filename']}")

            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'success': True,
                    'error': ''
                })
            
        except Exception as e:
            response.error = str(e)
            print(f"[AFS Server] Error closing file: {e}")
            
            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'success': False,
                    'error': str(e)
                })
                
        return response

    # idempotent function
    def ListFiles(self, request: messages.ListFilesRequest, context: grpc.ServicerContext):
        response = messages.ListFilesResponse()
        
        # Support both path-based and legacy mode
        if hasattr(request, 'file_path') and request.file_path:
            # New mode: support multiple directories
            target_dir = ""
            if request.file_path == "inputs":
                target_dir = self.input_dir
            elif request.file_path == "snapshots":
                target_dir = self.snapshot_dir
            elif request.file_path == "outputs":
                target_dir = self.output_dir

            try:
                if not target_dir:
                    response.error = f"Invalid or unsupported path: {request.file_path}"
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    return response

                filenames = [f for f in sorted(os.listdir(target_dir))
                     if os.path.isfile(os.path.join(target_dir, f))]
                     
                response.filenames.extend(filenames)
                print(f"[AFS Server] ListFiles (path={request.file_path}): found {len(filenames)} files.")

            except Exception as e:
                response.error = str(e)
                print(f"[AFS Server] Error listing files in '{target_dir}': {e}")
                context.set_code(grpc.StatusCode.INTERNAL)
        else:
            # Legacy mode: only list input files
            try:
                input_files = os.listdir(self.input_dir)
                filenames = [f for f in input_files if f.startswith("input_dataset_")]
                response.filenames.extend(sorted(filenames)) 
                print(f"[AFS Server] ListFiles: found {len(filenames)} files.")
                for filename in filenames:
                    print(f" - {filename}")

            except Exception as e:
                response.error = str(e)
                print(f"[AFS Server] Error listing files: {e}")
            
        return response

def start_afs_server(node_id):
    base_grpc_port = CONFIG.afs.port
    base_raft_port = 50000
    
    grpc_port = base_grpc_port + node_id
    raft_port = base_raft_port + node_id
    
    input_dir = CONFIG.afs.input_dir
    output_dir = CONFIG.afs.output_dir
    snapshot_dir = CONFIG.afs.snapshot_dir
    
    # Convert to absolute paths
    abs_input_dir = os.path.join(PROJECT_ROOT, input_dir)
    abs_output_dir = os.path.join(PROJECT_ROOT, output_dir)
    abs_snapshot_dir = os.path.join(PROJECT_ROOT, snapshot_dir)
    
    single_mode = os.getenv("SINGLE_MODE", "false").lower() == "true"

    if single_mode:
        print(f"[AFS Server({node_id})] Running in SINGLE MODE")
        raft_storage = SimpleStorage()
    else:
        self_address = f'localhost:{raft_port}'
        partners = []
        for i in range(3):
            if i != node_id:
                partner = f'localhost:{base_raft_port + i}'
                partners.append(partner)

        print(f"[AFS Server({node_id})] Initialize Raft with partners: {partners}")
        raft_storage = RaftStorage(self_address, partners)

        timeout = 20
        start = time.time()
        while not raft_storage.isReady() and time.time() - start < timeout:
            time.sleep(0.5)
            print(".", end="", flush=True)
        print()

        if raft_storage.isReady():
            leader = raft_storage._getLeader()
            if leader:
                print(f"[AFS Server({node_id})] Raft is ready! Current Leader: {leader.address}")
            else:
                print(f"[AFS Server({node_id})] Raft is ready! No leader yet")
        else:
            print(f"[AFS Server({node_id})] Raft is not ready, but will continue to retry.")

    max_workers = CONFIG.afs.can_handle_max_workers
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    service.add_FileOperationServiceServicer_to_server(
        FileOperationServiceServicer(abs_input_dir, abs_output_dir, abs_snapshot_dir, raft_storage), 
        grpc_server
    )
    grpc_server.add_insecure_port(f'[::]:{grpc_port}')
    grpc_server.start()
    
    print(f"[AFS Server({node_id})] starts on port {grpc_port}")
    print(f"[AFS Server({node_id})] Input dir: {abs_input_dir}")
    print(f"[AFS Server({node_id})] Output dir: {abs_output_dir}")
    print(f"[AFS Server({node_id})] Snapshot dir: {abs_snapshot_dir}")

    try:
        while True:
            time.sleep(10)
            if raft_storage._isLeader():
                print(f"[AFS Server({node_id})] Primary node is running")
            else:
                print(f"[AFS Server({node_id})] Backup node is running")

    except KeyboardInterrupt:
        print(f"[AFS Server({node_id})] is closing...")
        grpc_server.stop(0)
        print("Done!")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python afs_server.py <node_id>")
        sys.exit(1)
    node_id = int(sys.argv[1])
    start_afs_server(node_id)
