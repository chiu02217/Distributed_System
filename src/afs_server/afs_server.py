from concurrent import futures
import time
import grpc
from src.common.grpc.auto_generated import file_operation_message_pb2 as messages
from src.common.grpc.auto_generated import file_operation_service_pb2_grpc as service
from src.common.storage.simple_storage import SimpleStorage
from src.common.storage.raft_storage import RaftStorage
from src.common.config_loader import CONFIG
import os
import sys
import threading


# for test relative path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# service implementation, use the file_operation_service_pb2_grpc.py file
# includes gRPC functions for AFS system: OpenFile(), ReadFile(), CreateFile(), WriteFile(), CloseFile(), ListFiles()
# the server can be switched between primary-backup mode (with raft) and single mode (without raft) by env SINGLE_MODE
class FileOperationServiceServicer(service.FileOperationServiceServicer):
    def __init__(self, input_dir, output_dir, snapshot_dir, raft_storage):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.snapshot_dir = snapshot_dir
        self.raft = raft_storage
        self.next_handle = 1
        
        # lock for handle increment
        self.handle_lock = threading.Lock()    
        
        # initialization success    
        print(f"[AFS Server] initialized: input_dir={self.input_dir}, output_dir={self.output_dir}, snapshot_dir={self.snapshot_dir}")

    # [primary-backup server mode] check whether current node is leader node (responsible for write operations)
    def _is_primary(self):
        return self.raft._isLeader()

    # [primary-backup server mode] wait for raft cluster to be ready (loaded 3 servers successfully and they've elected the primary node)
    def _wait_ready(self, timeout=10):
        start = time.time()
        # wait for raft to be ready
        while time.time() - start < timeout:
            if self.raft.isReady():
                return True
            time.sleep(0.1)
        return False

    # get file paths according to file name
    # every type of files belongs to only one place 
    def _get_file_path(self, filename: str) -> str:
        if filename.startswith("input_dataset_"):
            return os.path.join(self.input_dir, filename)  # data/input/...
        elif filename.startswith("snapshot_"):
            return os.path.join(self.snapshot_dir, filename)  # data/snapshot/...
        elif filename.startswith("primes.txt"):
            return os.path.join(self.output_dir, filename)  # data/output/...
        else:
            # Default 
            return os.path.join(self.output_dir, filename)

    def _get_request_id(self, request):
        return getattr(request, 'request_id', None)
    
    # non-idempotent function
    # can be called several times by client and only run once for each request_id
    def OpenFile(self, request: messages.OpenFileRequest, context: grpc.ServicerContext):
        # returns: {handle, error, content}
        response = messages.OpenFileResponse()
        
        # error handling
        # check if raft cluster is ready 
        if not self._wait_ready():
            response.error = "The server cluster hasn't been ready, please try again later."
            print(f"[AFS Server] Error: {response.error}")
            return response
        
        # error handling
        # check if current connected server node is the primary node 
        if not self._is_primary():
            response.error = "Current node is not the primary server. Please connect to the primary server."
            print(f"[AFS Server] Error: {response.error}")
            return response
        
        # REQUEST_ID settled for safe_call
        # can be called several times by client and only run once for each request_id
        request_id = self._get_request_id(request)
        if request_id:
            # if the request_id has been executed before, just return cached response
            if self.raft.is_request_executed(request_id):
                cached_response = self.raft.get_cached_response(request_id)
                response.handle = cached_response.get('handle', 0)
                response.error = cached_response.get('error', '')
                print(f"[AFS Server] The request_id({request_id}) has been executed before, with handle {response.handle}")
                return response

        # main implementation for OpenFile
        try:
            # get file name by loading the filename from OpenFile request input {filename, request_id}
            file_path = self._get_file_path(request.filename)
            
            # error handling: check whether file is exist or not
            object_file_exist_or_not = request.filename.startswith(("snapshot_", "input_dataset_"))
            if object_file_exist_or_not:
                if not os.path.exists(file_path):
                    response.error = f"No such file or directory: {request.filename}"
                    return response
                
            # open file while handle increases
            with self.handle_lock:
                handle = self.next_handle
                self.next_handle += 1 
           
            # [raft cluster processing] send to raft cluster and let it to decide transmition
            self.raft.set(f"handle_{handle}", {
                'filename': request.filename,
                'path': file_path,
            })

            # return handle
            response.handle = handle
            print(f"[AFS Server] Opened file: {request.filename} with handle {handle}")
            
            # update the request_id condition
            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'handle': handle,
                    'error': ''
                })

        except Exception as e:
            # error handling
            response.error = str(e)
            print(f"[AFS Server] Error opening file: {e}")
            
            # update the request_id condition
            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'handle': 0,
                    'error': str(e)
                })
                
        return response

    # idempotent function
    # can be called multiple times with same result
    def ReadFile(self, request: messages.ReadFileRequest, context: grpc.ServicerContext):
        response = messages.ReadFileResponse()
        
        # for idempotent function, we can not use duplicate call for maintain. 
        
        # error handling
        # check if current connected server node is the primary node 
        if not self._is_primary():
            response.error = "Current node is not the primary server. Please connect to the primary server."
            print(f"[AFS Server] Error: {response.error}") 
            return response
        
        # main implementation for ReadFile
        try:
            handle_info = None
            
            # loop for max_tries times to get handle_info from raft cluster afs system
            max_tries = 5
            for i in range(max_tries):
                handle_info = self.raft.get(f"handle_{request.handle}")
                # if found the file then get out of loop
                if handle_info: 
                    break
                
                # have a little break then try again
                time.sleep(0.2)
                
            # error handling
            # file not found in raft cluster
            if not handle_info:
                response.error = f"Invalid handle: {request.handle}"
                return response

            # readin file content
            with open(handle_info['path'], 'rb') as f:
                response.content = f.read()
            print(f"[AFS Server] Read file: {handle_info['filename']}")

        except Exception as e:
            # error handling
            response.error = str(e)
            print(f"[AFS Server] Error reading file: {e}")
        return response

    # non-idempotent function
    # can be called several times by client and only run once for each request_id
    def CreateFile(self, request, context):
        response = messages.CreateFileResponse()

        # error handling
        # check if raft cluster is ready 
        if not self._wait_ready():
            response.error = "The server cluster hasn't been ready, please try again later."
            print(f"[AFS Server] Error: {response.error}")
            return response
        
        # error handling
        # check if current connected server node is the primary node 
        if not self._is_primary():
            response.error = "Current node is not the primary server. Please connect to the primary server."
            print(f"[AFS Server] Error: {response.error}") 
            return response
        
        # REQUEST_ID settled for safe_call
        # can be called several times by client and only run once for each request_id
        request_id = self._get_request_id(request)
        if request_id and self.raft.is_request_executed(request_id):
            # once the request_id has been called, return cached response
            cached_response = self.raft.get_cached_response(request_id)
            response.handle = cached_response.get('handle', 0)
            response.error = cached_response.get('error', '')
            print(f"[AFS Server] The request_id({request_id}) has been executed before, with handle {response.handle}")
            return response
        
        # main implementation for CreateFile
        try:
            # get file path from mounted directories
            file_path = self._get_file_path(request.filename)
            
            # create empty file
            with open(file_path, 'wb') as f: 
                pass
            
            # add handle when file created
            with self.handle_lock:
                handle = self.next_handle
                self.next_handle += 1

            # send the handle info to raft cluster server for replication (update condition)
            self.raft.set(f"handle_{handle}", {
                'filename': request.filename,
                'path': file_path,
            })

            # return handle
            # success response
            response.handle = handle
            print(f"[AFS Server] Created file: {request.filename} with handle {handle}")
            
            # update the request_id condition
            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'handle': handle,
                    'error': '',
                })

        except Exception as e:
            # error handling
            response.error = str(e)
            print(f"[AFS Server] Error creating file: {e}")
            
            # update the request_id condition
            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'handle': 0,
                    'error': str(e)
                })
                
        return response

    # non-idempotent function
    # can be called several times by client and only run once for each request_id
    def WriteFile(self, request: messages.WriteFileRequest, context: grpc.ServicerContext):
        # returns: {success, error_message, new_version}
        response = messages.WriteFileResponse()
        
        # error handling
        # check if raft cluster is ready 
        if not self._is_primary():
            response.error = "Current node is not the primary server. Please connect to the primary server."
            print(f"[AFS Server] Error: {response.error}") 
            return response

        
        # REQUEST_ID settled for safe_call
        # can be called several times by client and only run once for each request_id
        request_id = self._get_request_id(request)
        if request_id and self.raft.is_request_executed(request_id):
            # once the request_id has been called, return cached response
            cached_response = self.raft.get_cached_response(request_id)
            response.success = cached_response.get('success', False)
            response.error = cached_response.get('error', '')
            print(f"[AFS Server] The request_id({request_id}) has been executed before")
            return response
        
        # main implementation for WriteFile
        try:
            # get handle info from raft cluster
            handle_info = self.raft.get(f"handle_{request.handle}")
            
            # error handling
            # file not found in raft cluster
            if not handle_info:
                response.error = f"Invalid handle: {request.handle}"
                print(f"[AFS Server] {response.error}")

                # update the request_id condition
                if request_id:
                    self.raft.mark_request_executed(request_id, {
                        'success': False,
                        'error': response.error
                    })
                return response
            
            # write content to file
            with open(handle_info['path'], 'wb') as f:
                f.write(request.content)

            # return success
            response.success = True
            print(f"[AFS Server] Wrote to file: {handle_info['filename']}")

            # update the request_id condition
            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'success': True,
                    'error': ''
                })

        except Exception as e:
            # error handling
            response.error = str(e)
            print(f"[AFS Server] Error writing file: {e}")
            
            # update the request_id condition
            if request_id:
                self.raft.mark_request_executed(request_id, {
                   'success': False,
                   'error': str(e)
                })

        return response

    # non-idempotent function
    # can be called several times by client and only run once for each request_id
    def CloseFile(self, request: messages.CloseFileRequest, context: grpc.ServicerContext):
        # returns: {success, error}
        response = messages.CloseFileResponse()
        
        # error handling
        # check the raft cluster is ready
        if not self._is_primary():
            response.error = "Current node is not the primary server. Please connect to the primary server."
            print(f"[AFS Server] Error: {response.error}") 
            return response

        # REQUEST_ID settled for safe_call
        # can be called several times by client and only run once for each request_id
        request_id = self._get_request_id(request)
        if request_id and self.raft.is_request_executed(request_id):
            cached_response = self.raft.get_cached_response(request_id)
            response.success = cached_response.get('success', False)
            response.error = cached_response.get('error', '')
            print(f"[AFS Server] The request_id({request_id}) has been executed before")
            return response
        
        # main implementation for CloseFile
        try:
            handle_info = self.raft.get(f"handle_{request.handle}")
            
            # error handling
            # file not found in raft cluster
            if not handle_info:
                response.error = f"Invalid handle: {request.handle}"
                # update the request_id condition
                if request_id:
                    self.raft.mark_request_executed(request_id, {
                        'success': False,
                        'error': response.error
                    })
                print(f"[AFS Server] {response.error}")
                return response
            
            # if the file has been modified, write content back to file
            if request.modified:
                if request.content:
                    with open(handle_info['path'], 'wb') as f:
                        f.write(request.content)
                    print(f"[AFS Server] Updated file: {handle_info['filename']}")
           
            # remove handle from raft
            self.raft.delete(f"handle_{request.handle}")

            # success response
            response.success = True
            print(f"[AFS Server] Closed file {handle_info['filename']}")

            # update the request_id condition
            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'success': True,
                    'error': ''
                })
            
        except Exception as e:
            # error handling
            response.error = str(e)
            print(f"[AFS Server] Error closing file: {e}")
            
            # update the request_id condition
            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'success': False,
                    'error': str(e)
                })
            
        return response

    # idempotent function
    # can be called multiple times with same result
    def ListFiles(self, request: messages.ListFilesRequest, context: grpc.ServicerContext):
        # returns: {filenames, error}
        response = messages.ListFilesResponse()
        
        # Support both path-based and legacy mode
        if hasattr(request, 'file_path') and request.file_path:
            
            # New mode: support multiple directories
            # determine target directory
            # target_dir initialization
            target_dir = ""
            # determine target directory based on request.file_path
            # snapshots / inputs / outputs
            if request.file_path == "snapshots":
                target_dir = self.snapshot_dir
            elif request.file_path == "inputs":
                target_dir = self.input_dir
            elif request.file_path == "outputs":
                target_dir = self.output_dir

            # main implementation for ListFiles
            # list files in target directory
            try:
                # check valid directory
                if not target_dir:
                    response.error = f"Invalid or unsupported path: {request.file_path}"
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    
                    return response

                # list files in target directory
                # in sorted order
                filenames = [f for f in sorted(os.listdir(target_dir))
                     if os.path.isfile(os.path.join(target_dir, f))]
                
                # add filenames to response     
                response.filenames.extend(filenames)
                print(f"[AFS Server] ListFiles (path={request.file_path}): found {len(filenames)} files.")

            except Exception as e:
                # error handling
                response.error = str(e)
                print(f"[AFS Server] Error listing files in '{target_dir}': {e}")
                # set gRPC context code to INTERNAL error
                context.set_code(grpc.StatusCode.INTERNAL)
        else:
            # Legacy mode: only list input files
            try:
                input_files = os.listdir(self.input_dir)
                file_names = [f for f in input_files if f.startswith("input_dataset_")]
                response.filenames.extend(sorted(file_names)) 
                print(f"[AFS Server] ListFiles: found {len(file_names)} files.")
                # debug print filenames
                for filename in file_names:
                    print(f" - {filename}")

            except Exception as e:
                # error handling
                response.error = str(e)
                print(f"[AFS Server] Error listing files: {e}")
            
        return response

def start_afs_server(node_id):
    base_grpc_port = CONFIG.afs.port
    base_raft_port = 50000
    
    grpc_port = base_grpc_port + node_id
    raft_port = base_raft_port + node_id
    
    input_dir_path = CONFIG.afs.input_dir
    output_dir_path = CONFIG.afs.output_dir
    snapshot_dir_path = CONFIG.afs.snapshot_dir
    
    # Convert to absolute paths
    abs_input_dir = os.path.normpath(os.path.join(PROJECT_ROOT, input_dir_path))
    abs_output_dir = os.path.normpath(os.path.join(PROJECT_ROOT, output_dir_path))
    abs_snapshot_dir = os.path.normpath(os.path.join(PROJECT_ROOT, snapshot_dir_path))
    
    os.makedirs(abs_input_dir, exist_ok=True)
    os.makedirs(abs_output_dir, exist_ok=True)
    os.makedirs(abs_snapshot_dir, exist_ok=True)
    
    single_mode = os.getenv("SINGLE_MODE", "false").lower() == "true"
    
    # SINGLE MODE: 1 file server without replication
    if single_mode:
        print(f"[AFS Server({node_id})] Running in SINGLE MODE")
        raft_storage = SimpleStorage()
    # not SINGLE_MODE: consists of raft cluster, with 3 servers worked as primary-backup
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
        
        # flush every 0.5 seconds
        while not raft_storage.isReady() and time.time() - start < timeout:
            time.sleep(0.5)
            print(".", end="", flush=True)
        print()

        # get Leader node
        if raft_storage.isReady():
            leader = raft_storage._getLeader()
            if leader:
                print(f"[AFS Server({node_id})] Raft is ready! Current Leader: {leader.address}")
            else:
                print(f"[AFS Server({node_id})] Raft is ready! No leader yet")
        else:
            print(f"[AFS Server({node_id})] Raft is not ready, but will continue to retry.")

    # initialize afs system
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
        print("Cannot start AFS server -- missing node_id argument")
        sys.exit(1)
    node_id = int(sys.argv[1])

    # demo arguments
    if len(sys.argv) >= 3:
        CONFIG.afs.input_dir = sys.argv[2]
    if len(sys.argv) >= 4:
        CONFIG.afs.output_dir = sys.argv[3]
    if len(sys.argv) >= 5:
        CONFIG.afs.snapshot_dir = sys.argv[4]
        
    start_afs_server(node_id)
