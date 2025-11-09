from concurrent import futures
import grpc
import os
import sys
import threading
from src.common.grpc.auto_generated import file_operation_message_pb2 as messages
from src.common.grpc.auto_generated import file_operation_service_pb2_grpc as service
from src.common.config_loader import CONFIG

# for test relative path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# service implementation, use the file_operation_service_pb2_grpc.py file
class FileOperationServiceServicer(service.FileOperationServiceServicer):
    def __init__(self, input_dir, output_dir, snapshot_dir):
        # Initialize directories
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.snapshot_dir = snapshot_dir

        
        # Initialize file handle management
        self.file_handles = {} # {handle: {'filename', 'path', 'file_obj', 'mode'}}
        self.next_handle = 1
        self.handle_lock = threading.Lock() 
        
        # 3/11/2025: Idempotency support
        self.executed_requests = {} # To track executed requests for idempotency
        self.request_lock = threading.Lock()
        
        print(f"[AFS Server] initialized: input_dir={self.input_dir}, output_dir={self.output_dir}")

    def _get_file_path(self, filename:str) -> str:
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

    def _get_request_id(self, request) -> str:
        return getattr(request, 'request_id', None)
    
    # non-idempotent function
    def OpenFile(self, request: messages.OpenFileRequest, context: grpc.ServicerContext):
        response = messages.OpenFileResponse()
        request_id = self._get_request_id(request)
       
        if request_id:
            with self.request_lock:
                if request_id in self.executed_requests:
                    print(f"[AFS Server] Duplicate WriteFile request detected: {request.request_id}. Ignoring.")
                    return self.executed_requests[request_id]
        
        try:
            file_path = self._get_file_path(request.filename)  ## fail check here
        
            if not os.path.exists(file_path):
                response.error = f"File not found: {request.filename}"
                print(f"[AFS Server] Error: {response.error}")
                return response

            with open(file_path, 'rb') as f:
                response.content = f.read()  # put content into response

            with self.handle_lock:  # ensure thread safety
                response.handle = self.next_handle
                self.file_handles[self.next_handle] = {
                    'filename': request.filename,
                    'path': file_path,
                    'file_obj': None,
                    'mode': 'rb',
                }
            self.next_handle += 1

            print(f"[AFS Server] Opened file: {request.filename} with handle {response.handle}")

        except Exception as e:
            response.error = str(e)
            print(f"[AFS Server] Error opening file: {e}")
            
        if request_id:
            with self.request_lock:
                self.executed_requests[request_id] = response
                
        return response

    # idempotent function
    def ReadFile(self, request: messages.ReadFileRequest, context: grpc.ServicerContext):
        response = messages.ReadFileResponse()
        
        handle = request.handle
        try:
            if handle not in self.file_handles:
                response.error = f"Invalid handle: {handle}"
                return response
            
            info = self.file_handles[handle]
            with open(info['path'], 'rb') as f:
                response.content = f.read()
            
            print(f"[AFS Server] Read file: {info['filename']} with handle {handle}")

        except Exception as e:
            response.error = str(e)
            print(f"[AFS Server] Error reading file: {e}")
        return response

    # non-idempotent function
    def CreateFile(self, request, context):
        response = messages.CreateFileResponse()
        request_id = self._get_request_id(request)
        
        if request_id:
            with self.request_lock:
                if request_id in self.executed_requests:
                    print(f"[AFS Server] Duplicate WriteFile request detected: {request_id}. Ignoring.")
                    return self.executed_requests[request_id]
        
        try:
            file_path = self._get_file_path(request.filename)
            with open(file_path, 'wb') as f:  # create empty file
                pass

            with self.handle_lock:  # ensure thread safety
                response.handle = self.next_handle
                self.file_handles[self.next_handle] = {
                    'filename': request.filename,
                    'path': file_path,
                }
            self.next_handle += 1

            print(f"[AFS Server] Created file: {request.filename} with handle {self.next_handle}")

        except Exception as e:
            response.error = str(e)
            print(f"[AFS Server] Error creating file: {e}")
            
        if request_id:
            with self.request_lock:
                self.executed_requests[request_id] = response
                
        return response

    # non-idempotent function
    def WriteFile(self, request: messages.WriteFileRequest, context: grpc.ServicerContext):
        response = messages.WriteFileResponse()
        request_id = self._get_request_id(request)
        
        if request_id:
            with self.request_lock:
                if request_id in self.executed_requests:
                    print(f"[AFS Server] Duplicate WriteFile request detected: {request.request_id}. Ignoring.")
                    return self.executed_requests[request_id]
        
        handle = request.handle
        
        try:
            if handle not in self.file_handles:
                response.error = f"Invalid handle: {handle}"
                print(f"[AFS Server] {response.error}")
                return response
            
            info = self.file_handles[handle]
            with open(info['path'], 'wb') as f:
                f.write(request.content)
            response.success = True
            print(f"[AFS Server] Wrote to file: {info['filename']} with handle {handle}")

        except Exception as e:
            response.error = str(e)
            print(f"[AFS Server] Error writing file: {e}")
            
        if request_id:
            with self.request_lock:
                self.executed_requests[request_id] = response
                
        return response

    # non-idempotent function
    def CloseFile(self, request: messages.CloseFileRequest, context: grpc.ServicerContext):
        response = messages.CloseFileResponse()
        request_id = self._get_request_id(request)
        
        if request_id:
            with self.request_lock:
                if request_id in self.executed_requests:
                    print(f"[AFS Server] Duplicate WriteFile request detected: {request.request_id}. Ignoring.")
                    return self.executed_requests[request_id]
        
        handle = request.handle
        try:
            if handle not in self.file_handles:
                response.error = f"Invalid handle: {handle}"
                return response
            
            info = self.file_handles[handle]
            if request.modified and request.content:
                with open(info['path'], 'wb') as f:
                    f.write(request.content)
                print(f"[AFS Server] Updated file: {info['filename']} with handle {handle}")
            
            with self.handle_lock:
                del self.file_handles[handle]

            response.success = True
            print(f"[AFS Server] Closed file {info['filename']} with handle {handle}")
            
        except Exception as e:
            response.error = str(e)
            print(f"[AFS Server] Error closing file: {e}")
            
        if request_id:
            with self.request_lock:
                self.executed_requests[request_id] = response
                
        return response

    # idempotent function
    def ListFiles(self, request: messages.ListFilesRequest, context: grpc.ServicerContext):
        response = messages.ListFilesResponse()
        
        # try:
        #     input_files = os.listdir(self.input_dir)
        #     filenames = [f for f in sorted(os.listdir(self.input_dir)) if f.startswith("input_dataset_")]
        #     response.filenames.extend(filenames)
            
        #     print(f"[AFS Server] ListFiles: found {len(filenames)} files.")
        #     for filename in filenames:
        #         print(f" - {filename}")
        # except Exception as e:
        #     response.error = str(e)
        #     print(f"[AFS Server] Error listing files: {e}")
        # return response
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
            
        return response
def start_afs_server():
    port = CONFIG.afs.port
    input_dir = CONFIG.afs.input_dir
    output_dir = CONFIG.afs.output_dir
    snapshot_dir = CONFIG.afs.snapshot_dir
    max_workers = CONFIG.afs.can_handle_max_workers
    abs_input_dir = os.path.join(PROJECT_ROOT, input_dir)
    abs_output_dir = os.path.join(PROJECT_ROOT, output_dir)
    abs_snapshot_dir = os.path.join(PROJECT_ROOT, snapshot_dir)
    file_server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    service.add_FileOperationServiceServicer_to_server(
        FileOperationServiceServicer(input_dir=abs_input_dir, output_dir=abs_output_dir, snapshot_dir=abs_snapshot_dir), file_server
    )
    file_server.add_insecure_port(f'[::]:{port}')
    print(f"[AFS Server] starts on port {port}")
    print(f"[AFS Server] Input dir (Absolute): {abs_input_dir}")
    print(f"[AFS Server] Snapshot dir (Absolute): {abs_snapshot_dir}")
    print(f"[AFS Server] Output dir (Absolute): {abs_output_dir}")
    file_server.start()
    file_server.wait_for_termination()

if __name__ == '__main__':
    start_afs_server()
    