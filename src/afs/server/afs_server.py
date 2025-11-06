from concurrent import futures
import grpc
import os
import sys
import threading
from src.common.grpc.auto_generated import file_operation_message_pb2 as messages
from src.common.grpc.auto_generated import file_operation_service_pb2_grpc as service
from src.common.config_loader import CONFIG

# for test relative path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# service implementation, use the file_operation_service_pb2_grpc.py file
class FileOperationServiceServicer(service.FileOperationServiceServicer):
    def __init__(self, input_dir, output_dir):
        # input and output directories
        self.input_dir = input_dir
        self.output_dir = output_dir
        # {handle: {'filename', 'path', 'file_obj', 'mode'}}
        self.file_handles = {} 
        self.next_handle = 1
        self.handle_lock = threading.Lock() 
        
        print(f"Server initialized: input_dir={self.input_dir}, output_dir={self.output_dir}")

    # get full file path
    def _get_file_path(self, filename:str) -> str:
        if filename.startswith("input_dataset_"):
            return os.path.join(self.input_dir, filename)
        return os.path.join(self.output_dir, filename)

    # Open File request
    def OpenFile(self, request: messages.OpenFileRequest, context: grpc.ServicerContext):
        response = messages.OpenFileResponse()
        
        try:
            file_path = self._get_file_path(request.filename)  ## fail check here
            with open(file_path, 'rb') as f:  # read file content
                response.content = f.read()

            with self.handle_lock:  # ensure thread safety
                response.handle = self.next_handle
                self.file_handles[self.next_handle] = {
                    'filename': request.filename,
                    'path': file_path,
                }
            self.next_handle += 1

            print(f"[Server] Opened file: {request.filename} with handle {self.next_handle}")
        except Exception as e:
            response.error = str(e)
        return response
    
    # Create File request
    def CreateFile(self, request, context):
        response = messages.CreateFileResponse()
        
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

            print(f"[Server] Created file: {request.filename} with handle {self.next_handle}")
        except Exception as e:
            response.error = str(e)
        return response

    # Close File request
    def CloseFile(self, request: messages.CloseFileRequest, context: grpc.ServicerContext):
        response = messages.CloseFileResponse()
        
        handle = request.handle
        try:
            if handle not in self.file_handles:
                response.error = f"Invalid handle: {handle}"
                return response
            
            info = self.file_handles[handle]
            if request.modified and request.content:
                with open(info['path'], 'wb') as f:
                    f.write(request.content)
                print(f"[Server] Updated file: {info['filename']} with handle {handle}")
            
            with self.handle_lock:
                del self.file_handles[handle]

            response.success = True
            print(f"[Server] Closed file {info['filename']} with handle {handle}")
        except Exception as e:
            response.error = str(e)
        return response

    # Read File request
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
            
            print(f"[Afs Server] Read file: {info['filename']} with handle {handle}")

        except Exception as e:
            response.error = str(e)
        return response

    # Write File request
    def WriteFile(self, request: messages.WriteFileRequest, context: grpc.ServicerContext):

        response = messages.WriteFileResponse()
        
        handle = request.handle
        try:
            if handle not in self.file_handles:
                response.error = f"Invalid handle: {handle}"
                return response
            
            info = self.file_handles[handle]
            with open(info['path'], 'wb') as f:
                f.write(request.content)
            
            response.success = True
            print(f"[AfsServer] Wrote to file: {info['filename']} with handle {handle}")

        except Exception as e:
            response.error = str(e)
        return response

    # List Files request
    def ListFiles(self, request: messages.ListFilesRequest, context: grpc.ServicerContext):
        response = messages.ListFilesResponse()
        
        try:
            input_files = os.listdir(self.input_dir)
            filenames = [os.path.basename(f) for f in sorted(input_files)]
            response.filenames.extend(filenames)
            
            print(f"[Afs Server] ListFiles: found {len(filenames)} files.")
            for filename in filenames:
                print(f" - {filename}")
        except Exception as e:
            response.error = str(e)
            print(f"[Afs Server] Error listing files: {e}")
        return response

# start the server on port 8000
def start_afs_server():
    port = CONFIG.afs.port
    input_dir = CONFIG.afs.input_dir
    output_dir = CONFIG.afs.output_dir
    max_workers = CONFIG.afs.can_handle_max_workers
    abs_input_dir = os.path.join(PROJECT_ROOT, input_dir)
    abs_output_dir = os.path.join(PROJECT_ROOT, output_dir)
    file_server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    service.add_FileOperationServiceServicer_to_server(
        FileOperationServiceServicer(input_dir=abs_input_dir, output_dir=abs_output_dir), file_server
    )
    file_server.add_insecure_port(f'[::]:{port}')
    print(f"[Server] starts on port {port}")
    file_server.start()
    file_server.wait_for_termination()

if __name__ == '__main__':
    start_afs_server()
    