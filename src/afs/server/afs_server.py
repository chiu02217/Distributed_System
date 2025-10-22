from concurrent import futures
import grpc
import os
import sys
import threading
from src.common.grpc.auto_generated import file_operation_message_pb2 as messages
from src.common.grpc.auto_generated import file_operation_service_pb2_grpc as service

# service implementation, use the file_operation_service_pb2_grpc.py file
class FileOperationServiceServicer(service.FileOperationServiceServicer):
    def __init__(self, input_dir, output_dir):
        """
        Initialize the FileOperationServiceServicer with an input directory.
        Args:
            input_dir (str): The directory where files are stored.
            output_dir (str): The directory where output files will be saved.
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        
        self.file_handles = {} # {handle: {'filename', 'path', 'file_obj', 'mode'}}
        self.next_handle = 1
        self.handle_lock = threading.Lock()  # to ensure thread safety
        
        print(f"Server initialized: input_dir={self.input_dir}, output_dir={self.output_dir}")

    def _get_file_path(self, filename):
        if filename.startswith("input_dataset_"):
            return os.path.join(self.input_dir, filename)
        return os.path.join(self.output_dir, filename)

    def OpenFile(self, request, context):
        """
        Handle the OpenFile gRPC request.
        """
        response = messages.OpenResponse()
        
        try:
            file_path = self._get_file_path(request.filename)  ## fail check here
            with open(file_path, 'r') as f:  # read file content
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
    
    def CreateFile(self, request, context):
        """
        Handle the CreateFile gRPC request.
        """
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
    
    def CloseFile(self, request, context):
        """
        Handle the CloseFile gRPC request.
        """
        response = messages.CloseFileResponse()
        
        handle = request.handle
        try:
            if handle not in self.file_handles:
                response.error = f"Invalid handle: {handle}"
                return response
            
            info = self.file_handles[handle]
            if request.modified and request.content:
                with open(info['path'], 'w') as f:
                    f.write(request.content)
                print(f"[Server] Updated file: {info['filename']} with handle {handle}")
            
            with self.handle_lock:
                del self.file_handles[handle]

            response.success = True
            print(f"[Server] Closed file {info['filename']} with handle {handle}")
        except Exception as e:
            response.error = str(e)
        return response
    
    def ReadFile(self, request, context):
        """
        Handle the ReadFile gRPC request.
        """
        response = messages.ReadFileResponse()
        
        handle = request.handle
        try:
            if handle not in self.file_handles:
                response.error = f"Invalid handle: {handle}"
                return response
            
            info = self.file_handles[handle]
            with open(info['path'], 'rb') as f:
                response.content = f.read()
            
            print(f"[Server] Read file: {info['filename']} with handle {handle}")

        except Exception as e:
            response.error = str(e)
        return response

    def WriteFile(self, request, context):
        """
        Handle the WriteFile gRPC request.
        """
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
            print(f"[Server] Wrote to file: {info['filename']} with handle {handle}")

        except Exception as e:
            response.error = str(e)
        return response

# start the server on port 8000
def serve(input_dir = './data/server_storage/input', output_dir = './data/server_storage/output', port=8000):
    file_server = grpc.server(futures.ThreadPoolExecutor(max_workers=5)) # 5 threads for example
    service.add_FileOperationServiceServicer_to_server(
        FileOperationServiceServicer(input_dir, output_dir), file_server
    )
    file_server.add_insecure_port(f'[::]:{port}')
    print(f"[Server] starts on port {port}")
    file_server.start()
    file_server.wait_for_termination()

if __name__ == '__main__':
    serve(
        input_dir = sys.argv[1] if len(sys.argv) > 1 else './data/server_storage/input',
        output_dir = sys.argv[2] if len(sys.argv) > 2 else './data/server_storage/output',
        port = int(sys.argv[3]) if len(sys.argv) > 3 else 8000
    )