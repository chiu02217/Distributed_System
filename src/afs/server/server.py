from concurrent import futures
import grpc
from src.common.grpc.auto_generated import file_operation_service_pb2
from src.common.grpc.auto_generated import file_operation_service_pb2_grpc
import os

# service implementation, use the file_operation_service_pb2_grpc.py file
class FileOperationServiceServicer(file_operation_service_pb2_grpc.FileOperationServiceServicer):
    def __init__(self):
        self.file_handles = {}
        self.next_handle = 1
        
    def Open(self, request, context):
        filename = request.filename
        mode = request.mode
        response = file_operation_service_pb2.OpenResponse()
        
        try:
            if mode not in ['r', 'w']:
                raise ValueError("Invalid mode. Use 'r' or 'w'.")
            
            if not os.path.exists(filename):
                response.error = f"File does not exist: {filename}"
                return response
            
            file = open(filename, mode)
            
            handle = self.next_handle
            self.file_handles[handle] = file
            self.next_handle += 1
            
            response.handle = handle
            print(f"Opened file {filename} in mode {mode} with handle {handle}")
        
        except PermissionError:
            response.error = f"Permission denied: {filename}"
        except Exception as e:
            response.error = f"Error opening file {filename}: {str(e)}"
        
        return response
    
    # def Create(self, filename):
    #     request = file_operation_service_pb2.CreateRequest(filename=filename)

# start the server on port 8000
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5)) # 5 threads for example
    file_operation_service_pb2_grpc.add_FileOperationServiceServicer_to_server(FileOperationServiceServicer(), server)
    server.add_insecure_port('[::]:8000')
    print("Server starts on port 8000")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()