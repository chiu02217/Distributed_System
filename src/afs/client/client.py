import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

import grpc
from src.common.grpc.auto_generated import file_operation_service_pb2
from src.common.grpc.auto_generated import file_operation_service_pb2_grpc
import argparse

class FileOperationClient:
    def __init__(self, host='localhost', port=8000):
        self.channel = grpc.insecure_channel(f'{host}:{port}')
        self.stub = file_operation_service_pb2_grpc.FileOperationServiceStub(self.channel)

    def open_file(self, filename, mode):
        # request object
        request = file_operation_service_pb2.OpenRequest(filename=filename, mode=mode)
        response = self.stub.Open(request)
        if response.error:
            print(f"Error: {response.error}")
            return None
        else:
            print(f"File opened with handle: {response.handle}")
            return response.handle
        
    def close_connection(self):
        self.channel.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='File Operation Client')
    parser.add_argument('--filename', type=str, help='file name')
    parser.add_argument('--mode', type=str, default='r', help='file mode (r or w)')

    args = parser.parse_args()
    
    client = FileOperationClient()
    handle = client.open_file(args.filename, args.mode)
    client.close_connection()