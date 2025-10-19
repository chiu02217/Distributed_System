import sys
import os
import hashlib
import time
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

import grpc
from src.common.grpc.auto_generated import file_operation_message_pb2 as file_operation_message
from src.common.grpc.auto_generated import file_operation_service_pb2_grpc as file_operation_service
import argparse

class FileOperationClient:
    def __init__(self, host='localhost', port=8000):
        self.channel = grpc.insecure_channel(f'{host}:{port}')
        self.stub = file_operation_service.FileOperationServiceStub(self.channel)

    def open_file(self, filename, mode):
        # request object
        request = file_operation_message.OpenFileRequest(filename=filename, mode=mode)
        response = self.stub.OpenFile(request)
        if response.error:
            print(f"Error: {response.error}")
            return None
        else:
            print(f"File opened with handle: {response.handle}")
            return response.handle
        
    def close_connection(self):
        self.channel.close()

    # read file method
    @staticmethod
    def read_file(filepath: str) -> bytes:
        with open(filepath, 'rb') as f:
            return f.read()

    # write file method
    @staticmethod
    def write_file(filepath: str, content: bytes) -> None:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'wb') as f:
            f.write(content)
    
    # get file info method
    @staticmethod
    def get_file_info(filepath: str) -> Tuple[int, float]:
        stat = os.stat(filepath)
        return stat.st_size, stat.st_mtime

    # calculate checksum method
    @staticmethod
    def calculate_checksum(content: bytes) -> str:
        return hashlib.md5(content).hexdigest()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='File Operation Client')
    parser.add_argument('--filename', type=str, help='file name')
    parser.add_argument('--mode', type=str, default='r', help='file mode (r or w)')

    args = parser.parse_args()
    
    client = FileOperationClient()
    handle = client.open_file(args.filename, args.mode)
    client.close_connection()