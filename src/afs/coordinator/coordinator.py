from concurrent import futures
import grpc
import glob
import os
import sys
from queue import Queue
import threading
from src.common.grpc.auto_generated import coordinator_message_pb2 as messages
from src.common.grpc.auto_generated import coordinator_service_pb2_grpc as service
from src.afs.client.afs_client import AFSClient

class CoordinatorServiceServicer(service.CoordinatorServiceServicer):
    def __init__(self, input_dir, afs_server_address):
        
    def _load_tasks(self):
        
    def GetTask(self, request, context):
        
    def SubmitResult(self, request, context):
        
    def save_results(self):
    
    def serve_coordinator(input_dir, afs_server_address, port=9000):
    
    
if __name__ == '__main__':
    input_dir = sys.argv[1] if len(sys.argv) > 1 else './data/server_storage/input'
    afs_server_address = sys.argv[2] if len(sys.argv) > 2 else 'localhost:8000'
    port = int(sys.argv[3]) if len(sys.argv) > 3 else 9000
    
    serve_coordinator(input_dir, afs_server_address, port)
    