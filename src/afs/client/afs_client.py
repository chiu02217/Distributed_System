import grpc
import os
from src.common.grpc.auto_generated import file_operation_message_pb2 as messages
from src.common.grpc.auto_generated import file_operation_service_pb2_grpc as service

class AFSClient:
    def __init__(self, server_address='localhost:8000', cache_dir='/tmp/afs'):
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)
        
        self.channel = grpc.insecure_channel(server_address)
        self.stub = service.FileOperationServiceStub(self.channel)
        
        self.open_files = {}
        
    def open_file(self, filename):
        request = messages.OpenFileRequest(filename=filename)
        response = self.stub.OpenFile(request)
        
        if response.error:
            print(f"[AFS] Error: {response.error}")
            return None
        
        local_path = os.path.join(self.cache_dir, filename)
        with open(local_path, 'w') as f:
            f.write(response.content)
            
        self.open_files[response.handle] = {
            'filename': filename,
            'path': local_path,
            'modified': False
        }
        return response.handle
    
    def create(self, filename):
        request = messages.CreateFileRequest(filename=filename)
        response = self.stub.Create(request)
        
        if response.error:
            print(f"[AFS] Error: {response.error}")
            return None
        
        local_path = os.path.join(self.cache_dir, filename)
        with open(local_path, 'wb') as f:
            pass  # create empty file
            
        self.open_files[response.handle] = {
            'filename': filename,
            'path': local_path,
            'modified': False
        }
        return response.handle
    
    def get_local_path(self, handle):
        if handle in self.open_files:
            return self.open_files[handle]['path']
        return None
    
    def mark_modified(self, handle):
        if handle in self.open_files:
            self.open_files[handle]['modified'] = True
            
    def close(self, handle):
        if handle not in self.open_files:
            print(f"[AFS] Invalid handle: {handle}")
            return
        
        file_info = self.open_files[handle]
        content = b''  # create empty content
        
        if file_info['modified']:
            with open(file_info['path'], 'rb') as f:
                content = f.read()
                
        request = messages.CloseFileRequest(
            handle = handle,
            modified = file_info['modified'],
            content = content
        )
        response = self.stub.CloseFile(request)
        
        if response.success:
            del self.open_files[handle]
            print(f"[AFS] Closed file: {file_info['filename']}")
            
        return response.success
        