import grpc
import os
import re
from src.afs.client.afs_client_interface import IAFSClient
from src.common.grpc.auto_generated import file_operation_message_pb2 as messages
from src.common.grpc.auto_generated import file_operation_service_pb2_grpc as service

class AFSClient(IAFSClient):
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
        # Determine file mode
        file_obj = open(local_path, 'r+')
        # Store file info    
        self.open_files[response.handle] = {
            'filename': filename,
            'path': local_path,
            'modified': False,
            'file_obj': file_obj
        }
        return response.handle
    
    # write file content to local cache
    # no need rpc call here
    def write_file(self, handle, data):
        if handle not in self.open_files:
            print(f"[AFS] Error: Invalid file handle {handle}")
            return None

        file_info = self.open_files[handle]
        try:
            # Write data to local cached file
            file_info['file_obj'].write(str(data) + '\n')
            file_info['file_obj'].flush()
            # Mark file as modified, important when you close the file
            file_info['modified'] = True
        except Exception as e:
            print(f"[AFS] Error writing or flushing to file: {e}")
            return False
        
        return True
    # read a line(per number per line) from local cache file
    def read_file(self, handle):
        if handle not in self.open_files:
            print(f"Read File Error {handle}")
            return None

        file_info = self.open_files[handle]
        line = file_info['file_obj'].readline()
        if not line:
            return None
        filter = re.search(r'\d+', line)
        
        # addressing number, only accept first number per line
        if filter:
            return filter.group(0)
        else:
            return None
    
    def create_file(self, filename):
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
            
    def close_file(self, handle):
        if handle not in self.open_files:
            print(f"[AFS] Invalid handle: {handle}")
            return False
        
        file_info = self.open_files[handle]
        # create empty content if not modified
        content = b''  
        
        if file_info['modified']:
            # Close the file object before reading, avoid 讀到舊資料
            file_info['file_obj'].close()
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
        