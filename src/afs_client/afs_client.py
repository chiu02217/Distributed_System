import grpc
import os
import re
from src.afs.afs_client.i_afs_client import IAFSClient
from src.common.grpc.auto_generated import file_operation_message_pb2 as messages
from src.common.grpc.auto_generated import file_operation_service_pb2_grpc as service
from src.common.config_loader import CONFIG

class AFSClient(IAFSClient):
    def __init__(self, server_address=CONFIG.afs.server_address, cache_dir=CONFIG.afs.afs_temp_path):
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)
        
        self.channel = grpc.insecure_channel(server_address)
        self.stub = service.FileOperationServiceStub(self.channel)
        
        self.open_files = {}
        print(f"Connected to server at {server_address}, cache dir: {self.cache_dir}")
        
    # open a file from AFS server, return handle
    def open_file(self, filename: str):
        try:
            request: messages.OpenFileRequest = messages.OpenFileRequest(filename=filename)
            response: messages.OpenFileResponse = self.stub.OpenFile(request)
            
            if response.error:
                print(f"Error: {response.error}")
                return None
            
            local_path = os.path.join(self.cache_dir, filename)
            with open(local_path, 'wb') as f:
                f.write(response.content)
                
            # Open file object for reading
            file_obj = open(local_path, 'rb')
            
            # Store file info
            self.open_files[response.handle] = {
                'filename': filename,
                'path': local_path,
                'modified': False,
                'file_obj': file_obj,
                'mode': 'read'
            }
            return response.handle
        
        except Exception as e:
            print(f"Exception during open_file: {e}")
            return None
    
    # create a new file on AFS server
    def create_file(self, filename: str):
        try:
            request: messages.CreateFileRequest = messages.CreateFileRequest(filename=filename)
            response: messages.CreateFileResponse = self.stub.CreateFile(request)
            
            if response.error:
                print(f"File creating error: {response.error}")
                return None
            
            local_path = os.path.join(self.cache_dir, filename)
            file_obj = open(local_path, 'wb')
                
            self.open_files[response.handle] = {
                'filename': filename,
                'path': local_path,
                'modified': False,
                'file_obj': file_obj,
                'mode': 'write'
            }
            
            print(f"Created file: {filename} with handle {response.handle}")
            return response.handle
        
        except Exception as e:
            print(f"Exception during create_file: {e}")
            return None
    
    # write data to local cached file (will uploaded on close)
    def write_file(self, handle, data):
        if handle not in self.open_files:
            print(f"Error: Invalid file handle {handle}")
            return None

        file_info = self.open_files[handle]
        
        try:
            line = str(data) + '\n'
            # Write data to local cached file
            file_info['file_obj'].write(line.encode('utf-8'))
            file_info['file_obj'].flush()
            file_info['modified'] = True
            return True
        
        except Exception as e:
            print(f"Error writing or flushing to file: {e}")
            return False
        
    # read a line(per number per line) from local cache file
    def read_file(self, handle):
        if handle not in self.open_files:
            print(f" Error reading file: {handle}")
            return None

        file_info = self.open_files[handle]
        
        try:
            line = file_info['file_obj'].readline()
            if not line:
                return None
            
            line_str = line.decode('utf-8').strip()
            filter = re.search(r'\d+', line_str)
            # addressing number, only accept first number per line
            if filter:
                return filter.group(0)
            else:
                return None
            
        except Exception as e:
            print(f"Error during read_file: {e}")
            return None
    
    def close_file(self, handle):
        if handle not in self.open_files:
            print(f"Invalid handle: {handle}")
            return False
        
        file_info = self.open_files[handle]
        content = b'' # create empty content if not modified
        
        if 'file_obj' in file_info and file_info['file_obj']:
            # Close the file object before reading, avoid duplicate open
            file_info['file_obj'].close()
            
        if file_info['modified']:
            with open(file_info['path'], 'rb') as f:
                content = f.read()
                
        request = messages.CloseFileRequest(
            handle = handle,
            modified = file_info['modified'],
            content = content
        )
        response: messages.CloseFileResponse = self.stub.CloseFile(request)
        
        if response.success:
            del self.open_files[handle]
            print(f"Closed file: {file_info['filename']}")
            return True
        else:
            print(f"Error closing file: {response.error}")
            return False
        
    def get_local_path(self, handle):
        if handle in self.open_files:
            return self.open_files[handle]['path']
        return None
    
    def mark_modified(self, handle):
        if handle in self.open_files:
            self.open_files[handle]['modified'] = True
            
    def list_files(self):
        try:
            request = messages.ListFilesRequest()
            response: messages.ListFilesResponse = self.stub.ListFiles(request)
            
            if response.error:
                print(f"Error listing files: {response.error}")
                return []
            
            return list(response.filenames)
        
        except Exception as e:
            print(f"Exception during list_files: {e}")
            return []