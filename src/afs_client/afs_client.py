import grpc
import os
import re
from src.afs_client.i_afs_client import IAFSClient
from src.common.grpc.auto_generated import file_operation_message_pb2 as messages
from src.common.grpc.auto_generated import file_operation_service_pb2_grpc as service
from src.common.config_loader import CONFIG

class AFSClient(IAFSClient):
    def __init__(self, channel: grpc.Channel, cache_dir=CONFIG.afs.afs_temp_path):
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)
        self.channel = channel
        self.stub = service.FileOperationServiceStub(self.channel)
        
        self.open_files = {}
        print(f"Connected to server, cache dir: {self.cache_dir}")

    # open a file from AFS server, return handle
    def open_file(self, filename: str, request_id: str = None):
        try:
            print(f"AFSClient: opening file {filename} ")
            request: messages.OpenFileRequest = messages.OpenFileRequest(filename=filename, request_id=request_id)
            response: messages.OpenFileResponse = self.stub.OpenFile(request)
            
            if response.error:
                print(f"Error: {response.error}")
                return None
            
            handle = response.handle
            
            local_path = os.path.join(self.cache_dir, filename)
            
            self.open_files[handle] = {
                'filename': filename,
                'path': local_path,
                'modified': False,
                'file_obj': None,
                'mode': 'read'
            }
            return handle

        except Exception as e:
            print(f"Exception during open_file: {e}")
            return None
    
    # create a new file on AFS server
    def create_file(self, filename: str, request_id:str = None):
        try:
            request =  messages.CreateFileRequest(filename=filename, request_id=request_id)
            response = self.stub.CreateFile(request)
            
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
       
        if file_info['file_obj'] is None:
            try:
                read_request = messages.ReadFileRequest(handle=handle)
                read_response = self.stub.ReadFile(read_request)

                if read_response.error:
                    print(f"Error reading file from server: {read_response.error}")
                    return None
                
                with open(file_info['path'], 'wb') as f:
                    f.write(read_response.content)

                file_info['file_obj'] = open(file_info['path'], 'rb')

            except Exception as e:
                print(f"Error during remote read_file: {e}")
                return None
        
        try:
            line = file_info['file_obj'].readline()
            if not line:
                return None
            
            line_str = line.decode('utf-8').strip()
            filter = re.search(r'\d+', line_str)
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
    
    # can specify directory
    def list_files(self, path: str):
        # only these 3 paths are valid(for inputs, snapshot, and the final results)
        if path not in ("inputs", "snapshots", "outputs"):
            print(f"[AFSClient] Error: '{path}' not a valid logical path.")
            return None
        try:
            request = messages.ListFilesRequest(file_path=path)
            print(f"debug: path={path}")
            response: messages.ListFilesResponse = self.stub.ListFiles(request)
            print(f"debug: response = {response}")
            
            if response.error:
                print(f"Error listing files ({path}): {response.error}")
                return None
            
            return list(response.filenames)
        
        except Exception as e:
            print(f"Exception during list_files: {e}")
            return []
        
    # find the latest coordinator snapshot file
    def find_latest_coordinator_snapshot(self):
        print("finding latest coordinator snapshot...")
        try:
            # using existing list_files method to get snapshot files
            all_coor_snapshot_files = self.list_files(path="snapshots")
            if all_coor_snapshot_files is None:
                print("find latest coor snapshot Error: no file or listFiles() failed")
                return None

            coor_snapshot_files = []
            
            #  according to naming convention "snapshot_<ID>.json"
            # be careful when changing it
            for f in all_coor_snapshot_files:
                coor_snapshot_regex = re.match(r'^snapshot_(\d+)\.json$', f)
                if coor_snapshot_regex:
                    valid_snapshot_id = int(coor_snapshot_regex.group(1))
                    coor_snapshot_files.append((valid_snapshot_id, f))
            
            if not coor_snapshot_files:
                print("Error: No coordinator snapshots found.")
                return None
                
            # sort files by snapshot_id in descending order, so the latest is the fist element
            coor_snapshot_files.sort(key=lambda x: x[0], reverse=True)
            
            latest_coor_snapshot_file = coor_snapshot_files[0][1] 
            print(f"find {latest_coor_snapshot_file}")
            return latest_coor_snapshot_file

        except Exception as e:
            print(f"error when finding latest snapshot for coor: {e}")
            return None
    
    # read json file (for snapshot)
    def read_json_file(self, open_file):
        if open_file not in self.open_files:
            print(f"[AFS] Error: Invalid file : {open_file}, you must pass the response of Openfiles()")
            return None

        file_obj = self.open_files[open_file]
        
        try:
            if file_obj['file_obj'] is None:
                try:
                    read_request = messages.ReadFileRequest(handle=open_file)
                    read_response:messages.ReadFileResponse = self.stub.ReadFile(read_request)
                    if read_response.error:
                        print(f"read_json_file() Error reading file from server: {read_response.error}")
                        return None
                    with open(file_obj['path'], 'wb') as f:
                        f.write(read_response.content)

                    # open local cache
                    file_obj['file_obj'] = open(file_obj['path'], 'rb')
                except Exception as e:
                    print(f"read json file error: {e}")

            # Reset file pointer to the beginning
            file_obj['file_obj'].seek(0)
            file_content = file_obj['file_obj'].read()
            return file_content
            
        except Exception as e:
            print(f"read_json_file Error: {e}")
            return None
    
    # as title say
    def find_latest_worker_snapshot(self, worker_id: str):
        print(f"[AFS]  searching snapshot for {worker_id} ...")
        try:
            # using ListFiles RPC
            all_worker_snpashots = self.list_files(path="snapshots")
            if all_worker_snpashots is None:
                print("find latest worker snapshot Error: no file or listFiles() failed")
                return None

            found_worker_snapshot_files = []
            
            # worker file regex
            worker_snapshot_regex = re.compile(f"^snapshot_worker_{re.escape(worker_id)}_(\d+)\.json$")
            
            for worker_snapshot in all_worker_snpashots:
                match = worker_snapshot_regex.match(worker_snapshot)
                if match:
                    snapshot_id = int(match.group(1)) 
                    found_worker_snapshot_files.append((snapshot_id, worker_snapshot))
            
            if not found_worker_snapshot_files:
                print(f"No snapshot found for {worker_id}")
                return None
            # same logic as find latest snapshot for coor
            found_worker_snapshot_files.sort(key=lambda x: x[0], reverse=True)
            # get filename
            latest_worker_file = found_worker_snapshot_files[0][1] 
            print(f"find the latest snapshot for {worker_id}: {latest_worker_file}")
            return latest_worker_file

        except Exception as e:
            print(f"error when finding latest snapshot for worker: {e}")
            return None
