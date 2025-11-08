from concurrent import futures
from pysyncobj import SyncObj, replicated
import time
import grpc
import os
import sys
import threading
from src.common.grpc.auto_generated import file_operation_message_pb2 as messages
from src.common.grpc.auto_generated import file_operation_service_pb2_grpc as service
from src.common.config_loader import CONFIG

class RaftStorage(SyncObj):
    def __init__(self, self_address, other_server_addresses):
        super().__init__(self_address, other_server_addresses)
        self._data = {}

    @replicated
    def sef(self, key, value):
        self._data[key] = value

    def get(self, key, default=None):
        return self._data.get(key, default)

    @replicated
    def delete(self, key):
        if key in self._data:
            del self._data[key]

    def is_request_executed(self, request_id):
        if not request_id:
            return False
        return f"request_{request_id}" in self._data

    @replicated
    def mark_request_executed(self, request_id, response_data):
        if request_id:
            self._data[f"req_{request_id}"] = response_data

    def get_cached_response(self, request_id):
        return self._data.get(f"req_{request_id}")

class FileOperationServiceServicer(service.FileOperationServiceServicer):
    def __init__(self, input_dir, output_dir, raft_storage):
        self.input_dir = input_dir
        self.output_dir = output_dir
       
        self.raft = raft_storage
        self.next_handle = 1
        
        print(f"[AFS Server] initialized: input_dir={self.input_dir}, output_dir={self.output_dir}")

    def _is_primary(self):
        return self.raft._isLeader()

    def _wait_ready(self, timeout=10):
        start = time.time()
        while time.time() - start < timeout:
            if self.raft.isReady():
                return True
            time.sleep(0.1)
        return False

    def _get_file_path(self, filename:str):
        if filename.startswith("input_dataset_"):
            file_path = os.path.join(self.input_dir, filename)
            return file_path
        file_path = os.path.join(self.output_dir, filename)
        return file_path

    def _get_request_id(self, request):
        return getattr(request, 'request_id', None)
    
    # non-idempotent function
    def OpenFile(self, request: messages.OpenFileRequest, context: grpc.ServicerContext):
        response = messages.OpenFileResponse()
       
        if not self._wait_ready():
            response.error = "The server cluster hasn't been ready, please try again later."
            print(f"[AFS Server] Error: {response.error}")
            return response
        
        if not self._is_primary():
            response.error = "Current node is not the primary server. Please connect to the primary server."
            print(f"[AFS Server] Error: {response.error}") 
      
        request_id = self._get_request_id(request)
        if request_id and self.raft.is_request_executed(request_id):
            cached_response = self.raft.get_cached_response(request_id)
            response.handle = cached_response.get('handle', 0)
            response.error = cached.get('error','')
            print(f"[AFS Server] The request_id({request_id}) has been executed before, with handle{response.handle}")
            return response

        try:
            file_path = self._get_file_path(request.filename)
            handle = self.next_handle
            self.next_handle += 1
            self.raft.set(f"handle_{handle}",{
                    'filename': request.filename,
                    'path': file_path,
            })
            response.handle = handle
            print(f"[AFS Server] Opened file: {request.filename} with handle {handle}")
            
            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'handle': handle,
                    'error': ''
                })

        except Exception as e:
            response.error = str(e)
            print(f"[AFS Server] Error opening file: {e}")
            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'handle': 0,
                    'error': str(e)
                })

        return response

    # idempotent function
    def ReadFile(self, request: messages.ReadFileRequest, context: grpc.ServicerContext):
        response = messages.ReadFileResponse()
        
        if not self._is_primary():
            response.error = "Current node is not the primary server. Please connect to the primary server."
            print(f"[AFS Server] Error: {response.error}") 

        try:
            handle_info = self.raft.get(f"handle_{request.handle}")
            if not handle_info:
                response.error = f"Invalid handle: {handle_info}"
                return response
            
            with open(handle_info['path'], 'rb') as f:
                response.content = f.read()
            
            print(f"[AFS Server] Read file: {handle_info['filename']}")

        except Exception as e:
            response.error = str(e)
            print(f"[AFS Server] Error reading file: {e}")
        return response

    # non-idempotent function
    def CreateFile(self, request, context):
        response = messages.CreateFileResponse()

        if not self._wait_ready():
            response.error = "The server cluster hasn't been ready, please try again later."
            print(f"[AFS Server] Error: {response.error}")
            return response
        
        if not self._is_primary():
            response.error = "Current node is not the primary server. Please connect to the primary server."
            print(f"[AFS Server] Error: {response.error}") 
      
        request_id = self._get_request_id(request)
        if request_id and self.raft.is_request_executed(request_id):
            cached_response = self.raft.get_cached_response(request_id)
            response.handle = cached_response.get('handle', 0)
            response.error = cached.get('error','')
            print(f"[AFS Server] The request_id({request_id}) has been executed before, with handle{response.handle}")
            return response
        
        try:
            file_path = self._get_file_path(request.filename)
            with open(file_path, 'wb') as f: 
                pass

            handle = self.next_handle
            self.next_handle += 1
            self.raft.set(f"handle_{handle}", {
                    'filename': request.filename,
                    'path': file_path,
            })           
            response.handle = handle
            print(f"[AFS Server] Created file: {request.filename} with handle {handle}")

            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'handle': handle,
                    'error': ''
                })

        except Exception as e:
            response.error = str(e)
            print(f"[AFS Server] Error creating file: {e}")
            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'handle': 0,
                    'error': str(e)
                })

        return response

    # non-idempotent function
    def WriteFile(self, request: messages.WriteFileRequest, context: grpc.ServicerContext):
        response = messages.WriteFileResponse()
        
        if not self._is_primary():
            response.error = "Current node is not the primary server. Please connect to the primary server."
            print(f"[AFS Server] Error: {response.error}") 
      
        request_id = self._get_request_id(request)
        if request_id and self.raft.is_request_executed(request_id):
            cached_response = self.raft.get_cached_response(request_id)
            response.handle = cached_response.get('handle', 0)
            response.error = cached.get('error','')
            print(f"[AFS Server] The request_id({request_id}) has been executed before, with handle{response.handle}")
            return response
        
        try:
            handle_info = self.raft.get(f"handle_{request.handle}")
            if not handle_info:
                response.error = f"Invalid handle: {request.handle}"
                print(f"[AFS Server] {response.error}")

                if request_id:
                    self.raft.mark_request_executed(request_id, {
                        'success': False,
                        'error': response.error
                    })
                return response
            
            with open(handle_info['path'], 'wb') as f:
                f.write(request.content)

            response.success = True
            print(f"[AFS Server] Wrote to file: {handle_info['filename']}")

            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'success': True,
                    'error': ''
                })

        except Exception as e:
            response.error = str(e)
            print(f"[AFS Server] Error writing file: {e}")
            if request_id:
                self.raft.mark_request_executed(request_id, {
                   'success': False,
                   'error': str(e)
                })

        return response

    # non-idempotent function
    def CloseFile(self, request: messages.CloseFileRequest, context: grpc.ServicerContext):
        response = messages.CloseFileResponse()
        
        if not self._is_primary():
            response.error = "Current node is not the primary server. Please connect to the primary server."
            print(f"[AFS Server] Error: {response.error}") 
        
        request_id = self._get_request_id(request)
        if request_id and self.raft.is_request_executed(request_id):
            cached_response = self.raft.get_cached_response(request_id)
            response.handle = cached_response.get('handle', 0)
            response.error = cached.get('error','')
            print(f"[AFS Server] The request_id({request_id}) has been executed before, with handle{response.handle}")
            return response
        
        try:
            handle_info = self.raft.get(f"handle_{request.handle}")
            if not handle_info:
                response.error = f"Invalid handle: {request.handle}"
                if request_id:
                    self.raft.mark_request_executed(request_id, {
                        'success': False,
                        'error': response.error
                    })

                return response
            
            if request.modified and request.content:
                with open(handle_info['path'], 'wb') as f:
                    f.write(request.content)
                print(f"[AFS Server] Updated file: {handle_info['filename']}")
           
            self.raft.delete(f"handle_{request.handle}")

            response.success = True
            print(f"[AFS Server] Closed file {handle_info['filename']}")

            if request_id:
                self.raft.mark_request_executed(request_id, {
                    'success': True,
                    'error': ''
                })
            
        except Exception as e:
            response.error = str(e)
            print(f"[AFS Server] Error closing file: {e}")
            
        if request_id:
            self.raft.mark_request_executed(request_id, {
                'success': False,
                'error': str(e)
            })
                
        return response

    # idempotent function
    def ListFiles(self, request: messages.ListFilesRequest, context: grpc.ServicerContext):
        response = messages.ListFilesResponse()
        
        try:
            input_files = os.listdir(self.input_dir)
            filenames = [f for f in input_files if f.startswith("input_dataset_")]
            response.filenames.extend(sorted(filenames)) 
            print(f"[AFS Server] ListFiles: found {len(filenames)} files.")
            for filename in filenames:
                print(f" - {filename}")

        except Exception as e:
            response.error = str(e)
            print(f"[AFS Server] Error listing files: {e}")
        return response

def start_afs_server(node_id):
    base_grpc_port = CONFIG.afs.port
    base_raft_port = 50000
    
    grpc_port = base_grpc_port + node_id
    raft_port = base_raft_port + node_id
    
    input_dir = CONFIG.afs.input_dir
    output_dir = CONFIG.afs.output_dir
    
    self_address = f'localhost:{raft_port}'
    partners = []
    for i in range(3):
        if i != node_id:
            partner = f'localhost:{base_raft_port + i}'
            partners.append(partner)

    print(f"[AFS Server({node_id})] Initialize Raft...")
    raft_storage = RaftStorage(self_address, partners)

    timeout = 20
    start = time.time()
    while not raft_storage.isReady() and time.time() - start < timeout:
        time.sleep(0.5)
        print(".", end="", flush=True)
    print()

    if raft_storage.isReady():
        leader = raft_storage._getLeader()
        print(f"[AFS Server({node_id})] Raft is ready! Current Leader: {leader.address}")
    else:
        print(f"[AFS Server({node_id})] Raft is not ready, but will continue to retry.")

    max_workers = CONFIG.afs.can_handle_max_workers
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    service.add_FileOperationServiceServicer_to_server(
        FileOperationServiceServicer(input_dir, output_dir, raft_storage), grpc_server
    )
    grpc_server.add_insecure_port(f'[::]:{port}')
    grpc_server.start()
    print(f"[AFS Server({node_id})] starts on port {port}")

    try:
        while True:
            time.sleep(10)
            if raft_store._isLeader():
                print(f"[AFS Server({node_id})] Primary node is running")
            else:
                print(f"[AFS Server({node_id})] Backup node is running")

    except KeyboardInterrupt:
        print(f"[AFS Server({node_id})] is closing...")
        grpc_server.stop(0)
        print("Done!")

if __name__ == '__main__':
    node_id = int(sys.argv[1])
    start_afs_server(node_id)
    
   
