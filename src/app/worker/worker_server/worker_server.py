import grpc
from concurrent import futures
from src.common.grpc.auto_generated import snapshot_service_pb2_grpc as snapshot_service
from src.common.grpc.auto_generated import snapshot_message_pb2 as messages
from src.common.config_loader import CONFIG

# private
class _SnapshotServicer(snapshot_service.SnapshotServiceServicer):

    def __init__(self, trigger_snapshot_callback):
        # store callback
        self.trigger_snapshot_callback = trigger_snapshot_callback

    # GRPC
    def TriggerSnapshot(self, request, context):
        self.trigger_snapshot_callback(request.snapshot_id)
        
        return messages.TriggerSnapshotResponse(success=True)

def run_worker_server(trigger_snapshot_callback, port=CONFIG.worker.worker_snapshot_port, max_workers=CONFIG.worker.worker_snapshot_max_workers):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    servicer = _SnapshotServicer(trigger_snapshot_callback)
    snapshot_service.add_SnapshotServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[WorkerServer] starting, port : {port}...")
    return server
