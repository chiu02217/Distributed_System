import json
from src.worker.snapshot.i_worker_snapshot import IWorkerSnapshotHandler
from typing import TYPE_CHECKING # <-- 1. 匯入 TYPE_CHECKING

if TYPE_CHECKING:
    from src.worker.worker_client.worker_client import Worker 

class WorkerSnapshotHandler(IWorkerSnapshotHandler):
    def __init__(self, worker_instance: 'Worker'):
        self.worker = worker_instance
        self.fixed_snapshot_filename = f"snapshot_worker_{self.worker.worker_id}.json"

    def handle_snapshot(self, snapshot_id):
        # consistent snapshot
        with self.worker.state_lock:
            if snapshot_id > self.worker.current_snapshot_id:
                print(f"[Worker {self.worker.worker_id}] ---receive snapshot {snapshot_id} ---")
                self.worker.current_snapshot_id = snapshot_id
                self._save_worker_state_unsafe(snapshot_id) 
    # save current progressing lines
    def save_current_progress(self):
        with self.worker.state_lock:
            self._save_worker_state_unsafe()
    
    # save worker state to AFS
    def _save_worker_state_unsafe(self):

        print(f"[Worker {self.worker.worker_id}] is saving state to snapshot {self.fixed_snapshot_filename}...")
        state = {
            "worker_id": self.worker.worker_id,
            "current_snapshot_id": self.worker.current_snapshot_id,
            "current_task_filename": self.worker.current_task_filename,
            "current_task_line": self.worker.current_task_line,
            "current_primes": list(self.worker.current_primes)
        }        
        handle = None
        
        try:
            handle = self.worker.afs_client.create_file(self.fixed_snapshot_filename)
            if handle is None:
                print(f"[Worker {self.worker.worker_id}] AFS error: failed to create snapshot file {self.fixed_snapshot_filename}")
                return
            data_str = json.dumps(state, indent=2)
            self.worker.afs_client.write_file(handle, data_str)
        except Exception as e:
            print(f"[Worker {self.worker.worker_id}] error while writing snapshot: {e}")
        finally:
            self.worker.afs_client.close_file(handle)
            print(f"[Worker {self.worker.worker_id}] snapshot {self.fixed_snapshot_filename} saved.")