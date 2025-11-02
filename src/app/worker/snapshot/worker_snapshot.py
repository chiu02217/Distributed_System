import json
from src.app.worker.snapshot.i_worker_snapshot import IWorkerSnapshotHandler
from typing import TYPE_CHECKING # <-- 1. 匯入 TYPE_CHECKING

if TYPE_CHECKING:
    from src.app.worker.worker_client.worker_client import Worker 

class WorkerSnapshotHandler(IWorkerSnapshotHandler):
    def __init__(self, worker_instance: Worker):
        self.worker = worker_instance

    def handle_snapshot(self, snapshot_id):
        # consistent snapshot
        with self.worker.state_lock:
            if snapshot_id > self.worker.current_snapshot_id:
                print(f"[Worker {self.worker.worker_id}] ---receive snapshot {snapshot_id} ---")
                self.worker.current_snapshot_id = snapshot_id
                # 呼叫 "unsafe" 版本，因為我們已經在鎖內部
                self._save_worker_state_unsafe(snapshot_id) 

    def _save_worker_state_unsafe(self, snapshot_id):

        print(f"[Worker {self.worker.worker_id}] is saving snapshot {snapshot_id}...")
        state = {
            "worker_id": self.worker.worker_id,
            "current_snapshot_id": self.worker.current_snapshot_id,
            "current_task_filename": self.worker.current_task_filename,
            "current_task_line": self.worker.current_task_line
        }
        state_filename = f"snapshot_worker_{self.worker.worker_id}_{snapshot_id}.json"
        
        # 使用 worker 實例的 afs_client
        handle = self.worker.afs_client.create_file(state_filename)
        if handle is None:
            print(f"[Worker {self.worker.worker_id}] AFS error: failed to create snapshot file {state_filename}")
            return
        
        try:
            data_str = json.dumps(state, indent=2)
            self.worker.afs_client.write_file(handle, data_str)
        except Exception as e:
            print(f"[Worker {self.worker.worker_id}] error while writing snapshot: {e}")
        finally:
            self.worker.afs_client.close_file(handle)
            print(f"[Worker {self.worker.worker_id}] snapshot {snapshot_id} saved.")