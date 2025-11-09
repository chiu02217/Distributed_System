import threading
import time
import json
from src.common.grpc.auto_generated import coordinator_message_pb2 as messages
from src.afs_coordinator.snapshot.i_coordinator_snapshot import ICoordinatorSnapshotHandler
from typing import TYPE_CHECKING 

if TYPE_CHECKING:
    from src.afs.coordinator.coordinator_server import CoordinatorServicer 

# snapshot frequency
SNAPSHOT_FREQUENCY_SECONDS = 30

class CoordinatorSnapshotHandler(ICoordinatorSnapshotHandler):
    def __init__(self, coordinator: 'CoordinatorServicer'):
        self.coordinator = coordinator
        self.afs_client = coordinator.afs_client
        self.current_snapshot_id = 0
        # 儲存 { snapshot_id: state_data }
        self.snapshot_state = {}

    # backeground thread(trigger snapshot periodically)
    def start_snapshot_thread(self):
        print("[Snapshot] Starting snapshot thread...")
        thread = threading.Thread(target=self.snapshot_loop, daemon=True)
        thread.start()

    # every 30s take a snapshot
    def snapshot_loop(self):
        while True:
            time.sleep(SNAPSHOT_FREQUENCY_SECONDS)
            self.initiate_snapshot()

    # Chandy Lamport
    def initiate_snapshot(self):
        # id from 1
        self.current_snapshot_id += 1
        snapshot_id = self.current_snapshot_id + 1
        print(f"[SnapshotManager] Snapshot {snapshot_id}")
        #  Coordinator state
        coord_state = {}
        with self.coordinator.task_lock:
            coord_state = {
                "to_do_tasks": list(self.coordinator.task_queue.queue),
                "in_progress_tasks": self.coordinator.assigned_tasks.copy(),
                "total_tasks": self.coordinator.total_tasks,
                # indeed need this
                "completed_tasks": self.coordinator.completed_tasks,
            }
        # record primes which are found so far
        with self.coordinator.primes_lock:
            coord_state["temp_primes"] = list(self.coordinator.all_primes)

        # store so far state to AFS in json format
        state_filename = f"snapshot_{snapshot_id}.json"
        self.save_state_to_afs(state_filename, coord_state)
        
        # get worker list
        workers = self.get_all_worker_ids()
        self.snapshot_state[snapshot_id] = {
            "coordinator_state_saved": True,
            "channels_to_record": workers.copy(),
            "channel_messages": {worker_id: [] for worker_id in workers}
        }

        # already sent to all Worker (在下次 GetTask 時)
        print(f"[Snapshot] Coordinator state saved for snapshot {snapshot_id}.")

    # send snapshot_id to worker
    def send_snapshot_id_to_worker(self, response: messages.GetTaskResponse):
        response.snapshot_id = self.current_snapshot_id

    # add marker to incoming SubmitResultRequest
    # 2 duties:
    # a. Chandy-Lamport Receiver Rule
    # b. Record in-flight messages
    def process_result_from_worker(self, request: messages.SubmitResultRequest):
        worker_id = request.worker_id
        incoming_snapshot_id = getattr(request, "snapshot_id", None)
        filename = getattr(request, "filename", None)

        # ensure a lock for snapshot_state exists
        if not hasattr(self, "_snapshot_lock"):
            self._snapshot_lock = threading.Lock()

        with self._snapshot_lock:
            # If snapshot not active, ignore
            if incoming_snapshot_id not in self.snapshot_state:
                return

            state_data: dict = self.snapshot_state[incoming_snapshot_id]

            # Receiver rule: first marker from this worker for this snapshot
            if worker_id in state_data.get("channels_to_record", []):
                state_data["channels_to_record"].remove(worker_id)
                print(f"[Snapshot] Received snapshot {incoming_snapshot_id} from {worker_id}.")

                # persist recorded in-flight messages for this channel
                msgs = state_data.get("channel_messages", {}).get(worker_id, [])
                channel_filename = f"snapshot_channel_{worker_id}_{incoming_snapshot_id}.json"
                self.save_state_to_afs(channel_filename, msgs)

                # if all channels done, finalize snapshot
                if not state_data.get("channels_to_record"):
                    print(f"[Snapshot] Global Snapshot {incoming_snapshot_id} COMPLETED ---")
                    # remove snapshot entry
                    del self.snapshot_state[incoming_snapshot_id]

            # Record in-flight messages for any active snapshots where this channel is still being recorded.
            # Iterate over a static list to avoid mutation issues.
            for snapshot_id, data in list(self.snapshot_state.items()):
                # ensure channel_messages structure exists
                data.setdefault("channel_messages", {})
                data["channel_messages"].setdefault(worker_id, [])

                # If this snapshot is still recording this channel and the incoming marker id
                # indicates this message should be considered in-flight, append it.
                # (Keeps original comparison logic: incoming_snapshot_id < snapshot_id)
                if worker_id in data.get("channels_to_record", []) and incoming_snapshot_id < snapshot_id:
                    print(f"[Snapshot] Recording in-flight message from {worker_id} for snapshot {snapshot_id}")
                    if filename is not None:
                        data["channel_messages"][worker_id].append(filename)


    def save_state_to_afs(self, filename, state_data):
        handle = self.afs_client.create_file(filename)
        if handle is None:
            print(f"[Snapshot] AFS Error: Could not create snapshot file {filename}")
            return

        try:
            data_str = json.dumps(state_data, indent=2)
            # write in chunks if too large
            for i in range(0, len(data_str), 1024):
                chunk = data_str[i:i+1024]
                self.afs_client.write_file(handle, chunk)
        except Exception as e:
            print(f"[Snapshot] Error writing snapshot data: {e}")
        finally:
            self.afs_client.close_file(handle)
            print(f"[Snapshot] Saved state to {filename}")
            

    # get all registered worker ids        
    def get_all_worker_ids(self):
        with self.coordinator.worker_regis_lock:
            return set(self.coordinator.workers.keys())
        

    # recover from snapshot at startup
    def recover_from_snapshot(self):
        print("[Coordinator] Checking for existing coordinator snapshots...")
        try:
            # Find the latest coordinator snapshot file in AFS.
            latest_snapshot_file = self.afs_client.find_latest_coordinator_snapshot()
            # if no snapshot file
            if latest_snapshot_file is None:
                print("[Coordinator] No existing snapshot found.")
                return False
            print(f"[Coordinator] Found snapshot '{latest_snapshot_file}'. Recovering state...")

            # Read the snapshot file from AFS.
            handle = self.afs_client.open_file(latest_snapshot_file)
            state_data = self.afs_client.read_json_file(handle)
            self.afs_client.close_file(handle)
            state = json.loads(state_data)

            # Restore task-related state.
            with self.coordinator.task_lock:
                for task in state.get("to_do_tasks", []):
                    self.coordinator.task_queue.put(task)

                # Any in-progress tasks should be re-queued so workers can pick them up.
                self.coordinator.assigned_tasks = state.get("in_progress_tasks", {})
                for task in self.coordinator.assigned_tasks.values():
                    self.coordinator.task_queue.put(task)
                # Restore counters.
                self.coordinator.total_tasks = state.get("total_tasks", 0)
                self.coordinator.completed_tasks = state.get("completed_tasks", 0)

                # Clear assigned_tasks because they were re-queued for reassignment.
                self.coordinator.assigned_tasks = {}

            # Restore temporary primes set.
            with self.coordinator.primes_lock:
                self.coordinator.all_primes = set(state.get("temp_primes", []))

            print("[Coordinator] State recovery complete.")
            return True

        except Exception as e:
            print(f"[Coordinator] Failed to load snapshot: {e}. Starting fresh.")
            return False