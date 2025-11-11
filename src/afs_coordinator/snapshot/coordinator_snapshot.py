import threading
import time
import json
from src.common.config_loader import CONFIG
from src.common.grpc.auto_generated import coordinator_message_pb2 as messages
from src.afs_coordinator.snapshot.i_coordinator_snapshot import ICoordinatorSnapshotHandler
from src.common.grpc.auto_generated import snapshot_message_pb2 as snapshot_messages
from typing import TYPE_CHECKING 

if TYPE_CHECKING:
    from src.afs_coordinator.coordinator import CoordinatorServicer 

# snapshot frequency
SNAPSHOT_FREQUENCY_SECONDS = 30

class CoordinatorSnapshotHandler(ICoordinatorSnapshotHandler):
    def __init__(self, coordinator: 'CoordinatorServicer'):
        self.coordinator = coordinator
        self.afs_client = coordinator.afs_client
        self.snapshot_dir = CONFIG.afs.snapshot_dir
        self.current_snapshot_id = 0
        # 儲存 { snapshot_id: state_data }
        self.snapshot_state = {}

    # backeground thread(trigger snapshot periodically)
    def start_snapshot_thread(self):
        print("[Snapshot] Starting snapshot thread...")
        thread = threading.Thread(target=self.global_snapshot_loop, daemon=True)
        thread.start()

    # every 30s take a snapshot
    def global_snapshot_loop(self):
        while True:
            time.sleep(SNAPSHOT_FREQUENCY_SECONDS)
            self.initiate_coor_snapshot()

    # Chandy Lamport
    def initiate_coor_snapshot(self):
        # id from 1
        self.current_snapshot_id += 1
        snapshot_id = self.current_snapshot_id
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

        # store coordinator snpashot to AFS in json format
        coordinator_snapshot_filename = f"snapshot_{snapshot_id}.json"
        self.save_coor_snapshot_to_afs(coordinator_snapshot_filename, coord_state)
        
        # get worker list
        workers_ids = self.get_all_worker_ids()
        self.snapshot_state[snapshot_id] = {
            "coordinator_state_saved_or_not": True,
            "worker_channels": workers_ids.copy(),
            "channel_messages": {worker_id: [] for worker_id in workers_ids}
        }

        # already sent to all Worker (在下次 GetTask 時)
        print(f"[Snapshot] Coordinator state saved for snapshot {snapshot_id}.")

        # ensure snapshot lock exists
        if not hasattr(self, "_snapshot_lock"):
            self._snapshot_lock = threading.Lock()

        with self.coordinator.worker_regis_lock:
            for worker_id, coordinator_stub in self.coordinator.worker_stubs.items():
                # only send to currently known workers
                if worker_id in workers_ids:
                    try:
                        # call TriggerSnapshot RPC on Worker server
                        trigger_snapshot_req = snapshot_messages.TriggerSnapshotRequest(snapshot_id=snapshot_id)
                        coordinator_stub.TriggerSnapshot(trigger_snapshot_req)
                    except Exception as e:
                        print(f"[Snapshot] Failed to send marker to {worker_id}: {e}")

    # send snapshot_id to worker
    def send_snapshot_id_to_worker(self, response: messages.GetTaskResponse):
        response.snapshot_id = self.current_snapshot_id

    # add marker to incoming SubmitResultRequest
    # 2 duties:
    # a. Chandy-Lamport Receiver Rule
    # b. Record in-flight messages
    def process_result_from_worker(self, request: messages.SubmitResultRequest):
        worker_id = request.worker_id
        # get snpshot id when worker submitResult
        incoming_snapshot_id_from_worker = getattr(request, "snapshot_id", None)
        # get worker completed filename's content
        filename_from_worker = getattr(request, "filename", None)

        # ensure a lock for snapshot_state exists
        if not hasattr(self, "_snapshot_lock"):
            self._snapshot_lock = threading.Lock()

        # lock needed because multiple worker might call this function simultanously
        with self._snapshot_lock:
            # If worker's snapshot_id is not we are following(old or...) then ignore and return 
            if incoming_snapshot_id_from_worker not in self.snapshot_state:
                return
            # accoring to worker's snapshot id, get according snapshot coordinator state
            inflight_state_data: dict = self.snapshot_state[incoming_snapshot_id_from_worker]

            # Receiver rule: first marker from this worker for this snapshot
            if worker_id in inflight_state_data.get("worker_channels", []):
                # if this is the forst time what we got from worker, then delete it from waiting list
                inflight_state_data["worker_channels"].remove(worker_id)
                print(f"[Snapshot] Received snapshot {incoming_snapshot_id_from_worker} from {worker_id}.")

                # store in-flight messages
                channel_msgs = inflight_state_data.get("channel_messages", {}).get(worker_id, [])
                # a file for storing the in-flight messages
                channel_filename = f"snapshot_channel_{worker_id}_{incoming_snapshot_id_from_worker}.json"
                self.save_coor_snapshot_to_afs(channel_filename, channel_msgs)

                # if all channels done, means that snapshot is done
                if not inflight_state_data.get("worker_channels"):
                    print(f"[Snapshot] Global Snapshot {incoming_snapshot_id_from_worker} COMPLETED ---")
                    # remove this snapshot state from mem 
                    del self.snapshot_state[incoming_snapshot_id_from_worker]
            # iterate all in-progressing coor snapshot
            for snapshot_id, channel_data in list(self.snapshot_state.items()):
                # ensure channel_messages &channel_messages structure exists
                channel_data.setdefault("channel_messages", {})
                channel_data["channel_messages"].setdefault(worker_id, [])

                # If this snapshot is still recording this worker_channel and the incoming marker id
                # If this incoming_snapshot_id, earlier than the id we are processing now
                # If both yes, then this is a in-flight_messages
                if worker_id in channel_data.get("worker_channels", []) and incoming_snapshot_id_from_worker < snapshot_id:
                    print(f"[Snapshot] Recording in-flight message from {worker_id} for snapshot {snapshot_id}")
                    if filename_from_worker is not None:
                        channel_data["channel_messages"][worker_id].append(filename_from_worker)


    def save_coor_snapshot_to_afs(self, filename, state_data):
        create_success_or_not = self.afs_client.create_file(filename)
        if create_success_or_not is None:
            print(f"[Snapshot] AFS Error: Could not create coordinator snapshot file {filename}")
            return

        try:
            raw_data = json.dumps(state_data, indent=2)
            # write in chunks if too large
            for i in range(0, len(raw_data), 1024):
                data_chunk = raw_data[i:i+1024]
                self.afs_client.write_file(create_success_or_not, data_chunk)
        except Exception as e:
            print(f"[Snapshot] Error writing coordinator snapshot data: {e}")
        finally:
            self.afs_client.close_file(create_success_or_not)
            print(f"[Snapshot] Saved coordinator state to {filename}")
            

    # get all registered worker ids        
    def get_all_worker_ids(self):
        with self.coordinator.worker_regis_lock:
            return set(self.coordinator.workers.keys())
        

    # recover from snapshot at startup
    def coor_recover_from_snapshot(self):
        print("[Coordinator] Checking for existing coordinator snapshots...")
        try:
            # Find the latest coordinator snapshot file in AFS.
            latest_snapshot_file = self.afs_client.find_latest_coordinator_snapshot()
            # if no snapshot file
            if latest_snapshot_file is None:
                print("[Coordinator] No existing coordinator snapshot found.")
                return False
            print(f"[Coordinator] Found snapshot '{latest_snapshot_file}'. Recovering state...")

            # Read the snapshot file from AFS.
            coor_snapshot = self.afs_client.open_file(latest_snapshot_file)
            coor_state_data = self.afs_client.read_json_file(coor_snapshot)
            self.afs_client.close_file(coor_snapshot)
            coor_snapshot_json_data = json.loads(coor_state_data)

            # Restore task-related state.
            with self.coordinator.task_lock:
                for task in coor_snapshot_json_data.get("to_do_tasks", []):
                    self.coordinator.task_queue.put(task)

                # Any in-progress tasks should be re-queued so workers can pick them up.
                self.coordinator.assigned_tasks = coor_snapshot_json_data.get("in_progress_tasks", {})
                for task in self.coordinator.assigned_tasks.values():
                    self.coordinator.task_queue.put(task)
                self.coordinator.total_tasks = coor_snapshot_json_data.get("total_tasks", 0)
                self.coordinator.completed_tasks = coor_snapshot_json_data.get("completed_tasks", 0)

                # because crash so u have to reassign those asssigned tasks 
                self.coordinator.assigned_tasks = {}

            # Restore temporary primes set.
            with self.coordinator.primes_lock:
                self.coordinator.all_primes = set(coor_snapshot_json_data.get("temp_primes", []))

            print("[Coordinator] Coordinator State recovery complete.")
            return True

        except Exception as e:
            print(f"[Coordinator] Failed to load coordinator snapshot: {e}. Starting from begin")
            return False