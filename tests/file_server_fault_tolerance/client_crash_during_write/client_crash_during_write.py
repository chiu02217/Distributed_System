import time
import subprocess
import os
import threading
from queue import Queue, Empty


TEST_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_INPUT = os.path.join(TEST_DIR, "data", "input")
TEST_OUTPUT = os.path.join(TEST_DIR, "data", "output")
TEST_SNAPSHOT = os.path.join(TEST_DIR, "data", "snapshot")

def start_component(component_name, cmd, env=None):
    print(f"[TEST] Starting: {component_name}")
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    return subprocess.Popen(
        ["python", "-u", *cmd[1:]],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=merged_env
    )


def enqueue_output(proc, queue):
    for line in iter(proc.stdout.readline, ''):
        queue.put(f"{line.strip()}")
    proc.stdout.close()


def start_log_thread(proc, shared_queue):
    thread = threading.Thread(target=enqueue_output, args=(proc, shared_queue))
    thread.daemon = True
    thread.start()


def wait_for_log_in_queue(queue, keyword, timeout=10):
    start = time.time()
    while time.time() - start < timeout:
        try:
            log = queue.get(timeout=0.1)
            print(log)
            if keyword in log:
                #print(log)
                return True
        except Empty:
            continue
    return False



def test_client_crash_during_write():
    try:
        log_queue = Queue()

        # start the server
        server = start_component("Server", [
            "python", "-m", "src.afs_server.afs_server", "0", TEST_INPUT, TEST_OUTPUT, TEST_SNAPSHOT
        ], env={"SINGLE_MODE": "true"})
        start_log_thread(server, log_queue)
        assert wait_for_log_in_queue(log_queue, "[AFS Server(0)] Primary node is running", timeout=20), "[TEST] Server did not start"




        coordinator = start_component("Coordinator", [
            "python", "-m", "src.afs_coordinator.coordinator"
        ])
        start_log_thread(coordinator, log_queue)
        assert wait_for_log_in_queue(log_queue, "[Coordinator] Loaded tasks from AFS", timeout=5), "Coordinator could not load tasks"


        worker = start_component("Worker-1", [
            "python", "-m", "src.worker.worker_client.worker_client", "worker-1"
        ], env={"SINGLE_MODE": "true"})
        start_log_thread(worker, log_queue)
        assert wait_for_log_in_queue(log_queue, "[Worker worker-1] Started!", timeout=5), "[TEST] Worker 1 did not start"


        assert wait_for_log_in_queue(log_queue, "[AFSClient] Writing to cache: 5", timeout=50), "[TEST] AFSClient did not write to cache"

        #crash worker
        print("[TEST] Crashing worker...")
        worker.kill()

        assert wait_for_log_in_queue(log_queue, "[Coordinator] Worker worker-1 timed out", timeout=10), "[TEST] Coordinator did not detect worker failure"

        #restart worker
        print("[TEST] Restarting worker...")
        worker = start_component("Worker-1", [
            "python", "-m", "src.worker.worker_client.worker_client", "worker-1"
        ], env={"SINGLE_MODE": "true"})
        start_log_thread(worker, log_queue)
        assert wait_for_log_in_queue(log_queue, "[Worker worker-1] Started!", timeout=5), "[TEST] Worker 1 did not start"


        assert wait_for_log_in_queue(log_queue, "[Worker worker-1] Submitted", timeout=500), "[TEST] Worker 1 did not submit primes"



    finally:
        if 'server' in locals():
            server.terminate()
        if 'coordinator' in locals():
            coordinator.terminate()
        if 'worker' in locals():
            worker.terminate()


    

if __name__ == "__main__":
    test_client_crash_during_write()