import time
import subprocess
import os
import threading
from queue import Queue, Empty


TEST_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_INPUT = os.path.join(TEST_DIR, "test_input1")
TEST_OUTPUT = os.path.join(TEST_DIR, "test_output1")

def start_component(cmd):
    print(f"[TEST] Starting: {' '.join(cmd)}")
    return subprocess.Popen(
        ["python", "-u", *cmd[1:]],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
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
            if keyword in log:
                print(log)
                return True
        except Empty:
            continue
    return False



def test_single_worker_single_file():
    try:
        log_queue = Queue()

        # start the server
        server = start_component([
            "python", "-m", "src.afs_server.afs_server", "--input", TEST_INPUT, "--output", TEST_OUTPUT
        ])
        start_log_thread(server, log_queue)
        assert wait_for_log_in_queue(log_queue, "[AFS Server] starts", timeout=5), "[TEST] Server did not start"




        coordinator = start_component([
            "python", "-m", "src.afs_coordinator.coordinator"
        ])
        start_log_thread(coordinator, log_queue)
        assert wait_for_log_in_queue(log_queue, "[Coordinator] Loaded tasks from AFS", timeout=5), "Coordinator could not load tasks"


        worker = start_component([
            "python", "-m", "src.worker.worker_client.worker_client", "worker-1"
        ])
        start_log_thread(worker, log_queue)
        assert wait_for_log_in_queue(log_queue, "[Worker worker-1] Started!", timeout=5), "[TEST] Worker 1 did not start"

        assert wait_for_log_in_queue(log_queue, "[AFS Server] Wrote", timeout=5), "[TEST] AFS server did not write to prime.txt"
        assert wait_for_log_in_queue(log_queue, "[Worker worker-1] Submitted", timeout=5), "[TEST] Worker 1 did not submit primes"


    finally:
        if 'server' in locals():
            server.terminate()
        if 'coordinator' in locals():
            coordinator.terminate()
        if 'worker' in locals():
            worker.terminate()


    

if __name__ == "__main__":
    test_single_worker_single_file()