import time
import subprocess
import os
import threading
from queue import Queue, Empty


TEST_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_INPUT = os.path.join(TEST_DIR, "test_input2")
TEST_OUTPUT = os.path.join(TEST_DIR, "test_output2")

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
            print(log)
            if keyword in log:
                return True
        except Empty:
            continue
    return False




def test_multiple_workers_multiple_files():
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


        worker1 = start_component([
            "python", "-m", "src.worker.worker_client.worker_client", "worker-1", "--port", "9001"
        ])
        start_log_thread(worker1, log_queue)
        assert wait_for_log_in_queue(log_queue, "[Worker worker-1] Started!", timeout=5), "[TEST] Worker 1 did not start"



        worker2 = start_component([
            "python", "-m", "src.worker.worker_client.worker_client", "worker-2"
        ])
        start_log_thread(worker2, log_queue)
        assert wait_for_log_in_queue(log_queue, "[Worker worker-2] Started!", timeout=5), "[TEST] Worker 2 did not start"



        worker3 = start_component([
            "python", "-m", "src.worker.worker_client.worker_client", "worker-3"
        ])
        start_log_thread(worker3, log_queue)
        assert wait_for_log_in_queue(log_queue, "[Worker worker-3] Started!", timeout=5), "[TEST] Worker 3 did not start"


        time.sleep(10)
    finally:
        if 'server' in locals():
            server.terminate()
        if 'coordinator' in locals():
            coordinator.terminate()
        if 'worker1' in locals():
            worker1.terminate()
        if 'worker2' in locals():
            worker2.terminate()
        if 'worker3' in locals():
            worker3.terminate()

if __name__ == "__main__":
    test_multiple_workers_multiple_files()