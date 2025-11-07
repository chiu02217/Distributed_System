import time
import subprocess
import os
import threading
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor



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
            if keyword in log:
                print(log)
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
        assert wait_for_log_in_queue(log_queue, "[Coordinator] Loaded tasks from AFS", timeout=5), "Coordinator could not load tasks"


        '''
        worker1 = start_component([
            "python", "-m", "src.worker.worker_client.worker_client", "worker-1"
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
        '''

        #workers for multithreading
        worker_ids = ["worker-1", "worker-2", "worker-3"]

        #multithread workers
        with ThreadPoolExecutor() as executor:
            worker_futures = [
                executor.submit(
                    start_component,
                    ["python", "-m", "src.worker.worker_client.worker_client", worker_id]
                )
                for worker_id in worker_ids
            ]

        workers = [future.result() for future in worker_futures]

        #log workers
        for i, worker in enumerate(workers):
            start_log_thread(worker, log_queue)

        #worker submission logs
        assert wait_for_log_in_queue(log_queue, "Submitted 3 primes from input_dataset", timeout=5), "[TEST] Failed to submit primes from input_dataset"
        assert wait_for_log_in_queue(log_queue, "Submitted 3 primes from input_dataset", timeout=5), "[TEST] Failed to submit primes from input_dataset"
        assert wait_for_log_in_queue(log_queue, "Submitted 3 primes from input_dataset", timeout=5), "[TEST] Failed to submit primes from input_dataset"



    finally:
        if 'server' in locals():
            server.terminate()
        if 'coordinator' in locals():
            coordinator.terminate()
        if 'workers' in locals():
            for worker in workers:
                if worker:
                    worker.terminate()

if __name__ == "__main__":
    test_multiple_workers_multiple_files()