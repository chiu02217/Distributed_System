import time
import subprocess
import os
import threading
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor



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
            #print(log)
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


        #workers for multithreading
        worker_ids = ["worker-1", "worker-2", "worker-3"]

        #multithread workers
        with ThreadPoolExecutor() as executor:
            worker_futures = [
                executor.submit(
                    start_component, worker_id, ["python", "-m", "src.worker.worker_client.worker_client", worker_id], env={"SINGLE_MODE": "true"}
                )
                for worker_id in worker_ids
            ]

        workers = [future.result() for future in worker_futures]

        #log workers
        for i, worker in enumerate(workers):
            start_log_thread(worker, log_queue)




        #worker submission logs
        assert wait_for_log_in_queue(log_queue, "Submitted 4 primes from input_dataset", timeout=5), "[TEST] Failed to submit primes from input_dataset"
        assert wait_for_log_in_queue(log_queue, "Submitted 4 primes from input_dataset", timeout=5), "[TEST] Failed to submit primes from input_dataset"
        assert wait_for_log_in_queue(log_queue, "Submitted 4 primes from input_dataset", timeout=5), "[TEST] Failed to submit primes from input_dataset"


        output_file = os.path.join(TEST_OUTPUT, "primes.txt")
        assert os.path.exists(output_file), "[TEST] Output file not found"

        expected_primes = [7, 11, 13, 17, 19, 23, 29, 31]
        
        output_primes = []
        with open(output_file, "r") as f:
            for line in f:
                output_primes.append(int(line.strip()))


        output_primes.sort()
        
        #check correct primes outputted
        print("\n\n[TEST] Duplicate primes:")
        print("[TEST] 11 - input_dataset_001, input_dataset_003")
        print("[TEST] 29 - input_dataset_001, input_dataset_002")
        print("[TEST] 17 - input_dataset_001, input_dataset_002, input_dataset_003\n\n")
        print(f"[TEST] Primes expected: {expected_primes}")
        print(f"[Test] Primes outputted: {output_primes}")
        if expected_primes==output_primes:
            print(f"[TEST] Test passed")
        else:
            print("[TEST] Test failed")



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