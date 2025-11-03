import time
import subprocess


def start_component(cmd):
    print(f"[TEST] Starting: {' '.join(cmd)}")
    return subprocess.Popen(
        ["python", "-u", *cmd[1:]],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

def wait_for_log(proc, keyword, timeout=10):
    #wait until the log is seen before continuing
    start = time.time()
    while time.time() - start < timeout:
        line = proc.stdout.readline()
        print(line.strip())
        if not line:
            time.sleep(0.1)
            continue
        if keyword in line:
            return True
    print("Timed out waiting for log...")
    return False

def crash_worker(proc, keyword, crash_keyword, timeout=10):
    #wait until the log is seen
    start = time.time()
    while time.time() - start < timeout:
        line = proc.stdout.readline()
        print(line.strip())
        if not line:
            time.sleep(0.1)
            continue
        if keyword in line:
            print("[TEST] Worker 1 started")
        #Crash worker in the middle of its operations
        elif crash_keyword in line:
            print("[TEST] Crashing worker 1...")
            proc.kill()
            print("[TEST] Worker 1 crashed")
            return True
    print("[TEST] Timed out waiting for log...")
    return False



def test_worker_crash_detection():
    # start the server
    server = start_component([
        "python", "-m", "src.afs.server.afs_server",
        "data/server_storage/input", "data/server_storage/output", "50051"
    ])
    assert wait_for_log(server, "[Server] starts"), "AFS server failed to start"
    time.sleep(2)

    coordinator = start_component([
        "python", "-m", "src.afs.coordinator.coordinator", "localhost:50051", "50052"
    ])
    assert wait_for_log(coordinator, "[Coordinator] Server"), "Coordinator failed to start"
    time.sleep(2)

    worker = start_component([
        "python", "-m", "src.app.worker.worker", "worker-1", "localhost:50052", "localhost:50051", "/tmp/worker1"
    ])
    #worker 1 crashes after crash_key

    assert crash_worker(worker, "[Worker worker-1] Started!", "[Worker worker-1] Received task: input_dataset_002.txt"), "Crash simulation Failed"
    time.sleep(2)


    # Wait for coordinator to detect timeout
    print("[TEST] Waiting for coordinator to detect timeout...")
    assert wait_for_log(coordinator, "Worker worker-1 timed out", timeout=15), "Coordinator did not detect worker timeout"

    print("[TEST] Worker crash detection test passed!")



if __name__ == "__main__":
    test_worker_crash_detection()