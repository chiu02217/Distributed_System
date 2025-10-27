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
        if not line:
            time.sleep(0.1)
            continue
        print(line.strip())
        if keyword in line:
            return True
    print("Timed out waiting for log...")
    return False


def test_worker_reassignment():
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
    assert wait_for_log(coordinator, "[Coordinator] Server")
    time.sleep(2)


if __name__ == "__main__":
    test_worker_reassignment()