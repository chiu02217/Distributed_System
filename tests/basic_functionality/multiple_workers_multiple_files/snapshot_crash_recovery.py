import time
import subprocess
import os
import threading
import json
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor


def start_component(component_name, cmd, env=None):
    print(f"Starting: {component_name}")
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding='utf-8', 
        errors='replace', 
        env=merged_env
    )
    return proc

def enqueue_output(proc, component_name, queue):
    for line in iter(proc.stdout.readline, ''):
        log_line = f"[{component_name}] {line.strip()}"
        queue.put(log_line)
    proc.stdout.close()
# start thread to supervise sub process
def start_log_thread(proc, component_name, shared_queue):
    thread = threading.Thread(target=enqueue_output, args=(proc, component_name, shared_queue))
    thread.daemon = True 
    thread.start()
# check the terminal log 
def wait_for_log_in_queue(queue, keyword, timeout=10):
    start = time.time()
    #clear old terminal log first
    while not queue.empty():
        try:
            queue.get_nowait()
        except Empty:
            break
            
    print(f" Waiting for log: '{keyword}'...")
    while time.time() - start < timeout:
        try:
            log = queue.get(timeout=0.1)
            if keyword in log:
                print(log)
                return True
        except Empty:
            continue
    
    print(f"Timed out waiting for '{keyword}'")
    return False

# test snapshot crash and recover

def test_snapshot_crash_recovery():
    procs = {}
    log_queue = Queue()
    output_file = os.path.join("data", "output", "primes.txt")
    if os.path.exists(output_file):
        os.remove(output_file)
        print(f"Removed old output file: {output_file} first")

    try:
        # start afs
        env = {"SINGLE_MODE": "true"}
        procs['afs'] = start_component("AFS Server", ["python", "-m", "src.afs_server.afs_server", "0"], env)
        start_log_thread(procs['afs'], "AFS", log_queue)
        
        # start coor
        procs['coordinator'] = start_component("Coordinator", ["python", "-m", "src.afs_coordinator.coordinator"])
        start_log_thread(procs['coordinator'], "Coord", log_queue)

        # check startup success
        assert wait_for_log_in_queue(log_queue, "[AFS Server] Primary node is running", 20), "AFS Server 未啟動"
        assert wait_for_log_in_queue(log_queue, "[Coordinator] Loaded tasks from AFS", 10), "Coordinator 未載入任務"

        # start workers
        worker_ids = ["worker-1", "worker-2", "worker-3"]
        for worker_id in worker_ids:
            procs[worker_id] = start_component(worker_id, ["python", "-m", "src.worker.worker_client.worker_client", worker_id], env)
            start_log_thread(procs[worker_id], worker_id, log_queue)

        # check all workers has registered to coor
        assert wait_for_log_in_queue(log_queue, "[worker-1] register to coordinator success", 10), "Worker-1 未註冊"
        assert wait_for_log_in_queue(log_queue, "[worker-2] register to coordinator success", 10), "Worker-2 未註冊"
        assert wait_for_log_in_queue(log_queue, "[worker-3] register to coordinator success", 10), "Worker-3 未註冊"

        print("[TEST] All services started successfully")
        
        # wait for 15s
        print("[TEST] wait 15 seconds before first crash...")
        time.sleep(15)
        # step : worker 2 crash monitor
        print("[TEST]  crashing worker 2 ")
        procs['worker-2'].terminate()
        procs.pop('worker-2')

        # check coor has found that worker 2 is offline
        assert wait_for_log_in_queue(log_queue, "[Coordinator] Worker worker-2 timed out", 20), "Coordinator does not detect worker-2 is off line"
        print("detected Worker 2 crash successfully.")

        # recovery worker 2
        print("[TEST]  restart worker 2")
        # wait 10s
        time.sleep(10)
        
        procs['worker-2'] = start_component("Worker 2 (Restored)", ["python", "-m", "src.worker.worker_client.worker_client", "worker-2"], env)
        start_log_thread(procs['worker-2'], "W2-Restored", log_queue)
        
        # whether worker 2 is recovery or not
        assert wait_for_log_in_queue(log_queue, "[worker-2] successfully recovered from snapshot!", 10), "Worker 2 no recover"
        assert wait_for_log_in_queue(log_queue, "[worker-2] register to coordinator success", 10), "worker-2 no regis to coor"
        
        print("[TEST] Worker 2 successfully recovered from snapshot and re-registered.")

        # wait 10s
        time.sleep(10)

        # crash coor
        print("[TEST] crashing coor")
        procs['coordinator'].terminate()
        procs.pop('coordinator')

        assert wait_for_log_in_queue(log_queue, "Heartbeat failed", 20), "workers do not detect coor is offline"
        print("[TEST] Workers successfully detected Coordinator crash.")

        # recover coor
        print("[TEST] restart coor")
        # wait 10s
        time.sleep(10)
        
        procs['coordinator'] = start_component("Coordinator (Restored)", ["python", "-m", "src.afs_coordinator.coordinator"])
        start_log_thread(procs['coordinator'], "Coord-Restored", log_queue)

        # coor recover from snapshot
        assert wait_for_log_in_queue(log_queue, "[Coordinator] State recovery complete.", 10), "Coordinator 未從快照恢復"
        
        # whether workers register to coor again
        assert wait_for_log_in_queue(log_queue, "[worker-1] register to coordinator success", 10), "Worker 1 未重新註冊"
        assert wait_for_log_in_queue(log_queue, "[worker-2] register to coordinator success", 10), "Worker 2 未重新註冊"
        assert wait_for_log_in_queue(log_queue, "[worker-3] register to coordinator success", 10), "Worker 3 未重新註冊"
        
        print("[TEST] Coordinator successfully recovered and all Workers re-registered.")

        # system finish correct
        print("Waiting for system to complete all tasks")
        
        assert wait_for_log_in_queue(log_queue, "[Coordinator] All tasks completed. Saving results...")

    finally:
        for name, proc in procs.items():
            if proc:
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except Exception as e:
                    proc.kill() 
    

if __name__ == "__main__":
    test_snapshot_crash_recovery()