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
    processes = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding='utf-8', 
        errors='replace', 
        env=merged_env
    )
    return processes

def enqueue_output(processes, component_name, queue):
    for line in iter(processes.stdout.readline, ''):
        log_line = f"[{component_name}] {line.strip()}"
        queue.put(log_line)
    processes.stdout.close()
# start thread to supervise sub process
def start_log_thread(processes, component_name, shared_queue):
    thread = threading.Thread(target=enqueue_output, args=(processes, component_name, shared_queue))
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
    processes = {}
    log_queue = Queue()
    output_file = os.path.join("data", "output", "primes.txt")
    if os.path.exists(output_file):
        os.remove(output_file)
        print(f"Removed old output file: {output_file} first")

    try:
        # start afs
        env = {"SINGLE_MODE": "true"}
        processes['afs'] = start_component("AFS Server", ["python", "-m", "src.afs_server.afs_server", "0"], env)
        start_log_thread(processes['afs'], "AFS", log_queue)
        
        # start coor
        processes['coordinator'] = start_component("Coordinator", ["python", "-m", "src.afs_coordinator.coordinator"])
        start_log_thread(processes['coordinator'], "Coord", log_queue)

        # check startup success
        assert wait_for_log_in_queue(log_queue, "[AFS Server(0)] Primary node is running", 20), "AFS Server not starting"
        assert wait_for_log_in_queue(log_queue, "[Coordinator] Loaded tasks from AFS", 10), "Coordinator not laading tasks"

        # start workers
        worker_ids = ["worker-1", "worker-2", "worker-3"]
        for worker_id in worker_ids:
            processes[worker_id] = start_component(worker_id, ["python", "-m", "src.worker.worker_client.worker_client", worker_id], env)
            start_log_thread(processes[worker_id], worker_id, log_queue)

        # check all workers has registered to coor
        assert wait_for_log_in_queue(log_queue, "[worker-1] register to coordinator success", 10), "Worker-1 not registering"
        assert wait_for_log_in_queue(log_queue, "[worker-2] register to coordinator success", 10), "Worker-2 not registering"
        assert wait_for_log_in_queue(log_queue, "[worker-3] register to coordinator success", 10), "Worker-3 not registering"

        print("[TEST] All services started successfully")
        
        # wait for 15s
        print("[TEST] wait 15 seconds before first crash...")
        time.sleep(15)
        # step : worker 2 crash monitor
        print("[TEST]  crashing worker 2 ")
        processes['worker-2'].terminate()
        processes.pop('worker-2')

        # check coor has found that worker 2 is offline
        assert wait_for_log_in_queue(log_queue, "[Coordinator] Worker worker-2 timed out", 20), "Coordinator does not detect worker-2 is off line"
        print("detected Worker 2 crash successfully.")

        # recovery worker 2
        print("[TEST]  restart worker 2")
        # wait 10s
        time.sleep(10)
        
        processes['worker-2'] = start_component("Worker 2 (Restored)", ["python", "-m", "src.worker.worker_client.worker_client", "worker-2"], env)
        start_log_thread(processes['worker-2'], "W2-Restored", log_queue)
        
        # whether worker 2 is recovery or not
        assert wait_for_log_in_queue(log_queue, "[worker-2] successfully recovered from snapshot!", 10), "Worker 2 no recover"
        assert wait_for_log_in_queue(log_queue, "[worker-2] register to coordinator success", 10), "worker-2 no regis to coor"
        
        print("[TEST] Worker 2 successfully recovered from snapshot and re-registered.")

        # wait 10s
        time.sleep(10)

        # crash coor
        print("[TEST] crashing coor")
        processes['coordinator'].terminate()
        processes.pop('coordinator')

        assert wait_for_log_in_queue(log_queue, "Heartbeat failed", 20), "workers do not detect coor is offline"
        print("[TEST] Workers successfully detected Coordinator crash.")

        # recover coor
        print("[TEST] restart coor")
        # wait 10s
        time.sleep(10)
        
        processes['coordinator'] = start_component("Coordinator (Restored)", ["python", "-m", "src.afs_coordinator.coordinator"])
        start_log_thread(processes['coordinator'], "Coord-Restored", log_queue)

        
        # whether workers register to coor again
        assert wait_for_log_in_queue(log_queue, "[worker-1] register to coordinator success", 10), "Worker 1 not re-registering"
        assert wait_for_log_in_queue(log_queue, "[worker-2] register to coordinator success", 10), "Worker 2 not re-registering"
        assert wait_for_log_in_queue(log_queue, "[worker-3] register to coordinator success", 10), "Worker 3 not re-registering"
        
        print("[TEST] Coordinator successfully recovered and all Workers re-registered.")

        # system finish correct
        print("Waiting for system to complete all tasks")
        
        assert wait_for_log_in_queue(log_queue, "[Coordinator] All tasks completed. Saving results...")

    finally:
        for name, proc in processes.items():
            if proc:
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except Exception as e:
                    proc.kill() 
    

if __name__ == "__main__":
    test_snapshot_crash_recovery()