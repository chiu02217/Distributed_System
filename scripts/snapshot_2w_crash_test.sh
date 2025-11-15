#!/bin/bash
set -e

# cleanup() {
#     echo "[TEST] Cleaning up all services..."
#     [ ! -z "$AFS_PID" ] && (kill $AFS_PID 2>/dev/null || true)
#     [ ! -z "$COORDINATOR_PID" ] && (kill $COORDINATOR_PID 2>/dev/null || true)
#     [ ! -z "$WORKER1_PID" ] && (kill $WORKER1_PID 2>/dev/null || true)
#     [ ! -z "$WORKER2_PID" ] && (kill $WORKER2_PID 2>/dev/null || true)
#     [ ! -z "$WORKER3_PID" ] && (kill $WORKER3_PID 2>/dev/null || true)
#     echo "Cleanup complete."
# }
# trap cleanup EXIT INT
cleanup() {
    echo "[TEST] Cleaning up all services..."
    # kill -9  force
    [ ! -z "$AFS_PID" ] && (kill -9 $AFS_PID 2>/dev/null || true)
    [ ! -z "$COORDINATOR_PID" ] && (kill -9 $COORDINATOR_PID 2>/dev/null || true)
    [ ! -z "$WORKER1_PID" ] && (kill -9 $WORKER1_PID 2>/dev/null || true)
    [ ! -z "$WORKER2_PID" ] && (kill -9 $WORKER2_PID 2>/dev/null || true)
    [ ! -z "$WORKER3_PID" ] && (kill -9 $WORKER3_PID 2>/dev/null || true)
    echo "Cleanup complete."
}
trap cleanup EXIT INT

cd "$(dirname "$0")"
cd ..

export SINGLE_MODE=true

echo "[Step 1] Starting all services..."
python3 -m src.afs_server.afs_server 0 &
AFS_PID=$!
sleep 2

python3 -m src.afs_coordinator.coordinator &
COORDINATOR_PID=$!
sleep 2

python3 -m src.worker.worker_client.worker_client worker-1 &
WORKER1_PID=$!

python3 -m src.worker.worker_client.worker_client worker-2 &
WORKER2_PID=$!

python3 -m src.worker.worker_client.worker_client worker-3 &
WORKER3_PID=$!

echo "[Step 2] System running. Waiting 15 seconds before simulated crash..."
sleep 15

echo "CRASHING WORKER 2..."
kill $WORKER2_PID 2>/dev/null || true
echo "CRASHING WORKER 3..."
kill $WORKER3_PID 3>/dev/null || true

echo "[Step 3] Worker 2 & 3  crashed. Waiting 10 seconds before recovery..."
sleep 10

echo "RESTARTING WORKER 2 & 3 (simulating recovery)..."
python3 -m src.worker.worker_client.worker_client worker-2 &
WORKER2_PID=$!

python3 -m src.worker.worker_client.worker_client worker-3 &
WORKER3_PID=$!

sleep 10


echo "Crash and recovery test finish!!!!."
echo ""

read -p "Press ENTER to clean up all services"