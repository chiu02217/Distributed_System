@echo off
ECHO Starting Automated Crash Test...
cd /D "%~dp0"

:: use single mode
set SINGLE_MODE=true
ECHO [Config] Set SINGLE_MODE=true

:: 1. startup all services (coor, afs server, workers)
ECHO [Step 1] Starting all services...
start "AFS Server" cmd /k python -m src.afs_server.afs_server 0
timeout /t 2 > nul
start "Coordinator" cmd /k python -m src.afs_coordinator.coordinator
timeout /t 2 > nul
start "Worker 1" cmd /k python -m src.worker.worker_client.worker_client worker-1
start "Worker 2" cmd /k python -m src.worker.worker_client.worker_client worker-2
start "Worker 3" cmd /k python -m src.worker.worker_client.worker_client worker-3

:: 2. wait for 15 seconds to sumulate worker 2 crash
ECHO [Step 2] System running. Waiting 15 seconds before simulated crash...
timeout /t 15 > nul

ECHO CRASHING WORKER 2...
wmic process where "name='python.exe' and commandline like '%%worker_client.worker_client worker-2%%'" call terminate > nul

:: 3. wait for 10 seconds to recover worker 2
ECHO [Step 3] Worker 2 crashed. Waiting 10 seconds before recovery...
timeout /t 10 > nul

:: 4. simulate recovering of worker 2
ECHO RESTARTING WORKER 2 (simulating recovery)...
start "Worker 2 (Restored)" cmd /k python -m src.worker.worker_client.worker_client worker-2

ECHO  Waiting 10 seconds for Worker 2 to register..
timeout /t 10 > nul

ECHO [Step 4] CRASHING COORDINATOR...
wmic process where "name='python.exe' and commandline like '%%coordinator%%'" call terminate > nul


ECHO [Step 5] Coordinator crashed. Waiting 10s to recover (Workers will failing calling gRPC)
timeout /t 10 > nul


ECHO [Step 6] RESTARTING COORDINATOR (simulating recovery)
start "Coordinator (Restored)" cmd /k python -m src.afs_coordinator.coordinator

ECHO [Fianlly ] Crash and recovery test finish!!!!.
ECHO.


:: press enter to execute cleaning services
set /p "cleanup=Press ENTER to clean up all services..."

ECHO Cleaning up all services...
wmic process where "name='python.exe' and commandline like '%%afs_server%%'" call terminate > nul
wmic process where "name='python.exe' and commandline like '%%coordinator%%'" call terminate > nul
wmic process where "name='python.exe' and commandline like '%%worker_client worker-1%%'" call terminate > nul
wmic process where "name='python.exe' and commandline like '%%worker_client worker-2%%'" call terminate > nul
wmic process where "name='python.exe' and commandline like '%%worker_client worker-3%%'" call terminate > nul

ECHO Cleanup complete.