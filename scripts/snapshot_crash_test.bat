@echo off
ECHO Starting Automated Crash Test...
cd /D "%~dp0"

:: 1. 啟動所有服務 (保持不變)
ECHO [Step 1] Starting all services...
start "AFS Server" cmd /k python -m src.afs_server.afs_server
timeout /t 2 > nul
start "Coordinator" cmd /k python -m src.afs_coordinator.coordinator
timeout /t 2 > nul
start "Worker 1" cmd /k python -m src.worker.worker_client.worker_client worker-1
start "Worker 2" cmd /k python -m src.worker.worker_client.worker_client worker-2
start "Worker 3" cmd /k python -m src.worker.worker_client.worker_client worker-3

:: 2. 等待 15 秒
ECHO [Step 2] System running. Waiting 15 seconds before simulated crash...
timeout /t 15 > nul

:: 3. 模擬崩潰 (*** 修正：改用 WMIC ***)
ECHO [Step 3] CRASHING WORKER 2...
:: 尋找 "python.exe" 處理程序，且其 "commandline" 包含 "worker_client worker-2"
wmic process where "name='python.exe' and commandline like '%%worker_client.worker_client worker-2%%'" call terminate > nul

:: 4. 等待 10 秒
ECHO [Step 4] Worker 2 crashed. Waiting 10 seconds before recovery...
timeout /t 10 > nul

:: 5. 模擬恢復 (重啟 Worker 2)
ECHO [Step 5] RESTARTING WORKER 2 (simulating recovery)...
start "Worker 2 (Restored)" cmd /k python -m src.worker.worker_client.worker_client worker-2

ECHO [NEW STEP] Waiting 10 seconds for Worker 2 to register..
timeout /t 10 > nul

:: 6. 模擬 COORDINATOR 崩潰 (*** 修正：改用 WMIC ***)
ECHO [Step 6] *** CRASHING COORDINATOR ***
:: 尋找 "python.exe" 處理程序，且其 "commandline" 包含 "coordinator_server"
wmic process where "name='python.exe' and commandline like '%%coordinator%%'" call terminate > nul


:: 7. 等待 10 秒 (模擬停機時間)
ECHO [Step 7] Coordinator crashed. Workers should now be failing RPCs.
timeout /t 10 > nul


:: 8. 模擬 COORDINATOR 恢復 (重啟)
ECHO [Step 8] *** RESTARTING COORDINATOR (simulating recovery) ***
start "Coordinator (Restored)" cmd /k python -m src.afs_coordinator.coordinator

ECHO [Step 9] Crash and recovery test initiated.
ECHO.
ECHO +---------------------------------------------------------+
ECHO | 測試正在運行中！ 按下 ENTER 鍵來 "清理所有視窗"。       |
ECHO +---------------------------------------------------------+

:: 9. 暫停 (*** 修正：改用 set /p，這不會產生亂碼 ***)
set /p "cleanup=Press ENTER to clean up all services..." > nul

:: 10. 清理所有服務 (*** 修正：改用 WMIC ***)
ECHO [Step 10] Cleaning up all services...
wmic process where "name='python.exe' and commandline like '%%afs_server%%'" call terminate > nul
wmic process where "name='python.exe' and commandline like '%%coordinator%%'" call terminate > nul
wmic process where "name='python.exe' and commandline like '%%worker_client worker-1%%'" call terminate > nul
wmic process where "name='python.exe' and commandline like '%%worker_client worker-2%%'" call terminate > nul
wmic process where "name='python.exe' and commandline like '%%worker_client worker-3%%'" call terminate > nul

ECHO Cleanup complete.