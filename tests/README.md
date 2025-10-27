├── tests/ 
    ├── fault_tolerence/ #fault tolerance scenario tests here
        ├── ft1_task_reassignment.py
    ├── README.md 


# Tests for AFS distributed system

This folder contains test cases for fault scenarios and solutions

## Quick start
### Install Dependencies
```bash
pip install grpcio grpcio-tools
```
## Run
python -m tests.fault_tolerance.ft1_task_reassignment

## Rerun
```powershell
netstat -ano | findstr :50051
```
```powershell
taskkill /PID LISTENING /F
```
Replace LSITENING with the number


# Scenario 1
Worker crashes mid work
Coordinator able to detect worker failure
Coordinator reassigns work marked "in progress"






