```
DS/
├── src/
│   ├── __init__.py              
│   ├── common/
│   │   ├── grpc/
│   │   │   ├── auto_generated   # put the automatically generated code by compiler here
│   │   │   ├── protos
│   │   │   │   ├── messages     # put messages here
│   │   │   │   ├── services     # put services here
│   │   ├── __init__.py
│   │   └── utils.py             # common functions
│   ├── afs/                     # main system
│   │   ├── __init__.py
│   │   ├── client/
│   │   └── server/
│   └── app/                     # task 2 prime number app logic
├── config/                       # put setting files here (related to server blablabla)
├── tests/                        # put tests scripts (here)
├── scripts/                      # put startup scripts here
├── docker/
├── docs/                         # design and function docs here
│   └── design.md
├── requirements.txt              # Python dependencies
├── setup.py                      # pakaging setting (optional)
├── .env                          # environment variable setting (sensitive settings, pls put this file to gitignore and share only with each other) 
├── .gitignore                    # Git ignore
└── README.md                     # project notification
```
# AFS Distributed System

# Project Overview
AFS (Abstract File System) is a distributed file processing system that implements a coordinator-worker architecture for parallel task execution. The system consists of four main components: AFS Server, Coordinator, Workers, and AFS Client, which communicate via gRPC.

# File Structure

```
.
├── README.md
├── data/
│   ├── client_storage/
│   └── server_storage/
│       ├── input/  #### input prime files
│       │   └── input_dataset_001.txt
│       └── output/ #### output prime results
│           └── primes.txt
├── scripts/    #### Protobuf Compilation
│   ├── compile_proto.sh
│   └── compile_proto.bat
└── src/
    ├── afs/
    │   ├── client/
    │   │   └── afs_client.py  #### AFSClient: User interface for file operations
    │   ├── server/
    │   │   └── afs_server.py      #### AFSServer: File storage and transfer management
    │   ├── coordinator/
    │   │   └── coordinator.py #### Coordinator: Task distribution and result aggregation
    │   └── worker/
    │       └── worker.py      #### Worker: Task execution
    ├── common/
    │   ├── grpc/
    │   │   ├── protos/        #### Protobuf Documentation
    │   │   │   ├── messages/
    │   │   │   │   ├── file_operation_message.proto
    │   │   │   │   └── coordinator_message.proto
    │   │   │   └── services/
    │   │   │       ├── file_operation_service.proto
    │   │   │       └── coordinator_service.proto
    │   │   └── auto_generated/
    │   └── utils.py
    └── app/

```

## Quick Start
### Install Dependencies
```bash
pip install grpcio grpcio-tools pysyncobj
```
### Generate gRPC Files
```bash
# Linux/Mac
./scripts/compile_proto.sh

# Windows
scripts\compile_proto.bat
```
### Run
Example code for quick start: (must in separate terminal!)
```
# single server start:
SINGLE_MODE=true python -m src.afs_server.afs_server 0

# primary-backup server start:
python -m src.afs_server.afs_server 0
python -m src.afs_server.afs_server 1
python -m src.afs_server.afs_server 2

python -m src.afs_coordinator.coordinator

# single server - worker start:
SINGLE_MODE=true python -m src.worker.worker_client.worker_client worker-1
SINGLE_MODE=true python -m src.worker.worker_client.worker_client worker-2
SINGLE_MODE=true python -m src.worker.worker_client.worker_client worker-3

# primary-backup server - worker start:
python -m src.worker.worker_client.worker_client worker-1
python -m src.worker.worker_client.worker_client worker-2
python -m src.worker.worker_client.worker_client worker-3
```
## System Architecture
The AFS system follows a distributed computing pattern with the following components:
### AFS Server
### AFS Client
### Coordinator
### Worker

