# AFS Distributed File Processing System

## 1. Introduction

The **AFS (Abstract File System)** project implements a fault-tolerant distributed computing framework utilizing a **Coordinator–Worker architecture** for parallel file processing. The primary communication mechanism across all components is **gRPC**, ensuring well-defined and efficient inter-process data exchange.

The system is designed to handle large-scale computational tasks, such as **prime number generation**, while maintaining data consistency and operational resilience through **state management** and **replication**.

---

## 2. Core Components

The system comprises four primary components that interact via remote procedure calls:

### **AFS Server**

* Acts as the **data storage layer**.
* Manages file storage, chunking, and data transfer requests from Workers and the AFS Client.
* Supports **Primary–Backup/Raft-based replication** for high availability.

### **Coordinator**

* Serves as the **central control unit**.
* Manages the global state, distributes tasks (file segments) to Workers, monitors progress, and aggregates results.

### **Worker**

* Functions as the **computational unit**.
* Retrieves file segments from the AFS Server, executes the processing algorithm, and reports task completion to the Coordinator.
* Implements **snapshotting** for localized fault recovery.

### **AFS Client**

* Provides the **user interface** to interact with the system.
* Enables file uploads, task initiation, and result retrieval.

---

## 3. Deployment and Execution

### 3.1 Prerequisites

The project is developed using **Python 3.10+**. Install dependencies before execution:

```bash
pip install -r requirements.txt
```

> **Key dependencies:** `grpcio`, `grpcio-tools`, `pysyncobj`

---

### 3.2 gRPC Code Generation

Compile the Protocol Buffer definitions before running:

```bash
# Linux/macOS
./scripts/compile_proto.sh

# Windows
scripts\compile_proto.bat
```

---

### 3.3 Execution Modes

Each core component must run in a separate terminal instance.

#### **Mode 3.3.1: Single Server Development Mode**

Run with a single AFS Server (no high availability):

```bash
# 1. Launch AFS Server (Instance 0)
SINGLE_MODE=true python -m src.afs_server.afs_server 0

# 2. Launch Coordinator
python -m src.afs_coordinator.coordinator

# 3. Launch Workers
SINGLE_MODE=true python -m src.worker.worker_client.worker_client worker-1
SINGLE_MODE=true python -m src.worker.worker_client.worker_client worker-2
SINGLE_MODE=true python -m src.worker.worker_client.worker_client worker-3
```

---

#### **Mode 3.3.2: High Availability (HA) Cluster Mode**

Run multiple AFS Server instances to form a **Primary–Backup/Raft cluster**:

```bash
# 1. Launch AFS Servers (e.g., 3 replicas)
python -m src.afs_server.afs_server 0
python -m src.afs_server.afs_server 1
python -m src.afs_server.afs_server 2

# 2. Launch Coordinator
python -m src.afs_coordinator.coordinator

# 3. Launch Workers
python -m src.worker.worker_client.worker_client worker-1
python -m src.worker.worker_client.worker_client worker-2
python -m src.worker.worker_client.worker_client worker-3
```

---

## 4. Directory Structure

| Directory               | Description                                                                                |
| ----------------------- | ------------------------------------------------------------------------------------------ |
| **src/afs_server**      | AFS File Server implementation and storage management logic                                |
| **src/afs_coordinator** | Coordinator’s task distribution, state management, and snapshot logic                      |
| **src/worker**          | Worker implementation: task execution, reporting, and service interfaces                   |
| **src/afs_client**      | Command-line client for system interaction                                                 |
| **src/common/grpc**     | Protobuf message/service definitions and generated Python files                            |
| **src/common/storage**  | Persistent storage backends (e.g., `raft_storage.py`, `simple_storage.py`)                 |
| **src/common**          | Shared utilities, configuration management, logging, and core algorithms (`prime_algo.py`) |
| **config**              | External configuration files (e.g., `config.json` for network setup)                       |
| **data**                | Persistent data directory (input files, snapshots, logs)                                   |
| **tests**               | Unit and integration test suite                                                            |
| **scripts**             | System utility scripts (e.g., Protobuf compilation)                                        |

---
