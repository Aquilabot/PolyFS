# PolyFS - Simplified Distributed File System

PolyFS is a distributed file system that allows storing and accessing files through a network of nodes, with support for data replication, fault tolerance, and load distribution.

## Features

- Network communication using gRPC
- Distributed consensus using the Raft algorithm
- Data serialization with Protocol Buffers
- Large-scale concurrency using goroutines and channels
- Data replication and partitioning
- Metadata and directory management

## Architecture

The system consists of:

1. **Storage Nodes**: Servers that store files and participate in consensus.
2. **Coordination Service**: Manages file location and node communication.
3. **Client**: Interface to interact with the file system.

## Usage

### Start a node

```bash
go run cmd/node/main.go -id [NODE_ID] -addr [ADDRESS:PORT] -join [LEADER_ADDRESS]
```

### Client

```bash
go run cmd/client/main.go -nodes [NODE_LIST]
```

## Supported Operations

- `put`: Store a file
- `get`: Retrieve a file
- `delete`: Delete a file
- `list`: List files
- `mkdir`: Create directory
- `rmdir`: Remove directory 

## Generate Protocol Buffers

```bash
make proto
```

## Build the system

```bash
make build
```

## Run a leader node

```bash
make run-leader
```

## Run a follower node

```bash
make run-follower
```

## Run the client

```bash
make run-client
``` 