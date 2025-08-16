# Database Recovery and Sharding (Java)

A simulation project that demonstrates **sharding** (splitting a dataset across multiple database shards) and **recovery** (restoring consistency after a failure).  
This project shows how distributed database systems manage scaling and fault tolerance.

## Overview
- **Sharding**: data is distributed across multiple shards using a key-based strategy.
- **Recovery**: if a shard fails, logs/checkpoints are used to replay or restore data.
- **Client**: simple driver program to insert, query, and simulate failures.

This project highlights fundamental **software engineering topics in databases**: partitioning, replication, and recovery.

## Architecture and Key Decisions
- **ShardManager**: decides which shard a key belongs to.
- **Shard**: represents a database partition with its own storage.
- **RecoveryManager**: simulates logging and rollback/redo.
- **Main**: demonstrates inserting records, simulating a crash, and recovering.

## Features
- Insert and query data across multiple shards.
- Distribute data using a modular sharding strategy.
- Simulate shard failure.
- Replay logged operations to recover to a consistent state.
- Unit tests verify correct sharding and recovery.

## Getting Started

### Prerequisites
- Java 17
- Gradle wrapper (`./gradlew`)

### Build
```bash
./gradlew build