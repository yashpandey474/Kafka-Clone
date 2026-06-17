# Kafka Clone

A simplified distributed event streaming platform inspired by Apache Kafka, built from scratch in Java to deeply understand the internals of modern messaging systems and distributed logs.

This project aims to progressively implement core Kafka concepts such as:
- Topics
- Partitions
- Producers
- Consumers
- Consumer Groups
- Offset Management
- Persistence
- Replication
- Fault Tolerance

The goal is not just to replicate Kafka APIs, but to understand *why* Kafka is designed the way it is.

---

## Why This Project?

Apache Kafka is one of the most widely used distributed systems in modern backend infrastructure.  
Instead of treating Kafka as a black box, this project explores the underlying engineering concepts by rebuilding core components from scratch.

Some important concepts explored in this project:
- Distributed logs
- Sequential disk writes
- Message ordering guarantees
- Partitioning strategies
- Consumer offset tracking
- Throughput vs consistency tradeoffs
- Broker architecture
- Fault tolerance
- Replication protocols

---

## Current Status

🚧 Project is currently in the early development phase.

Implemented / In Progress:
- [x] Topic abstraction
- [x] Partition abstraction
- [x] Topic creation
- [ ] Producer API
- [ ] Consumer API
- [ ] Message persistence
- [ ] Offset tracking
- [ ] Multi-threaded broker
- [ ] Consumer groups
- [ ] Replication
- [ ] Leader election
- [ ] Distributed brokers

---

## Planned Architecture

```text
Producer
   |
   v
Broker -----> Topic -----> Partition -----> Log File
   ^
   |
Consumer

## Contract for Message serialisation
Message should be stored in bytes rather than using ':' as a separator.
We need a contract for how many bytes each field will occupy.
Fields required: offset, timestamp, key length, key value, value length, value field value 
Initial contract: offset - 8 bytes, timestamp - 8 bytes, key length - 4 bytes, key value - variable, value length - 4 bytes, value field value - variable