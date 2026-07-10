# "Eventually Consistent" — GFS MVP Replica

A functional, distributed, single-master Google File System (GFS) MVP built solo in Python using gRPC and containerized with Docker Compose. This system implements control/data path decoupling, automated node registration, background heartbeat monitoring, and high-availability liveness tracking.

---

## Architecture Overview

This replica implements a decoupled storage topology across an isolated virtual bridge network:

1. **Master Node (Control Path):** Manages the virtual memory namespace directory tree and monitors cluster health metrics. It stays completely out of raw file data pipelines.
2. **Chunkservers (Data Path):** Independent data storage engines handling localized operations. They report health and active block allocations to the Master.

---

## Features Implemented (Days 1–2)

* **gRPC Communication Fabric:** Unified protocol buffer communication layer spanning the client, coordinator, and block servers.
* **Decoupled Topology:** Thread-safe in-memory directory tracking isolated from the host machine's native file system.
* **Automated Node Registration:** Dynamic discovery allows chunkservers to automatically declare themselves to the Master upon initialization.
* **Heartbeat & Liveness Framework:** Background loops track node viability. The Master isolates any storage node that misses three consecutive heartbeat intervals (~9 seconds).

---

## Local Deployment & Chaos Testing

### 1. Launching the Cluster
To build the images and run the multi-node infrastructure in real-time logging mode:
```
docker compose up --build

```

To verify the operational state from an independent terminal session:

```
docker compose ps

```

### 2. Injecting Node Failures (Fault Tolerance Validation)

To simulate an instantaneous hardware or network crash on a storage target:

```
docker compose stop chunkserver2

```

*Observe the Master logs to watch the node transition through a `SUSPECTED` warning state into an explicit `DEAD` isolation classification.*

### 3. Node Recovery

To bring the isolated container back online and witness automatic cluster re-integration:

```
docker compose start chunkserver2

```
