# GFS MVP Architecture Design

## Core Philosophy

The architecture separates **metadata operations** from **file data transfers**. The Master is responsible only for maintaining metadata, while clients communicate directly with Chunkservers for reading and writing file data. This prevents the Master from becoming a bottleneck and allows the system to handle many concurrent file operations efficiently.

## Component Responsibilities

### 1. Master Node

The Master maintains all filesystem metadata. It keeps track of the directory structure, maps files to their corresponding chunks, and knows which Chunkservers store each chunk. Whenever a client needs to access a file, it first contacts the Master to obtain the required chunk information.

The Master does **not** participate in transferring file contents. Its role is limited to managing metadata, assigning leases, and coordinating the system.

---

### 2. Chunkserver

Chunkservers are responsible for storing the actual file data. Each server stores fixed-size chunks (1 MB in the MVP) on its local filesystem, identified by unique chunk IDs.

A Chunkserver does not know anything about file names or directory structures. It simply performs operations such as reading, writing, appending, or replicating chunks when instructed by a client or the Master.

---

### 3. Client Library

The client library acts as the interface between applications and the distributed file system. It translates application requests (such as reading from a file at a specific offset) into chunk-level operations.

The client first contacts the Master to determine which Chunkserver stores the required chunk, caches this information for future requests, and then communicates directly with the appropriate Chunkserver to perform the actual read or write. This reduces unnecessary communication with the Master and improves overall performance.
