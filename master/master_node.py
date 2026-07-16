from concurrent import futures
import logging
import time
import grpc
import threading
import random
import uuid
import gfs_pb2
import gfs_pb2_grpc
from namespace_manager import NamespaceManager

class MasterNode(gfs_pb2_grpc.MasterServiceServicer):
    def __init__(self):
        self.namespace = NamespaceManager()
        self.chunk_locations: dict[str, list[str]] = {}
        self.active_chunkservers: dict[str, str] = {} 
        self.last_seen: dict[str, float] = {}         

        self.dead_chunkservers: set[str] = set()
        self._lock = threading.RLock()                 

        self.monitor_thread = threading.Thread(target=self._liveness_monitor_loop, daemon=True)
        self.monitor_thread.start()               # Protects cluster metadata structures

    def RegisterChunkserver(self, request, context):
        chunkserver_id = request.chunkserver_id
        address = request.address
        
        with self._lock:
            self.active_chunkservers[chunkserver_id] = address
            self.last_seen[chunkserver_id] = time.time()
            
        logging.info(f"[Cluster State] Successfully registered chunkserver '{chunkserver_id}' at location [{address}]")
        return gfs_pb2.RegisterReply(success=True)

    def Heartbeat(self, request, context):
        chunkserver_id = request.chunkserver_id
        chunk_ids = request.chunk_ids
        free_space = request.free_space_bytes

        with self._lock:
            # If a chunkserver crashed and came back without re-registering, catch it
            if chunkserver_id not in self.active_chunkservers:
                logging.warning(f"[Heartbeat] Unknown chunkserver '{chunkserver_id}' sent a heartbeat. Rejecting.")
                return gfs_pb2.HeartbeatReply(command_acknowledged=False)

            # Mark the current time as the last time we saw this node alive
            self.last_seen[chunkserver_id] = time.time()
            
            # Day 5/6 hint: Here we will eventually update our chunk-to-location mappings
            # based on the chunk_ids list reported by this server.

        logging.info(f"[Heartbeat] Node '{chunkserver_id}' is healthy. Free Space: {free_space / 1024 / 1024:.2f} MB. Managing {len(chunk_ids)} chunks.")
        return gfs_pb2.HeartbeatReply(command_acknowledged=True)

    def CreateFile(self, request, context):
        path = request.path

        # 1. Update the virtual tree namespace
        if not self.namespace.create_entry(path, is_dir=False):
            logging.warning(f"[Metadata] Failed to create file {path}. Already exists or parent missing.")
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            context.set_details("File already exists or invalid path.")
            return gfs_pb2.CreateFileReply()

        with self._lock:
            if not self.active_chunkservers:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details("No storage chunkservers registered.")
                return gfs_pb2.CreateFileReply()

            # 2. Allocate a unique chunk ID
            chunk_id = str(uuid.uuid4())

            # 3. Pick replica targets (Up to 3 based on current active nodes)
            available_nodes = list(self.active_chunkservers.keys())
            replica_count = min(3, len(available_nodes))
            selected_nodes = random.sample(available_nodes, replica_count)

            # Save configuration map
            self.chunk_locations[chunk_id] = selected_nodes

            # Map file path to its initial chunk index 0
            if not hasattr(self, 'file_to_chunks'):
                self.file_to_chunks: dict[str, list[str]] = {}
            self.file_to_chunks[path] = [chunk_id]

        addresses = [self.active_chunkservers[node] for node in selected_nodes]
        logging.info(f"[Metadata] File '{path}' allocated chunk {chunk_id} on replicas: {selected_nodes}")
        return gfs_pb2.CreateFileReply(chunk_id=chunk_id, chunkserver_addresses=addresses)

    def GetChunkLocations(self, request, context):
        path = request.path
        chunk_index = request.chunk_index

        with self._lock:
            if path not in getattr(self, 'file_to_chunks', {}) or chunk_index >= len(self.file_to_chunks[path]):
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return gfs_pb2.ChunkLocationsReply()

            chunk_id = self.file_to_chunks[path][chunk_index]
            nodes = self.chunk_locations.get(chunk_id, [])
            addresses = [self.active_chunkservers[node] for node in nodes]

        return gfs_pb2.ChunkLocationsReply(chunk_id=chunk_id, chunkserver_addresses=addresses)

    def DeleteFile(self, request, context):
        raise NotImplementedError("Lazy garbage collection logic planned for Day 8")
    
    def trigger_rereplication(self, dead_node_id: str):
        """Finds chunks affected by a node death and commands healthy servers to clone them."""
        logging.info(f"[Recovery Engine] Scanning for vulnerable chunks previously hosted on dead node: {dead_node_id}")

        with self._lock:
            for chunk_id, locations in list(self.chunk_locations.items()):
                if dead_node_id in locations:
                    # Remove the broken node identifier from the location list
                    locations.remove(dead_node_id)

                    if not locations:
                        logging.error(f"[Recovery Critical] Chunk {chunk_id} lost all replicas! Permanent data loss.")
                        continue

                    # Locate a healthy server that does not already hold this chunk
                    source_node = locations[0]
                    source_address = self.active_chunkservers.get(source_node)

                    available_replacements = [
                        node for node in self.active_chunkservers.keys() 
                        if node not in locations
                    ]

                    if not available_replacements:
                        logging.warning(f"[Recovery Stalled] No alternative storage targets available for chunk {chunk_id}")
                        continue

                    target_node = available_replacements[0]
                    target_address = self.active_chunkservers[target_node]

                    # Fire an independent background worker thread to execute the inter-container clone
                    threading.Thread(
                        target=self._send_replicate_command, 
                        args=(chunk_id, source_address, target_node, target_address),
                        daemon=True
                    ).start()

    def _send_replicate_command(self, chunk_id: str, source_address: str, target_node: str, target_address: str):
        """Instructs the replacement chunkserver to pull data from a healthy source replica."""
        try:
            # Establish gRPC connection directly to the destination replacement chunkserver
            channel = grpc.insecure_channel(target_address)
            stub = gfs_pb2_grpc.ChunkServiceStub(channel)

            logging.info(f"[Recovery Route] Issuing replicate command for chunk {chunk_id}: {source_address} -> {target_node}")
            req = gfs_pb2.ReplicateChunkRequest(chunk_id=chunk_id, source_address=source_address)
            res = stub.ReplicateChunk(req, timeout=5)

            if res.success:
                with self._lock:
                    # Register the new target into the authoritative chunk map location array
                    if chunk_id in self.chunk_locations:
                        self.chunk_locations[chunk_id].append(target_node)
                logging.info(f"[Recovery Success] Chunk {chunk_id} successfully restored to {target_node}.")
        except grpc.RpcError as e:
            logging.error(f"[Recovery Failed] Failed to replicate chunk {chunk_id} to {target_node}: {e}")
    
    def _liveness_monitor_loop(self):
        """Runs continuously in the background to check chunkserver health."""
        logging.info("Master liveness monitor thread started.")
        while True:
            time.sleep(2)  # Check status every 2 seconds
            now = time.time()

            with self._lock:
                # Check all known active chunkservers
                for cs_id in list(self.active_chunkservers.keys()):
                    elapsed = now - self.last_seen.get(cs_id, now)

                    # Context-defined interval thresholding
                    if elapsed > 9.0:  # ~3 missed heartbeats
                        if cs_id not in self.dead_chunkservers:
                            logging.error(f"[LIVENESS ALERT] Node '{cs_id}' has missed heartbeats for {elapsed:.1f}s! Marking DEAD.")
                            self.dead_chunkservers.add(cs_id)

                            # Remove from active routing lists
                            del self.active_chunkservers[cs_id]
                            self.trigger_rereplication(cs_id)  # Placeholder for future re-replication logic

                            # Day 4 hint: This is where trigger_rereplication() will hook in
                    elif elapsed > 6.0:
                        logging.warning(f"[LIVENESS WARNING] Node '{cs_id}' hasn't reported for {elapsed:.1f}s. Suspecting degradation.")

def serve():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # gfs_pb2_grpc.add_add_services_to_server_servicer_to_server(MasterNode(), server)
    gfs_pb2_grpc.add_MasterServiceServicer_to_server(MasterNode(), server)
    server.add_insecure_port("[::]:50051")
    logging.info("Starting Master Node skeleton server on port 50051...")
    server.start()
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()