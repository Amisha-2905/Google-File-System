from concurrent import futures
import logging
import time
import grpc
import threading

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
        self._lock = threading.Lock()                 

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
        raise NotImplementedError("File creation logic planned for Day 3")

    def GetChunkLocations(self, request, context):
        raise NotImplementedError("Chunk location resolution planned for Day 3")

    def DeleteFile(self, request, context):
        raise NotImplementedError("Lazy garbage collection logic planned for Day 8")
    
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