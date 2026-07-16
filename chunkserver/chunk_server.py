from concurrent import futures
import logging
import time
import grpc
import os
import threading

import gfs_pb2
import gfs_pb2_grpc

class ChunkServer(gfs_pb2_grpc.ChunkServiceServicer):
    def __init__(self):
        # Chunks will be stored inside the container volume path
        self.storage_dir = "/data"
        os.makedirs(self.storage_dir, exist_ok=True)
        logging.info(f"Storage directory initialized at: {self.storage_dir}")

    def _get_chunk_path(self, chunk_id: str) -> str:
        return os.path.join(self.storage_dir, f"chunk_{chunk_id}.bin")

    def ReadChunk(self, request, context):
        chunk_path = self._get_chunk_path(request.chunk_id)

        if not os.path.exists(chunk_path):
            logging.error(f"[IO Error] Chunk {request.chunk_id} not found.")
            return gfs_pb2.ReadChunkReply(success=False, data=b"")

        try:
            with open(chunk_path, "rb") as f:
                f.seek(request.offset)
                data = f.read(request.length)
            logging.info(f"[IO Success] Read {len(data)} bytes from chunk {request.chunk_id}")
            return gfs_pb2.ReadChunkReply(success=True, data=data)
        except Exception as e:
            logging.error(f"[IO Fail] Failed reading chunk {request.chunk_id}: {e}")
            return gfs_pb2.ReadChunkReply(success=False, data=b"")

    def WriteChunk(self, request, context):
        chunk_path = self._get_chunk_path(request.chunk_id)

        try:
            # Open in 'rb+' if it exists to overwrite offsets, or 'wb+' if new
            mode = "rb+" if os.path.exists(chunk_path) else "wb+"
            with open(chunk_path, mode) as f:
                if mode == "wb+":
                    # Pre-allocate zero bytes to maintain fixed size structure easily
                    f.write(b'\x00' * (1024 * 1024)) # 1MB pre-allocation 
                f.seek(request.offset)
                f.write(request.data)
            logging.info(f"[IO Success] Wrote {len(request.data)} bytes to chunk {request.chunk_id} at offset {request.offset}")
            return gfs_pb2.WriteChunkReply(success=True)
        except Exception as e:
            logging.error(f"[IO Fail] Failed writing chunk {request.chunk_id}: {e}")
            return gfs_pb2.WriteChunkReply(success=False)

    def AppendRecord(self, request, context):
        raise NotImplementedError("Atomic appends planned for Day 6")

    def ReplicateChunk(self, request, context):
        raise NotImplementedError("Inter-replica copying planned for Day 4")

def register_with_master(chunkserver_id, my_address):
    """Attempts to contact the GFS Master node to register this instance."""
    # Docker's internal DNS automatically resolves "master" to the correct container 
    master_channel = grpc.insecure_channel("master:50051")
    client_stub = gfs_pb2_grpc.MasterServiceStub(master_channel)
    
    # Retry loop in case the chunkserver initializes slightly before the Master is ready
    while True:
        try:
            logging.info(f"Attempting to register with GFS Master at master:50051...")
            request = gfs_pb2.RegisterRequest(chunkserver_id=chunkserver_id, address=my_address)
            reply = client_stub.RegisterChunkserver(request, timeout=2)
            if reply.success:
                logging.info("Successfully registered with GFS Master node!")
                break
        except grpc.RpcError as e:
            logging.warning(f"Master not reachable yet ({e.details() if hasattr(e, 'details') else e}). Retrying in 2 seconds...")
            time.sleep(2)

def start_heartbeat_loop(chunkserver_id):
    """Runs in a background thread, pinging the Master every 3 seconds."""
    master_channel = grpc.insecure_channel("master:50051")
    client_stub = gfs_pb2_grpc.MasterServiceStub(master_channel)

    logging.info(f"Starting background heartbeat thread for {chunkserver_id}...")

    while True:
        try:
            # Simulate free space and chunk reporting (Day 3 will implement actual files)
            simulated_free_space = 100 * 1024 * 1024  # 100 MB placeholder
            simulated_chunks = []                     # Empty list placeholder for Day 2

            request = gfs_pb2.HeartbeatRequest(
                chunkserver_id=chunkserver_id,
                chunk_ids=simulated_chunks,
                free_space_bytes=simulated_free_space
            )

            reply = client_stub.Heartbeat(request, timeout=2)

            if not reply.command_acknowledged:
                logging.warning("Master acknowledged heartbeat but requested re-registration!")

        except grpc.RpcError as e:
            logging.error(f"Heartbeat failed to reach Master: {e.details() if hasattr(e, 'details') else e}")

        # Wait 3 seconds before pinging again
        time.sleep(3)

def serve():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    # Identify who I am using Docker environment variables
    CHUNKSERVER_ID = os.environ.get("CHUNKSERVER_ID", "unknown_chunkserver")
    MY_ADDRESS = f"{CHUNKSERVER_ID}:50052"
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    gfs_pb2_grpc.add_ChunkServiceServicer_to_server(ChunkServer(), server)
    
    server.add_insecure_port("[::]:50052")
    logging.info(f"Starting Chunkserver [{CHUNKSERVER_ID}] on port 50052...")
    server.start()
    
    # Fire off the registration sequence over the network 
    register_with_master(CHUNKSERVER_ID, MY_ADDRESS)
    
    hb_thread = threading.Thread(target=start_heartbeat_loop, args=(CHUNKSERVER_ID,), daemon=True)
    hb_thread.start()
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()