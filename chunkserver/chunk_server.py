from concurrent import futures
import logging
import time
import grpc
import os
import threading

import gfs_pb2
import gfs_pb2_grpc

class ChunkServer(gfs_pb2_grpc.ChunkServiceServicer):
    def ReadChunk(self, request, context):
        raise NotImplementedError("Data I/O paths planned for Day 3")

    def WriteChunk(self, request, context):
        raise NotImplementedError("Data I/O paths planned for Day 3")

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