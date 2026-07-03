from concurrent import futures
import logging
import time
import grpc

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

def serve():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # gfs_pb2_grpc.add_add_services_to_server_servicer_to_server(ChunkServer(), server)
    gfs_pb2_grpc.add_ChunkServiceServicer_to_server(ChunkServer(), server)
    server.add_insecure_port("[::]:50052")
    logging.info("Starting Chunkserver skeleton server on port 50052...")
    server.start()
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()