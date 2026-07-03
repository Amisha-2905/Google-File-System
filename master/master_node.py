from concurrent import futures
import logging
import time
import grpc

import gfs_pb2
import gfs_pb2_grpc
from namespace_manager import NamespaceManager

class MasterNode(gfs_pb2_grpc.MasterServiceServicer):
    def __init__(self):
        self.namespace = NamespaceManager()
        # Maps chunk_id -> list of chunkserver_ids holding replicas
        self.chunk_locations: dict[str, list[str]] = {}
        # Maps chunkserver_id -> active networking address (e.g. "chunkserver1:50052")
        self.active_chunkservers: dict[str, str] = {}

    def RegisterChunkserver(self, request, context):
        logging.info(f"Received registration from chunkserver: {request.chunkserver_id} at {request.address}")
        self.active_chunkservers[request.chunkserver_id] = request.address
        return gfs_pb2.RegisterReply(success=True)

    def Heartbeat(self, request, context):
        return gfs_pb2.HeartbeatReply(command_acknowledged=True)

    def CreateFile(self, request, context):
        raise NotImplementedError("File creation logic planned for Day 3")

    def GetChunkLocations(self, request, context):
        raise NotImplementedError("Chunk location resolution planned for Day 3")

    def DeleteFile(self, request, context):
        raise NotImplementedError("Lazy garbage collection logic planned for Day 8")

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