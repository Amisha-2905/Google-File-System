import logging
import grpc
import gfs_pb2
import gfs_pb2_grpc

logging.basicConfig(level=logging.INFO, format="%(asctime)s - CLIENT - %(message)s")

class GFSClient:
    def __init__(self, master_address="localhost:50051"):
        self.master_channel = grpc.insecure_channel(master_address)
        self.master_stub = gfs_pb2_grpc.MasterServiceStub(self.master_channel)
        
        # Maps internal Docker container endpoints to host-accessible ports
        self._address_translation_map = {
            "chunkserver1:50052": "localhost:50052",
            "chunkserver2:50052": "localhost:50053",
            "chunkserver3:50052": "localhost:50054"
        }

    def _translate_address(self, internal_address: str) -> str:
        """Translates container hostnames to localhost ports for host-side testing."""
        return self._address_translation_map.get(internal_address, internal_address)

    def write(self, path: str, data: bytes, offset: int = 0):
        try:
            logging.info(f"Querying Master for chunk allocations: {path}")
            req = gfs_pb2.CreateFileRequest(path=path)
            res = self.master_stub.CreateFile(req)
            
            chunk_id = res.chunk_id
            
            # Day 5 Architecture Update: Master provides primary/secondary configuration profiles 
            # For testing with address translation, we intercept targets [cite: 514]
            primary = self._translate_address(res.chunkserver_addresses[0])
            secondaries = list(res.chunkserver_addresses[1:])
            logging.info(f"Streaming data directly to Primary replica [{primary}]...")
            
            cs_channel = grpc.insecure_channel(primary)
            cs_stub = gfs_pb2_grpc.ChunkServiceStub(cs_channel)
            
            # The client provides the secondaries list to let the primary coordinate replication 
            write_req = gfs_pb2.WriteChunkRequest(
                chunk_id=chunk_id,
                offset=offset,
                data=data,
                secondary_addresses=secondaries,
                version=1  # Version tracking placeholder
            )
            
            write_res = cs_stub.WriteChunk(write_req, timeout=5)
            return write_res.success
            
        except grpc.RpcError as e:
            logging.error(f"Write sequence rejected: {e.details()}")
            return False

    def read(self, path: str, offset: int = 0, length: int = 1024) -> bytes:
        try:
            req = gfs_pb2.ChunkLocationsRequest(path=path, chunk_index=0)
            res = self.master_stub.GetChunkLocations(req)
            
            chunk_id = res.chunk_id
            targets = res.chunkserver_addresses
            
            if not targets:
                logging.error("File chunk locations not found.")
                return b""
                
            target_address = self._translate_address(targets[0])
            cs_channel = grpc.insecure_channel(target_address)
            cs_stub = gfs_pb2_grpc.ChunkServiceStub(cs_channel)
            
            read_req = gfs_pb2.ReadChunkRequest(chunk_id=chunk_id, offset=offset, length=length)
            read_res = cs_stub.ReadChunk(read_req)
            
            if read_res.success:
                return read_res.data
            return b""
        except grpc.RpcError as e:
            logging.error(f"Read operation failed: {e.details()}")
            return b""