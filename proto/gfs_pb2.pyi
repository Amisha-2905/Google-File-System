from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class RegisterRequest(_message.Message):
    __slots__ = ("chunkserver_id", "address")
    CHUNKSERVER_ID_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    chunkserver_id: str
    address: str
    def __init__(self, chunkserver_id: _Optional[str] = ..., address: _Optional[str] = ...) -> None: ...

class RegisterReply(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: _Optional[bool] = ...) -> None: ...

class HeartbeatRequest(_message.Message):
    __slots__ = ("chunkserver_id", "chunk_ids", "free_space_bytes")
    CHUNKSERVER_ID_FIELD_NUMBER: _ClassVar[int]
    CHUNK_IDS_FIELD_NUMBER: _ClassVar[int]
    FREE_SPACE_BYTES_FIELD_NUMBER: _ClassVar[int]
    chunkserver_id: str
    chunk_ids: _containers.RepeatedScalarFieldContainer[str]
    free_space_bytes: int
    def __init__(self, chunkserver_id: _Optional[str] = ..., chunk_ids: _Optional[_Iterable[str]] = ..., free_space_bytes: _Optional[int] = ...) -> None: ...

class HeartbeatReply(_message.Message):
    __slots__ = ("command_acknowledged",)
    COMMAND_ACKNOWLEDGED_FIELD_NUMBER: _ClassVar[int]
    command_acknowledged: bool
    def __init__(self, command_acknowledged: _Optional[bool] = ...) -> None: ...

class CreateFileRequest(_message.Message):
    __slots__ = ("path",)
    PATH_FIELD_NUMBER: _ClassVar[int]
    path: str
    def __init__(self, path: _Optional[str] = ...) -> None: ...

class CreateFileReply(_message.Message):
    __slots__ = ("chunk_id", "chunkserver_addresses")
    CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    CHUNKSERVER_ADDRESSES_FIELD_NUMBER: _ClassVar[int]
    chunk_id: str
    chunkserver_addresses: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, chunk_id: _Optional[str] = ..., chunkserver_addresses: _Optional[_Iterable[str]] = ...) -> None: ...

class ChunkLocationsRequest(_message.Message):
    __slots__ = ("path", "chunk_index")
    PATH_FIELD_NUMBER: _ClassVar[int]
    CHUNK_INDEX_FIELD_NUMBER: _ClassVar[int]
    path: str
    chunk_index: int
    def __init__(self, path: _Optional[str] = ..., chunk_index: _Optional[int] = ...) -> None: ...

class ChunkLocationsReply(_message.Message):
    __slots__ = ("chunk_id", "chunkserver_addresses")
    CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    CHUNKSERVER_ADDRESSES_FIELD_NUMBER: _ClassVar[int]
    chunk_id: str
    chunkserver_addresses: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, chunk_id: _Optional[str] = ..., chunkserver_addresses: _Optional[_Iterable[str]] = ...) -> None: ...

class DeleteFileRequest(_message.Message):
    __slots__ = ("path",)
    PATH_FIELD_NUMBER: _ClassVar[int]
    path: str
    def __init__(self, path: _Optional[str] = ...) -> None: ...

class DeleteFileReply(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: _Optional[bool] = ...) -> None: ...

class ReadChunkRequest(_message.Message):
    __slots__ = ("chunk_id", "offset", "length")
    CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    LENGTH_FIELD_NUMBER: _ClassVar[int]
    chunk_id: str
    offset: int
    length: int
    def __init__(self, chunk_id: _Optional[str] = ..., offset: _Optional[int] = ..., length: _Optional[int] = ...) -> None: ...

class ReadChunkReply(_message.Message):
    __slots__ = ("data", "success")
    DATA_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    success: bool
    def __init__(self, data: _Optional[bytes] = ..., success: _Optional[bool] = ...) -> None: ...

class WriteChunkRequest(_message.Message):
    __slots__ = ("chunk_id", "offset", "data")
    CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    chunk_id: str
    offset: int
    data: bytes
    def __init__(self, chunk_id: _Optional[str] = ..., offset: _Optional[int] = ..., data: _Optional[bytes] = ...) -> None: ...

class WriteChunkReply(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: _Optional[bool] = ...) -> None: ...

class AppendRequest(_message.Message):
    __slots__ = ("chunk_id", "data")
    CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    chunk_id: str
    data: bytes
    def __init__(self, chunk_id: _Optional[str] = ..., data: _Optional[bytes] = ...) -> None: ...

class AppendReply(_message.Message):
    __slots__ = ("offset_written", "success")
    OFFSET_WRITTEN_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    offset_written: int
    success: bool
    def __init__(self, offset_written: _Optional[int] = ..., success: _Optional[bool] = ...) -> None: ...

class ReplicateRequest(_message.Message):
    __slots__ = ("chunk_id", "source_address")
    CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    chunk_id: str
    source_address: str
    def __init__(self, chunk_id: _Optional[str] = ..., source_address: _Optional[str] = ...) -> None: ...

class ReplicateReply(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: _Optional[bool] = ...) -> None: ...
