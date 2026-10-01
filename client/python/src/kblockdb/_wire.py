"""Encoding and decoding for kBlockDB's binary protocol."""

from dataclasses import dataclass
import socket
import struct
from typing import TypeAlias

from .exceptions import ProtocolError
from .models import (
    Affected,
    Bool,
    CellMeta,
    Column,
    F64,
    Health,
    I64,
    QueryResult,
    QueryRow,
    QueryValue,
    Rows,
    Stats,
    Str,
    Value,
    ValueType,
)

MAX_FRAME_LEN = 64 * 1024 * 1024


def _pack_short_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    if len(encoded) > 0xFF:
        raise ValueError("Hello fields cannot exceed 255 UTF-8 bytes")
    return struct.pack("<B", len(encoded)) + encoded


def _pack_key(key: str) -> bytes:
    encoded = key.encode("utf-8")
    if len(encoded) > 0xFFFF:
        raise ValueError("keys cannot exceed 65535 UTF-8 bytes")
    return struct.pack("<H", len(encoded)) + encoded


def _pack_coord(coord: tuple[int, ...]) -> bytes:
    if len(coord) > 0xFF:
        raise ValueError("coordinates cannot exceed 255 axes")
    try:
        return struct.pack(f"<B{len(coord)}i", len(coord), *coord)
    except struct.error as error:
        raise ValueError("coordinate components must be signed 32-bit integers") from error


def _pack_value(value: Value) -> bytes:
    if isinstance(value, Str):
        encoded = value.value.encode("utf-8")
        if len(encoded) > 0xFFFFFFFF:
            raise ValueError("string value exceeds the protocol limit")
        return b"\x00" + struct.pack("<I", len(encoded)) + encoded
    if isinstance(value, F64):
        return b"\x01" + struct.pack("<d", value.value)
    if isinstance(value, I64):
        try:
            return b"\x02" + struct.pack("<q", value.value)
        except struct.error as error:
            raise ValueError("I64 values must be signed 64-bit integers") from error
    if isinstance(value, Bool):
        return b"\x03" + struct.pack("<B", value.value)
    raise TypeError(f"unsupported value type: {type(value).__name__}")


def encode_hello(username: str, password: str) -> bytes:
    return b"\x00" + _pack_short_string(username) + _pack_short_string(password)


def encode_get(coord: tuple[int, ...], key: str) -> bytes:
    return b"\x01" + _pack_coord(coord) + _pack_key(key)


def encode_set(coord: tuple[int, ...], key: str, value: Value) -> bytes:
    return b"\x02" + _pack_coord(coord) + _pack_key(key) + _pack_value(value)


def encode_remove(coord: tuple[int, ...], key: str) -> bytes:
    return b"\x03" + _pack_coord(coord) + _pack_key(key)


def encode_health() -> bytes:
    return b"\x04"


def encode_stats() -> bytes:
    return b"\x05"


def encode_get_region(origin: tuple[int, ...], extent: tuple[int, ...], key: str) -> bytes:
    return b"\x06" + _pack_coord(origin) + _pack_coord(extent) + _pack_key(key)


def encode_set_region(
    origin: tuple[int, ...],
    extent: tuple[int, ...],
    key: str,
    values: tuple[Value, ...],
) -> bytes:
    if len(values) > 0xFFFFFFFF:
        raise ValueError("region contains too many values")
    encoded_values = b"".join(_pack_value(value) for value in values)
    return (
        b"\x07"
        + _pack_coord(origin)
        + _pack_coord(extent)
        + _pack_key(key)
        + struct.pack("<I", len(values))
        + encoded_values
    )


def encode_remove_region(
    origin: tuple[int, ...], extent: tuple[int, ...], key: str
) -> bytes:
    return b"\x08" + _pack_coord(origin) + _pack_coord(extent) + _pack_key(key)


def encode_query(query: str) -> bytes:
    encoded = query.encode("utf-8")
    if len(encoded) > 0xFFFFFFFF:
        raise ValueError("query exceeds the protocol limit")
    return b"\x09" + struct.pack("<I", len(encoded)) + encoded


def encode_list_columns() -> bytes:
    return b"\x0a"


def encode_add_column(key: str, value_type: ValueType) -> bytes:
    return b"\x0b" + _pack_key(key) + struct.pack("<B", value_type.value)


def encode_remove_column(key: str) -> bytes:
    return b"\x0c" + _pack_key(key)


def _recv_exact(sock: socket.socket, length: int, context: str) -> bytes:
    chunks = bytearray()
    while len(chunks) < length:
        chunk = sock.recv(length - len(chunks))
        if not chunk:
            raise EOFError(
                f"connection closed mid-frame, while reading {context} "
                f"({len(chunks)} of {length} bytes)"
            )
        chunks.extend(chunk)
    return bytes(chunks)


def write_frame(sock: socket.socket, payload: bytes) -> None:
    if len(payload) > MAX_FRAME_LEN:
        raise ProtocolError(
            f"payload of {len(payload)} bytes exceeds the {MAX_FRAME_LEN}-byte frame limit"
        )
    sock.sendall(struct.pack("<I", len(payload)) + payload)


def read_frame(sock: socket.socket) -> bytes:
    first = sock.recv(1)
    if not first:
        raise EOFError("the server closed the connection")
    prefix = first + _recv_exact(sock, 3, "the length prefix")
    (length,) = struct.unpack("<I", prefix)
    if length > MAX_FRAME_LEN:
        raise ProtocolError(
            f"frame of {length} bytes exceeds the {MAX_FRAME_LEN}-byte limit"
        )
    return _recv_exact(sock, length, "the payload")


@dataclass(frozen=True, slots=True)
class HelloOk:
    axes: int
    world_dim: int
    read_only: bool


@dataclass(frozen=True, slots=True)
class Ok:
    pass


@dataclass(frozen=True, slots=True)
class ValueResponse:
    value: Value
    meta: CellMeta


@dataclass(frozen=True, slots=True)
class NotFound:
    pass


@dataclass(frozen=True, slots=True)
class ErrorResponse:
    status: int
    message: str


@dataclass(frozen=True, slots=True)
class HealthResponse:
    health: Health


@dataclass(frozen=True, slots=True)
class StatsResponse:
    stats: Stats


@dataclass(frozen=True, slots=True)
class RegionValues:
    values: tuple[Value | None, ...]


@dataclass(frozen=True, slots=True)
class QueryResponse:
    result: QueryResult


@dataclass(frozen=True, slots=True)
class ColumnsResponse:
    columns: tuple[Column, ...]


Response: TypeAlias = (
    HelloOk
    | Ok
    | ValueResponse
    | NotFound
    | ErrorResponse
    | HealthResponse
    | StatsResponse
    | RegionValues
    | QueryResponse
    | ColumnsResponse
)


class _Reader:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._position = 0

    def _take(self, length: int) -> bytes:
        end = self._position + length
        if length < 0 or end > len(self._payload):
            raise ProtocolError("truncated frame")
        value = self._payload[self._position : end]
        self._position = end
        return value

    def _unpack(self, fmt: str) -> int | float:
        return struct.unpack(fmt, self._take(struct.calcsize(fmt)))[0]

    def u8(self) -> int:
        return int(self._unpack("<B"))

    def u16(self) -> int:
        return int(self._unpack("<H"))

    def u32(self) -> int:
        return int(self._unpack("<I"))

    def i32(self) -> int:
        return int(self._unpack("<i"))

    def i64(self) -> int:
        return int(self._unpack("<q"))

    def u64(self) -> int:
        return int(self._unpack("<Q"))

    def f64(self) -> float:
        return float(self._unpack("<d"))

    def string(self, length: int) -> str:
        try:
            return self._take(length).decode("utf-8")
        except UnicodeDecodeError as error:
            raise ProtocolError("invalid UTF-8") from error

    def key(self) -> str:
        return self.string(self.u16())

    def coord(self) -> tuple[int, ...]:
        return tuple(self.i32() for _ in range(self.u8()))

    def value(self) -> Value:
        tag = self.u8()
        if tag == 0:
            return Str(self.string(self.u32()))
        if tag == 1:
            return F64(self.f64())
        if tag == 2:
            return I64(self.i64())
        if tag == 3:
            return Bool(self.u8() != 0)
        raise ProtocolError(f"unknown value type tag 0x{tag:x}")

    def meta(self) -> CellMeta:
        return CellMeta(self.u64(), self.u64(), self.u64())

    def region_values(self) -> tuple[Value | None, ...]:
        count = self.u32()
        return tuple(None if self.u8() == 0 else self.value() for _ in range(count))

    def query_result(self) -> QueryResult:
        kind = self.u8()
        if kind == 1:
            return Affected(self.u64())
        if kind != 0:
            raise ProtocolError(f"unknown query result kind 0x{kind:x}")
        rows = []
        for _ in range(self.u32()):
            coord = self.coord()
            values = tuple(
                QueryValue(self.key(), self.value(), self.meta())
                for _ in range(self.u32())
            )
            rows.append(QueryRow(coord, values))
        return Rows(tuple(rows))


def _read_columns(reader: "_Reader") -> tuple[Column, ...]:
    return tuple(
        Column(reader.key(), _read_value_type(reader)) for _ in range(reader.u32())
    )


def _read_value_type(reader: "_Reader") -> ValueType:
    tag = reader.u8()
    try:
        return ValueType(tag)
    except ValueError:
        raise ProtocolError(f"unknown value type tag 0x{tag:x}") from None


def _read_health(reader: "_Reader") -> Health:
    """``[u8 axes][u32 LE world_dim][u32 LE chunk_dim][u64 LE ts]<hostname>``.

    Read field by field rather than inline: ``Health``'s fields are in a
    different order than the wire puts them in, and Python evaluates
    constructor arguments left to right, so building it inline would read
    the frame out of order.
    """
    axes = reader.u8()
    world_dim = reader.u32()
    chunk_dim = reader.u32()
    timestamp = reader.u64()
    return Health(reader.key(), axes, world_dim, chunk_dim, timestamp)


def decode_response(payload: bytes) -> Response:
    reader = _Reader(payload)
    status = reader.u8()
    if status == 0x00:
        return HelloOk(reader.u8(), reader.u32(), reader.u8() != 0)
    if status == 0x01:
        return Ok()
    if status == 0x02:
        return ValueResponse(reader.value(), reader.meta())
    if status == 0x03:
        return NotFound()
    if 0x04 <= status <= 0x07 or status == 0x0D:
        return ErrorResponse(status, reader.string(reader.u16()))
    if status == 0x08:
        return HealthResponse(_read_health(reader))
    if status == 0x09:
        return StatsResponse(Stats(reader.u64(), reader.u64(), reader.u64()))
    if status == 0x0A:
        return RegionValues(reader.region_values())
    if status == 0x0B:
        return QueryResponse(reader.query_result())
    if status == 0x0C:
        return ColumnsResponse(_read_columns(reader))
    raise ProtocolError(f"unknown status 0x{status:x}")
