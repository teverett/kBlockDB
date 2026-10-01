"""Public synchronous kBlockDB client."""

import socket
from typing import Iterable

from . import _wire
from .exceptions import (
    BadRequestError,
    ForbiddenError,
    InternalServerError,
    ProtocolError,
    UnauthorizedError,
)
from .models import (
    Health,
    QueryResult,
    Stats,
    Value,
    ValueWithMeta,
)


def _coord(value: Iterable[int]) -> tuple[int, ...]:
    return tuple(value)


class KBlockDBClient:
    """One authenticated, synchronous binary-protocol connection.

    Instances are context managers and are not thread-safe. Open multiple
    clients when requests need to run concurrently.
    """

    def __init__(self, sock: socket.socket, hello: _wire.HelloOk) -> None:
        self._socket = sock
        self._broken = False
        self._apply_hello(hello)

    @classmethod
    def connect(
        cls,
        host: str,
        port: int,
        username: str,
        password: str,
        *,
        timeout: float | None = None,
    ) -> "KBlockDBClient":
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            _wire.write_frame(sock, _wire.encode_hello(username, password))
            response = _wire.decode_response(_wire.read_frame(sock))
            if isinstance(response, _wire.HelloOk):
                return cls(sock, response)
            raise _to_exception(response)
        except BaseException:
            sock.close()
            raise

    def _apply_hello(self, hello: _wire.HelloOk) -> None:
        self.axes = hello.axes
        self.world_dim = hello.world_dim
        self.read_only = hello.read_only

    def _roundtrip(self, payload: bytes) -> _wire.Response:
        if self._broken:
            raise ProtocolError(
                "this connection is no longer usable after a framing failure"
            )
        # A failure between writing a request and reading its whole response
        # leaves the stream at an unknown offset, so the socket can never be
        # trusted again. Decoding happens after the frame is fully consumed and
        # therefore keeps the connection usable.
        try:
            _wire.write_frame(self._socket, payload)
            frame = _wire.read_frame(self._socket)
        except BaseException:
            self._broken = True
            self._socket.close()
            raise
        return _wire.decode_response(frame)

    def reauthenticate(self, username: str, password: str) -> None:
        response = self._roundtrip(_wire.encode_hello(username, password))
        if isinstance(response, _wire.HelloOk):
            self._apply_hello(response)
            return
        raise _to_exception(response)

    def get(self, coord: Iterable[int], key: str) -> Value | None:
        result = self.get_with_meta(coord, key)
        return None if result is None else result.value

    def get_with_meta(
        self, coord: Iterable[int], key: str
    ) -> ValueWithMeta | None:
        response = self._roundtrip(_wire.encode_get(_coord(coord), key))
        if isinstance(response, _wire.ValueResponse):
            return ValueWithMeta(response.value, response.meta)
        if isinstance(response, _wire.NotFound):
            return None
        raise _to_exception(response)

    def set(self, coord: Iterable[int], key: str, value: Value) -> None:
        self._expect_ok(self._roundtrip(_wire.encode_set(_coord(coord), key, value)))

    def remove(self, coord: Iterable[int], key: str) -> None:
        self._expect_ok(self._roundtrip(_wire.encode_remove(_coord(coord), key)))

    def health(self) -> Health:
        response = self._roundtrip(_wire.encode_health())
        if isinstance(response, _wire.HealthResponse):
            return response.health
        raise _to_exception(response)

    def stats(self) -> Stats:
        response = self._roundtrip(_wire.encode_stats())
        if isinstance(response, _wire.StatsResponse):
            return response.stats
        raise _to_exception(response)

    def get_region(
        self, origin: Iterable[int], extent: Iterable[int], key: str
    ) -> tuple[Value | None, ...]:
        response = self._roundtrip(
            _wire.encode_get_region(_coord(origin), _coord(extent), key)
        )
        if isinstance(response, _wire.RegionValues):
            return response.values
        raise _to_exception(response)

    def set_region(
        self,
        origin: Iterable[int],
        extent: Iterable[int],
        key: str,
        values: Iterable[Value],
    ) -> None:
        response = self._roundtrip(
            _wire.encode_set_region(
                _coord(origin), _coord(extent), key, tuple(values)
            )
        )
        self._expect_ok(response)

    def remove_region(
        self, origin: Iterable[int], extent: Iterable[int], key: str
    ) -> None:
        response = self._roundtrip(
            _wire.encode_remove_region(_coord(origin), _coord(extent), key)
        )
        self._expect_ok(response)

    def query(self, query: str) -> QueryResult:
        response = self._roundtrip(_wire.encode_query(query))
        if isinstance(response, _wire.QueryResponse):
            return response.result
        raise _to_exception(response)

    @staticmethod
    def _expect_ok(response: _wire.Response) -> None:
        if not isinstance(response, _wire.Ok):
            raise _to_exception(response)

    def close(self) -> None:
        self._broken = True
        self._socket.close()

    def __enter__(self) -> "KBlockDBClient":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


def _to_exception(response: _wire.Response) -> OSError:
    if isinstance(response, _wire.ErrorResponse):
        error_types = {
            0x04: BadRequestError,
            0x05: UnauthorizedError,
            0x06: ForbiddenError,
            0x07: InternalServerError,
        }
        return error_types[response.status](response.message)
    return ProtocolError(f"unexpected {type(response).__name__} response")
