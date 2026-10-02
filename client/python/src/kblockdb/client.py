"""Public synchronous kBlockDB client."""

import socket
from typing import Iterable

from . import _wire
from .exceptions import (
    BadRequestError,
    ConflictError,
    ForbiddenError,
    InternalServerError,
    ProtocolError,
    UnauthorizedError,
)
from .models import (
    Column,
    Health,
    QueryResult,
    Stats,
    Value,
    ValueType,
    ValueWithMeta,
)


def _coord(value: Iterable[int]) -> tuple[int, ...]:
    return tuple(value)


class KBlockDBClient:
    """One authenticated, synchronous binary-protocol connection, selecting
    one database for its whole life.

    Instances are context managers and are not thread-safe. Open multiple
    clients when requests need to run concurrently.

    ``connect`` always names a database. If it doesn't exist yet, the
    connection still authenticates (:attr:`database_selected` is ``False``),
    but every data method (:meth:`get`, :meth:`set`, :meth:`query`, ...)
    raises :class:`~kblockdb.exceptions.ProtocolError` until the database is
    created (:meth:`create_database`, which needs no selected database) and
    selected (:meth:`use_database`, which re-sends ``Hello`` on this same
    connection with the credentials ``connect`` was given)::

        client = KBlockDBClient.connect("localhost", 8081, "admin", "pw", "newdb")
        if not client.database_selected:
            client.create_database("newdb")
            client.use_database("newdb")
        client.set((1, 2, 3), "material", Str("stone"))
    """

    def __init__(
        self,
        sock: socket.socket,
        hello: _wire.HelloOk,
        username: str,
        password: str,
        database: str,
    ) -> None:
        self._socket = sock
        self._broken = False
        self._username = username
        self._password = password
        self.database = database
        self._apply_hello(hello)

    @classmethod
    def connect(
        cls,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        *,
        timeout: float | None = None,
    ) -> "KBlockDBClient":
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            _wire.write_frame(sock, _wire.encode_hello(username, password, database))
            response = _wire.decode_response(_wire.read_frame(sock))
            if isinstance(response, _wire.HelloOk):
                return cls(sock, response, username, password, database)
            raise _to_exception(response)
        except BaseException:
            sock.close()
            raise

    def _apply_hello(self, hello: _wire.HelloOk) -> None:
        self.read_only = hello.read_only
        self.database_selected = hello.database is not None
        self.axes = hello.database.axes if hello.database else None
        self.world_dim = hello.database.world_dim if hello.database else None
        self.chunk_dim = hello.database.chunk_dim if hello.database else None

    def _require_database(self) -> None:
        if not self.database_selected:
            raise ProtocolError(
                f"no database selected -- '{self.database}' doesn't exist yet; call "
                "create_database() then use_database(), or connect() to an existing "
                "database"
            )

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
        """Re-authenticates as a different account on this same connection,
        keeping the currently selected database (see :meth:`use_database`
        for the other way around)."""
        response = self._roundtrip(_wire.encode_hello(username, password, self.database))
        if isinstance(response, _wire.HelloOk):
            self._username = username
            self._password = password
            self._apply_hello(response)
            return
        raise _to_exception(response)

    def use_database(self, name: str) -> None:
        """Selects a different database on this same connection, re-using
        the credentials ``connect``/:meth:`reauthenticate` last gave this
        client. The usual way to pick up a database just created with
        :meth:`create_database` -- see this class's docstring."""
        response = self._roundtrip(
            _wire.encode_hello(self._username, self._password, name)
        )
        if isinstance(response, _wire.HelloOk):
            self.database = name
            self._apply_hello(response)
            return
        raise _to_exception(response)

    def get(self, coord: Iterable[int], key: str) -> Value | None:
        result = self.get_with_meta(coord, key)
        return None if result is None else result.value

    def get_with_meta(
        self, coord: Iterable[int], key: str
    ) -> ValueWithMeta | None:
        self._require_database()
        response = self._roundtrip(_wire.encode_get(_coord(coord), key))
        if isinstance(response, _wire.ValueResponse):
            return ValueWithMeta(response.value, response.meta)
        if isinstance(response, _wire.NotFound):
            return None
        raise _to_exception(response)

    def set(self, coord: Iterable[int], key: str, value: Value) -> None:
        self._require_database()
        self._expect_ok(self._roundtrip(_wire.encode_set(_coord(coord), key, value)))

    def remove(self, coord: Iterable[int], key: str) -> None:
        self._require_database()
        self._expect_ok(self._roundtrip(_wire.encode_remove(_coord(coord), key)))

    def health(self) -> Health:
        """Server-wide liveness/identity -- needs no selected database (or
        even a valid one in ``Hello``), see this class's docstring."""
        response = self._roundtrip(_wire.encode_health())
        if isinstance(response, _wire.HealthResponse):
            return response.health
        raise _to_exception(response)

    def stats(self) -> Stats:
        self._require_database()
        response = self._roundtrip(_wire.encode_stats())
        if isinstance(response, _wire.StatsResponse):
            return response.stats
        raise _to_exception(response)

    def get_region(
        self, origin: Iterable[int], extent: Iterable[int], key: str
    ) -> tuple[Value | None, ...]:
        self._require_database()
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
        self._require_database()
        response = self._roundtrip(
            _wire.encode_set_region(
                _coord(origin), _coord(extent), key, tuple(values)
            )
        )
        self._expect_ok(response)

    def remove_region(
        self, origin: Iterable[int], extent: Iterable[int], key: str
    ) -> None:
        self._require_database()
        response = self._roundtrip(
            _wire.encode_remove_region(_coord(origin), _coord(extent), key)
        )
        self._expect_ok(response)

    def query(self, query: str) -> QueryResult:
        self._require_database()
        response = self._roundtrip(_wire.encode_query(query))
        if isinstance(response, _wire.QueryResponse):
            return response.result
        raise _to_exception(response)

    def list_databases(self) -> tuple[str, ...]:
        """Every database the server manages, sorted by name. Needs no
        selected database."""
        response = self._roundtrip(_wire.encode_list_databases())
        if isinstance(response, _wire.DatabasesResponse):
            return response.databases
        raise _to_exception(response)

    def create_database(
        self,
        name: str,
        *,
        axes: int | None = None,
        world_dim: int | None = None,
        chunk_size: int | None = None,
    ) -> None:
        """Creates database ``name`` using the server's configured default
        shape. Needs no selected database -- this is how a connection
        bootstraps a brand-new one (see this class's docstring).

        The binary protocol's ``CreateDatabase`` request has no way to
        request a custom shape -- only the REST API's ``PUT
        /rest/databases/{name}`` can. ``axes``/``world_dim``/``chunk_size``
        exist only so passing one raises a clear :class:`ValueError` instead
        of silently being ignored.
        """
        if axes is not None or world_dim is not None or chunk_size is not None:
            raise ValueError(
                "the binary protocol's CreateDatabase has no way to override a "
                "database's shape -- use the REST API's PUT /rest/databases/{name} "
                "instead, or omit axes/world_dim/chunk_size to accept the server's "
                "configured default shape"
            )
        self._expect_ok(self._roundtrip(_wire.encode_create_database(name)))

    def remove_database(self, name: str) -> bool:
        """Deletes database ``name`` -- its directory and every byte of data
        in it. Returns ``False`` if there was no such database. Needs no
        selected database. Irreversible."""
        response = self._roundtrip(_wire.encode_remove_database(name))
        if isinstance(response, _wire.Ok):
            return True
        if isinstance(response, _wire.NotFound):
            return False
        raise _to_exception(response)

    def columns(self) -> tuple[Column, ...]:
        """Every column in this database's schema, sorted by key.

        Includes columns created implicitly by :meth:`set` as well as
        those declared with :meth:`add_column`.
        """
        self._require_database()
        response = self._roundtrip(_wire.encode_list_columns())
        if isinstance(response, _wire.ColumnsResponse):
            return response.columns
        raise _to_exception(response)

    def add_column(self, key: str, value_type: ValueType) -> None:
        """Create a column for ``key``, fixing it to ``value_type``.

        Only needed to declare a column's type up front -- writing a value
        creates its column implicitly otherwise.

        Raises :class:`ConflictError` if ``key`` already has a column.
        """
        self._require_database()
        self._expect_ok(self._roundtrip(_wire.encode_add_column(key, value_type)))

    def remove_column(self, key: str) -> bool:
        """Drop ``key``'s column and every value ever written for it.

        Returns ``False`` if there was no such column. Not reversible:
        re-creating the column later starts it empty, and it may be given
        a different :class:`ValueType` than it had.
        """
        self._require_database()
        response = self._roundtrip(_wire.encode_remove_column(key))
        if isinstance(response, _wire.Ok):
            return True
        if isinstance(response, _wire.NotFound):
            return False
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
            0x0D: ConflictError,
        }
        return error_types[response.status](response.message)
    return ProtocolError(f"unexpected {type(response).__name__} response")
