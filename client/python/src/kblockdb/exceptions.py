"""Exceptions reported by the kBlockDB server or binary protocol."""


class KBlockDBError(OSError):
    """Base class for well-formed errors returned by kBlockDB."""


class BadRequestError(KBlockDBError):
    """The request contained an invalid coordinate, value, region, or query."""


class UnauthorizedError(KBlockDBError):
    """Authentication failed."""


class ForbiddenError(KBlockDBError):
    """The authenticated account cannot perform the requested write."""


class InternalServerError(KBlockDBError):
    """The server encountered a storage error."""


class ProtocolError(KBlockDBError):
    """The peer sent malformed or incompatible binary protocol data."""
