"""Dependency-free Python client for kBlockDB's binary protocol."""

from .client import KBlockDBClient
from .exceptions import (
    BadRequestError,
    ForbiddenError,
    InternalServerError,
    KBlockDBError,
    ProtocolError,
    UnauthorizedError,
)
from .models import (
    Affected,
    Bool,
    CellMeta,
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
    ValueWithMeta,
)

__all__ = [
    "Affected",
    "BadRequestError",
    "Bool",
    "CellMeta",
    "F64",
    "ForbiddenError",
    "Health",
    "I64",
    "InternalServerError",
    "KBlockDBClient",
    "KBlockDBError",
    "ProtocolError",
    "QueryResult",
    "QueryRow",
    "QueryValue",
    "Rows",
    "Stats",
    "Str",
    "UnauthorizedError",
    "Value",
    "ValueWithMeta",
]
