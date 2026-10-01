"""Immutable values and response models exposed by the client."""

from dataclasses import dataclass
import enum
from typing import TypeAlias


@dataclass(frozen=True, slots=True)
class Str:
    value: str


@dataclass(frozen=True, slots=True)
class F64:
    value: float


@dataclass(frozen=True, slots=True)
class I64:
    value: int


@dataclass(frozen=True, slots=True)
class Bool:
    value: bool


Value: TypeAlias = Str | F64 | I64 | Bool


class ValueType(enum.Enum):
    """The type a schema column is fixed to.

    The same four kinds :data:`Value` has, but named on their own, without
    a value attached. Each member's value is the one-byte tag a value of
    that type carries on the wire, so a column declaration and an encoded
    value always agree about what a type is.
    """

    STR = 0
    F64 = 1
    I64 = 2
    BOOL = 3

    @property
    def wire_name(self) -> str:
        """The short name the server uses for this type (``"str"``, ...)."""
        return _WIRE_NAMES[self]

    @staticmethod
    def of(value: Value) -> "ValueType":
        """The type of ``value`` -- the column it would need to be stored."""
        return _VALUE_TYPES[type(value)]


_WIRE_NAMES: dict[ValueType, str] = {
    ValueType.STR: "str",
    ValueType.F64: "f64",
    ValueType.I64: "i64",
    ValueType.BOOL: "bool",
}

_VALUE_TYPES: dict[type, ValueType] = {
    Str: ValueType.STR,
    F64: ValueType.F64,
    I64: ValueType.I64,
    Bool: ValueType.BOOL,
}


@dataclass(frozen=True, slots=True)
class Column:
    """One column in a world's schema."""

    key: str
    value_type: ValueType


@dataclass(frozen=True, slots=True)
class CellMeta:
    created_at_ms: int
    modified_at_ms: int
    version: int


@dataclass(frozen=True, slots=True)
class ValueWithMeta:
    value: Value
    meta: CellMeta


@dataclass(frozen=True, slots=True)
class Health:
    axes: int
    world_dim: int
    timestamp: int


@dataclass(frozen=True, slots=True)
class Stats:
    total_chunks: int
    total_bytes: int
    total_blocks: int


@dataclass(frozen=True, slots=True)
class QueryValue:
    key: str
    value: Value
    meta: CellMeta


@dataclass(frozen=True, slots=True)
class QueryRow:
    coord: tuple[int, ...]
    values: tuple[QueryValue, ...]


@dataclass(frozen=True, slots=True)
class Rows:
    rows: tuple[QueryRow, ...]

    @property
    def total_rows(self) -> int:
        return len(self.rows)


@dataclass(frozen=True, slots=True)
class Affected:
    affected_cells: int


QueryResult: TypeAlias = Rows | Affected
