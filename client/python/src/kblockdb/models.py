"""Immutable values and response models exposed by the client."""

from dataclasses import dataclass
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
