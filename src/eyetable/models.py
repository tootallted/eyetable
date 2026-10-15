"""What a connector reports about the data it can reach."""

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    type: str
    nullable: bool = True


@dataclass
class DatasetInfo:
    """One set of rows a source exposes: a table, a view, or an API request."""

    key: str  # unique within the source, e.g. "public.customers" or "Users/List users"
    name: str  # table-friendly base name used for the core storage table
    kind: str  # "table" | "view" | "api"
    columns: list[ColumnInfo] = field(default_factory=list)
    row_count: int | None = None
    readable: bool = True  # False for things ingest-all should skip (e.g. POST requests)
    detail: dict[str, Any] = field(default_factory=dict)

    @property
    def shape(self) -> str:
        rows = "?" if self.row_count is None else str(self.row_count)
        return f"{len(self.columns)} x {rows}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DatasetInfo":
        data = dict(data)
        data["columns"] = [ColumnInfo(**c) for c in data.get("columns", [])]
        return cls(**data)
