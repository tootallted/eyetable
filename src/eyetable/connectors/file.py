"""Files dropped into eyetable. CSV for now.

A file source holds one dataset (`data`). Each upload of a new version of the
file either replaces the table's contents or appends to them; the engine
keeps every uploaded file in core storage (`eyetable.uploads`).

Config: {"format": "csv", "delimiter": null}  (null delimiter = detect , ; tab | from the header)

CSV cells are text, so each column is typed by looking at all of its values:
a column becomes bigint, double, boolean, date or timestamp only when every
non-empty value parses as one. Values with leading zeros ("007") stay text so
codes and zip codes are not mangled. Empty cells become NULL.
"""

import csv
import datetime as dt
import io
import re
from collections.abc import Iterator

from eyetable.connectors.base import Connector, Record
from eyetable.models import ColumnInfo, DatasetInfo
from eyetable.types import infer_columns

DATASET_KEY = "data"
FORMATS = ("csv",)
DELIMITERS = (",", ";", "\t", "|")

_INT = re.compile(r"[+-]?(0|[1-9]\d*)")
_FLOAT = re.compile(r"[+-]?((0|[1-9]\d*)(\.\d*)?|\.\d+)([eE][+-]?\d+)?")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_DATETIME = re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?")
_BOOLS = {"true": True, "false": False}


def decode(content: bytes) -> str:
    """UTF-8 (with or without Excel's BOM), falling back to Windows-1252."""
    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return content.decode("cp1252", errors="replace")


def unique_headers(raw: list[str], width: int) -> list[str]:
    headers: list[str] = []
    seen: set[str] = set()
    for i in range(width):
        name = (raw[i].strip() if i < len(raw) else "") or f"column_{i + 1}"
        base, n = name, 2
        while name.lower() in seen:
            name, n = f"{base}_{n}", n + 1
        seen.add(name.lower())
        headers.append(name)
    return headers


def detect_delimiter(text: str) -> str:
    """The candidate that splits the header line the most (ties go to the comma).

    The header is the most reliable line: data rows can hold "1,5"-style
    decimals that fool sniffing across the whole sample.
    """
    header = next((line for line in text.splitlines() if line.strip()), "")
    return max(DELIMITERS, key=header.count) if any(d in header for d in DELIMITERS) else ","


def _parse_datetime(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


# (name, does every value match?, convert one value)
_CONVERSIONS = (
    ("bigint", lambda v: _INT.fullmatch(v), int),
    ("double", lambda v: _FLOAT.fullmatch(v), float),
    ("boolean", lambda v: v.lower() in _BOOLS, lambda v: _BOOLS[v.lower()]),
    ("date", lambda v: _DATE.fullmatch(v), dt.date.fromisoformat),
    ("timestamp", lambda v: _DATETIME.fullmatch(v), _parse_datetime),
)


def _column_converter(values: list[str]):
    present = [v for v in values if v != ""]
    if not present:
        return None
    for _, matches, convert in _CONVERSIONS:
        if all(matches(v) for v in present):
            try:
                for v in present:
                    convert(v)
            except ValueError:  # e.g. 2026-02-30 matches the pattern but is no date
                continue
            return convert
    return None


def parse_csv(content: bytes, delimiter: str | None = None) -> tuple[list[str], list[Record]]:
    """Header names and typed records from CSV bytes."""
    text = decode(content)
    if delimiter is None:
        delimiter = detect_delimiter(text)
    rows = [r for r in csv.reader(io.StringIO(text, newline=""), delimiter=delimiter) if any(c.strip() for c in r)]
    if not rows:
        raise ValueError("the file is empty")
    raw_header, body = rows[0], rows[1:]
    width = max(len(r) for r in rows)
    headers = unique_headers(raw_header, width)

    columns = [[r[i] if i < len(r) else "" for r in body] for i in range(width)]
    converters = [_column_converter(col) for col in columns]
    records = []
    for r in range(len(body)):
        record: Record = {}
        for i, name in enumerate(headers):
            value = columns[i][r]
            if value == "":
                record[name] = None
            else:
                convert = converters[i]
                record[name] = convert(value) if convert else value
        records.append(record)
    return headers, records


class FileConnector(Connector):
    type = "file"

    def __init__(self, config: dict, content: bytes | None = None, filename: str | None = None):
        super().__init__(config)
        if config.get("format", "csv") not in FORMATS:
            raise ValueError(f"unsupported file format {config.get('format')!r}; supported: {', '.join(FORMATS)}")
        self.content = content
        self.filename = filename
        self._parsed: tuple[list[str], list[Record]] | None = None

    def _records(self) -> tuple[list[str], list[Record]]:
        if self.content is None:
            raise ValueError("file sources are loaded by uploading a file (web page, API, or `eyetable upload`)")
        if self._parsed is None:
            self._parsed = parse_csv(self.content, self.config.get("delimiter"))
        return self._parsed

    def test(self) -> None:
        if self.content is not None:
            self._records()

    def discover(self) -> list[DatasetInfo]:
        headers, records = self._records()
        kinds = infer_columns(records)
        return [
            DatasetInfo(
                key=DATASET_KEY,
                name=DATASET_KEY,
                kind="file",
                columns=[ColumnInfo(h, kinds.get(h) or "text") for h in headers],
                row_count=len(records),
                detail={"filename": self.filename},
            )
        ]

    def read(self, key: str, batch_size: int = 1000) -> Iterator[list[Record]]:
        if key != DATASET_KEY:
            raise KeyError(f"file sources have one dataset, {DATASET_KEY!r}")
        _, records = self._records()
        for start in range(0, len(records), batch_size):
            yield records[start : start + batch_size]

    def origin(self, key: str) -> str:
        return self.filename or key
