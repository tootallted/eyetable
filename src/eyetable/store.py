"""Core storage: the Postgres database eyetable owns.

Two schemas live inside it:
  * meta schema (default `eyetable`): registered sources, the discovered
    dataset catalog, a log of every ingestion run, and every file uploaded.
  * data schema (default `eyetable_raw`): one table per ingested dataset,
    named `<source>__<dataset>`.

Every row written to a data table carries these columns, ahead of its data:
  * ET_id         bigint identity; 1 for a new table's first row, +1 per row
  * ET_timestamp  when the row was written
  * ET_source     where it came from: "<source> - <table or API endpoint>"
  * ET_run_id     the ingestion run (eyetable.runs.id) that wrote it

SQLite is accepted too (schemas are ignored) so the engine can run in tests
and quick local experiments without a Postgres server.
"""

import datetime as dt
import hashlib
import json
import secrets
import threading
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.schema import CreateSchema

from eyetable.identifiers import column_name
from eyetable.models import DatasetInfo
from eyetable.types import infer_columns, merge_kinds

JSONType = sa.JSON().with_variant(JSONB(), "postgresql")

ID_COLUMN = "ET_id"
TIMESTAMP_COLUMN = "ET_timestamp"
SOURCE_COLUMN = "ET_source"
RUN_ID_COLUMN = "ET_run_id"
# Data columns may not take these names (compared case-insensitively, as SQLite does).
# A history table's columns (see CoreStore._merge): which version of a row is current, and when
# each version was current from and to (datetime, no zone; current versions run to 9999-12-31).
ACTIVE_COLUMN = "ET_active"
FROM_COLUMN = "ET_fromdt"
TO_COLUMN = "ET_todt"
HISTORY_COLUMNS = (ACTIVE_COLUMN, FROM_COLUMN, TO_COLUMN)
FOREVER = dt.datetime(9999, 12, 31)
RESERVED_COLUMNS = {c.lower() for c in (ID_COLUMN, TIMESTAMP_COLUMN, SOURCE_COLUMN, RUN_ID_COLUMN, *HISTORY_COLUMNS)}
# Every loaded table: ET_id, then the source's own columns, then these.
META_COLUMNS = {TIMESTAMP_COLUMN: "timestamptz", SOURCE_COLUMN: "varchar", RUN_ID_COLUMN: "bigint"}


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


class UTCDateTime(sa.TypeDecorator):
    """timestamptz that always comes back timezone-aware.

    Postgres does this already; SQLite stores no offset, so values written as
    UTC are tagged UTC again on the way out.
    """

    impl = sa.DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if isinstance(value, dt.datetime) and value.tzinfo is not None:
            return value.astimezone(dt.timezone.utc)
        return value

    def process_result_value(self, value, dialect):
        if isinstance(value, dt.datetime) and value.tzinfo is None:
            return value.replace(tzinfo=dt.timezone.utc)
        return value


def sa_type(kind: str | None) -> sa.types.TypeEngine:
    return {
        "bigint": sa.BigInteger(),
        "double": sa.Double(),
        "numeric": sa.Numeric(),
        "boolean": sa.Boolean(),
        "timestamp": sa.DateTime(timezone=False),
        "timestamptz": UTCDateTime(),
        "date": sa.Date(),
        "time": sa.Time(),
        "json": JSONType,
        "binary": sa.LargeBinary(),
        "varchar": sa.String(),
    }.get(kind or "text", sa.Text())


def kind_of(type_: sa.types.TypeEngine) -> str:
    """Best-effort reverse of `sa_type`, for tables that already exist."""
    if isinstance(type_, sa.Boolean):
        return "boolean"
    if isinstance(type_, sa.Integer):
        return "bigint"
    if isinstance(type_, sa.Float):
        return "double"
    if isinstance(type_, sa.Numeric):
        return "numeric"
    if isinstance(type_, sa.DateTime):
        return "timestamptz" if type_.timezone else "timestamp"
    if isinstance(type_, sa.Date):
        return "date"
    if isinstance(type_, sa.Time):
        return "time"
    if isinstance(type_, sa.JSON):
        return "json"
    if isinstance(type_, sa.LargeBinary):
        return "binary"
    if isinstance(type_, sa.String) and not isinstance(type_, sa.Text):
        return "varchar"
    return "text"


def coerce(value: Any, kind: str) -> Any:
    if value is None or kind == "json":
        return value
    if kind in ("text", "varchar"):
        if isinstance(value, (dict, list)):
            return json.dumps(value, default=str)
        if isinstance(value, bool):
            return "true" if value else "false"
        return value if isinstance(value, str) else str(value)
    if kind == "double":
        return float(value)
    return value


class SourceNotFound(LookupError):
    pass


@dataclass
class Source:
    name: str
    type: str
    config: dict[str, Any]
    created_at: dt.datetime
    updated_at: dt.datetime


@dataclass
class Upload:
    id: int
    source: str
    filename: str
    size_bytes: int
    sha256: str
    mode: str
    run_id: int | None
    uploaded_at: dt.datetime


@dataclass
class Group:
    id: int
    name: str
    members: list[str]
    schedule: dict[str, Any] | None
    enabled: bool
    next_run_at: dt.datetime | None
    created_at: dt.datetime
    updated_at: dt.datetime


@dataclass
class GroupRun:
    id: int
    group_id: int
    trigger: str  # "manual" | "schedule" | "catch-up" | "tag"
    status: str  # "running" | "succeeded" | "partial" | "failed"
    started_at: dt.datetime
    finished_at: dt.datetime | None
    results: list[dict[str, Any]]


TAG_RUN_GROUP = 0  # group_runs.group_id of a run of a tag, which is no saved group


class GroupNotFound(LookupError):
    pass


@dataclass
class Run:
    id: int
    source: str
    dataset_key: str
    target_table: str
    mode: str
    status: str
    rows_written: int
    started_at: dt.datetime
    finished_at: dt.datetime | None
    error: str | None


class CoreStore:
    def __init__(self, url: str, meta_schema: str = "eyetable", data_schema: str = "eyetable_raw"):
        self.db = sa.create_engine(url)
        sqlite = self.db.dialect.name == "sqlite"
        self.meta_schema = None if sqlite else meta_schema
        self.data_schema = None if sqlite else data_schema
        self.schema_hooks: list = []  # called with each schema ensure_schema creates (QueryRunner shares it)
        self._merges = threading.local()  # the last history merge's counts, per thread (take_merge_stats)
        self._schemas_made: set[str] = set()

        self.metadata = sa.MetaData(schema=self.meta_schema)
        self.sources = sa.Table(
            "sources",
            self.metadata,
            sa.Column("name", sa.String(64), primary_key=True),
            sa.Column("type", sa.String(32), nullable=False),
            sa.Column("config", JSONType, nullable=False),
            sa.Column("created_at", UTCDateTime(), nullable=False),
            sa.Column("updated_at", UTCDateTime(), nullable=False),
        )
        self.datasets = sa.Table(
            "datasets",
            self.metadata,
            sa.Column("source", sa.String(64), primary_key=True),
            sa.Column("key", sa.Text, primary_key=True),
            sa.Column("info", JSONType, nullable=False),
            sa.Column("discovered_at", UTCDateTime(), nullable=False),
        )
        self.runs = sa.Table(
            "runs",
            self.metadata,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("source", sa.String(64), nullable=False),
            sa.Column("dataset_key", sa.Text, nullable=False),
            sa.Column("target_table", sa.Text, nullable=False),
            sa.Column("mode", sa.String(16), nullable=False),
            sa.Column("status", sa.String(16), nullable=False),
            sa.Column("rows_written", sa.BigInteger, nullable=False, default=0),
            sa.Column("started_at", UTCDateTime(), nullable=False),
            sa.Column("finished_at", UTCDateTime()),
            sa.Column("error", sa.Text),
        )
        self.groups = sa.Table(
            "groups",
            self.metadata,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("name", sa.String(80), nullable=False, unique=True),
            sa.Column("members", JSONType, nullable=False),
            sa.Column("schedule", JSONType),
            sa.Column("enabled", sa.Boolean, nullable=False, default=True),
            sa.Column("next_run_at", UTCDateTime()),
            sa.Column("created_at", UTCDateTime(), nullable=False),
            sa.Column("updated_at", UTCDateTime(), nullable=False),
        )
        self.group_runs = sa.Table(
            "group_runs",
            self.metadata,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("group_id", sa.Integer, nullable=False, index=True),
            sa.Column("trigger", sa.String(16), nullable=False),
            sa.Column("status", sa.String(16), nullable=False),
            sa.Column("started_at", UTCDateTime(), nullable=False),
            sa.Column("finished_at", UTCDateTime()),
            sa.Column("results", JSONType, nullable=False),
        )
        # The latest run of each tag run on its own (Run by tag), in group_runs.
        self.tag_runs = sa.Table(
            "tag_runs",
            self.metadata,
            sa.Column("tag", sa.String(40), primary_key=True),
            sa.Column("run_id", sa.Integer, nullable=False),
        )
        # One row at most: the Google account scheduled runs read sheets as. The
        # refresh token is stored encrypted (see google_auth.py).
        self.source_tags = sa.Table(
            "source_tags",
            self.metadata,
            sa.Column("source", sa.String(64), primary_key=True),
            sa.Column("tag", sa.String(40), primary_key=True),
        )
        self.google_connection = sa.Table(
            "google_connection",
            self.metadata,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("email", sa.Text, nullable=False),
            sa.Column("refresh_token", sa.Text, nullable=False),
            sa.Column("scopes", sa.Text, nullable=False),
            sa.Column("status", sa.String(16), nullable=False),
            sa.Column("error", sa.Text),
            sa.Column("connected_at", UTCDateTime(), nullable=False),
            sa.Column("last_used_at", UTCDateTime()),
        )
        # One row at most: the Microsoft Entra sign-ins database connections use
        # (MSAL's token cache, encrypted; see entra.py).
        self.entra_cache = sa.Table(
            "entra_cache",
            self.metadata,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("cache", sa.Text, nullable=False),
            sa.Column("updated_at", UTCDateTime(), nullable=False),
        )
        # Per loaded table: the latest key scan (see keys.py) and the natural key chosen from it.
        self.table_keys = sa.Table(
            "table_keys",
            self.metadata,
            sa.Column("table_name", sa.Text, primary_key=True),
            sa.Column("scan", JSONType),
            sa.Column("scanned_at", UTCDateTime()),
            sa.Column("key_columns", JSONType),
            sa.Column("key_set_at", UTCDateTime()),
        )
        # Per loaded table: the column incremental loads go by (see incremental.py).
        self.table_increments = sa.Table(
            "table_increments",
            self.metadata,
            sa.Column("table_name", sa.Text, primary_key=True),
            sa.Column("column_name", sa.Text, nullable=False),
            sa.Column("set_at", UTCDateTime(), nullable=False),
        )
        # Tables that keep history: loads merge into them by their natural key (see _merge) instead
        # of replacing them, keeping every version of each row.
        self.table_history = sa.Table(
            "table_history",
            self.metadata,
            sa.Column("table_name", sa.Text, primary_key=True),
            sa.Column("set_at", UTCDateTime(), nullable=False),
        )
        # Tables hidden in Sources ("commented out"): every load skips them (see Engine.reload_source).
        self.hidden_tables = sa.Table(
            "hidden_tables",
            self.metadata,
            sa.Column("table_name", sa.Text, primary_key=True),
            sa.Column("hidden_at", UTCDateTime(), nullable=False),
        )
        self.uploads = sa.Table(
            "uploads",
            self.metadata,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("source", sa.String(64), nullable=False, index=True),
            sa.Column("filename", sa.Text, nullable=False),
            sa.Column("size_bytes", sa.BigInteger, nullable=False),
            sa.Column("sha256", sa.String(64), nullable=False),
            sa.Column("mode", sa.String(16), nullable=False),
            sa.Column("run_id", sa.Integer),
            sa.Column("uploaded_at", UTCDateTime(), nullable=False),
            sa.Column("content", sa.LargeBinary, nullable=False),
        )

    # -- setup -------------------------------------------------------------

    def init(self) -> None:
        """Create the schemas and metadata tables (safe to run repeatedly)."""
        with self.db.begin() as conn:
            for schema in {self.meta_schema, self.data_schema} - {None}:
                conn.execute(CreateSchema(schema, if_not_exists=True))
        self.metadata.create_all(self.db)

    # -- sources -----------------------------------------------------------

    def save_source(self, name: str, type_: str, config: dict[str, Any]) -> None:
        now = _now()
        with self.db.begin() as conn:
            existing = conn.execute(
                sa.select(self.sources.c.created_at).where(self.sources.c.name == name)
            ).scalar()
            conn.execute(sa.delete(self.sources).where(self.sources.c.name == name))
            conn.execute(
                sa.insert(self.sources).values(
                    name=name, type=type_, config=config, created_at=existing or now, updated_at=now
                )
            )

    def get_source(self, name: str) -> Source:
        with self.db.connect() as conn:
            row = conn.execute(sa.select(self.sources).where(self.sources.c.name == name)).first()
        if row is None:
            raise SourceNotFound(f"no source named {name!r}")
        return Source(**row._mapping)

    def source_exists(self, name: str) -> bool:
        try:
            self.get_source(name)
        except SourceNotFound:
            return False
        return True

    def list_sources(self) -> list[Source]:
        with self.db.connect() as conn:
            rows = conn.execute(sa.select(self.sources).order_by(self.sources.c.name)).all()
        return [Source(**r._mapping) for r in rows]

    def remove_source(self, name: str) -> None:
        """Forget a source and its catalog. Tables already ingested are kept."""
        self.get_source(name)
        with self.db.begin() as conn:
            conn.execute(sa.delete(self.datasets).where(self.datasets.c.source == name))
            conn.execute(sa.delete(self.source_tags).where(self.source_tags.c.source == name))
            conn.execute(sa.delete(self.sources).where(self.sources.c.name == name))

    # -- tags --------------------------------------------------------------

    def set_source_tags(self, name: str, tags: list[str]) -> list[str]:
        """Replace a source's tags (already normalized: no '#', lowercase)."""
        self.get_source(name)
        tags = sorted(set(tags))
        with self.db.begin() as conn:
            conn.execute(sa.delete(self.source_tags).where(self.source_tags.c.source == name))
            if tags:
                conn.execute(sa.insert(self.source_tags), [{"source": name, "tag": t} for t in tags])
        return tags

    def tags_by_source(self) -> dict[str, list[str]]:
        with self.db.connect() as conn:
            rows = conn.execute(sa.select(self.source_tags).order_by(self.source_tags.c.tag)).all()
        out: dict[str, list[str]] = {}
        for r in rows:
            out.setdefault(r.source, []).append(r.tag)
        return out

    def sources_with_tag(self, tag: str) -> list[str]:
        with self.db.connect() as conn:
            rows = conn.execute(
                sa.select(self.source_tags.c.source).where(self.source_tags.c.tag == tag).order_by(self.source_tags.c.source)
            ).all()
        return [r.source for r in rows]

    def all_tags(self) -> list[tuple[str, int]]:
        query = (sa.select(self.source_tags.c.tag, sa.func.count())
                 .group_by(self.source_tags.c.tag).order_by(self.source_tags.c.tag))
        with self.db.connect() as conn:
            return [(r[0], r[1]) for r in conn.execute(query).all()]

    # -- catalog -----------------------------------------------------------

    def save_catalog(self, source: str, datasets: list[DatasetInfo]) -> None:
        now = _now()
        with self.db.begin() as conn:
            conn.execute(sa.delete(self.datasets).where(self.datasets.c.source == source))
            if datasets:
                conn.execute(
                    sa.insert(self.datasets),
                    [
                        {"source": source, "key": d.key, "info": d.to_dict(), "discovered_at": now}
                        for d in datasets
                    ],
                )

    def list_datasets(self, source: str | None = None) -> list[tuple[str, DatasetInfo]]:
        query = sa.select(self.datasets.c.source, self.datasets.c.info).order_by(
            self.datasets.c.source, self.datasets.c.key
        )
        if source is not None:
            query = query.where(self.datasets.c.source == source)
        with self.db.connect() as conn:
            rows = conn.execute(query).all()
        return [(r.source, DatasetInfo.from_dict(r.info)) for r in rows]

    # -- runs --------------------------------------------------------------

    def start_run(self, source: str, dataset_key: str, target_table: str, mode: str) -> int:
        with self.db.begin() as conn:
            result = conn.execute(
                sa.insert(self.runs).values(
                    source=source,
                    dataset_key=dataset_key,
                    target_table=target_table,
                    mode=mode,
                    status="running",
                    rows_written=0,
                    started_at=_now(),
                )
            )
            return result.inserted_primary_key[0]

    def finish_run(self, run_id: int, status: str, rows_written: int, error: str | None = None) -> None:
        with self.db.begin() as conn:
            conn.execute(
                sa.update(self.runs)
                .where(self.runs.c.id == run_id)
                .values(status=status, rows_written=rows_written, finished_at=_now(), error=error)
            )

    def list_runs(self, source: str | None = None, limit: int = 50) -> list[Run]:
        query = sa.select(self.runs).order_by(self.runs.c.id.desc()).limit(limit)
        if source is not None:
            query = query.where(self.runs.c.source == source)
        with self.db.connect() as conn:
            return [Run(**r._mapping) for r in conn.execute(query).all()]

    # -- groups ------------------------------------------------------------

    def create_group(self, name: str, members: list[str], schedule: dict | None, enabled: bool,
                     next_run_at: dt.datetime | None) -> Group:
        now = _now()
        with self.db.begin() as conn:
            if conn.execute(sa.select(self.groups.c.id).where(self.groups.c.name == name)).first():
                raise ValueError(f"a group named {name!r} already exists")
            result = conn.execute(sa.insert(self.groups).values(
                name=name, members=members, schedule=schedule, enabled=enabled,
                next_run_at=next_run_at, created_at=now, updated_at=now,
            ))
            group_id = result.inserted_primary_key[0]
        return self.get_group(group_id)

    def update_group(self, group_id: int, **values: Any) -> Group:
        self.get_group(group_id)
        if "name" in values:
            with self.db.connect() as conn:
                clash = conn.execute(sa.select(self.groups.c.id).where(
                    self.groups.c.name == values["name"], self.groups.c.id != group_id)).first()
            if clash:
                raise ValueError(f"a group named {values['name']!r} already exists")
        with self.db.begin() as conn:
            conn.execute(sa.update(self.groups).where(self.groups.c.id == group_id).values(**values, updated_at=_now()))
        return self.get_group(group_id)

    def delete_group(self, group_id: int) -> None:
        self.get_group(group_id)
        with self.db.begin() as conn:
            conn.execute(sa.delete(self.group_runs).where(self.group_runs.c.group_id == group_id))
            conn.execute(sa.delete(self.groups).where(self.groups.c.id == group_id))

    def get_group(self, group_id: int) -> Group:
        with self.db.connect() as conn:
            row = conn.execute(sa.select(self.groups).where(self.groups.c.id == group_id)).first()
        if row is None:
            raise GroupNotFound(f"no group with id {group_id}")
        return Group(**row._mapping)

    def list_groups(self) -> list[Group]:
        with self.db.connect() as conn:
            rows = conn.execute(sa.select(self.groups).order_by(self.groups.c.name)).all()
        return [Group(**r._mapping) for r in rows]

    def due_groups(self, now: dt.datetime) -> list[Group]:
        query = sa.select(self.groups).where(
            self.groups.c.enabled.is_(True),
            self.groups.c.next_run_at.is_not(None),
            self.groups.c.next_run_at <= now,
        ).order_by(self.groups.c.next_run_at)
        with self.db.connect() as conn:
            return [Group(**r._mapping) for r in conn.execute(query).all()]

    def claim_group(self, group_id: int, expected: dt.datetime, next_run_at: dt.datetime | None) -> bool:
        """Move a due group to its next slot; False if someone else already did (no double runs)."""
        with self.db.begin() as conn:
            result = conn.execute(
                sa.update(self.groups)
                .where(self.groups.c.id == group_id, self.groups.c.next_run_at == expected)
                .values(next_run_at=next_run_at)
            )
            return result.rowcount == 1

    def start_group_run(self, group_id: int, trigger: str) -> int:
        with self.db.begin() as conn:
            result = conn.execute(sa.insert(self.group_runs).values(
                group_id=group_id, trigger=trigger, status="running", started_at=_now(), results=[],
            ))
            return result.inserted_primary_key[0]

    def start_tag_run(self, tag: str) -> int:
        run_id = self.start_group_run(TAG_RUN_GROUP, "tag")
        with self.db.begin() as conn:
            conn.execute(sa.delete(self.tag_runs).where(self.tag_runs.c.tag == tag))
            conn.execute(sa.insert(self.tag_runs).values(tag=tag, run_id=run_id))
        return run_id

    def last_tag_runs(self) -> dict[str, GroupRun]:
        query = sa.select(self.tag_runs.c.tag, self.group_runs).join(
            self.group_runs, self.group_runs.c.id == self.tag_runs.c.run_id)
        with self.db.connect() as conn:
            rows = conn.execute(query).all()
        return {r.tag: GroupRun(**{k: v for k, v in r._mapping.items() if k != "tag"}) for r in rows}

    def update_group_run(self, run_id: int, results: list[dict[str, Any]], status: str | None = None) -> None:
        values: dict[str, Any] = {"results": results}
        if status is not None:
            values.update(status=status, finished_at=_now())
        with self.db.begin() as conn:
            conn.execute(sa.update(self.group_runs).where(self.group_runs.c.id == run_id).values(**values))

    def get_group_run(self, run_id: int) -> GroupRun:
        with self.db.connect() as conn:
            row = conn.execute(sa.select(self.group_runs).where(self.group_runs.c.id == run_id)).first()
        if row is None:
            raise LookupError(f"no group run {run_id}")
        return GroupRun(**row._mapping)

    def list_group_runs(self, group_id: int, limit: int = 20) -> list[GroupRun]:
        query = (sa.select(self.group_runs).where(self.group_runs.c.group_id == group_id)
                 .order_by(self.group_runs.c.id.desc()).limit(limit))
        with self.db.connect() as conn:
            return [GroupRun(**r._mapping) for r in conn.execute(query).all()]

    def fail_stale_group_runs(self) -> int:
        """Runs left "running" by a process that stopped mid-run (call at startup)."""
        with self.db.begin() as conn:
            result = conn.execute(
                sa.update(self.group_runs).where(self.group_runs.c.status == "running")
                .values(status="failed", finished_at=_now())
            )
            return result.rowcount

    # -- Google connection ---------------------------------------------------

    def save_google_connection(self, email: str, refresh_token_encrypted: str, scopes: str) -> None:
        with self.db.begin() as conn:
            conn.execute(sa.delete(self.google_connection))
            conn.execute(sa.insert(self.google_connection).values(
                id=1, email=email, refresh_token=refresh_token_encrypted, scopes=scopes,
                status="ok", error=None, connected_at=_now(),
            ))

    def get_entra_cache(self) -> str | None:
        with self.db.connect() as conn:
            return conn.execute(sa.select(self.entra_cache.c.cache)).scalar()

    def save_entra_cache(self, encrypted: str) -> None:
        with self.db.begin() as conn:
            conn.execute(sa.delete(self.entra_cache))
            conn.execute(sa.insert(self.entra_cache).values(id=1, cache=encrypted, updated_at=_now()))

    def get_google_connection(self) -> dict[str, Any] | None:
        with self.db.connect() as conn:
            row = conn.execute(sa.select(self.google_connection)).first()
        return dict(row._mapping) if row else None

    def set_google_connection_status(self, status: str, error: str | None = None) -> None:
        with self.db.begin() as conn:
            conn.execute(sa.update(self.google_connection).values(status=status, error=error))

    def touch_google_connection(self) -> None:
        with self.db.begin() as conn:
            conn.execute(sa.update(self.google_connection).values(last_used_at=_now()))

    def delete_google_connection(self) -> None:
        with self.db.begin() as conn:
            conn.execute(sa.delete(self.google_connection))

    # -- uploads -----------------------------------------------------------

    def save_upload(self, source: str, filename: str, content: bytes, mode: str) -> int:
        """Keep the uploaded file itself, so every loaded version can be traced back."""
        with self.db.begin() as conn:
            result = conn.execute(
                sa.insert(self.uploads).values(
                    source=source,
                    filename=filename,
                    size_bytes=len(content),
                    sha256=hashlib.sha256(content).hexdigest(),
                    mode=mode,
                    uploaded_at=_now(),
                    content=content,
                )
            )
            return result.inserted_primary_key[0]

    def link_upload(self, upload_id: int, run_id: int) -> None:
        with self.db.begin() as conn:
            conn.execute(sa.update(self.uploads).where(self.uploads.c.id == upload_id).values(run_id=run_id))

    def list_uploads(self, source: str | None = None, limit: int = 50) -> list[Upload]:
        cols = [c for c in self.uploads.c if c.name != "content"]
        query = sa.select(*cols).order_by(self.uploads.c.id.desc()).limit(limit)
        if source is not None:
            query = query.where(self.uploads.c.source == source)
        with self.db.connect() as conn:
            return [Upload(**r._mapping) for r in conn.execute(query).all()]

    def load_history(self, source: str) -> list[tuple[Upload, str | None]]:
        """Every stored upload for a source, oldest first, with its run's status."""
        cols = [c for c in self.uploads.c if c.name != "content"]
        query = (
            sa.select(*cols, self.runs.c.status)
            .select_from(self.uploads.outerjoin(self.runs, self.uploads.c.run_id == self.runs.c.id))
            .where(self.uploads.c.source == source)
            .order_by(self.uploads.c.id)
        )
        with self.db.connect() as conn:
            rows = conn.execute(query).all()
        out = []
        for r in rows:
            data = dict(r._mapping)
            status = data.pop("status")
            out.append((Upload(**data), status))
        return out

    def table_loads(self, source: str) -> dict[str, tuple[dt.datetime, int, str]]:
        """The latest successful load of each dataset of a source: key -> (finished, rows written, mode)."""
        latest = (sa.select(self.runs.c.dataset_key, sa.func.max(self.runs.c.id).label("id"))
                  .where(self.runs.c.source == source, self.runs.c.status == "succeeded")
                  .group_by(self.runs.c.dataset_key).subquery())
        query = sa.select(self.runs.c.dataset_key, self.runs.c.finished_at, self.runs.c.rows_written, self.runs.c.mode) \
            .join(latest, self.runs.c.id == latest.c.id)
        with self.db.connect() as conn:
            return {r.dataset_key: (r.finished_at, r.rows_written, r.mode) for r in conn.execute(query).all()}

    def drop_table(self, table: str) -> None:
        """Delete a table of loaded data (a database source's table that was unchosen)."""
        with self.db.begin() as conn:
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {self.qualified(table)}"))
            conn.execute(sa.delete(self.table_keys).where(self.table_keys.c.table_name == table))
            conn.execute(sa.delete(self.table_increments).where(self.table_increments.c.table_name == table))
            conn.execute(sa.delete(self.hidden_tables).where(self.hidden_tables.c.table_name == table))
            conn.execute(sa.delete(self.table_history).where(self.table_history.c.table_name == table))

    # -- key scans and natural keys ------------------------------------------

    def get_table_key(self, table: str) -> dict[str, Any] | None:
        """The latest key scan of a table and its chosen natural key, if either exists."""
        with self.db.connect() as conn:
            row = conn.execute(sa.select(self.table_keys).where(self.table_keys.c.table_name == table)).first()
        if row is None:
            return None
        return {"scan": row.scan, "scanned_at": row.scanned_at, "key": row.key_columns, "key_set_at": row.key_set_at}

    def _upsert_key_row(self, table: str, **values: Any) -> None:
        with self.db.begin() as conn:
            done = conn.execute(sa.update(self.table_keys).where(self.table_keys.c.table_name == table).values(**values))
            if not done.rowcount:
                conn.execute(sa.insert(self.table_keys).values(table_name=table, **values))

    def save_key_scan(self, table: str, scan: dict[str, Any]) -> None:
        self._upsert_key_row(table, scan=json.loads(json.dumps(scan, default=str)), scanned_at=_now())

    def save_table_key(self, table: str, columns: list[str] | None) -> None:
        """Set (or, with None, clear) the natural key of a table."""
        self._upsert_key_row(table, key_columns=columns, key_set_at=_now() if columns else None)

    # -- renaming a source ---------------------------------------------------------

    def rename_source(self, old: str, new: str) -> None:
        """Rename a source in eyetable's records: its config, catalog, runs, files, tags and groups.
        (Its tables are Engine.rename's job.)"""
        if self.source_exists(new):
            raise ValueError(f"there is already a source named {new}")
        with self.db.begin() as conn:
            for table in (self.sources, ):
                conn.execute(sa.update(table).where(table.c.name == old).values(name=new))
            for table in (self.datasets, self.runs, self.uploads, self.source_tags):
                conn.execute(sa.update(table).where(table.c.source == old).values(source=new))
            for row in conn.execute(sa.select(self.groups.c.id, self.groups.c.members)).all():
                if old in row.members:
                    conn.execute(sa.update(self.groups).where(self.groups.c.id == row.id)
                                 .values(members=[new if m == old else m for m in row.members]))

    # -- history tables -------------------------------------------------------------

    def keyed_tables(self) -> set[str]:
        """The tables with a natural key."""
        with self.db.connect() as conn:  # a cleared key is JSON null, so it's checked here, not in SQL
            rows = conn.execute(sa.select(self.table_keys.c.table_name, self.table_keys.c.key_columns)).all()
        return {r.table_name for r in rows if r.key_columns}

    def histories(self) -> set[str]:
        """The tables that keep history."""
        with self.db.connect() as conn:
            return set(conn.execute(sa.select(self.table_history.c.table_name)).scalars())

    def has_history(self, table: str) -> bool:
        return table in self.histories()

    def set_history(self, table: str, on: bool) -> None:
        with self.db.begin() as conn:
            conn.execute(sa.delete(self.table_history).where(self.table_history.c.table_name == table))
            if on:
                conn.execute(sa.insert(self.table_history).values(table_name=table, set_at=_now()))

    def history_key(self, table: str) -> list[str] | None:
        """The natural key a history table merges by; None if the table doesn't keep history."""
        if not self.has_history(table):
            return None
        saved = self.get_table_key(table)
        return saved["key"] if saved and saved.get("key") else None

    def take_merge_stats(self) -> dict[str, int] | None:
        """The counts of the last history merge on this thread (once)."""
        stats = getattr(self._merges, "stats", None)
        self._merges.stats = None
        return stats

    def current_rows(self, ref: str) -> str:
        """A table as SQL to select from: a history table's current versions only, else the table."""
        if self.has_history(ref) and ACTIVE_COLUMN in {c["name"] for c in self.columns(ref)}:
            prep = self.db.dialect.identifier_preparer
            return f"(SELECT * FROM {self.qualified(ref)} WHERE {prep.quote(ACTIVE_COLUMN)} = 1) current_rows"
        return self.qualified(ref)

    def _merge(self, table: str, batches: Iterable[list[dict[str, Any]]], run_id: int, origin: Any,
               key: list[str]) -> int:
        """Load batches into a history table by its natural key `key`: a key not there yet is added
        (ET_active 1, ET_fromdt now, ET_todt 9999-12-31); a key whose other columns changed gets a
        new version the same way and its current version is closed (ET_active 0, ET_todt now); an
        unchanged row is left as it is. Nothing is ever deleted. Returns the rows read."""
        from eyetable import loadlog

        schema, name = self.split(table)
        incoming = self.ref(schema, f"{name[:50]}__incoming")
        loadlog.step(f"{table} keeps history: reading the load into {incoming}, then merging it by {' + '.join(key)}")
        rows = self.write(incoming, batches, "replace", run_id, origin=origin)
        try:
            stats = self._merge_into(table, incoming, key, rows)
        finally:
            with self.db.begin() as conn:
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {self.qualified(incoming)}"))
        self._merges.stats = stats
        loadlog.step(f"History merge into {table}: {stats['new']:,} new, {stats['changed']:,} changed "
                     f"(their old versions closed), {stats['unchanged']:,} unchanged")
        return rows

    def _merge_into(self, table: str, incoming: str, key: list[str], rows: int) -> dict[str, int]:
        db = self.db
        prep = db.dialect.identifier_preparer
        q = prep.quote
        clock = dt.datetime.now()  # the server's local time, kept to the millisecond
        now = clock.replace(microsecond=clock.microsecond // 1000 * 1000)
        inc_cols = {c["name"]: c["type"] for c in self.columns(incoming)}
        missing = [k for k in key if k not in inc_cols]
        if missing:
            raise ValueError(f"this load has no column {', '.join(missing)}, part of {table}'s natural key; "
                             "a history table merges by it")
        qi = self.qualified(incoming)
        keys_sql = ", ".join(q(k) for k in key)
        with db.connect() as conn:
            nulls = conn.execute(sa.text(f"SELECT COUNT(*) FROM {qi} WHERE " +
                                         " OR ".join(f"{q(k)} IS NULL" for k in key))).scalar_one()
            dups = conn.execute(sa.text(f"SELECT COUNT(*) FROM (SELECT {keys_sql} FROM {qi} GROUP BY {keys_sql} "
                                        f"HAVING COUNT(*) > 1) d")).scalar_one()
        if nulls or dups:
            raise ValueError(f"a history table needs one row per {' + '.join(key)} in each load; this one has "
                             + " and ".join(x for x in [f"{nulls:,} row{'s' if nulls != 1 else ''} with no value in it" if nulls else "",
                                                        f"{dups:,} value{'s' if dups != 1 else ''} more than once" if dups else ""] if x)
                             + ". Nothing was changed.")
        schema, name = self.split(table)
        meta = {ID_COLUMN, *META_COLUMNS, *HISTORY_COLUMNS}
        if not self.table_exists(table):  # the first load: every row is a first, current version
            with db.begin() as conn:
                conn.execute(sa.text(f"ALTER TABLE {qi} RENAME TO {q(name)}"))
                self._add_history_columns(conn, table, now)
            return {"new": rows, "changed": 0, "unchanged": 0}
        tgt_cols = {c["name"]: c["type"] for c in self.columns(table)}
        with db.begin() as conn:
            if ACTIVE_COLUMN not in tgt_cols:  # history just turned on: today's rows are the first versions
                self._add_history_columns(conn, table, now)
            for c, typ in inc_cols.items():  # a column first seen in this load
                if c not in tgt_cols and c not in meta:
                    conn.execute(sa.text(f"ALTER TABLE {self.qualified(table)} ADD COLUMN {q(c)} "
                                         f"{typ.compile(dialect=db.dialect)}"))
                    tgt_cols[c] = typ
            data = [c for c in tgt_cols if c not in meta]
            t, i = q(name), "i"
            text = "TEXT"
            distinct = "IS DISTINCT FROM" if db.dialect.name == "postgresql" else "IS NOT"

            def as_text(side: str, col: str) -> str:
                return f"CAST({side}.{q(col)} AS {text})"

            same_key = " AND ".join(f"{as_text(i, k)} = {as_text('{T}', k)}" for k in key)
            changed = " OR ".join(
                f"{as_text(i, c) if c in inc_cols else 'NULL'} {distinct} {as_text('{T}', c)}"
                for c in data if c not in key) or "1 = 0"
            closed = conn.execute(sa.text(
                f"UPDATE {self.qualified(table)} SET {q(ACTIVE_COLUMN)} = 0, {q(TO_COLUMN)} = :now "
                f"WHERE {q(ACTIVE_COLUMN)} = 1 AND EXISTS (SELECT 1 FROM {qi} {i} WHERE "
                f"{same_key.replace('{T}', t)} AND ({changed.replace('{T}', t)}))"), {"now": now}).rowcount or 0

            def value(c: str) -> str:
                if c not in inc_cols:
                    return "NULL"
                if type(inc_cols[c]) is type(tgt_cols[c]) or db.dialect.name == "sqlite":
                    return f"{i}.{q(c)}"
                return f"CAST({i}.{q(c)} AS {tgt_cols[c].compile(dialect=db.dialect)})"

            cols = data + [TIMESTAMP_COLUMN, SOURCE_COLUMN, RUN_ID_COLUMN, ACTIVE_COLUMN, FROM_COLUMN, TO_COLUMN]
            select = [value(c) for c in data] + [f"{i}.{q(TIMESTAMP_COLUMN)}", f"{i}.{q(SOURCE_COLUMN)}",
                                                 f"{i}.{q(RUN_ID_COLUMN)}", "1", ":now", ":forever"]
            inserted = conn.execute(sa.text(
                f"INSERT INTO {self.qualified(table)} ({', '.join(q(c) for c in cols)}) "
                f"SELECT {', '.join(select)} FROM {qi} {i} WHERE NOT EXISTS (SELECT 1 FROM {self.qualified(table)} h "
                f"WHERE h.{q(ACTIVE_COLUMN)} = 1 AND {same_key.replace('{T}', 'h')})"),
                {"now": now, "forever": FOREVER}).rowcount or 0
        return {"new": inserted - closed, "changed": closed, "unchanged": rows - inserted}

    def _add_history_columns(self, conn: sa.Connection, table: str, now: dt.datetime) -> None:
        q = self.db.dialect.identifier_preparer.quote
        stamp = sa.DateTime().compile(dialect=self.db.dialect)
        for col, ddl in ((ACTIVE_COLUMN, "INTEGER"), (FROM_COLUMN, stamp), (TO_COLUMN, stamp)):
            conn.execute(sa.text(f"ALTER TABLE {self.qualified(table)} ADD COLUMN {q(col)} {ddl}"))
        conn.execute(sa.text(f"UPDATE {self.qualified(table)} SET {q(ACTIVE_COLUMN)} = 1, {q(FROM_COLUMN)} = :now, "
                             f"{q(TO_COLUMN)} = :forever"), {"now": now, "forever": FOREVER})

    # -- hidden tables -----------------------------------------------------------

    def hidden(self) -> set[str]:
        """The tables hidden in Sources, which loads skip."""
        with self.db.connect() as conn:
            return set(conn.execute(sa.select(self.hidden_tables.c.table_name)).scalars())

    def set_hidden(self, table: str, hidden: bool) -> None:
        with self.db.begin() as conn:
            conn.execute(sa.delete(self.hidden_tables).where(self.hidden_tables.c.table_name == table))
            if hidden:
                conn.execute(sa.insert(self.hidden_tables).values(table_name=table, hidden_at=_now()))

    # -- incremental loads ----------------------------------------------------

    def get_increment(self, table: str) -> str | None:
        with self.db.connect() as conn:
            return conn.execute(sa.select(self.table_increments.c.column_name)
                                .where(self.table_increments.c.table_name == table)).scalar()

    def increments(self) -> dict[str, str]:
        """Every table's incremental column: table -> column."""
        with self.db.connect() as conn:
            return {r.table_name: r.column_name for r in conn.execute(sa.select(self.table_increments))}

    def save_increment(self, table: str, column: str | None) -> None:
        """Set (or, with None, clear) the column a table's incremental loads go by."""
        with self.db.begin() as conn:
            conn.execute(sa.delete(self.table_increments).where(self.table_increments.c.table_name == table))
            if column:
                conn.execute(sa.insert(self.table_increments).values(table_name=table, column_name=column, set_at=_now()))

    def max_value(self, table: str, column: str, type_: sa.types.TypeEngine | None = None) -> Any:
        """The largest value of a column, read as its own type (a datetime for a time column)."""
        if type_ is not None and isinstance(type_, sa.DateTime):
            type_ = UTCDateTime() if type_.timezone else sa.DateTime()
        col = sa.column(column, type_ if type_ is not None else sa.Text())
        schema, name = self.split(table)
        query = sa.select(sa.func.max(col)).select_from(sa.table(name, schema=schema))
        with self.db.connect() as conn:
            return conn.execute(query).scalar()

    def replace_older(self, table: str, key: list[str], run_id: int) -> int:
        """After an incremental load, delete each row that a row of run `run_id` with the same key replaces."""
        prep = self.db.dialect.identifier_preparer
        q, idc, run = self.qualified(table), prep.quote(ID_COLUMN), prep.quote(RUN_ID_COLUMN)
        same = " AND ".join(f"n.{prep.quote(c)} = o.{prep.quote(c)}" for c in key)
        with self.db.begin() as conn:
            done = conn.execute(sa.text(
                f"DELETE FROM {q} WHERE {idc} IN (SELECT o.{idc} FROM {q} o JOIN {q} n "
                f"ON n.{run} = :run AND n.{idc} > o.{idc} AND {same})"), {"run": run_id})
        return done.rowcount or 0

    def delete_run_rows(self, table: str, run_id: int) -> None:
        """Take out the rows a failed incremental load had already added."""
        if not self.table_exists(table):
            return
        prep = self.db.dialect.identifier_preparer
        with self.db.begin() as conn:
            conn.execute(sa.text(f"DELETE FROM {self.qualified(table)} WHERE {prep.quote(RUN_ID_COLUMN)} = :run"),
                         {"run": run_id})

    def last_loaded(self, source: str) -> dt.datetime | None:
        """When data last finished loading into this source successfully."""
        query = sa.select(sa.func.max(self.runs.c.finished_at)).where(
            self.runs.c.source == source, self.runs.c.status == "succeeded"
        )
        with self.db.connect() as conn:
            return conn.execute(query).scalar()

    def upload_content(self, upload_id: int) -> bytes:
        with self.db.connect() as conn:
            content = conn.execute(
                sa.select(self.uploads.c.content).where(self.uploads.c.id == upload_id)
            ).scalar()
        if content is None:
            raise LookupError(f"no upload {upload_id}")
        return content

    # -- data --------------------------------------------------------------

    # A table is named by a "ref": "name" for a table in the data schema, or "schema.name" for a
    # table a source was given a name of its own for, in a schema of its own.

    def split(self, ref: str) -> tuple[str | None, str]:
        """(schema, table name) of a table ref."""
        if "." in ref:
            schema, name = ref.split(".", 1)
            return schema, name
        return self.data_schema, ref

    def ref(self, schema: str | None, name: str) -> str:
        """The ref of a table: plain in the data schema, schema.name anywhere else."""
        return name if schema in (None, self.data_schema) else f"{schema}.{name}"

    def ensure_schema(self, schema: str | None) -> None:
        """Create a schema if it isn't there yet, and tell whoever asked (the query window shares it)."""
        if not schema or schema in self._schemas_made or self.db.dialect.name == "sqlite":
            return
        with self.db.begin() as conn:
            conn.execute(CreateSchema(schema, if_not_exists=True))
        self._schemas_made.add(schema)
        for hook in self.schema_hooks:
            try:
                hook(schema)
            except Exception:
                pass  # sharing with the query window is a nicety; a load must not fail on it

    def other_schemas(self) -> list[str]:
        """Schemas besides the data schema that hold source tables: ones sources were given, and
        the archives (<schema>_archived) of those and of the data schema. Only those that exist."""
        if self.db.dialect.name == "sqlite":
            return []
        from eyetable import targets

        wanted = {f"{self.data_schema}_archived"}
        for src in self.list_sources():
            try:
                refs = targets.tables(src)
            except Exception:
                continue
            for ref in refs:
                schema = self.split(ref)[0]
                if schema and schema != self.data_schema:
                    wanted |= {schema, f"{schema}_archived"}
        existing = set(sa.inspect(self.db).get_schema_names())
        return sorted(wanted & existing)

    def columns(self, ref: str) -> list[dict[str, Any]]:
        schema, name = self.split(ref)
        return sa.inspect(self.db).get_columns(name, schema=schema)

    def table_exists(self, table: str) -> bool:
        schema, name = self.split(table)
        return sa.inspect(self.db).has_table(name, schema=schema)

    def archive_table(self, ref: str, when: dt.datetime | None = None) -> str | None:
        """Keep a table out of the way under a dated name: eyetable_raw.ledger becomes
        eyetable_raw_archived.ledger_20261113_161034. Returns its new ref (None if there was no table)."""
        if not self.table_exists(ref):
            return None
        schema, name = self.split(ref)
        stamp = (when or dt.datetime.now()).strftime("%Y%m%d_%H%M%S")
        new_name = f"{name[:63 - len(stamp) - 1]}_{stamp}"
        prep = self.db.dialect.identifier_preparer
        if self.db.dialect.name == "sqlite" or not schema:  # no schemas: a dated name beside it
            with self.db.begin() as conn:
                conn.execute(sa.text(f"ALTER TABLE {self.qualified(ref)} RENAME TO {prep.quote(new_name)}"))
            return new_name
        archive = f"{schema}_archived"[:63]
        self.ensure_schema(archive)
        with self.db.begin() as conn:
            conn.execute(sa.text(f"ALTER TABLE {self.qualified(ref)} RENAME TO {prep.quote(new_name)}"))
            conn.execute(sa.text(f"ALTER TABLE {prep.quote_schema(schema)}.{prep.quote(new_name)} "
                                 f"SET SCHEMA {prep.quote_schema(archive)}"))
        return f"{archive}.{new_name}"

    def move_table_settings(self, old: str, new: str) -> None:
        """A table's natural key and scan, incremental column and hidden flag follow it to a new name."""
        with self.db.begin() as conn:
            for table in (self.table_keys, self.table_increments, self.hidden_tables, self.table_history):
                conn.execute(sa.delete(table).where(table.c.table_name == new))
                conn.execute(sa.update(table).where(table.c.table_name == old).values(table_name=new))

    def count_rows(self, table: str) -> int | None:
        if not self.table_exists(table):
            return None
        with self.db.connect() as conn:
            return conn.execute(sa.text(f"SELECT COUNT(*) FROM {self.qualified(table)}")).scalar_one()

    def qualified(self, table: str) -> str:
        prep = self.db.dialect.identifier_preparer
        schema, name = self.split(table)
        if schema:
            return f"{prep.quote_schema(schema)}.{prep.quote(name)}"
        return prep.quote(name)

    def write(
        self,
        table: str,
        batches: Iterable[list[dict[str, Any]]],
        mode: str = "replace",
        run_id: int = 0,
        *,
        origin: str,
    ) -> int:
        """Write record batches into `table` in the data schema; return rows written.

        `origin` is stored in every row's ET_source. `replace` loads into a
        staging table and swaps it in only once every batch has landed, so a
        failed pull never leaves a half-empty table; ET_id restarts at 1 in the
        new table. `append` adds rows to the existing table, continuing ET_id
        and adding columns as needed.
        """
        if mode not in ("replace", "append"):
            raise ValueError(f"unknown write mode {mode!r}")
        key = self.history_key(table)
        if key:  # a history table is never replaced: every load merges into it
            return self._merge(table, batches, run_id, origin, key)
        from eyetable import loadlog

        target = f"{table}__stg" if mode == "replace" else table
        loadlog.step(f"Writing into {target}" + (f", to be swapped in for {table} when every row is in" if mode == "replace"
                                                 else f" ({mode})"))
        writer = _TableWriter(self, target, fresh=(mode == "replace"))
        rows = 0
        try:
            for batch in batches:
                meta = {TIMESTAMP_COLUMN: _now(), SOURCE_COLUMN: origin() if callable(origin) else origin, RUN_ID_COLUMN: run_id}
                rows += writer.write(batch, meta)
            writer.finish(reorder=(mode == "replace"))
        except BaseException:
            if mode == "replace":  # don't leave a half-built staging table behind
                with self.db.begin() as conn:
                    conn.execute(sa.text(f"DROP TABLE IF EXISTS {self.qualified(target)}"))
            raise
        if mode == "replace":
            prep = self.db.dialect.identifier_preparer
            with self.db.begin() as conn:
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {self.qualified(table)}"))
                conn.execute(sa.text(f"ALTER TABLE {self.qualified(target)} RENAME TO {prep.quote(self.split(table)[1])}"))
            loadlog.step(f"Swapped {target} in as {table}: {rows:,} rows, {len(writer.columns) + 1} columns")
        return rows

    def read_table(self, table: str, limit: int | None = None) -> list[dict[str, Any]]:
        schema, name = self.split(table)
        reflected = sa.Table(
            name,
            sa.MetaData(),
            sa.Column(TIMESTAMP_COLUMN, UTCDateTime()),
            schema=schema,
            autoload_with=self.db,
        )
        query = sa.select(reflected).order_by(reflected.c[ID_COLUMN]).limit(limit)
        with self.db.connect() as conn:
            return [dict(r._mapping) for r in conn.execute(query)]


class _TableWriter:
    """Creates and evolves one data table as batches arrive.

    A new table is created from the first batch, so its columns run ET_id, the
    source's columns, then ET_timestamp, ET_source, ET_run_id. A column first
    seen later can only be added at the end; finish(reorder=True) then rebuilds
    the table in that order (a replace's staging table, before it's swapped in).
    """

    def __init__(self, store: CoreStore, table: str, fresh: bool):
        self.store = store
        self.table = table
        self.columns: dict[str, str] = {}  # column name -> kind, for every column we insert into
        self.key_to_column: dict[str, str] = {}
        self.pending = False  # created with the first batch, so its columns come first
        self.out_of_order = False  # a source column was added after the ET columns

        qualified = store.qualified(table)
        self.schema, self.name = store.split(table)
        insp = sa.inspect(store.db)
        exists = insp.has_table(self.name, schema=self.schema)
        if exists and fresh:
            with store.db.begin() as conn:
                conn.execute(sa.text(f"DROP TABLE {qualified}"))
            exists = False
        if exists:
            existing = insp.get_columns(self.name, schema=self.schema)
            if ID_COLUMN not in {c["name"] for c in existing}:
                raise ValueError(f"{table} has no {ID_COLUMN} column; ingest it with mode=replace once to rebuild it")
            for col in existing:
                if col["name"] != ID_COLUMN:  # the database assigns ET_id
                    self.columns[col["name"]] = kind_of(col["type"])
        else:
            self.pending = True
        self._rebuild()

    def _create(self, name: str, data: dict[str, str]) -> None:
        """A table of ET_id, the source's columns (`data`), then the other ET columns (`name`: a bare
        name, in this writer's schema)."""
        self.store.ensure_schema(self.schema)
        cols = [
            # BIGINT identity on Postgres; SQLite only auto-numbers an INTEGER PRIMARY KEY.
            sa.Column(
                ID_COLUMN,
                sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
                sa.Identity(start=1, increment=1, always=True),
                primary_key=True,
            ),
            *(sa.Column(n, sa_type(k)) for n, k in data.items()),
            *(sa.Column(n, sa_type(k), nullable=False) for n, k in META_COLUMNS.items()),
        ]
        sa.Table(name, sa.MetaData(), *cols, schema=self.schema).create(self.store.db)

    def _create_from(self, kinds: dict[str, str | None]) -> None:
        data = {self._column_for(key): kind or "text" for key, kind in kinds.items()}
        self._create(self.name, data)
        self.columns = {**data, **META_COLUMNS}
        self.pending = False
        self._rebuild()

    def finish(self, reorder: bool = False) -> None:
        """Create the table if no rows came; with `reorder`, put late columns before the ET ones."""
        if self.pending:
            self._create_from({})
        if not (reorder and self.out_of_order):
            return
        from eyetable import loadlog

        loadlog.step(f"Rebuilding {self.table} so columns that arrived late sit before the ET_ columns")
        db, prep = self.store.db, self.store.db.dialect.identifier_preparer
        data = {n: k for n, k in self.columns.items() if n not in META_COLUMNS}
        temp_name = f"et_reorder_{secrets.token_hex(6)}"
        self._create(temp_name, data)
        temp = self.store.ref(self.schema, temp_name)
        names = ", ".join(prep.quote(c) for c in [ID_COLUMN, *data, *META_COLUMNS])
        overriding = " OVERRIDING SYSTEM VALUE" if db.dialect.name == "postgresql" else ""
        with db.begin() as conn:
            conn.execute(sa.text(f"INSERT INTO {self.store.qualified(temp)} ({names}){overriding} "
                                 f"SELECT {names} FROM {self.store.qualified(self.table)} ORDER BY {prep.quote(ID_COLUMN)}"))
            if db.dialect.name == "postgresql":  # the copied ET_ids were given, so move the counter past them
                conn.execute(sa.text(
                    f"SELECT setval(pg_get_serial_sequence(:t, :c), COALESCE(MAX({prep.quote(ID_COLUMN)}), 1), "
                    f"MAX({prep.quote(ID_COLUMN)}) IS NOT NULL) FROM {self.store.qualified(temp)}"
                ), {"t": self.store.qualified(temp), "c": ID_COLUMN})
            conn.execute(sa.text(f"DROP TABLE {self.store.qualified(self.table)}"))
            conn.execute(sa.text(f"ALTER TABLE {self.store.qualified(temp)} RENAME TO {prep.quote(self.name)}"))
        self.columns = {**data, **META_COLUMNS}
        self.out_of_order = False
        self._rebuild()

    def _rebuild(self) -> None:
        cols = [sa.column(n, sa_type(k)) for n, k in self.columns.items()]
        self._table = sa.table(self.name, *cols, schema=self.schema)

    def _column_for(self, key: str) -> str:
        if key not in self.key_to_column:
            name = column_name(key)
            if name.lower() in RESERVED_COLUMNS:
                name = f"{name}_src"
            taken = set(self.key_to_column.values())
            base, n = name, 2
            while name in taken:
                name, n = f"{base}_{n}", n + 1
            self.key_to_column[key] = name
        return self.key_to_column[key]

    def _evolve(self, kinds: dict[str, str | None]) -> None:
        db = self.store.db
        prep = db.dialect.identifier_preparer
        qualified = self.store.qualified(self.table)
        changed = False
        with db.begin() as conn:
            for key, kind in kinds.items():
                col = self._column_for(key)
                if col not in self.columns:
                    kind = kind or "text"
                    ddl = sa_type(kind).compile(dialect=db.dialect)
                    conn.execute(sa.text(f"ALTER TABLE {qualified} ADD COLUMN {prep.quote(col)} {ddl}"))
                    from eyetable import loadlog

                    loadlog.step(f"New column {col} ({kind}) added to {self.table}")
                    self.columns[col] = kind
                    self.out_of_order = self.out_of_order or any(m in self.columns for m in META_COLUMNS)
                    changed = True
                    continue
                current = self.columns[col]
                widened = merge_kinds(current, kind)
                if widened != current:
                    # SQLite is dynamically typed, so only the bookkeeping changes there.
                    if db.dialect.name != "sqlite":
                        ddl = sa_type(widened).compile(dialect=db.dialect)
                        q = prep.quote(col)
                        conn.execute(
                            sa.text(f"ALTER TABLE {qualified} ALTER COLUMN {q} TYPE {ddl} USING {q}::{ddl}")
                        )
                    self.columns[col] = widened
                    changed = True
        if changed:
            self._rebuild()

    def write(self, records: list[dict[str, Any]], meta: dict[str, Any]) -> int:
        if not records:
            return 0
        kinds = infer_columns(records)
        if self.pending:
            self._create_from(kinds)
        self._evolve(kinds)
        rows = []
        for record in records:
            row = {c: None for c in self.columns}
            row.update(meta)
            for key, value in record.items():
                col = self.key_to_column[key]
                row[col] = coerce(value, self.columns[col])
            rows.append(row)
        with self.store.db.begin() as conn:
            conn.execute(sa.insert(self._table), rows)
        return len(rows)
