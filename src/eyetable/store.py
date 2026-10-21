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
RESERVED_COLUMNS = {c.lower() for c in (ID_COLUMN, TIMESTAMP_COLUMN, SOURCE_COLUMN, RUN_ID_COLUMN)}


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
    trigger: str  # "manual" | "schedule" | "catch-up"
    status: str  # "running" | "succeeded" | "partial" | "failed"
    started_at: dt.datetime
    finished_at: dt.datetime | None
    results: list[dict[str, Any]]


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

    def table_exists(self, table: str) -> bool:
        return sa.inspect(self.db).has_table(table, schema=self.data_schema)

    def count_rows(self, table: str) -> int | None:
        if not self.table_exists(table):
            return None
        with self.db.connect() as conn:
            return conn.execute(sa.text(f"SELECT COUNT(*) FROM {self.qualified(table)}")).scalar_one()

    def qualified(self, table: str) -> str:
        prep = self.db.dialect.identifier_preparer
        if self.data_schema:
            return f"{prep.quote_schema(self.data_schema)}.{prep.quote(table)}"
        return prep.quote(table)

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
        target = f"{table}__stg" if mode == "replace" else table
        writer = _TableWriter(self, target, fresh=(mode == "replace"))
        rows = 0
        for batch in batches:
            meta = {TIMESTAMP_COLUMN: _now(), SOURCE_COLUMN: origin, RUN_ID_COLUMN: run_id}
            rows += writer.write(batch, meta)
        if mode == "replace":
            prep = self.db.dialect.identifier_preparer
            with self.db.begin() as conn:
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {self.qualified(table)}"))
                conn.execute(sa.text(f"ALTER TABLE {self.qualified(target)} RENAME TO {prep.quote(table)}"))
        return rows

    def read_table(self, table: str, limit: int | None = None) -> list[dict[str, Any]]:
        reflected = sa.Table(
            table,
            sa.MetaData(),
            sa.Column(TIMESTAMP_COLUMN, UTCDateTime()),
            schema=self.data_schema,
            autoload_with=self.db,
        )
        query = sa.select(reflected).order_by(reflected.c[ID_COLUMN]).limit(limit)
        with self.db.connect() as conn:
            return [dict(r._mapping) for r in conn.execute(query)]


class _TableWriter:
    """Creates and evolves one data table as batches arrive."""

    def __init__(self, store: CoreStore, table: str, fresh: bool):
        self.store = store
        self.table = table
        self.columns: dict[str, str] = {}  # column name -> kind, for every column we insert into
        self.key_to_column: dict[str, str] = {}

        qualified = store.qualified(table)
        insp = sa.inspect(store.db)
        exists = insp.has_table(table, schema=store.data_schema)
        if exists and fresh:
            with store.db.begin() as conn:
                conn.execute(sa.text(f"DROP TABLE {qualified}"))
            exists = False
        if exists:
            existing = insp.get_columns(table, schema=store.data_schema)
            if ID_COLUMN not in {c["name"] for c in existing}:
                raise ValueError(f"{table} has no {ID_COLUMN} column; ingest it with mode=replace once to rebuild it")
            for col in existing:
                if col["name"] != ID_COLUMN:  # the database assigns ET_id
                    self.columns[col["name"]] = kind_of(col["type"])
        else:
            self.columns = {TIMESTAMP_COLUMN: "timestamptz", SOURCE_COLUMN: "varchar", RUN_ID_COLUMN: "bigint"}
            cols = [
                # BIGINT identity on Postgres; SQLite only auto-numbers an INTEGER PRIMARY KEY.
                sa.Column(
                    ID_COLUMN,
                    sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
                    sa.Identity(start=1, increment=1, always=True),
                    primary_key=True,
                ),
                *(sa.Column(n, sa_type(k), nullable=False) for n, k in self.columns.items()),
            ]
            sa.Table(table, sa.MetaData(), *cols, schema=store.data_schema).create(store.db)
        self._rebuild()

    def _rebuild(self) -> None:
        cols = [sa.column(n, sa_type(k)) for n, k in self.columns.items()]
        self._table = sa.table(self.table, *cols, schema=self.store.data_schema)

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
                    self.columns[col] = kind
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
        self._evolve(infer_columns(records))
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
