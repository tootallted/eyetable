"""The tables and views of one database connection, as a source (see eyetable.dbconnect)."""

import datetime as dt
import re
import time
from decimal import Decimal

import sqlalchemy as sa

from eyetable.connectors.database import DatabaseConnector
from eyetable.dbconnect import ConnectionError_, db_tables, friendly, list_objects, make_engine
from eyetable.entra import EntraSignInNeeded
from eyetable.models import ColumnInfo, DatasetInfo


class DbTableConnector(DatabaseConnector):
    """Chosen tables and views of one database, each loaded into its own table.

    Config: the connection ("odbc" or "url", plus an encrypted "password"), a
    display "label" (server/database), and "tables": [{key, schema, table,
    kind, name}]. A source made before it could hold several tables has
    "schema", "table" and "kind" instead, read as one table with key "data".
    """

    type = "dbtable"
    reads_since = True  # read() can ask the database for only the rows past a value (incremental loads)
    reads_in_parts = True  # read() can split a big table into ranges of a column (read_in_parts)

    def __init__(self, config: dict):
        self.config = config
        self._engine, self.warnings = make_engine(config)
        self.parts_read: dict[str, int] = {}  # key -> how many parts the last read_in_parts took

    def test(self) -> None:
        try:
            with self._engine.connect():
                pass
        except EntraSignInNeeded:
            raise
        except Exception as exc:
            raise ConnectionError_(f"couldn't connect: {friendly(exc)}") from None

    def objects(self) -> list[dict]:
        """Every table and view the database has (see dbconnect.list_objects)."""
        return list_objects(self._engine)

    def _item(self, key: str) -> dict:
        for item in db_tables(self.config):
            if item["key"] == key:
                return item
        raise KeyError(f"this source has no table {key!r}")

    def discover(self) -> list[DatasetInfo]:
        return [self._dataset(item) for item in db_tables(self.config)]

    def _dataset(self, item: dict) -> DatasetInfo:
        schema, table = item["schema"], item["table"]
        try:
            insp = sa.inspect(self._engine)
            columns = [ColumnInfo(c["name"], str(c["type"]), bool(c.get("nullable", True)))
                       for c in insp.get_columns(table, schema=schema)]
        except sa.exc.NoSuchTableError:
            columns = []
        except EntraSignInNeeded:
            raise
        except Exception as exc:
            # no column catalog to read (Dataverse has no INFORMATION_SCHEMA.COLUMNS): ask the table itself
            columns = self._columns_from_query(schema, table)
            if columns is None:
                raise ConnectionError_(f"couldn't read {schema}.{table}: {friendly(exc)}") from None
        if not columns:
            raise ConnectionError_(f"{schema}.{table} wasn't found (or has no columns visible to this login)")
        rows = self._count(schema, table) if item.get("kind") != "view" else None
        return DatasetInfo(key=item["key"], name=item["name"], kind=item.get("kind", "table"),
                           columns=columns, row_count=rows,
                           detail={"schema": schema, "table": table, "label": self.config.get("label")})

    def _columns_from_query(self, schema: str, table: str) -> list[ColumnInfo] | None:
        """A table's columns from an empty select of it, for databases without a column catalog."""
        query = sa.select(sa.text("*")).select_from(sa.table(table, schema=schema)).limit(0)
        try:
            with self._engine.connect() as conn:
                description = conn.execute(query).cursor.description
        except EntraSignInNeeded:
            raise
        except sa.exc.SQLAlchemyError:
            return None
        return [ColumnInfo(d[0], getattr(d[1], "__name__", str(d[1])), True) for d in description]

    def _source(self, schema: str, table: str):
        """The table to select from, with every column named (never SELECT *: Dataverse gives a
        SELECT * only two minutes instead of five). Reflected when the database can describe it;
        otherwise (Dataverse has no column catalog) its columns come from an empty select."""
        try:
            return sa.Table(table, sa.MetaData(), schema=schema, autoload_with=self._engine)
        except EntraSignInNeeded:
            raise
        except sa.exc.SQLAlchemyError:
            columns = self._columns_from_query(schema, table)
            if not columns:
                raise ConnectionError_(f"couldn't read the columns of {schema}.{table}") from None
            # aliased, so columns read t.name: Dataverse can't bind schema.table.column names
            return sa.table(table, *(sa.column(c.name) for c in columns), schema=schema).alias("t")

    @staticmethod
    def _since(source, since: tuple):
        value = since[1]
        if isinstance(value, dt.datetime) and value.tzinfo:  # databases mostly keep UTC without a zone
            value = value.astimezone(dt.timezone.utc).replace(tzinfo=None)
        column = source.c[since[0]]
        return column >= value if since[2] else column > value

    def read(self, key: str, batch_size: int = 1000, since: tuple | None = None, split: str | None = None):
        """Rows of one table; with `since` = (column, value, inclusive), only rows past that value.

        With `split` (or the table's saved "split" setting) the table is read in parts by that
        column (see read_in_parts), for tables too big to read in one query before it times out.
        """
        item = self._item(key)
        split = split or (item.get("split") or {}).get("column")
        if split:
            yield from self.read_in_parts(key, split, batch_size, since)
            return
        schema, table = item["schema"], item["table"]
        source = self._source(schema, table)
        query = sa.select(source)
        if since:
            query = query.where(self._since(source, since))
        try:
            with self._engine.connect() as conn:
                result = conn.execution_options(stream_results=True, yield_per=batch_size).execute(query)
                for part in result.mappings().partitions(batch_size):
                    yield [dict(row) for row in part]
        except sa.exc.SQLAlchemyError as exc:
            raise ConnectionError_(f"reading {schema}.{table} failed: {friendly(exc)}") from None

    def read_in_parts(self, key: str, column: str, batch_size: int = 1000, since: tuple | None = None):
        """Read a table as a series of ranges of `column` (a date/time or number), each its own query.

        Each part aims at a few thousand rows (fewer for wide tables): the first range is a
        guess, and each next one is sized from how many rows the last one held. A part that
        still times out is halved and tried again. Rows with no value in the column come last.
        Each part is read in full before its rows are passed on, so a part that fails and is
        tried again never passes on a row twice.
        """
        item = self._item(key)
        schema, table = item["schema"], item["table"]
        source = self._source(schema, table)
        if column not in source.c:
            raise ConnectionError_(f"{schema}.{table} has no column {column} to read it in parts by")
        col = source.c[column]
        base = [self._since(source, since)] if since else []
        target = max(1000, min(PART_MAX_ROWS, PART_CELLS // max(1, len(source.c))))
        try:
            with self._engine.connect() as conn:
                low, high = conn.execute(sa.select(sa.func.min(col), sa.func.max(col)).select_from(source).where(*base)).one()
        except sa.exc.SQLAlchemyError as exc:
            raise ConnectionError_(f"finding the range of {column} in {schema}.{table} failed: {friendly(exc)}") from None
        from eyetable import loadlog

        loadlog.step(f"Reading {schema}.{table} in parts by {column}: it runs {low} .. {high}; each part aims at "
                     f"{target:,} rows ({len(source.c)} columns)")
        parts = 0
        if low is not None:
            step = _Step(low, high, target)
            at = low
            while True:
                last = step.past(at, high)
                where = col >= at if last else sa.and_(col >= at, col < step.after(at))
                t0 = time.perf_counter()
                try:
                    rows = self._fetch(sa.select(source).where(*base, where))
                except sa.exc.SQLAlchemyError as exc:
                    if is_timeout(exc) and step.halve():
                        loadlog.emit("warn", f"Part {parts + 1} ({column} from {at}) timed out after "
                                             f"{time.perf_counter() - t0:.0f} s; trying it half as wide")
                        continue  # the same start, half as wide
                    raise ConnectionError_(f"reading {schema}.{table} (part {parts + 1}, {column} from {at}) failed: "
                                           f"{friendly(exc)}") from None
                parts += 1
                loadlog.step(f"Part {parts}: {column} from {at}" + ("" if last else f" up to {step.after(at)}") +
                             f": {len(rows):,} rows in {time.perf_counter() - t0:.1f} s")
                for i in range(0, len(rows), batch_size):
                    yield rows[i:i + batch_size]
                if last:
                    break
                at = step.after(at)
                step.adapt(len(rows))
        if not since:  # an incremental load never picks up rows without a value anyway
            try:
                rows = self._fetch(sa.select(source).where(col.is_(None)))
            except sa.exc.SQLAlchemyError as exc:
                raise ConnectionError_(f"reading {schema}.{table} (the rows with no {column}) failed: {friendly(exc)}") from None
            if rows:
                parts += 1
                for i in range(0, len(rows), batch_size):
                    yield rows[i:i + batch_size]
        self.parts_read[key] = parts

    def _fetch(self, query) -> list[dict]:
        with self._engine.connect() as conn:
            return [dict(row) for row in conn.execute(query).mappings()]

    def split_columns(self, key: str) -> list[str]:
        """Columns this table could be read in parts by, best first (see split_candidates)."""
        item = self._item(key)
        source = self._source(item["schema"], item["table"])
        if isinstance(source, sa.Table):  # reflected: the database's own types
            return split_candidates([ColumnInfo(c.name, _type_word(c.type), True) for c in source.c])
        # not describable (Dataverse): the types the driver reports for an empty select
        return split_candidates(self._columns_from_query(item["schema"], item["table"]) or [])

    def origin(self, key: str) -> str:
        item = self._item(key)
        return f"{self.config.get('label', 'database')}/{item['schema']}.{item['table']}"


# -- reading in parts ------------------------------------------------------------------

PART_CELLS = 1_000_000  # a part aims at about this many values (rows x columns): ~2,100 rows of a 467-column Dataverse table, about a minute...
PART_MAX_ROWS = 50_000  # ...and at most this many rows
_TIMEOUT_WORDS = ("timeout", "timed out", "hyt00", "hyt01", "query cancelled", "canceling statement due to statement timeout")
_CREATED = re.compile(r"creat|insert|added|entered", re.IGNORECASE)
_ID = re.compile(r"(^|_)(id|seq|sequence|number|no|num)$|id$", re.IGNORECASE)


def is_timeout(exc: BaseException) -> bool:
    """Whether a database error is a query running out of time."""
    text = str(getattr(exc, "orig", exc)).lower() + " " + str(exc).lower()
    return any(w in text for w in _TIMEOUT_WORDS)


def _type_word(type_) -> str:
    try:
        return str(type_)
    except Exception:
        return type(type_).__name__


def _kind(type_name: str) -> str | None:
    t = type_name.lower()
    if t in ("timestamp", "rowversion"):  # SQL Server's TIMESTAMP is a row version (binary), not a time
        return None
    if "date" in t or "time" in t:
        return "time"
    if any(w in t for w in ("int", "numeric", "decimal")):
        return "number"
    return None


def split_candidates(columns: list[ColumnInfo]) -> list[str]:
    """Columns a table could be read in parts by, best first.

    A creation time is best: rows never move from one part to another while the table is
    being read (a modified time can move a changed row into a later part, read twice). Then
    an increasing number (an ID), then other dates and times.
    """
    ranked = []
    for i, c in enumerate(columns):
        kind = _kind(c.type)
        if kind is None:
            continue
        if kind == "time" and _CREATED.search(c.name):
            score = 0
        elif kind == "number" and _ID.search(c.name):
            score = 1
        elif kind == "time":
            score = 2
        else:
            continue  # other numbers (amounts, counts) make poor parts
        ranked.append((score, i, c.name))
    return [name for _, _, name in sorted(ranked)]


class _Step:
    """How wide each part's range of the split column is: a time span or a number."""

    def __init__(self, low, high, target: int):
        self.target = target
        if isinstance(low, dt.datetime) or isinstance(low, dt.date):
            self.minimum = dt.timedelta(seconds=1) if isinstance(low, dt.datetime) else dt.timedelta(days=1)
            span = high - low
        elif isinstance(low, (int, Decimal, float)) and not isinstance(low, bool):
            self.minimum = 1 if isinstance(low, int) else Decimal("0.000001") if isinstance(low, Decimal) else 1e-6
            span = high - low
        else:
            raise ConnectionError_(f"can't read in parts by a column holding {type(low).__name__} values; "
                                   "choose a date, time or number column")
        # No row count to go on (counting a huge table can itself time out): start narrow, as if
        # there were a million rows; each next part grows (up to 4x) until parts hold `target` rows.
        ratio = target / 1_000_000
        self.width = self._clamp(span * (Decimal(str(ratio)) if isinstance(span, Decimal) else ratio) if span else self.minimum)

    def _clamp(self, width):
        if isinstance(self.minimum, int):
            width = int(width)
        return width if width > self.minimum else self.minimum

    def after(self, at):
        return at + self.width

    def past(self, at, high) -> bool:
        """Whether the part starting at `at` is the last (it reaches past the highest value)."""
        return at + self.width > high

    def halve(self) -> bool:
        """Make the parts half as wide; False when they can't get narrower."""
        if self.width <= self.minimum:
            return False
        self.width = self._clamp(self.width / 2)
        return True

    def adapt(self, rows: int) -> None:
        """Size the next part from how many rows this one held."""
        factor = 4.0 if not rows else min(4.0, max(0.25, self.target / rows))
        width = self.width * (Decimal(str(factor)) if isinstance(self.width, Decimal) else factor)
        self.width = self._clamp(width)
