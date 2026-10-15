"""Any database SQLAlchemy can talk to: Postgres, SQL Server, MySQL, SQLite, ...

Config is either a full URL:
    {"url": "postgresql+psycopg://user:${PG_PASSWORD}@host:5432/db"}
or the parts of one:
    {"driver": "mssql+pyodbc", "host": "...", "port": 1433, "database": "...",
     "username": "...", "password": "${SQL_PASSWORD}",
     "query": {"driver": "ODBC Driver 18 for SQL Server"}}
Optional: "schemas" (list to restrict discovery), "count_rows" (default true).
"""

from collections.abc import Iterator

import sqlalchemy as sa
from sqlalchemy.engine import URL, make_url

from eyetable.connectors.base import Connector, Record
from eyetable.models import ColumnInfo, DatasetInfo

_SYSTEM_SCHEMAS = {
    "information_schema",
    "pg_catalog",
    "pg_toast",
    "sys",
    "guest",
    "mysql",
    "performance_schema",
    "temp",
}
_SYSTEM_PREFIXES = ("pg_temp_", "pg_toast_temp_", "db_")


def is_system_schema(name: str) -> bool:
    lowered = name.lower()
    return lowered in _SYSTEM_SCHEMAS or lowered.startswith(_SYSTEM_PREFIXES)


def build_url(config: dict) -> URL:
    if config.get("url"):
        return make_url(config["url"])
    if not config.get("driver"):
        raise ValueError("database source needs either 'url' or 'driver' (e.g. postgresql+psycopg)")
    return URL.create(
        drivername=config["driver"],
        username=config.get("username"),
        password=config.get("password"),
        host=config.get("host"),
        port=config.get("port"),
        database=config.get("database"),
        query=config.get("query") or {},
    )


def _type_name(type_: sa.types.TypeEngine) -> str:
    try:
        return str(type_)
    except Exception:
        return type(type_).__name__


class DatabaseConnector(Connector):
    type = "database"

    def __init__(self, config: dict):
        super().__init__(config)
        self._engine = sa.create_engine(build_url(config), pool_pre_ping=True)

    def close(self) -> None:
        self._engine.dispose()

    def test(self) -> None:
        with self._engine.connect():
            pass

    def discover(self) -> list[DatasetInfo]:
        insp = sa.inspect(self._engine)
        default_schema = insp.default_schema_name
        schemas = self.config.get("schemas") or [
            s for s in insp.get_schema_names() if not is_system_schema(s)
        ]
        count_rows = self.config.get("count_rows", True)

        datasets: list[DatasetInfo] = []
        for schema in schemas:
            listings = (
                ("table", insp.get_table_names(schema=schema)),
                ("view", insp.get_view_names(schema=schema)),
            )
            for kind, names in listings:
                for name in names:
                    columns = [
                        ColumnInfo(c["name"], _type_name(c["type"]), bool(c.get("nullable", True)))
                        for c in insp.get_columns(name, schema=schema)
                    ]
                    datasets.append(
                        DatasetInfo(
                            key=f"{schema}.{name}",
                            name=name if schema == default_schema else f"{schema}_{name}",
                            kind=kind,
                            columns=columns,
                            row_count=self._count(schema, name) if count_rows else None,
                            detail={"schema": schema, "table": name},
                        )
                    )
        return datasets

    def _count(self, schema: str, name: str) -> int | None:
        query = sa.select(sa.func.count()).select_from(sa.table(name, schema=schema))
        try:
            with self._engine.connect() as conn:
                return conn.execute(query).scalar_one()
        except sa.exc.DBAPIError:
            return None

    def read(self, key: str, batch_size: int = 1000) -> Iterator[list[Record]]:
        schema, _, name = key.partition(".")
        if not name:
            raise KeyError(f"dataset key must be 'schema.table', got {key!r}")
        table = sa.Table(name, sa.MetaData(), schema=schema, autoload_with=self._engine)
        with self._engine.connect() as conn:
            result = conn.execution_options(stream_results=True, yield_per=batch_size).execute(
                sa.select(table)
            )
            for part in result.mappings().partitions(batch_size):
                yield [dict(row) for row in part]
