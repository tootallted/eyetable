"""One table or view of a database as a source (see eyetable.dbconnect)."""

import sqlalchemy as sa

from eyetable.connectors.database import DatabaseConnector
from eyetable.dbconnect import DATASET_KEY, ConnectionError_, friendly, make_engine
from eyetable.models import ColumnInfo, DatasetInfo


class DbTableConnector(DatabaseConnector):
    """One table or view of a database, as a single-table source.

    Config: the connection ("odbc" or "url", plus an encrypted "password"),
    "schema", "table", "kind" and a display "label" (server/database).
    """

    type = "dbtable"

    def __init__(self, config: dict):
        self.config = config
        self._engine, self.warnings = make_engine(config)

    def test(self) -> None:
        try:
            with self._engine.connect():
                pass
        except Exception as exc:
            raise ConnectionError_(f"couldn't connect: {friendly(exc)}") from None

    def discover(self) -> list[DatasetInfo]:
        schema, table = self.config["schema"], self.config["table"]
        try:
            insp = sa.inspect(self._engine)
            columns = [ColumnInfo(c["name"], str(c["type"]), bool(c.get("nullable", True)))
                       for c in insp.get_columns(table, schema=schema)]
        except sa.exc.NoSuchTableError:
            columns = []
        except Exception as exc:
            raise ConnectionError_(f"couldn't read {schema}.{table}: {friendly(exc)}") from None
        if not columns:
            raise ConnectionError_(f"{schema}.{table} wasn't found (or has no columns visible to this login)")
        rows = self._count(schema, table) if self.config.get("kind") != "view" else None
        return [DatasetInfo(key=DATASET_KEY, name=DATASET_KEY, kind=self.config.get("kind", "table"),
                            columns=columns, row_count=rows,
                            detail={"schema": schema, "table": table, "label": self.config.get("label")})]

    def read(self, key: str, batch_size: int = 1000):
        if key != DATASET_KEY:
            raise KeyError(f"a database table source has one dataset, {DATASET_KEY!r}")
        try:
            yield from super().read(f"{self.config['schema']}.{self.config['table']}", batch_size)
        except sa.exc.SQLAlchemyError as exc:
            raise ConnectionError_(f"reading {self.config['schema']}.{self.config['table']} failed: {friendly(exc)}") from None

    def origin(self, key: str) -> str:
        return f"{self.config.get('label', 'database')}/{self.config['schema']}.{self.config['table']}"
