"""The tables and views of one database connection, as a source (see eyetable.dbconnect)."""

import sqlalchemy as sa

from eyetable.connectors.database import DatabaseConnector
from eyetable.dbconnect import ConnectionError_, db_tables, friendly, list_objects, make_engine
from eyetable.models import ColumnInfo, DatasetInfo


class DbTableConnector(DatabaseConnector):
    """Chosen tables and views of one database, each loaded into its own table.

    Config: the connection ("odbc" or "url", plus an encrypted "password"), a
    display "label" (server/database), and "tables": [{key, schema, table,
    kind, name}]. A source made before it could hold several tables has
    "schema", "table" and "kind" instead, read as one table with key "data".
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
        except Exception as exc:
            raise ConnectionError_(f"couldn't read {schema}.{table}: {friendly(exc)}") from None
        if not columns:
            raise ConnectionError_(f"{schema}.{table} wasn't found (or has no columns visible to this login)")
        rows = self._count(schema, table) if item.get("kind") != "view" else None
        return DatasetInfo(key=item["key"], name=item["name"], kind=item.get("kind", "table"),
                           columns=columns, row_count=rows,
                           detail={"schema": schema, "table": table, "label": self.config.get("label")})

    def read(self, key: str, batch_size: int = 1000):
        item = self._item(key)
        try:
            yield from super().read(f"{item['schema']}.{item['table']}", batch_size)
        except sa.exc.SQLAlchemyError as exc:
            raise ConnectionError_(f"reading {item['schema']}.{item['table']} failed: {friendly(exc)}") from None

    def origin(self, key: str) -> str:
        item = self._item(key)
        return f"{self.config.get('label', 'database')}/{item['schema']}.{item['table']}"
