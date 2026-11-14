"""Which table each of a source's datasets loads into.

By default a source's table is named after it: `<source>__<dataset>` (`ledger__data`)
in the data schema. A table can be given a name of its own, in a schema of its own
(`finance.ledger`), kept in the source's config as its "target": the config's
"target" for a one-table source, each table entry's "target" for a database source.

Tables are named by "refs": a plain name for a table in the data schema, or
`schema.name` (see CoreStore.split).
"""

from __future__ import annotations

import re
from typing import Any

from .connectors.file import DATASET_KEY
from .identifiers import table_name

NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


def entries(src: Any) -> list[dict[str, Any]]:
    """A source's tables: [{key, name, target, ref}] (target: its own name, if it has one)."""
    if src.type == "dbtable":
        from . import dbconnect

        items = dbconnect.db_tables(src.config)
        return [{"key": i["key"], "name": i["name"], "target": i.get("target"),
                 "ref": i.get("target") or table_name(src.name, i["name"])} for i in items]
    target = src.config.get("target")
    return [{"key": DATASET_KEY, "name": DATASET_KEY, "target": target, "ref": target or table_name(src.name, DATASET_KEY)}]


def table_for(src: Any, name: str = DATASET_KEY) -> str:
    """The table a source's dataset `name` loads into."""
    for e in entries(src):
        if e["name"] == name:
            return e["ref"]
    return table_name(src.name, name)


def tables(src: Any) -> list[str]:
    return [e["ref"] for e in entries(src)]


def parse(text: str, data_schema: str | None, meta_schema: str | None) -> tuple[str | None, str]:
    """A table name as typed ("finance.ledger", "eyetable_raw.ledger" or "ledger") -> (schema, name).
    Raises ValueError for a name that can't be a table."""
    text = (text or "").strip().strip('"')
    if not text:
        raise ValueError("give the table a name")
    schema, _, name = text.rpartition(".")
    schema = schema or None
    if schema == "eyetable_raw" and data_schema is None:  # the page shows plain tables as eyetable_raw.x
        schema = None
    for part, what in ((schema, "schema"), (name, "table")):
        if part is not None and not NAME.match(part):
            raise ValueError(f"{part!r} can't be a {what} name: use letters, digits and _, starting with a letter or _ "
                             f"(63 at most)")
    if schema and schema.lower() != schema or name.lower() != name:
        raise ValueError("use lowercase names, so the table can be queried without quoting it")
    if schema and data_schema is None:
        raise ValueError("this database has no schemas; give just the table name")
    if schema and (schema == meta_schema or schema.startswith("pg_") or schema == "information_schema"):
        raise ValueError(f"{schema} is reserved; choose another schema")
    if schema and schema.endswith("_archived"):
        raise ValueError(f"{schema} holds archived tables; choose another schema")
    if name.endswith("__stg"):
        raise ValueError("a table name can't end in __stg (eyetable's staging tables do)")
    return (schema or data_schema), name
