"""Destinations: the databases a source's data can be loaded into.

Every source loads into exactly one destination. The built-in one, "eyetable", is
eyetable's own Postgres database (EYETABLE_DATABASE_URL, data schema eyetable_raw),
where everything has loaded so far. Others are added in the page's Destinations
panel from a connection string, the same kinds the Load data panel takes:

  * SQL Server on this PC:
      Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=eyetable;Trusted_Connection=yes;Encrypt=no;
  * another Postgres: postgresql://user:password@host:5432/db

Each has a schema its tables go into (dbo by default on SQL Server). A password in
the string is taken out and kept encrypted (EYETABLE_SECRET_KEY), as for database
sources. A source names its destination in its config ("destination"); none means
the built-in one. A destination in use by a source can't be removed.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Any

import sqlalchemy as sa

from eyetable import dbconnect
from eyetable.credentials import decrypt_secrets
from eyetable.store import CoreStore

BUILTIN = "eyetable"
NAME = re.compile(r"^[a-z][a-z0-9_]{0,39}$")
SCHEMA = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


class DestinationError(ValueError):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def kind_of(config: dict[str, Any]) -> str:
    """What kind of database a connection config is for, in words."""
    if "odbc" in config:
        driver = dbconnect._get(dbconnect.parse_odbc(config["odbc"]), "driver") or ""
        return "SQL Server" if "sql server" in driver.lower() else driver or "ODBC"
    name = sa.engine.make_url(config["url"]).get_backend_name()
    return {"postgresql": "Postgres", "mysql": "MySQL", "mssql": "SQL Server", "sqlite": "SQLite"}.get(name, name)


def destination_of(config: dict[str, Any] | None) -> str:
    """The destination a source's config routes it to."""
    return (config or {}).get("destination") or BUILTIN


class Destinations:
    def __init__(self, store: CoreStore, secret_key: str | None):
        self.store = store
        self.secret_key = secret_key

    # -- reading -------------------------------------------------------------------

    def builtin(self) -> dict[str, Any]:
        url = self.store.db.url
        return {
            "id": BUILTIN, "name": BUILTIN, "builtin": True,
            "kind": {"postgresql": "Postgres", "sqlite": "SQLite"}.get(url.get_backend_name(), url.get_backend_name()),
            "server": url.host or url.get_backend_name(), "database": url.database or "",
            "schema": self.store.data_schema or "", "created_at": None, "updated_at": None,
        }

    def _summary(self, row: Any) -> dict[str, Any]:
        config = row.config or {}
        info = self._where(config)
        return {"id": row.id, "name": row.id, "builtin": False, "kind": kind_of(config), "server": info[0],
                "database": info[1], "schema": row.schema_name, "has_password": "password" in config,
                "created_at": row.created_at, "updated_at": row.updated_at}

    @staticmethod
    def _where(config: dict[str, Any]) -> tuple[str, str]:
        if "odbc" in config:
            pairs = dbconnect.parse_odbc(config["odbc"])
            server = re.sub(r"^tcp:", "", dbconnect._get(pairs, "server", "address", "addr", "data source") or "?", flags=re.I)
            return server, dbconnect._get(pairs, "database", "initial catalog") or ""
        url = sa.engine.make_url(config["url"])
        return url.host or url.get_backend_name(), url.database or ""

    def list(self) -> list[dict[str, Any]]:
        """Every destination, the built-in one first, each with the sources routed to it."""
        routed: dict[str, list[str]] = {}
        for src in self.store.list_sources():
            routed.setdefault(destination_of(src.config), []).append(src.name)
        out = [self.builtin()] + [self._summary(r) for r in self.store.list_destinations()]
        for d in out:
            d["sources"] = sorted(routed.get(d["id"], []))
        return out

    def get(self, dest_id: str) -> dict[str, Any]:
        for d in self.list():
            if d["id"] == dest_id:
                return d
        raise DestinationError(f"there's no destination named {dest_id}", 404)

    # -- changing ------------------------------------------------------------------

    def _clean(self, name: str, schema: str | None, kind: str) -> tuple[str, str]:
        name = (name or "").strip().lower()
        if not NAME.match(name):
            raise DestinationError("a destination's name is lowercase letters, digits and _, starting with a letter (40 at most)")
        schema = (schema or "").strip() or ("dbo" if kind == "SQL Server" else "public")
        if not SCHEMA.match(schema):
            raise DestinationError("the schema is letters, digits and _, starting with a letter or _")
        return name, schema

    def _prepare(self, connection: str) -> dict[str, Any]:
        try:
            prepared = dbconnect.prepare(connection, self.secret_key)
        except dbconnect.ConnectionError_ as exc:
            raise DestinationError(str(exc)) from None
        except ValueError as exc:  # no EYETABLE_SECRET_KEY to keep a password with
            raise DestinationError(str(exc)) from None
        if prepared.config.get("entra"):
            raise DestinationError("a destination can't sign in with Microsoft Entra MFA yet; use a SQL login or Windows authentication")
        return prepared.config

    def add(self, name: str, connection: str, schema: str | None = None) -> dict[str, Any]:
        config = self._prepare(connection)
        name, schema = self._clean(name, schema, kind_of(config))
        if name == BUILTIN or self.store.get_destination(name):
            raise DestinationError(f"there's already a destination named {name}", 409)
        self.store.save_destination(name, config, schema)
        return self.get(name)

    def update(self, dest_id: str, name: str | None = None, connection: str | None = None,
               schema: str | None = None) -> dict[str, Any]:
        """Change a destination's name, connection string (blank keeps it) or schema. Renaming it
        moves the sources routed to it along."""
        if dest_id == BUILTIN:
            raise DestinationError("the built-in eyetable destination is set in .env (EYETABLE_DATABASE_URL)")
        row = self.store.get_destination(dest_id)
        if row is None:
            raise DestinationError(f"there's no destination named {dest_id}", 404)
        config = self._prepare(connection) if (connection or "").strip() else row.config
        new_name, schema = self._clean(name or dest_id, schema if schema is not None else row.schema_name, kind_of(config))
        if new_name != dest_id and (new_name == BUILTIN or self.store.get_destination(new_name)):
            raise DestinationError(f"there's already a destination named {new_name}", 409)
        self.store.save_destination(new_name, config, schema, replaces=dest_id)
        if new_name != dest_id:
            for src in self.store.list_sources():
                if destination_of(src.config) == dest_id:
                    self.store.save_source(src.name, src.type, {**src.config, "destination": new_name})
        return self.get(new_name)

    def remove(self, dest_id: str) -> dict[str, Any]:
        d = self.get(dest_id)
        if d["builtin"]:
            raise DestinationError("the built-in eyetable destination can't be removed")
        if d["sources"]:
            raise DestinationError(f"{dest_id} still has sources routed to it: {', '.join(d['sources'])}; "
                                   "route them elsewhere first", 409)
        self.store.delete_destination(dest_id)
        return d

    # -- checking ------------------------------------------------------------------

    def test(self, dest_id: str | None = None, connection: str | None = None, schema: str | None = None) -> dict[str, Any]:
        """Connect and report what's there: the server's version, the database, whether the schema
        exists and whether eyetable may create tables in it. Changes nothing."""
        if connection and connection.strip():
            config = self._prepare(connection)
        elif dest_id == BUILTIN or dest_id is None:
            return self._test_engine(self.store.db, "Postgres", schema or self.store.data_schema or "public", dispose=False)
        else:
            row = self.store.get_destination(dest_id)
            if row is None:
                raise DestinationError(f"there's no destination named {dest_id}", 404)
            config, schema = row.config, schema or row.schema_name
        kind = kind_of(config)
        schema = (schema or "").strip() or ("dbo" if kind == "SQL Server" else "public")
        try:
            engine, warnings = dbconnect.make_engine(decrypt_secrets(config, self.secret_key), login_timeout=10)
        except dbconnect.ConnectionError_ as exc:
            return {"ok": False, "message": str(exc)}
        result = self._test_engine(engine, kind, schema)
        if warnings:
            result["warnings"] = warnings
        return result

    def _test_engine(self, engine: sa.Engine, kind: str, schema: str, dispose: bool = True) -> dict[str, Any]:
        started = dt.datetime.now()
        try:
            with engine.connect() as conn:
                if engine.dialect.name == "mssql":
                    version = conn.execute(sa.text("SELECT CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(64)), "
                                                   "CAST(SERVERPROPERTY('Edition') AS nvarchar(128))")).one()
                    version = f"SQL Server {version[0]} ({version[1]})"
                    database = conn.execute(sa.text("SELECT DB_NAME()")).scalar()
                    login = conn.execute(sa.text("SELECT SUSER_SNAME()")).scalar()
                    exists = conn.execute(sa.text("SELECT SCHEMA_ID(:s)"), {"s": schema}).scalar() is not None
                    can_create = bool(conn.execute(sa.text(
                        "SELECT HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE TABLE') * "
                        "ISNULL(HAS_PERMS_BY_NAME(:s, 'SCHEMA', 'ALTER'), 0)"), {"s": schema}).scalar())
                elif engine.dialect.name == "postgresql":
                    version = conn.execute(sa.text("SHOW server_version")).scalar()
                    version = f"Postgres {version}"
                    database, login = conn.execute(sa.text("SELECT current_database(), current_user")).one()
                    exists = conn.execute(sa.text("SELECT 1 FROM pg_namespace WHERE nspname = :s"), {"s": schema}).scalar() is not None
                    can_create = bool(exists and conn.execute(sa.text("SELECT has_schema_privilege(:s, 'CREATE')"), {"s": schema}).scalar())
                else:
                    version, database, login, exists, can_create = engine.dialect.name, "", "", True, True
        except Exception as exc:
            return {"ok": False, "message": dbconnect.friendly(exc)}
        finally:
            if dispose:
                engine.dispose()
        problems = []
        if not exists:
            problems.append(f"the schema {schema} doesn't exist in {database} yet")
        elif not can_create:
            problems.append(f"{login} may not create tables in {schema}")
        return {"ok": not problems, "kind": kind, "version": version, "database": database, "login": login,
                "schema": schema, "schema_exists": exists, "can_create": can_create,
                "ms": int((dt.datetime.now() - started).total_seconds() * 1000),
                "message": "; ".join(problems) or f"Connected to {database} as {login}; tables can be created in {schema}."}
