"""Database sources from a pasted connection string, one source per table or view.

Accepted connection strings:

  * ODBC, e.g. SQL Server / Azure SQL:
      Driver={ODBC Driver 18 for SQL Server};Server=tcp:x.database.windows.net,1433;
      Database=db;Uid=user;Pwd={secret};Encrypt=yes;TrustServerCertificate=no;
  * a SQLAlchemy URL, e.g. postgresql+psycopg://user:secret@host:5432/db

The password (Pwd=/Password= in ODBC, or the URL's) is taken out of the stored
string and kept encrypted with EYETABLE_SECRET_KEY; it is put back only to
open a connection. If the ODBC driver the string names isn't installed, the
nearest installed version of the same driver is used (e.g. 18 -> 17).

Listing reads the database's own catalog (sys.objects + sys.partitions on SQL
Server, pg_class on Postgres, information schema elsewhere) so it is quick
even for large databases; row counts are the database's estimates.
"""

import re
import urllib.parse
from dataclasses import dataclass, field
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import make_url

from eyetable.credentials import encrypt_secret
from eyetable.identifiers import shorten, sanitize, source_name

DATASET_KEY = "data"
_PASSWORD_KEYS = {"pwd", "password"}
_DRIVER_FAMILY = re.compile(r"^ODBC Driver (\d+) for SQL Server$", re.I)


class ConnectionError_(ValueError):
    """A connection string that can't be used (named to avoid shadowing the builtin)."""


# -- ODBC strings -------------------------------------------------------------------

def parse_odbc(text: str) -> list[tuple[str, str]]:
    """key=value pairs of an ODBC connection string; values may be {braced} (}} escapes })."""
    pairs, i, n = [], 0, len(text)
    while i < n:
        while i < n and text[i] in "; \t\r\n":
            i += 1
        if i >= n:
            break
        eq = text.find("=", i)
        if eq < 0:
            raise ConnectionError_(f"expected key=value near {text[i:i + 20]!r}")
        key = text[i:eq].strip()
        i = eq + 1
        if i < n and text[i] == "{":
            i += 1
            value = []
            while i < n:
                if text[i] == "}":
                    if i + 1 < n and text[i + 1] == "}":
                        value.append("}")
                        i += 2
                        continue
                    i += 1
                    break
                value.append(text[i])
                i += 1
            else:
                raise ConnectionError_(f"the value of {key} has an opening {{ without a closing }}")
            val = "".join(value)
        else:
            end = text.find(";", i)
            end = n if end < 0 else end
            val = text[i:end].strip()
            i = end
        if not key:
            raise ConnectionError_("a key is missing before an =")
        pairs.append((key, val))
    return pairs


def _odbc_value(value: str) -> str:
    return "{" + value.replace("}", "}}") + "}" if re.search(r"[;{}\s=]", value) or value != value.strip() else value


def build_odbc(pairs: list[tuple[str, str]]) -> str:
    return ";".join(f"{k}={_odbc_value(v)}" for k, v in pairs) + ";"


def _get(pairs: list[tuple[str, str]], *keys: str) -> str | None:
    wanted = {k.lower() for k in keys}
    for k, v in pairs:
        if k.lower() in wanted:
            return v
    return None


def installed_driver(requested: str) -> tuple[str, str | None]:
    """(driver to use, warning). Falls back to another installed version of the same driver."""
    try:
        import pyodbc
    except ImportError:
        raise ConnectionError_("pyodbc isn't installed on the eyetable server (pip install pyodbc)") from None
    available = pyodbc.drivers()
    if requested in available:
        return requested, None
    family = _DRIVER_FAMILY.match(requested or "")
    if family:
        versions = sorted(
            (int(m.group(1)), d) for d in available if (m := _DRIVER_FAMILY.match(d))
        )
        if versions:
            wanted = int(family.group(1))
            best = min(versions, key=lambda v: (abs(v[0] - wanted), -v[0]))[1]
            return best, f"{requested} isn't installed on the eyetable server, so {best} is used instead."
    raise ConnectionError_(
        f"the ODBC driver {requested!r} isn't installed on the eyetable server "
        f"(installed: {', '.join(available) or 'none'})"
    )


# -- prepared connections ---------------------------------------------------------------

@dataclass
class Prepared:
    config: dict[str, Any]  # stored in each source: no plain password
    password: str | None  # the plain password, for this request only
    server: str
    database: str
    warnings: list[str] = field(default_factory=list)

    @property
    def label(self) -> str:
        return f"{self.server}/{self.database}" if self.database else self.server


def prepare(text: str, secret_key: str | None) -> Prepared:
    text = (text or "").strip()
    if not text:
        raise ConnectionError_("paste a connection string")
    if "://" in text.split(";")[0]:
        try:
            url = make_url(text)
        except Exception as exc:
            raise ConnectionError_(f"that URL couldn't be read ({exc})") from None
        password = url.password
        # URL.set(password=None) means "unchanged", so drop it explicitly
        stored = url._replace(password=None).render_as_string(hide_password=False)
        config = {"url": stored}
        server = url.host or url.drivername
        database = url.database or ""
    else:
        pairs = parse_odbc(text)
        if not _get(pairs, "driver"):
            raise ConnectionError_("the connection string has no Driver={...}; or paste a URL like postgresql://…")
        password = _get(pairs, *_PASSWORD_KEYS)
        kept = [(k, v) for k, v in pairs if k.lower() not in _PASSWORD_KEYS]
        config = {"odbc": build_odbc(kept)}
        server = re.sub(r"^tcp:", "", _get(pairs, "server", "address", "addr", "data source") or "?", flags=re.I)
        server = re.sub(r",\d+$", "", server)  # the port isn't worth showing
        database = _get(pairs, "database", "initial catalog") or ""
    if password and re.fullmatch(r"\s*(your_password_here|<password>|\*+|password)\s*", password, re.I):
        raise ConnectionError_("the connection string still has a placeholder password; put the real one in")
    if password:
        config["password"] = encrypt_secret(password, secret_key)
    return Prepared(config, password or None, server, database)


def make_engine(config: dict[str, Any], password: str | None = None, login_timeout: int = 20) -> tuple[sa.Engine, list[str]]:
    """An engine for a (decrypted) connection config, and any warnings."""
    password = password if password is not None else config.get("password")
    warnings: list[str] = []
    if "odbc" in config:
        pairs = parse_odbc(config["odbc"])
        driver = _get(pairs, "driver")
        use, warning = installed_driver(driver)
        if warning:
            warnings.append(warning)
        pairs = [(k, use if k.lower() == "driver" else v) for k, v in pairs]
        if password:
            pairs.append(("PWD", password))
        odbc = build_odbc(pairs)
        engine = sa.create_engine(
            "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc),
            connect_args={"timeout": login_timeout}, pool_pre_ping=True,
        )
        return engine, warnings
    url = make_url(config["url"])
    if password:
        url = url.set(password=password)
    return sa.create_engine(url, pool_pre_ping=True), warnings


_NOISE = {"invalid connection string attribute", "login timeout expired"}


def friendly(exc: Exception) -> str:
    """A database driver error as a readable sentence or two, with a hint for common causes."""
    orig = getattr(exc, "orig", exc)
    args = getattr(orig, "args", ())
    # pyodbc errors are (sqlstate, message); the message is what matters
    text = args[1] if len(args) >= 2 and isinstance(args[1], str) else (str(orig) or type(exc).__name__)
    text = re.sub(r"\(Background on this error.*", "", text, flags=re.S)
    text = re.sub(r"\[[^\]]*\]", "", text)  # [Microsoft][ODBC Driver 17 for SQL Server] prefixes
    text = re.sub(r"\(\d+\)|\(SQL\w+\)", "", text)  # native error numbers, (SQLDriverConnect)
    parts: list[str] = []
    for part in re.split(r";|\r?\n|\\r\\n", text):
        part = part.strip(" .\t")
        if part and part.lower() not in _NOISE and part not in parts:
            parts.append(part)
    text = ". ".join(parts) or type(exc).__name__
    lowered = text.lower()
    if "client with ip address" in lowered and "not allowed" in lowered:
        text += ". Add this IP to the server's firewall rules (in Azure: the SQL server's Networking page)."
    elif "login failed" in lowered:
        text += ". Check the user name (Uid) and password (Pwd)."
    elif "not found or not accessible" in lowered or "timed out" in lowered or "could not translate host" in lowered:
        text += (". Check the server name and port, that the server accepts remote (TCP) connections, "
                 "and that no firewall blocks this PC.")
    return text[:600]


def list_objects(engine: sa.Engine) -> list[dict[str, Any]]:
    """Every user table and view: schema, name, kind ('table'/'view') and estimated rows."""
    from eyetable.connectors.database import is_system_schema
    dialect = engine.dialect.name
    try:
        with engine.connect() as conn:
            if dialect == "mssql":
                rows = conn.execute(sa.text("""
                    SELECT s.name, o.name, CASE o.type WHEN 'V' THEN 'view' ELSE 'table' END,
                           (SELECT SUM(p.rows) FROM sys.partitions p WHERE p.object_id = o.object_id AND p.index_id IN (0, 1))
                    FROM sys.objects o JOIN sys.schemas s ON s.schema_id = o.schema_id
                    WHERE o.type IN ('U', 'V') AND o.is_ms_shipped = 0
                    ORDER BY s.name, o.name
                """)).all()
            elif dialect == "postgresql":
                rows = conn.execute(sa.text("""
                    SELECT n.nspname, c.relname, CASE c.relkind WHEN 'v' THEN 'view' WHEN 'm' THEN 'view' ELSE 'table' END,
                           CASE WHEN c.relkind IN ('r', 'p') AND c.reltuples >= 0 THEN c.reltuples::bigint END
                    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relkind IN ('r', 'p', 'v', 'm') AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                      AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%'
                    ORDER BY n.nspname, c.relname
                """)).all()
            else:
                insp = sa.inspect(conn)
                rows = []
                for schema in insp.get_schema_names():
                    if is_system_schema(schema):
                        continue
                    rows += [(schema, t, "table", None) for t in insp.get_table_names(schema=schema)]
                    rows += [(schema, v, "view", None) for v in insp.get_view_names(schema=schema)]
    except Exception as exc:
        raise ConnectionError_(f"couldn't connect: {friendly(exc)}") from None
    return [
        {"schema": s, "name": n, "kind": k, "rows": int(r) if r is not None else None}
        for s, n, k, r in rows if not is_system_schema(s)
    ]


def suggested_name(database: str, schema: str, name: str) -> str:
    parts = [database] if database else []
    if schema.lower() not in ("dbo", "public", "main"):
        parts.append(schema)
    parts.append(name)
    return source_name("_".join(sanitize(p) for p in parts))


def source_config(prepared: Prepared, schema: str, table: str, kind: str) -> dict[str, Any]:
    return {**prepared.config, "schema": schema, "table": table, "kind": kind, "label": prepared.label}


def shorten_label(label: str) -> str:
    return shorten(label, 120)
