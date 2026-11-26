"""Saving queries as views (or procedures) from the query window, and editing them there.

The query window itself only reads. Saving goes through a separate Postgres role,
eyetable_author (EYETABLE_AUTHOR_DATABASE_URL, made by scripts/setup-postgres.ps1),
that can read the data schemas and write only in its own schema, eyetable_views
(EYETABLE_VIEWS_SCHEMA). It can't see eyetable's own tables (sources, credentials,
runs), so nothing saved here can reach them either.

* A plain query (one SELECT, WITH, VALUES or TABLE statement that changes nothing)
  is saved as a view: CREATE VIEW eyetable_views.<name> WITH (security_invoker = true),
  so it reads with the rights of whoever queries it.
* Anything else (INSERT, UPDATE, DELETE, several statements, a cursor, SELECT INTO,
  CREATE TABLE, ...) is saved as a procedure: CREATE PROCEDURE eyetable_views.<name>()
  LANGUAGE plpgsql, its body the SQL as written (wrapped in BEGIN ... END unless it
  already starts with DECLARE or BEGIN). The page can run it (CALL) as eyetable_author.
* A source's name works in the query window (FROM parts) through a temporary view;
  saving rewrites FROM/JOIN <source> to the real table, since a saved view can't
  depend on a temporary one.
* Opening a saved object puts its CREATE OR REPLACE statement in the query window;
  deploying runs the edited statement as eyetable_author.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any

import sqlalchemy as sa
from sqlalchemy.pool import NullPool

from eyetable.query import READ_KEYWORDS, clean, first_keyword
from eyetable.store import CoreStore

NAME = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
DEFAULT_SCHEMA = "eyetable_views"
# Words that make SQL more than one read-only query, so it can't be a view.
_NOT_A_VIEW = re.compile(
    r"\b(insert|update|delete|merge|truncate|create|drop|alter|grant|revoke|call|do|copy|declare|fetch|"
    r"open|close|move|into|perform|execute|raise|loop|begin|commit|rollback|vacuum|analyze|lock|set|reset|notify)\b",
    re.I)
_DEFINITION = re.compile(r"^\s*create\s+(?:or\s+replace\s+)?(view|procedure)\s+((?:\"[^\"]+\"|[\w$]+)(?:\s*\.\s*(?:\"[^\"]+\"|[\w$]+))?)",
                         re.I)
_DOLLAR_TAG = "$eyetable$"

# as_of: a history table's rows as they were at a time, queried like a table:
#   SELECT * FROM as_of(NULL::eyetable_raw.parts__data, '2026-11-14')
# The first argument is the table, as a typed NULL: its row type tells Postgres the columns (a
# table name in quotes can't). A row counts if it was current then: ET_fromdate <= t < ET_todate
# (not BETWEEN, which would also catch the version that ended at that very moment). It runs with
# the caller's rights. Made (again) in the views schema whenever eyetable starts (Author.setup).
AS_OF_SQL = """
CREATE OR REPLACE FUNCTION {schema}.as_of(_table anyelement, _at timestamp)
RETURNS SETOF anyelement
LANGUAGE plpgsql STABLE
AS $fn$
BEGIN
  RETURN QUERY EXECUTE format(
    'SELECT * FROM %s WHERE "ET_fromdate" <= $1 AND "ET_todate" > $1',
    pg_typeof(_table))
  USING _at;
END
$fn$
"""
AS_OF_COMMENT = ("COMMENT ON FUNCTION {schema}.as_of(anyelement, timestamp) IS 'Rows of a history table as they were "
                 "at a time: SELECT * FROM as_of(NULL::schema.table, ''2026-11-14'')'")


class AuthorError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def mask(sql: str) -> str:
    """The SQL with string literals, quoted names, dollar-quoted bodies and comments blanked
    out (same length, so positions still match), for looking at its keywords."""
    out = list(sql)
    i, n = 0, len(sql)

    def blank(a: int, b: int) -> None:
        for k in range(a, min(b, n)):
            if out[k] != "\n":
                out[k] = " "

    while i < n:
        c = sql[i]
        if sql.startswith("--", i):
            j = sql.find("\n", i)
            j = n if j < 0 else j
            blank(i, j)
            i = j
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            j = n if j < 0 else j + 2
            blank(i, j)
            i = j
        elif c in ("'", '"'):
            j = i + 1
            while j < n:
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:  # '' or "" inside
                        j += 2
                        continue
                    break
                j += 1
            blank(i, j + 1)
            i = j + 1
        elif c == "$":
            m = re.match(r"\$([A-Za-z_]\w*)?\$", sql[i:])
            if m:
                tag = m.group(0)
                j = sql.find(tag, i + len(tag))
                j = n if j < 0 else j + len(tag)
                blank(i, j)
                i = j
            else:
                i += 1
        else:
            i += 1
    return "".join(out)


def kind_for(sql: str) -> str:
    """"view" for one read-only query, "procedure" for anything else."""
    sql = clean(sql)
    masked = mask(sql)
    if ";" in masked or first_keyword(masked) not in READ_KEYWORDS or _NOT_A_VIEW.search(masked):
        return "procedure"
    return "view"


def definition_target(sql: str) -> tuple[str, str | None, str] | None:
    """For CREATE [OR REPLACE] VIEW|PROCEDURE [schema.]name ...: (kind, schema, name); else None."""
    m = _DEFINITION.match(mask(sql))
    if not m:
        return None
    raw = sql[m.start(2):m.end(2)]
    parts = [p.strip().strip('"') for p in re.split(r"\s*\.\s*", raw, maxsplit=1)]
    schema, name = (parts[0], parts[1]) if len(parts) == 2 else (None, parts[0])
    if not raw.strip().startswith('"'):
        name = name.lower()
    return m.group(1).lower(), schema.lower() if schema and '"' not in raw else schema, name


def rewrite_aliases(sql: str, aliases: dict[str, str], qualified) -> str:
    """FROM parts / JOIN parts (a source's name, which the query window turns into a temporary
    view) become the real table, e.g. eyetable_raw.parts__data."""
    if not aliases:
        return sql
    masked = mask(sql)
    names = sorted(aliases, key=len, reverse=True)
    pattern = re.compile(r"\b(from|join)(\s+)(" + "|".join(re.escape(a) for a in names) + r")\b(?!\s*\.)", re.I)
    out, last = [], 0
    for m in pattern.finditer(masked):
        alias = sql[m.start(3):m.end(3)]
        table = aliases.get(alias) or aliases.get(alias.lower())
        if not table:
            continue
        out.append(sql[last:m.start(3)])
        out.append(qualified(table))
        last = m.end(3)
    out.append(sql[last:])
    return "".join(out)


@dataclass
class Saved:
    name: str
    kind: str  # "view" | "procedure" | "table"
    ref: str  # schema.name, how to query or call it

    def as_dict(self) -> dict[str, Any]:
        return {"name": self.name, "kind": self.kind, "ref": self.ref}


class Author:
    """Saves, lists, opens, deploys, runs and drops the objects in the views schema, as the
    author role. `reader` is the query window's role, which is given read access to them."""

    def __init__(self, store: CoreStore, url: str, schema: str = DEFAULT_SCHEMA, reader: str | None = None,
                 timeout_s: float = 300):
        if not re.match(r"^[a-z_][a-z0-9_]*$", schema):
            raise AuthorError("EYETABLE_VIEWS_SCHEMA must be lowercase letters, digits and _")
        self.store = store
        self.db = sa.create_engine(url, poolclass=NullPool)
        self.role = sa.make_url(url).username
        self.schema = schema
        self.reader = reader
        self.timeout_ms = int(timeout_s * 1000)
        self.problem: str | None = None  # why saving can't work right now (shown in the page)
        if self.db.dialect.name != "postgresql":
            raise AuthorError("saving views and procedures needs Postgres")

    # -- setup -------------------------------------------------------------------

    def setup(self) -> None:
        """Let the author role read the data schemas (granted by eyetable's own role, which owns
        them) and the query window read the views schema (granted by the author, which owns it)."""
        try:
            for schema in {self.store.data_schema, *self.store.other_schemas()} - {None}:
                self.share_schema(schema)
            self.store.schema_hooks.append(self.share_schema)
            q = self.db.dialect.identifier_preparer.quote
            with self.db.begin() as conn:
                owner = conn.execute(sa.text("SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = :s"),
                                     {"s": self.schema}).scalar()
                if owner is None:
                    raise AuthorError(f"the schema {self.schema} doesn't exist yet: run scripts/setup-postgres.ps1 "
                                      "as an administrator", 503)
                if owner != self.role:
                    raise AuthorError(f"the schema {self.schema} belongs to {owner}, not {self.role}", 503)
                if self.reader:
                    s, r = q(self.schema), q(self.reader)
                    conn.execute(sa.text(f"GRANT USAGE ON SCHEMA {s} TO {r}"))
                    conn.execute(sa.text(f"GRANT SELECT ON ALL TABLES IN SCHEMA {s} TO {r}"))
                    conn.execute(sa.text(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {s} GRANT SELECT ON TABLES TO {r}"))
                # as_of(NULL::table, time): any history table as it was at a time (see AS_OF_SQL)
                # through text(), which escapes the function body's % for the driver
                conn.execute(sa.text(AS_OF_SQL.format(schema=q(self.schema))))
                conn.execute(sa.text(AS_OF_COMMENT.format(schema=q(self.schema))))
            self.problem = None
        except AuthorError as exc:
            self.problem = str(exc)
        except Exception as exc:
            self.problem = f"saving views isn't available: {_message(exc)}"

    def share_schema(self, schema: str) -> None:
        q = self.store.db.dialect.identifier_preparer
        s, r = q.quote_schema(schema), q.quote(self.role)
        with self.store.db.begin() as conn:
            conn.execute(sa.text(f"GRANT USAGE ON SCHEMA {s} TO {r}"))
            conn.execute(sa.text(f"GRANT SELECT ON ALL TABLES IN SCHEMA {s} TO {r}"))
            conn.execute(sa.text(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {s} GRANT SELECT ON TABLES TO {r}"))

    def _check(self) -> None:
        if self.problem:
            raise AuthorError(self.problem, 503)

    def _search_path(self) -> str:
        q = self.db.dialect.identifier_preparer.quote_schema
        return ", ".join(q(s) for s in (self.schema, self.store.data_schema) if s)

    # -- reading -----------------------------------------------------------------

    def objects(self) -> list[Saved]:
        self._check()
        with self.db.connect() as conn:
            rows = conn.execute(sa.text(
                "SELECT c.relname AS name, CASE c.relkind WHEN 'v' THEN 'view' WHEN 'm' THEN 'view' ELSE 'table' END AS kind "
                "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :s AND c.relkind IN ('v', 'm', 'r', 'p') "
                "UNION ALL SELECT p.proname, 'procedure' FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                "WHERE n.nspname = :s AND p.prokind = 'p' ORDER BY 1"), {"s": self.schema}).all()
        return [Saved(r.name, r.kind, f"{self.schema}.{r.name}") for r in rows]

    def get(self, name: str) -> Saved:
        for o in self.objects():
            if o.name == name:
                return o
        raise AuthorError(f"there's no view or procedure named {name} in {self.schema}", 404)

    def definition(self, name: str) -> dict[str, Any]:
        """The object's CREATE OR REPLACE statement, to edit in the query window and deploy."""
        obj = self.get(name)
        q = self.db.dialect.identifier_preparer.quote
        ref = f"{q(self.schema)}.{q(name)}"
        with self.db.connect() as conn:
            if obj.kind == "view":
                body = conn.execute(sa.text("SELECT pg_get_viewdef(CAST(:r AS regclass), true)"), {"r": ref}).scalar()
                sql = f"CREATE OR REPLACE VIEW {ref} WITH (security_invoker = true) AS\n{body.rstrip().rstrip(';')};"
            elif obj.kind == "procedure":
                sql = conn.execute(sa.text(
                    "SELECT pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                    "WHERE n.nspname = :s AND p.proname = :n AND p.prokind = 'p' LIMIT 1"),
                    {"s": self.schema, "n": name}).scalar().rstrip() + ";"
            else:
                sql = f"SELECT *\nFROM {ref}\nLIMIT 100;"  # a table a procedure made: query it
        return {**obj.as_dict(), "sql": sql}

    # -- writing -----------------------------------------------------------------

    def build(self, name: str, sql: str, aliases: dict[str, str] | None = None) -> tuple[str, str]:
        """(kind, CREATE statement) for saving `sql` as `name`."""
        if not NAME.match(name or ""):
            raise AuthorError("names are lowercase letters, digits and _, starting with a letter or _ (63 at most)")
        sql = clean(sql)
        if not sql:
            raise AuthorError("write a query first")
        if definition_target(sql):
            raise AuthorError("this is already a CREATE statement: use Deploy to run it")
        sql = rewrite_aliases(sql, aliases or {}, self.store.qualified)
        q = self.db.dialect.identifier_preparer.quote
        ref = f"{q(self.schema)}.{q(name)}"
        kind = kind_for(sql)
        if kind == "view":
            return kind, f"CREATE VIEW {ref} WITH (security_invoker = true) AS\n{sql};"
        if _DOLLAR_TAG in sql:
            raise AuthorError(f"the SQL can't contain {_DOLLAR_TAG}")
        masked = mask(sql)
        body = sql if re.match(r"\s*(declare|begin)\b", masked, re.I) else f"BEGIN\n{sql};\nEND"
        return kind, f"CREATE PROCEDURE {ref}()\nLANGUAGE plpgsql\nAS {_DOLLAR_TAG}\n{body}\n{_DOLLAR_TAG};"

    def save(self, name: str, sql: str, aliases: dict[str, str] | None = None, replace: bool = False) -> dict[str, Any]:
        """Save `sql` as a view (or procedure) named `name`. An existing object of that name is
        replaced only when `replace`."""
        self._check()
        kind, ddl = self.build(name, sql, aliases)
        existing = {o.name: o for o in self.objects()}
        if name in existing and not replace:
            raise AuthorError(f"{self.schema}.{name} already exists ({existing[name].kind})", 409)
        drop = self._drop_sql(existing[name]) if name in existing else None
        self._execute(ddl, before=drop)
        return self.definition(name)

    def deploy(self, sql: str) -> dict[str, Any]:
        """Run an edited CREATE [OR REPLACE] VIEW|PROCEDURE statement for an object in the views schema."""
        self._check()
        sql = clean(sql)
        target = definition_target(sql)
        if not target:
            raise AuthorError("Deploy runs a CREATE [OR REPLACE] VIEW or PROCEDURE statement; open a saved "
                              "view or procedure to get one, or use Save to make a new one")
        kind, schema, name = target
        if schema and schema != self.schema:
            raise AuthorError(f"views and procedures are kept in {self.schema}, not {schema}")
        if not NAME.match(name):
            raise AuthorError("names are lowercase letters, digits and _, starting with a letter or _ (63 at most)")
        existing = {o.name: o for o in self.objects()}
        if name in existing and existing[name].kind not in (kind, ):
            raise AuthorError(f"{self.schema}.{name} is a {existing[name].kind}; drop it first or pick another name", 409)
        try:
            self._execute(sql + ";")
        except AuthorError as exc:
            # a view whose columns changed can't be CREATE OR REPLACEd: make it again instead
            if kind == "view" and name in existing and re.search(r"cannot (drop|change).*column", str(exc), re.I):
                self._execute(sql + ";", before=self._drop_sql(existing[name]))
            else:
                raise
        if kind == "view":  # however it was edited, a view reads with its reader's rights
            q = self.db.dialect.identifier_preparer.quote
            self._execute(f"ALTER VIEW {q(self.schema)}.{q(name)} SET (security_invoker = true);")
        return self.definition(name)

    def drop(self, name: str) -> Saved:
        self._check()
        obj = self.get(name)
        self._execute(self._drop_sql(obj))
        return obj

    def call(self, name: str) -> dict[str, Any]:
        """Run a saved procedure (CALL), as the author role; returns how long it took and its notices."""
        self._check()
        obj = self.get(name)
        if obj.kind != "procedure":
            raise AuthorError(f"{name} is a {obj.kind}; only procedures run (query a view with SELECT)")
        q = self.db.dialect.identifier_preparer.quote
        start = time.perf_counter()
        notices = self._execute(f"CALL {q(self.schema)}.{q(name)}();")
        return {**obj.as_dict(), "elapsed_ms": int((time.perf_counter() - start) * 1000), "notices": notices}

    def _drop_sql(self, obj: Saved) -> str:
        q = self.db.dialect.identifier_preparer.quote
        ref = f"{q(self.schema)}.{q(obj.name)}"
        return {"view": f"DROP VIEW {ref};", "procedure": f"DROP PROCEDURE {ref};", "table": f"DROP TABLE {ref};"}[obj.kind]

    def _execute(self, sql: str, before: str | None = None) -> list[str]:
        """Run DDL (or a CALL) as the author role, in one transaction; returns the server's notices."""
        conn = self.db.connect()
        raw = conn.connection.dbapi_connection
        notices: list[str] = []
        raw.add_notice_handler(lambda d: notices.append(d.message_primary))
        try:
            with raw.cursor() as cur:
                cur.execute(f"SET statement_timeout = {self.timeout_ms}")
                cur.execute(f"SET search_path TO {self._search_path()}")
                if before:
                    cur.execute(before)
                cur.execute(sql)
            raw.commit()
        except Exception as exc:
            try:
                raw.rollback()
            except Exception:
                pass
            raise AuthorError(_message(exc)) from None
        finally:
            conn.close()
        return notices


def _message(exc: Exception) -> str:
    text = str(exc).strip() or type(exc).__name__
    return re.sub(r"\n\(Background on this error.*", "", text, flags=re.S)
