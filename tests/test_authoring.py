"""Saving query-window SQL as views or procedures (authoring.py)."""

import os
import uuid

import pytest
import sqlalchemy as sa
from fastapi.testclient import TestClient

from eyetable.api import create_app
from eyetable.authoring import Author, AuthorError, definition_target, kind_for, mask, rewrite_aliases
from eyetable.engine import Engine
from eyetable.store import CoreStore

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
FAKE_PG = "postgresql+psycopg://author:pw@localhost:5432/nowhere"  # never connected to by these unit tests


@pytest.mark.parametrize("sql", [
    "SELECT * FROM parts",
    "  with x as (select 1) select * from x;",
    "SELECT 'insert into t values (1); delete' AS note FROM parts",  # words inside a string don't count
    "SELECT \"update\" FROM parts -- delete everything\n",
    "VALUES (1), (2)",
    "(SELECT 1) UNION (SELECT 2)",
])
def test_plain_queries_are_views(sql):
    assert kind_for(sql) == "view"


@pytest.mark.parametrize("sql", [
    "INSERT INTO totals SELECT * FROM parts",
    "SELECT * INTO new_table FROM parts",
    "WITH gone AS (DELETE FROM t RETURNING *) SELECT * FROM gone",
    "SELECT 1; SELECT 2",
    "DECLARE c CURSOR FOR SELECT 1",
    "CREATE TABLE t AS SELECT 1",
    "TRUNCATE t",
    "UPDATE t SET a = 1",
])
def test_anything_that_changes_data_is_a_procedure(sql):
    assert kind_for(sql) == "procedure"


def test_mask_keeps_positions():
    sql = "SELECT 'a;b' AS \"x;y\", $q$ ; $q$ /* ; */ -- ;\nFROM t"
    masked = mask(sql)
    assert len(masked) == len(sql) and ";" not in masked and masked.endswith("FROM t")


def test_definitions_are_recognised():
    assert definition_target("CREATE OR REPLACE VIEW eyetable_views.totals WITH (x) AS SELECT 1") == (
        "view", "eyetable_views", "totals")
    assert definition_target("create procedure Totals() language plpgsql as $$ begin end $$") == ("procedure", None, "totals")
    assert definition_target("SELECT 1") is None


def test_source_names_become_their_tables():
    aliases = {"parts": "parts__data", "orders": "orders__data"}
    q = lambda t: f"eyetable_raw.{t}"  # noqa: E731
    sql = "SELECT p.name, parts FROM parts p JOIN orders o ON o.id = p.id WHERE note = 'from parts'"
    assert rewrite_aliases(sql, aliases, q) == (
        "SELECT p.name, parts FROM eyetable_raw.parts__data p JOIN eyetable_raw.orders__data o ON o.id = p.id "
        "WHERE note = 'from parts'")
    assert rewrite_aliases("SELECT * FROM parts.x", aliases, q) == "SELECT * FROM parts.x"  # a schema, not the alias


def test_building_a_view_and_a_procedure(store):
    author = Author(store, FAKE_PG)
    kind, ddl = author.build("totals", "SELECT name FROM parts;", {"parts": "parts__data"})
    assert kind == "view"
    assert ddl == 'CREATE VIEW eyetable_views.totals WITH (security_invoker = true) AS\nSELECT name FROM parts__data;'
    kind, ddl = author.build("refresh", "TRUNCATE t; INSERT INTO t SELECT * FROM parts", {"parts": "parts__data"})
    assert kind == "procedure"
    assert ddl == ('CREATE PROCEDURE eyetable_views.refresh()\nLANGUAGE plpgsql\nAS $eyetable$\nBEGIN\n'
                   'TRUNCATE t; INSERT INTO t SELECT * FROM parts__data;\nEND\n$eyetable$;')
    kind, ddl = author.build("loop", "DECLARE n int; BEGIN SELECT 1 INTO n; END")
    assert "$eyetable$\nDECLARE n int; BEGIN SELECT 1 INTO n; END\n$eyetable$" in ddl
    with pytest.raises(AuthorError, match="lowercase"):
        author.build("Bad Name", "SELECT 1")
    with pytest.raises(AuthorError, match="Deploy"):
        author.build("x", "CREATE VIEW x AS SELECT 1")


def test_the_api_says_when_saving_isnt_set_up(store):
    client = TestClient(create_app(Engine(store), TOKEN))
    assert client.get("/api/objects", headers=AUTH).json()["ready"] is False
    r = client.post("/api/objects", headers=AUTH, json={"name": "x", "sql": "SELECT 1"})
    assert r.status_code == 503 and "EYETABLE_AUTHOR_DATABASE_URL" in r.json()["detail"]
    kind = client.post("/api/objects/kind", headers=AUTH, json={"sql": "DELETE FROM t"}).json()
    assert kind == {"kind": "procedure", "deploy": False, "target": None}
    assert client.get("/api/objects").status_code == 401


# -- on Postgres: set EYETABLE_TEST_DATABASE_URL to a role that may create schemas ------------------

PG_URL = os.environ.get("EYETABLE_TEST_DATABASE_URL")


@pytest.fixture
def pg():
    if not PG_URL:
        pytest.skip("set EYETABLE_TEST_DATABASE_URL to run the Postgres tests")
    tag = uuid.uuid4().hex[:8]
    meta, data, views = f"et_t{tag}_meta", f"et_t{tag}_raw", f"et_t{tag}_views"
    store = CoreStore(PG_URL, meta, data)
    store.init()
    with store.db.begin() as conn:
        conn.execute(sa.text(f"CREATE SCHEMA {views}"))  # owned by the same role, which acts as the author here
    try:
        yield store, views
    finally:
        with store.db.begin() as conn:
            for s in (meta, data, views, f"{data}_archived"):
                conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {s} CASCADE"))


def test_save_open_edit_deploy_call_and_drop_on_postgres(pg):
    store, views = pg
    engine = Engine(store)
    engine.ingest_upload("parts", "p.csv", b"name,qty\nbolt,4\nnut,9\n", create=True)
    author = Author(store, PG_URL, views)
    author.setup()
    assert author.problem is None
    client = TestClient(create_app(engine, TOKEN, author=author))

    r = client.post("/api/objects", headers=AUTH, json={"name": "big_parts", "sql": "SELECT name, qty FROM parts WHERE qty > 5"})
    assert r.status_code == 200, r.text
    saved = r.json()
    assert saved["kind"] == "view" and saved["sql"].startswith(f"CREATE OR REPLACE VIEW {views}.big_parts WITH (security_invoker = true) AS")
    with store.db.connect() as conn:
        assert conn.execute(sa.text(f"SELECT name FROM {views}.big_parts")).scalars().all() == ["nut"]
        assert conn.execute(sa.text("SELECT reloptions FROM pg_class WHERE relname = 'big_parts'")).scalar() == ["security_invoker=true"]

    again = client.post("/api/objects", headers=AUTH, json={"name": "big_parts", "sql": "SELECT 1"})
    assert again.status_code == 409 and "already exists" in again.json()["detail"]

    # open it, change its columns, deploy: a view whose columns change is made again
    opened = client.get("/api/objects/big_parts", headers=AUTH).json()
    edited = opened["sql"].replace("SELECT name,", "SELECT upper(name) AS name_upper,")
    assert edited != opened["sql"]
    r = client.post("/api/objects/deploy", headers=AUTH, json={"sql": edited})
    assert r.status_code == 200, r.text
    with store.db.connect() as conn:
        assert conn.execute(sa.text(f"SELECT name_upper FROM {views}.big_parts")).scalars().all() == ["NUT"]

    # anything that changes data becomes a procedure, which the page can run
    r = client.post("/api/objects", headers=AUTH, json={
        "name": "snapshot", "sql": "CREATE TABLE IF NOT EXISTS part_counts (n bigint, at timestamptz);\n"
                                   "INSERT INTO part_counts SELECT count(*), now() FROM parts"})
    assert r.status_code == 200, r.text
    assert r.json()["kind"] == "procedure" and "LANGUAGE plpgsql" in r.json()["sql"]
    ran = client.post("/api/objects/snapshot/call", headers=AUTH).json()
    assert ran["kind"] == "procedure" and ran["elapsed_ms"] >= 0
    listed = {o["name"]: o["kind"] for o in client.get("/api/objects", headers=AUTH).json()["objects"]}
    assert listed == {"big_parts": "view", "part_counts": "table", "snapshot": "procedure"}
    with store.db.connect() as conn:
        assert conn.execute(sa.text(f"SELECT n FROM {views}.part_counts")).scalar() == 2

    # deploy refuses other schemas and non-CREATE SQL; drop removes
    assert client.post("/api/objects/deploy", headers=AUTH, json={"sql": "CREATE VIEW public.x AS SELECT 1"}).status_code == 400
    assert client.post("/api/objects/deploy", headers=AUTH, json={"sql": "SELECT 1"}).status_code == 400
    assert client.post("/api/objects/big_parts/call", headers=AUTH).status_code == 400  # a view doesn't run
    assert client.delete("/api/objects/big_parts", headers=AUTH).json()["dropped"]["kind"] == "view"
    assert client.get("/api/objects/big_parts", headers=AUTH).status_code == 404
