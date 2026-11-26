"""Sources routed to another destination load there (CoreStore.data_for / DataStore).

The SQL Server tests run with EYETABLE_TEST_MSSQL_SERVER set to an ODBC connection string with no
Database= (e.g. Driver={ODBC Driver 17 for SQL Server};Server=localhost;Trusted_Connection=yes;Encrypt=no;)
for a login that may create databases; each test makes a throwaway database and drops it.
"""

import os
import uuid

import pyodbc
import pytest
import sqlalchemy as sa
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from eyetable import keys
from eyetable.api import create_app
from eyetable.destinations import Destinations
from eyetable.engine import Engine

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
KEY = Fernet.generate_key().decode()
SERVER = os.environ.get("EYETABLE_TEST_MSSQL_SERVER")


def test_tables_route_by_their_source(store, tmp_path):
    """Routing on SQLite alone: a source sent to another SQLite database loads there, not here."""
    engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
    engine.ingest_upload("parts", "p.csv", b"id,name\n1,bolt\n", create=True)
    other = tmp_path / "other.db"
    Destinations(store, KEY).add("elsewhere", f"sqlite:///{other.as_posix()}")
    src = store.get_source("parts")
    store.save_source("parts", src.type, {**src.config, "destination": "elsewhere"})
    assert store.route_of("parts__data") == "elsewhere" and store.route_of("parts__data__stg") == "elsewhere"
    assert store.route_of("orders__data") is None

    r = engine.ingest_upload("parts", "p.csv", b"id,name\n1,bolt\n2,nut\n", mode="replace")
    assert r.status == "succeeded", r.error
    there = sa.create_engine(f"sqlite:///{other.as_posix()}")
    with there.connect() as conn:
        assert conn.execute(sa.text("SELECT name FROM parts__data ORDER BY id")).scalars().all() == ["bolt", "nut"]
    assert store.count_rows("parts__data") == 2  # read where it lives
    assert [x["name"] for x in store.read_table("parts__data")] == ["bolt", "nut"]
    with store.db.connect() as conn:  # the old copy here is left as it was
        assert conn.execute(sa.text("SELECT COUNT(*) FROM parts__data")).scalar() == 1
    assert store.get_source("parts").config["destination"] == "elsewhere"  # bookkeeping stays here
    assert store.data_for("parts__data").get_source("parts").name == "parts"


@pytest.fixture
def mssql(store):
    if not SERVER:
        pytest.skip("set EYETABLE_TEST_MSSQL_SERVER to run the SQL Server tests")
    db = f"eyetable_t{uuid.uuid4().hex[:8]}"
    admin = pyodbc.connect(SERVER, autocommit=True)
    admin.execute(f"CREATE DATABASE [{db}]")
    try:
        engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
        Destinations(store, KEY).add("sqlhere", SERVER.rstrip(";") + f";Database={db};", "loads")
        yield engine, db
    finally:
        for s in list(store._dest_stores.values()):
            s[1].db.dispose()
        admin.execute(f"ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
        admin.execute(f"DROP DATABASE [{db}]")
        admin.close()


def on_sql_server(db, sql):
    with pyodbc.connect(SERVER.rstrip(";") + f";Database={db};") as cn:
        return [tuple(r) for r in cn.execute(sql).fetchall()]


def route(store, name, dest):
    src = store.get_source(name)
    store.save_source(name, src.type, {**src.config, "destination": dest})


def test_full_incremental_history_and_views_on_sql_server(mssql, store):
    engine, db = mssql
    engine.ingest_upload("parts", "p.csv", b"id,name,price\n1,bolt,1.5\n", create=True)
    route(store, "parts", "sqlhere")

    # a full load makes the table in the destination's schema, with its _current view
    r = engine.ingest_upload("parts", "p.csv", "id,name,price\n1,bolt,1.25\n2,café,2.5\n".encode(), mode="replace")
    assert r.status == "succeeded", r.error
    assert on_sql_server(db, "SELECT id, name, price FROM loads.parts__data ORDER BY id") == [(1, "bolt", 1.25), (2, "café", 2.5)]
    assert on_sql_server(db, "SELECT COUNT(*) FROM loads.parts__data_current") == [(2,)]
    assert store.count_rows("parts__data") == 2 and store.table_exists("parts__data")

    # appending: a new column is added
    r = engine.ingest_upload("parts", "p.csv", b"id,name,price,colour\n3,washer,0.1,red\n", mode="append")
    assert r.status == "succeeded", r.error
    assert on_sql_server(db, "SELECT id, colour FROM loads.parts__data ORDER BY id") == [(1, None), (2, None), (3, "red")]

    # a natural key turns history on (the key is checked in SQL Server); the next load merges there
    info = keys.choose_key(store, "parts__data", ["id"])
    assert info["history"] is True and info["key_check"]["ok"]
    r = engine.ingest_upload("parts", "p.csv", b"id,name,price,colour\n1,bolt,1.25,\n2,CAFE,2.5,\n3,washer,0.1,red\n", mode="replace")
    assert r.status == "succeeded", r.error
    assert "history: 0 new, 1 changed, 2 unchanged" in r.note
    assert on_sql_server(db, 'SELECT id, name, "ET_isCurrent" FROM loads.parts__data ORDER BY id, "ET_fromdate"') == [
        (1, "bolt", 1), (2, "café", 0), (2, "CAFE", 1), (3, "washer", 1)]
    assert on_sql_server(db, "SELECT id, name FROM loads.parts__data_current ORDER BY id") == [
        (1, "bolt"), (2, "CAFE"), (3, "washer")]
    scanned = keys.scan_and_save(store, "parts__data")  # a key scan runs there too
    assert "id" in scanned["scan"]["primary_key_candidates"]

    # each history table gets <table>_as_of(@at): the rows current at that time
    r = engine.ingest_upload("parts", "p.csv", b"id,name,price,colour\n1,BOLT,1.25,\n2,CAFE,2.5,\n3,washer,0.1,red\n", mode="replace")
    assert "1 changed, 2 unchanged" in r.note
    [(changed_at,)] = on_sql_server(db, "SELECT ET_fromdate FROM loads.parts__data WHERE id = 1 AND ET_isCurrent = 1")
    at = changed_at.isoformat(sep=" ")
    before = on_sql_server(db, "SELECT id, name FROM loads.parts__data_as_of("
                               f"DATEADD(millisecond, -1, CAST('{at}' AS datetime2))) ORDER BY id")
    after = on_sql_server(db, f"SELECT id, name FROM loads.parts__data_as_of('{at}') ORDER BY id")
    assert before == [(1, "bolt"), (2, "CAFE"), (3, "washer")]
    assert after == [(1, "BOLT"), (2, "CAFE"), (3, "washer")]
    assert on_sql_server(db, "SELECT COUNT(*) FROM loads.parts__data_as_of('2000-01-01')") == [(0,)]
    store.drop_table("parts__data")  # the function goes with its table
    assert on_sql_server(db, "SELECT OBJECT_ID('loads.parts__data_as_of')") == [(None,)]


def test_the_page_routes_a_source(mssql, store):
    engine, db = mssql
    engine.ingest_upload("parts", "p.csv", b"id\n1\n", create=True)
    client = TestClient(create_app(engine, TOKEN))
    r = client.put("/api/sources/parts/destination", headers=AUTH, json={"destination": "sqlhere"})
    assert r.status_code == 200, r.text
    assert r.json()["source"]["destination"] == "sqlhere" and "next load makes its tables there" in r.json()["note"]
    assert client.put("/api/sources/parts/destination", headers=AUTH, json={"destination": "nope"}).status_code == 404
    assert client.post("/api/sources/parts/reload", headers=AUTH, json={}).status_code == 200
    assert on_sql_server(db, "SELECT id FROM loads.parts__data") == [(1,)]
    r = client.put("/api/sources/parts/destination", headers=AUTH, json={"destination": "eyetable"})
    assert r.json()["source"]["destination"] == "eyetable" and "destination" not in store.get_source("parts").config
