"""Destinations: the databases sources load into (destinations.py)."""

import os

import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from eyetable.api import create_app
from eyetable.destinations import BUILTIN, Destinations, DestinationError, kind_of
from eyetable.engine import Engine

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
KEY = Fernet.generate_key().decode()
MSSQL = "Driver={ODBC Driver 17 for SQL Server};Server=sql.example;Database=warehouse;Uid=loader;Pwd=s3cret!;"


@pytest.fixture
def dests(store):
    return Destinations(store, KEY)


def test_the_builtin_destination_is_eyetables_own_database(dests, store):
    engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
    engine.ingest_upload("parts", "p.csv", b"id\n1\n", create=True)
    [builtin] = dests.list()
    assert builtin["id"] == BUILTIN and builtin["builtin"] and builtin["kind"] == "SQLite"
    assert builtin["sources"] == ["parts"]  # a source with no destination set loads here
    assert dests.test(BUILTIN)["ok"] is True


def test_adding_a_sql_server_destination_keeps_its_password_encrypted(dests, store):
    d = dests.add("local_sql", MSSQL)
    assert (d["kind"], d["server"], d["database"], d["schema"], d["has_password"]) == (
        "SQL Server", "sql.example", "warehouse", "dbo", True)
    row = store.get_destination("local_sql")
    assert "s3cret" not in str(row.config) and "Pwd" not in row.config["odbc"]
    assert [x["id"] for x in dests.list()] == [BUILTIN, "local_sql"]
    with pytest.raises(DestinationError, match="already"):
        dests.add("local_sql", MSSQL)
    with pytest.raises(DestinationError, match="lowercase"):
        dests.add("Local SQL", MSSQL)
    with pytest.raises(DestinationError, match="connection string"):
        dests.add("nothing", "")


def test_changing_and_removing(dests, store):
    dests.add("local_sql", MSSQL, "staging")
    engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
    engine.ingest_upload("parts", "p.csv", b"id\n1\n", create=True)
    src = store.get_source("parts")
    store.save_source("parts", src.type, {**src.config, "destination": "local_sql"})

    d = dests.update("local_sql", name="sql_here", schema="loads")  # blank connection: kept
    assert (d["id"], d["schema"], d["database"], d["sources"]) == ("sql_here", "loads", "warehouse", ["parts"])
    assert store.get_source("parts").config["destination"] == "sql_here"  # its sources follow a rename
    with pytest.raises(DestinationError, match="still has sources"):
        dests.remove("sql_here")
    with pytest.raises(DestinationError, match="can't be removed"):
        dests.remove(BUILTIN)
    with pytest.raises(DestinationError, match="set in .env"):
        dests.update(BUILTIN, name="x")
    store.save_source("parts", src.type, src.config)  # routed back to eyetable
    assert dests.remove("sql_here")["id"] == "sql_here"
    assert [x["id"] for x in dests.list()] == [BUILTIN]


def test_testing_a_connection_that_fails_says_why(dests, tmp_path):
    result = dests.test(connection=f"sqlite:///{(tmp_path / 'nope' / 'x.db').as_posix()}")
    assert result["ok"] is False and result["message"]
    ok = dests.test(connection=f"sqlite:///{(tmp_path / 'x.db').as_posix()}")
    assert ok["ok"] is True and kind_of({"url": "sqlite:///x.db"}) == "SQLite"


def test_the_api_and_the_source_summary(store):
    engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
    engine.ingest_upload("parts", "p.csv", b"id\n1\n", create=True)
    client = TestClient(create_app(engine, TOKEN))
    assert [d["id"] for d in client.get("/api/destinations", headers=AUTH).json()] == [BUILTIN]
    r = client.post("/api/destinations", headers=AUTH, json={"name": "local_sql", "connection": MSSQL, "schema": "dbo"})
    assert r.status_code == 200, r.text
    assert r.json()["kind"] == "SQL Server"
    r = client.put("/api/destinations/local_sql", headers=AUTH, json={"name": "local_sql", "schema": "stage"})
    assert r.json()["schema"] == "stage"
    assert client.post("/api/destinations", headers=AUTH, json={"name": "x", "connection": ""}).status_code == 400
    [src] = client.get("/api/sources", headers=AUTH).json()
    assert src["destination"] == BUILTIN
    assert client.delete("/api/destinations/local_sql", headers=AUTH).json()["removed"]["id"] == "local_sql"
    assert client.delete("/api/destinations/eyetable", headers=AUTH).status_code == 400
    assert client.get("/api/destinations").status_code == 401


@pytest.mark.skipif(not os.environ.get("EYETABLE_TEST_MSSQL"), reason="set EYETABLE_TEST_MSSQL to a SQL Server connection string")
def test_a_real_sql_server(dests):
    result = dests.test(connection=os.environ["EYETABLE_TEST_MSSQL"], schema="dbo")
    assert result["ok"] is True, result
    assert result["version"].startswith("SQL Server") and result["schema_exists"]
