"""Big tables read in parts by a column, and the switch to parts when one query times out."""

import datetime as dt
import sqlite3

import pytest
import sqlalchemy as sa
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from eyetable import dbconnect
from eyetable.api import create_app
from eyetable.connectors import dbtable
from eyetable.connectors.dbtable import DbTableConnector, is_timeout, split_candidates
from eyetable.dbconnect import ConnectionError_
from eyetable.engine import Engine
from eyetable.models import ColumnInfo

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
KEY = Fernet.generate_key().decode()
START = dt.datetime(2025, 1, 1)
TIMEOUT = "The reduced two minutes timeout period elapsed prior to completion of the operation."


def stamp(when):
    return when.isoformat(sep=" ", timespec="microseconds")


@pytest.fixture
def crm(tmp_path):
    path = tmp_path / "crm.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE appts (appointmentid INTEGER, subject TEXT, createdon DATETIME, modifiedon DATETIME)")
    # 12,000 rows over a year, bunched at the start (uneven), plus 5 with no creation time
    # dates as SQLAlchemy keeps them in SQLite (text with microseconds), so its comparisons line up
    rows = [(i, f"s{i}", stamp(START + dt.timedelta(minutes=i if i < 9000 else i * 40)), stamp(START)) for i in range(12000)]
    rows += [(20000 + i, "no date", None, None) for i in range(5)]
    conn.executemany("INSERT INTO appts VALUES (?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def engine(store, crm):
    engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
    engine.load_db_objects(f"sqlite:///{crm.as_posix()}", [{"schema": "main", "name": "appts", "kind": "table", "source": "crm"}])
    return engine


def ids(store):
    return sorted(r["appointmentid"] for r in store.read_table("crm__data"))


def test_a_table_read_in_parts_comes_back_whole(engine, store, monkeypatch):
    monkeypatch.setattr(dbtable, "PART_MAX_ROWS", 1000)  # parts of about 1,000 rows
    engine.set_split("crm", "data", "createdon")
    [r] = engine.reload_source("crm")
    assert r.status == "succeeded", r.error
    assert ids(store) == list(range(12000)) + [20000 + i for i in range(5)]  # every row once, the undated ones too
    assert r.note.startswith("read in ") and "parts by createdon" in r.note
    parts = int(r.note.split()[2].replace(",", ""))
    assert 8 <= parts <= 60, parts  # sized to the rows, not one per minute or one in all


def test_parts_by_a_number_column(engine, store, monkeypatch):
    monkeypatch.setattr(dbtable, "PART_MAX_ROWS", 1000)
    engine.set_split("crm", "data", "appointmentid")
    [r] = engine.reload_source("crm")
    assert r.status == "succeeded" and len(ids(store)) == 12005


def test_one_query_that_times_out_switches_the_table_to_parts(engine, store, monkeypatch):
    real = DbTableConnector.read
    asked = []

    def slow_database(self, key, batch_size=1000, since=None, split=None):
        asked.append(split or (self._item(key).get("split") or {}).get("column"))
        if not asked[-1]:
            raise ConnectionError_(f"reading main.appts failed: {TIMEOUT}")
        yield from real(self, key, batch_size, since, split)

    monkeypatch.setattr(DbTableConnector, "read", slow_database)
    [r] = engine.reload_source("crm")
    assert r.status == "succeeded", r.error
    assert asked == [None, "createdon"]
    assert "timed out" in r.note and "parts by createdon" in r.note
    assert len(ids(store)) == 12005
    item = dbconnect.db_tables(store.get_source("crm").config)[0]
    assert item["split"] == {"column": "createdon", "auto": True}
    # from now on it goes straight to parts
    [r] = engine.reload_source("crm")
    assert asked[-1] == "createdon" and r.status == "succeeded"


def test_other_errors_are_not_retried_in_parts(engine, monkeypatch):
    def broken(self, key, batch_size=1000, since=None, split=None):
        raise ConnectionError_("reading main.appts failed: Login failed for user")
        yield  # pragma: no cover

    monkeypatch.setattr(DbTableConnector, "read", broken)
    [r] = engine.reload_source("crm")
    assert r.status == "failed" and "Login failed" in r.error


def test_a_part_that_times_out_is_halved_and_tried_again(engine, store, monkeypatch):
    monkeypatch.setattr(dbtable, "PART_MAX_ROWS", 1000)
    engine.set_split("crm", "data", "createdon")
    real = DbTableConnector._fetch
    calls = {"n": 0}

    def flaky(self, query):
        calls["n"] += 1
        if calls["n"] in (2, 3):  # the second part times out twice
            raise sa.exc.OperationalError("SELECT ...", {}, Exception(f"('HYT00', '{TIMEOUT}')"))
        return real(self, query)

    monkeypatch.setattr(DbTableConnector, "_fetch", flaky)
    [r] = engine.reload_source("crm")
    assert r.status == "succeeded", r.error
    assert ids(store) == list(range(12000)) + [20000 + i for i in range(5)]


def test_incremental_loads_read_in_parts_too(engine, store, crm, monkeypatch):
    monkeypatch.setattr(dbtable, "PART_MAX_ROWS", 1000)
    engine.set_split("crm", "data", "createdon")
    store.save_increment("crm__data", "appointmentid")
    conn = sqlite3.connect(crm)
    conn.executemany("INSERT INTO appts VALUES (?, ?, ?, ?)", [(30000 + i, "new", stamp(START + dt.timedelta(days=400)), None) for i in range(3)])
    conn.commit()
    conn.close()
    [r] = engine.reload_source("crm", incremental=True)
    assert (r.status, r.mode, r.rows) == ("succeeded", "incremental", 3)
    assert "parts by createdon" in r.note


def test_choosing_split_columns():
    cols = [ColumnInfo("amount", "NUMERIC"), ColumnInfo("modifiedon", "DATETIME"), ColumnInfo("createdon", "datetime"),
            ColumnInfo("accountid", "UUID"), ColumnInfo("versionnumber", "TIMESTAMP"), ColumnInfo("ticket_id", "BIGINT"),
            ColumnInfo("name", "str")]
    assert split_candidates(cols) == ["createdon", "ticket_id", "modifiedon"]
    assert is_timeout(sa.exc.OperationalError("x", {}, Exception("Query timeout expired (HYT00)")))
    assert is_timeout(ConnectionError_(TIMEOUT)) and not is_timeout(ConnectionError_("Login failed"))


def test_a_table_the_database_cant_describe_is_still_read_column_by_column(engine, monkeypatch):
    """Dataverse can't be reflected; its columns come from an empty select, never SELECT *."""
    conn = engine.connector("crm")

    def cannot_reflect(*args, **kwargs):
        raise sa.exc.NoSuchTableError("appts")

    monkeypatch.setattr(dbtable.sa, "Table", cannot_reflect)
    source = conn._source("main", "appts")
    sql = str(sa.select(source))
    assert "*" not in sql and "createdon" in sql and "subject" in sql


def test_the_parts_setting_api(engine, store):
    client = TestClient(create_app(engine, TOKEN))
    info = client.get("/api/tables/crm__data/split", headers=AUTH).json()
    assert (info["column"], info["candidates"][0]) == (None, "createdon")
    assert client.put("/api/tables/crm__data/split", headers=AUTH, json={"column": "subject"}).status_code == 400
    assert client.put("/api/tables/crm__data/split", headers=AUTH, json={"column": "nope"}).status_code == 400
    r = client.put("/api/tables/crm__data/split", headers=AUTH, json={"column": "createdon"}).json()
    assert (r["column"], r["auto"]) == ("createdon", False)
    assert client.get("/api/sources", headers=AUTH).json()[0]["tables"][0]["split"] == "createdon"
    assert client.put("/api/tables/crm__data/split", headers=AUTH, json={"column": None}).json()["column"] is None
    client.post("/api/sources/parts/uploads", headers=AUTH, data={"create": "true"}, files={"file": ("p.csv", b"a\n1\n", "text/csv")})
    assert client.get("/api/tables/parts__data/split", headers=AUTH).status_code == 404
