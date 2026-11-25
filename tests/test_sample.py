"""Sampling a table: its metadata and first rows, to set it up before its first full load."""

import sqlite3

import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from eyetable import keys
from eyetable.api import create_app
from eyetable.connectors.base import Connector
from eyetable.engine import Engine

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
KEY = Fernet.generate_key().decode()


@pytest.fixture
def big(tmp_path):
    path = tmp_path / "erp.db"
    conn = sqlite3.connect(path)
    conn.executescript("CREATE TABLE orders (id INTEGER, amount REAL, modified TEXT);"
                       "CREATE TABLE customers (id INTEGER, name TEXT);")
    conn.executemany("INSERT INTO orders VALUES (?, ?, ?)",
                     [(i, i * 1.5, f"2026-11-{1 + i % 28:02d}T10:00:00") for i in range(1, 51)])
    conn.executemany("INSERT INTO customers VALUES (?, ?)", [(i, f"c{i}") for i in range(1, 31)])
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def engine(store, big):
    engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
    # customers is the source's first table (erp__data); orders is added beside it (erp__orders)
    engine.load_db_objects(f"sqlite:///{big.as_posix()}", [{"schema": "main", "name": "customers", "kind": "table", "source": "erp"}])
    engine.set_db_tables("erp", [{"schema": "main", "name": n, "kind": "table"} for n in ("customers", "orders")])
    return engine


def test_a_sample_loads_the_first_rows_of_one_table(engine, store):
    orders, customers = "erp__orders", "erp__data"
    store.drop_table(orders)  # as if the big table had never been loaded
    r = engine.sample_table("erp", orders)
    assert (r.status, r.rows, r.mode) == ("succeeded", 20, "sample"), r.error
    assert "the first 20 rows of about 50" in r.note
    assert store.sampled(orders) and not store.sampled(customers)
    assert store.count_rows(orders) == 20
    assert store.count_rows(customers) == 30  # the other table wasn't touched
    info = keys.key_info(store, orders)  # scanned at once: candidates to choose from
    assert "id" in info["scan"]["primary_key_candidates"]


def test_settings_chosen_on_a_sample_carry_into_a_clean_first_load(engine, store):
    orders = "erp__orders"
    r = engine.sample_table("erp", orders)
    assert r.status == "succeeded", r.error
    keys.choose_key(store, orders, ["id"])  # history turns on
    keys.choose_increment(store, orders, "modified")
    # the incremental button: a sample is no starting point, so this runs in full, all 50 rows
    [res] = [x for x in engine.reload_source("erp", incremental="auto", table=orders)]
    assert res.status == "succeeded" and res.rows == 50, res.error
    assert not store.sampled(orders)
    rows = store.read_table(orders)
    assert len(rows) == 50 and all(x["ET_isCurrent"] == 1 for x in rows)  # every row a first version
    assert store.get_increment(orders) == "modified" and store.get_table_key(orders)["key"] == ["id"]
    # and from now on incremental loads carry on from what's loaded
    [res] = engine.reload_source("erp", incremental="auto", table=orders)
    assert res.status == "succeeded" and res.mode == "incremental"


def test_sampling_again_replaces_the_sample_even_with_history_on(engine, store):
    orders = "erp__orders"
    engine.sample_table("erp", orders)
    keys.choose_key(store, orders, ["id"])
    r = engine.sample_table("erp", orders)
    assert r.status == "succeeded" and store.count_rows(orders) == 20  # not merged into
    assert "ET_isCurrent" not in store.read_table(orders)[0]


def test_file_sources_have_nothing_to_sample(store):
    engine = Engine(store)
    engine.ingest_upload("parts", "p.csv", b"id\n1\n", create=True)
    with pytest.raises(ValueError, match="nothing to sample"):
        engine.sample_table("parts")


def test_the_generic_sample_stops_reading_once_it_has_enough():
    read = []

    class Pages(Connector):
        type = "fake"

        def test(self):
            pass

        def discover(self):
            return []

        def read(self, key, batch_size=1000):
            for page in range(100):
                read.append(page)
                yield [{"n": page * 7 + i} for i in range(7)]

    rows = Pages({}).sample("data", 20)
    assert [r["n"] for r in rows] == list(range(20)) and read == [0, 1, 2]  # three pages, not a hundred


def test_the_api(engine, store):
    client = TestClient(create_app(engine, TOKEN))
    r = client.post("/api/sources/erp/sample", headers=AUTH, json={"table": "erp__data"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["result"]["rows"] == 20 and body["result"]["mode"] == "sample"
    [cust] = [t for t in body["source"]["tables"] if t["table"] == "erp__data"]
    assert cust["sampled"] is True and cust["source_rows"] == 30
    assert client.post("/api/sources/erp/sample", headers=AUTH, json={"table": "nope"}).status_code == 400
