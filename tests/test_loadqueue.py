"""The load queue: reloads in the background, with progress against the last row count, errors, and cancel."""

import sqlite3
import threading
import time

import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from eyetable.api import create_app
from eyetable.connectors.dbtable import DbTableConnector
from eyetable.engine import Engine
from eyetable.incremental import IncrementalProblem
from eyetable.loadqueue import LoadQueue

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
KEY = Fernet.generate_key().decode()


@pytest.fixture
def crm(tmp_path):
    path = tmp_path / "crm.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE accounts (id INTEGER, name TEXT)")
    conn.executemany("INSERT INTO accounts VALUES (?, ?)", [(i, f"n{i}") for i in range(2500)])
    conn.execute("CREATE TABLE notes (body TEXT)")
    conn.execute("INSERT INTO notes VALUES ('hi')")
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def engine(store, crm):
    engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
    url = f"sqlite:///{crm.as_posix()}"
    engine.load_db_objects(url, [{"schema": "main", "name": "accounts", "kind": "table", "source": "crm"}])
    engine.set_db_tables("crm", [{"schema": "main", "name": t, "kind": "table"} for t in ("accounts", "notes")])
    return engine


def test_a_reload_reports_progress_against_the_last_row_count(engine, monkeypatch):
    seen = []
    queue = LoadQueue(engine)
    real = queue._run

    def watch_progress(item_id):
        original = engine.reporting

        def spying(progress):
            def spy(table, rows):
                seen.append((table, rows, queue.get(item_id)["tables"][0]["expected"]))
                progress(table, rows)
            return original(spy)

        monkeypatch.setattr(engine, "reporting", spying)
        real(item_id)

    monkeypatch.setattr(queue, "_run", watch_progress)
    item, added = queue.add("crm", table="crm__data")
    assert added and item["status"] in ("queued", "running") and item["label"] == "main.accounts"
    done = queue.wait(item["id"], 30)
    assert done["status"] == "succeeded"
    [t] = done["tables"]
    assert (t["table"], t["before"], t["expected"], t["rows"], t["status"]) == ("crm__data", 2500, 2500, 2500, "succeeded")
    assert [rows for table, rows, _ in seen] == [0, 1000, 2000, 2500]  # one step per batch
    assert done["started_at"] <= done["finished_at"]


def test_the_whole_source_lists_every_table(engine):
    queue = LoadQueue(engine)
    done = queue.wait(queue.add("crm")[0]["id"], 30)
    assert [(t["label"], t["rows"], t["status"]) for t in done["tables"]] == [
        ("main.accounts", 2500, "succeeded"), ("main.notes", 1, "succeeded")]


def test_errors_come_with_their_details_and_fail_the_item(engine, monkeypatch):
    def broken(self, key, batch_size=1000, since=None):
        if key.endswith("notes"):
            raise RuntimeError("the server went away (SQLSTATE 08S01)")
        yield from real(self, key, batch_size, since)

    real = DbTableConnector.read
    monkeypatch.setattr(DbTableConnector, "read", broken)
    queue = LoadQueue(engine)
    done = queue.wait(queue.add("crm")[0]["id"], 30)
    assert done["status"] == "failed"
    assert [t["status"] for t in done["tables"]] == ["succeeded", "failed"]
    [err] = done["errors"]
    assert err["table"] == "crm__notes" and "went away" in err["message"]
    assert "Traceback" in err["detail"] and "RuntimeError" in err["detail"]


def test_a_problem_before_loading_is_reported_too(engine, store):
    queue = LoadQueue(engine)
    with store.db.begin() as conn:  # the source's database file is gone
        conn.execute(store.sources.update().values(config={"url": "sqlite:///Z:/nowhere/missing.db", "label": "gone",
                                                           "tables": [{"key": "main.accounts", "schema": "main", "table": "accounts",
                                                                       "kind": "table", "name": "accounts"}]}))
    done = queue.wait(queue.add("crm")[0]["id"], 30)
    assert done["status"] == "failed" and done["errors"] and done["errors"][0]["message"]


def test_a_running_load_can_be_cancelled_and_the_table_is_left_as_it_was(engine, store, monkeypatch):
    started, release = threading.Event(), threading.Event()
    real = DbTableConnector.read

    def slow(self, key, batch_size=1000, since=None):
        for i, batch in enumerate(real(self, key, batch_size, since)):
            if i == 1:
                started.set()
                release.wait(10)
            yield batch

    monkeypatch.setattr(DbTableConnector, "read", slow)
    queue = LoadQueue(engine)
    item, _ = queue.add("crm", table="crm__data")
    assert started.wait(10)
    running = queue.get(item["id"])
    assert running["status"] == "running" and running["tables"][0]["rows"] == 1000
    assert queue.cancel(item["id"])["cancelling"] is True
    release.set()
    done = queue.wait(item["id"], 30)
    assert done["status"] == "cancelled" and done["tables"][0]["status"] == "cancelled" and not done["errors"]
    assert store.count_rows("crm__data") == 2500  # the replace never swapped in
    assert not store.table_exists("crm__data__stg")  # and its staging table is gone


def test_the_same_reload_is_not_queued_twice_and_one_source_loads_at_a_time(engine, monkeypatch):
    release = threading.Event()
    real = DbTableConnector.read

    def held(self, key, batch_size=1000, since=None):
        release.wait(10)
        yield from real(self, key, batch_size, since)

    monkeypatch.setattr(DbTableConnector, "read", held)
    queue = LoadQueue(engine)
    first, _ = queue.add("crm", table="crm__data")
    again, added = queue.add("crm", table="crm__data")
    assert not added and again["id"] == first["id"]
    other, added = queue.add("crm", table="crm__notes")
    assert added
    time.sleep(0.3)
    assert queue.get(other["id"])["status"] == "queued"  # waits for the first load of the same source
    release.set()
    assert queue.wait(other["id"], 30)["status"] == "succeeded"
    assert queue.wait(first["id"], 30)["status"] == "succeeded"


def test_incremental_items_need_a_column_and_measure_new_rows(engine, store, crm):
    queue = LoadQueue(engine)
    with pytest.raises(IncrementalProblem):
        queue.add("crm", incremental=True)
    store.save_increment("crm__data", "id")
    conn = sqlite3.connect(crm)
    conn.executemany("INSERT INTO accounts VALUES (?, ?)", [(i, f"n{i}") for i in range(2500, 2600)])
    conn.commit()
    conn.close()
    done = queue.wait(queue.add("crm", incremental=True)[0]["id"], 30)
    [t] = done["tables"]
    assert (done["status"], t["expected"], t["rows"], t["mode"]) == ("succeeded", None, 100, "incremental")


def test_a_file_source_replays_its_files_with_progress_adding_up(store):
    engine = Engine(store)
    engine.ingest_upload("parts", "a.csv", b"n\n1\n2\n", create=True)
    engine.ingest_upload("parts", "b.csv", b"n\n3\n", mode="append")
    queue = LoadQueue(engine)
    done = queue.wait(queue.add("parts")[0]["id"], 30)
    assert done["status"] == "succeeded" and done["tables"][0]["rows"] == 3 and done["tables"][0]["expected"] == 3


def test_the_queue_api(engine):
    client = TestClient(create_app(engine, TOKEN))
    r = client.post("/api/queue", headers=AUTH, json={"source": "crm", "table": "crm__notes"})
    assert r.status_code == 202, r.text
    item = r.json()["item"]
    queue = client.app.state.load_queue
    queue.wait(item["id"], 30)
    items = client.get("/api/queue", headers=AUTH).json()["items"]
    assert items[0]["id"] == item["id"] and items[0]["status"] == "succeeded"
    assert client.post("/api/queue", headers=AUTH, json={"source": "crm", "incremental": True}).status_code == 400
    assert client.post("/api/queue", headers=AUTH, json={"source": "crm", "table": "nope"}).status_code == 400
    assert client.post("/api/queue", headers=AUTH, json={"source": "nope"}).status_code == 404
    assert client.post("/api/queue/zzz/cancel", headers=AUTH).status_code == 404
    assert client.delete(f"/api/queue/{item['id']}", headers=AUTH).json() == {"ok": True}
    assert client.get("/api/queue", headers=AUTH).json()["items"] == []
    assert client.get("/api/queue").status_code == 401
