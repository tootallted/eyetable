"""Loading a table that other views are built on (Postgres): in place, keeping its columns.

Runs only with EYETABLE_TEST_DATABASE_URL set (a role that may create schemas); uses throwaway schemas.
"""

import os
import uuid

import pytest
import sqlalchemy as sa

from eyetable import keys
from eyetable.engine import Engine
from eyetable.store import CoreStore

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
        conn.execute(sa.text(f"CREATE SCHEMA {views}"))
    try:
        yield store, data, views
    finally:
        with store.db.begin() as conn:
            for s in (meta, data, views, f"{data}_archived"):
                conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {s} CASCADE"))


def rows(store, sql):
    with store.db.connect() as conn:
        return [tuple(r) for r in conn.execute(sa.text(sql)).all()]


def test_a_full_load_refills_a_table_a_view_is_built_on(pg):
    store, data, views = pg
    engine = Engine(store)
    engine.ingest_upload("parts", "p.csv", b"name,qty,colour\nbolt,4,red\nnut,9,blue\n", create=True)
    with store.db.begin() as conn:  # a user's view on the _current view, and one on the table itself
        conn.execute(sa.text(f"CREATE VIEW {views}.big AS SELECT name, qty, colour FROM {data}.parts__data_current WHERE qty > 5"))
        conn.execute(sa.text(f"CREATE VIEW {views}.names AS SELECT name FROM {data}.parts__data"))
    assert store.dependent_views("parts__data") == [f"{views}.big", f"{views}.names"]

    # colour is gone from the source, size is new
    r = engine.ingest_upload("parts", "p.csv", b"name,qty,size\nbolt,4,S\nnut,12,M\nwasher,7,L\n", mode="replace")
    assert r.status == "succeeded", r.error
    assert "warning: colour is no longer in the source, so it was kept" in r.note
    assert f"{views}.big" in r.note and "loaded as NULL" in r.note
    assert rows(store, f"SELECT name, qty, colour FROM {views}.big ORDER BY name") == [("nut", 12, None), ("washer", 7, None)]
    assert rows(store, f"SELECT name FROM {views}.names ORDER BY name") == [("bolt",), ("nut",), ("washer",)]
    assert rows(store, f'SELECT "ET_id", name, size FROM {data}.parts__data_current ORDER BY 1') == [
        (1, "bolt", "S"), (2, "nut", "M"), (3, "washer", "L")]  # the new column reaches the _current view; ET_id restarts


def test_a_history_table_with_a_view_on_it_merges_and_keeps_the_view(pg):
    store, data, views = pg
    engine = Engine(store)
    engine.ingest_upload("parts", "p.csv", b"id,name,colour\n1,bolt,red\n2,nut,blue\n", create=True)
    keys.choose_key(store, "parts__data", ["id"])
    engine.ingest_upload("parts", "p.csv", b"id,name,colour\n1,bolt,red\n2,nut,blue\n", mode="replace")
    with store.db.begin() as conn:
        conn.execute(sa.text(f"CREATE VIEW {views}.colours AS SELECT id, colour FROM {data}.parts__data_current"))
    r = engine.ingest_upload("parts", "p.csv", b"id,name\n1,BOLT\n3,washer\n", mode="replace")
    assert r.status == "succeeded", r.error
    assert "history: 1 new, 1 changed, 0 unchanged" in r.note and "warning: colour is no longer in the source" in r.note
    assert rows(store, f"SELECT id, colour FROM {views}.colours ORDER BY id") == [(1, None), (2, "blue"), (3, None)]


def test_without_views_on_it_a_table_is_still_swapped(pg):
    store, data, views = pg
    engine = Engine(store)
    engine.ingest_upload("parts", "p.csv", b"name,colour\nbolt,red\n", create=True)
    r = engine.ingest_upload("parts", "p.csv", b"name\nnut\n", mode="replace")
    assert r.status == "succeeded" and not (r.note or "").count("warning")
    assert [c["name"] for c in store.columns("parts__data") if not c["name"].startswith("ET_")] == ["name"]  # colour dropped
