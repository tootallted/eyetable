"""Every loaded table: ET_id first, then the source's columns, then ET_timestamp, ET_source, ET_run_id."""

import sqlalchemy as sa

from eyetable.store import CoreStore

FIRST, LAST = ["ET_id"], ["ET_timestamp", "ET_source", "ET_run_id"]


def columns(store: CoreStore, table: str) -> list[str]:
    return [c["name"] for c in sa.inspect(store.db).get_columns(table, schema=store.data_schema)]


def batches(*groups):
    return iter([list(g) for g in groups])


def test_a_new_table_has_its_own_columns_between_the_et_ones(store):
    store.write("people__data", batches([{"name": "Ada", "age": 36}]), "replace", 1, origin="people - p.csv")
    assert columns(store, "people__data") == FIRST + ["name", "age"] + LAST
    assert store.read_table("people__data")[0]["name"] == "Ada"


def test_a_column_first_seen_later_still_comes_before_the_et_ones(store):
    store.write("jobs__data", batches([{"id": 1}], [{"id": 2, "status": "open"}], [{"id": 3, "note": "x"}]),
                "replace", 7, origin="jobs - api")
    assert columns(store, "jobs__data") == FIRST + ["id", "status", "note"] + LAST
    rows = store.read_table("jobs__data")
    assert [(r["ET_id"], r["id"], r["status"], r["note"], r["ET_run_id"]) for r in rows] == [
        (1, 1, None, None, 7), (2, 2, "open", None, 7), (3, 3, None, "x", 7)]
    # numbering carries on after the rebuild
    store.write("jobs__data", batches([{"id": 4}]), "append", 8, origin="jobs - api")
    assert [r["ET_id"] for r in store.read_table("jobs__data")] == [1, 2, 3, 4]


def test_an_existing_table_gets_the_new_order_on_its_next_replace(store):
    # a table made the old way: the ET columns first
    t = sa.Table("old__data", sa.MetaData(), sa.Column("ET_id", sa.Integer, primary_key=True),
                 sa.Column("ET_timestamp", sa.Text), sa.Column("ET_source", sa.Text), sa.Column("ET_run_id", sa.Integer),
                 sa.Column("name", sa.Text), schema=store.data_schema)
    t.create(store.db)
    store.write("old__data", batches([{"name": "Grace", "city": "Arlington"}]), "append", 2, origin="old")
    assert columns(store, "old__data") == ["ET_id", "ET_timestamp", "ET_source", "ET_run_id", "name", "city"]  # append keeps it
    store.write("old__data", batches([{"name": "Grace", "city": "Arlington"}]), "replace", 3, origin="old")
    assert columns(store, "old__data") == FIRST + ["name", "city"] + LAST


def test_an_empty_load_still_makes_the_table(store):
    store.write("empty__data", batches(), "replace", 1, origin="empty")
    assert columns(store, "empty__data") == FIRST + LAST
    store.write("empty__data", batches([{"a": 1}]), "append", 2, origin="empty")
    assert columns(store, "empty__data") == FIRST + LAST + ["a"]  # appended: the next replace puts it first
