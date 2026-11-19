"""The run webhook: other processes record their runs in eyetable.runs."""

import datetime as dt

import pytest
from fastapi.testclient import TestClient

from eyetable import hooks
from eyetable.api import create_app
from eyetable.engine import Engine

TOKEN = "test-token-0123456789"
HOOK_KEY = "hook-key-abcdefghijklmnop"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
NOW = dt.datetime(2026, 11, 19, 14, 30, tzinfo=dt.timezone.utc)


@pytest.fixture
def client(store):
    return TestClient(create_app(Engine(store), TOKEN, hook_key=HOOK_KEY))


def test_the_example_call_records_one_row(client, store):
    r = client.get("/hook", params={
        "key": HOOK_KEY, "source": "test", "target_table": "test", "mode": "test", "status": "test",
        "rows_written": "12", "started_at": "2026-11-19T06:02:13.602274-08:00",
        "finished_at": "2026-11-19T06:06:13.602274-08:00", "error": "test"})
    assert r.status_code == 201, r.text
    run = r.json()["recorded"]
    assert (run["source"], run["target_table"], run["mode"], run["status"], run["rows_written"], run["error"]) == \
        ("webhook - test", "test", "test", "test", 12, "test")
    assert run["dataset_key"] == "webhook"
    [stored] = store.list_runs()
    assert stored.started_at == dt.datetime(2026, 11, 19, 14, 2, 13, 602274, tzinfo=dt.timezone.utc)
    assert stored.finished_at == dt.datetime(2026, 11, 19, 14, 6, 13, 602274, tzinfo=dt.timezone.utc)


def test_only_source_and_table_are_needed():
    row = hooks.run_from({"source": "nightly export", "target_table": "erp.orders"}, now=NOW)
    assert row == {"source": "webhook - nightly export", "target_table": "erp.orders", "mode": None,
                   "status": "succeeded", "rows_written": 0, "started_at": None, "finished_at": NOW, "error": None}


@pytest.mark.parametrize("given, stored", [
    (None, "succeeded"), ("", "succeeded"), ("Success", "succeeded"), ("successful", "succeeded"), ("OK", "succeeded"),
    ("fail", "failed"), ("Failed", "failed"), ("error", "failed"), ("running", "running"), ("partial", "partial"),
])
def test_status_words(given, stored):
    assert hooks.run_from({"source": "s", "target_table": "t", "status": given}, now=NOW)["status"] == stored


@pytest.mark.parametrize("values, message", [
    ({"target_table": "t"}, "source is required"),
    ({"source": "s"}, "target_table is required"),
    ({}, "source and target_table are required"),
    ({"source": "  ", "target_table": "t"}, "source is required"),
    ({"source": "s", "target_table": "t", "rows_written": "test"}, "rows_written must be a whole number"),
    ({"source": "s", "target_table": "t", "rows_written": "-3"}, "rows_written must be a whole number"),
    ({"source": "s", "target_table": "t", "started_at": "yesterday"}, "started_at must be a time"),
    ({"source": "s", "target_table": "t", "status": "x" * 17}, "status can be at most 16"),
    ({"source": "s" * 55, "target_table": "t"}, "source can be at most 54"),
    ({"source": "s", "target_table": "t", "started_at": "2026-11-19T10:00:00Z",
      "finished_at": "2026-11-19T09:00:00Z"}, "started_at is after finished_at"),
])
def test_bad_calls_are_explained(values, message):
    with pytest.raises(hooks.HookError, match=message):
        hooks.run_from(values, now=NOW)


def test_numbers_and_times_are_read_leniently():
    row = hooks.run_from({"source": "s", "target_table": "t", "rows_written": "1,234",
                          "started_at": "2026-11-19T06:02:13 05:30"}, now=NOW)  # an unencoded + arrives as a space
    assert row["rows_written"] == 1234
    assert row["started_at"].utcoffset() == dt.timedelta(hours=5, minutes=30)
    local = hooks.run_from({"source": "s", "target_table": "t", "finished_at": "2026-11-19T06:00:00"})["finished_at"]
    assert local.tzinfo is not None  # no offset: this server's local time


def test_a_bad_call_adds_nothing(client, store):
    r = client.get("/hook", params={"key": HOOK_KEY, "source": "x", "target_table": "t", "rows_written": "test"})
    assert r.status_code == 400 and "rows_written" in r.json()["detail"]
    assert store.list_runs() == []


def test_the_key_is_needed(client, store):
    q = {"source": "s", "target_table": "t"}
    assert client.get("/hook", params=q).status_code == 401
    assert client.get("/hook", params={**q, "key": "wrong-key-0123456789"}).status_code == 401
    assert client.get("/hook", params=q, headers={"X-Hook-Key": HOOK_KEY}).status_code == 201
    assert client.get("/hook", params=q, headers={"Authorization": f"Bearer {HOOK_KEY}"}).status_code == 201
    assert client.get("/hook", params=q, headers=AUTH).status_code == 201  # the API token works too
    assert len(store.list_runs()) == 3


def test_the_hook_key_opens_nothing_else(client):
    assert client.get("/api/sources", headers={"Authorization": f"Bearer {HOOK_KEY}"}).status_code == 401


def test_post_json_or_form(client, store):
    r = client.post("/api/hook", headers={"X-Hook-Key": HOOK_KEY},
                    json={"source": "etl", "target_table": "dw.sales", "rows_written": 500, "status": "failed",
                          "error": "disk full"})
    assert r.status_code == 201, r.text
    r = client.post("/hook?key=" + HOOK_KEY, data={"source": "etl", "target_table": "dw.sales", "mode": "full"})
    assert r.status_code == 201, r.text
    assert [(x.status, x.rows_written, x.mode, x.error) for x in store.list_runs()] == [
        ("succeeded", 0, "full", None), ("failed", 500, None, "disk full")]
    assert client.post("/api/hook", headers={"X-Hook-Key": HOOK_KEY}, json=["no"]).status_code == 400


def test_webhook_runs_show_with_the_others(client, store):
    client.get("/hook", params={"key": HOOK_KEY, "source": "payroll", "target_table": "hr.pay",
                                "finished_at": "2026-11-19T06:00:00-08:00"})  # no start time
    runs = client.get("/api/runs", params={"source": "webhook", "since": "2026-11-19T00:00:00Z"}, headers=AUTH).json()["runs"]
    assert [(r["source"], r["started_at"]) for r in runs] == [("webhook - payroll", None)]
    assert client.get("/api/runs", params={"since": "2026-11-20T00:00:00Z"}, headers=AUTH).json()["runs"] == []
