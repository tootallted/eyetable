"""Earlier runs: every load, searchable by source or table name, time and status."""

import datetime as dt

from fastapi.testclient import TestClient

from eyetable.api import create_app
from eyetable.engine import Engine

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


def test_runs_are_searched_by_name_time_and_status(store):
    engine = Engine(store)
    engine.ingest_upload("ledger_2026", "l.csv", b"a\n1\n", create=True)
    engine.ingest_upload("parts", "p.csv", b"a\n1\n", create=True)
    engine.reload_source("parts")
    failed = store.start_run("parts", "data", "parts__data", "replace")
    store.finish_run(failed, "failed", 0, "boom")

    assert [r.source for r in store.search_runs("LEDGER")] == ["ledger_2026"]  # any case, part of a name
    assert len(store.search_runs("parts__d")) == 3  # the table name matches too
    assert [r.status for r in store.search_runs("parts", status="failed")] == ["failed"]
    later = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)
    assert store.search_runs(since=later) == []
    assert len(store.search_runs(until=later)) == 4
    assert [r.id for r in store.search_runs()] == sorted((r.id for r in store.search_runs()), reverse=True)  # newest first


def test_the_runs_api(store):
    engine = Engine(store)
    client = TestClient(create_app(engine, TOKEN))
    for name in ("alpha", "beta"):
        client.post(f"/api/sources/{name}/uploads", headers=AUTH, data={"create": "true"},
                    files={"file": (f"{name}.csv", b"a\n1\n2\n", "text/csv")})
    body = client.get("/api/runs", headers=AUTH, params={"source": "alp"}).json()
    assert [(r["source"], r["rows_written"], r["status"]) for r in body["runs"]] == [("alpha", 2, "succeeded")]
    assert body["more"] is False
    assert client.get("/api/runs", headers=AUTH, params={"limit": 1}).json()["more"] is True
    since = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)).isoformat()
    assert client.get("/api/runs", headers=AUTH, params={"since": since}).json()["runs"] == []
    assert client.get("/api/runs", headers=AUTH, params={"status": "weird"}).status_code == 400
    assert client.get("/api/runs").status_code == 401
