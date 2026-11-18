"""A full load of an API source that fails partway keeps its complete pages and can carry on from there."""

import httpx
from fastapi.testclient import TestClient

from eyetable.api import create_app
from eyetable.loadqueue import LoadQueue
from test_statements import COMMAND, TOKEN, Api, add_set, dates, engine_for, http

AUTH = {"Authorization": f"Bearer {TOKEN}"}
SIXTEEN = {"parts": [dates(**{"from": "2026-11-01", "to": "2026-11-16"})]}


def days(store, table="invoices__data"):
    return [r["day"] for r in store.read_table(table)]


def test_statement_15_of_16_fails_then_the_retry_finishes_the_run(store):
    api = Api()
    engine = engine_for(store, api)
    engine.load_curl(COMMAND, "invoices", client=http(api))
    before = days(store)
    add_set(store, "invoices", SIXTEEN)
    api.fail_on = "2026-11-15"
    [r] = engine.reload_source("invoices")
    assert r.status == "failed" and "statement 15 of 16" in r.error
    assert r.resume["where"] == "statement 15 of 16" and r.resume["rows"] == 28  # 14 statements x 2 invoices
    assert days(store) == before  # the live table is untouched
    assert len(store.read_table("invoices__data__stg")) == 28  # the complete pages are kept

    api.fail_on = None
    api.seen.clear()
    [r] = engine.resume("invoices", r.resume)
    assert (r.status, r.rows) == ("succeeded", 32), r.error
    assert "28 rows kept + 4 new" in r.note
    assert [u.split("ge%20")[1][:10] for u in api.seen] == ["2026-11-15", "2026-11-16"]  # only what was missing
    got = store.read_table("invoices__data")
    assert [x["day"] for x in got] == [f"2026-11-{d:02d}" for d in range(1, 17) for _ in (1, 2)]
    assert [x["ET_id"] for x in got] == list(range(1, 33))
    assert not store.table_exists("invoices__data__stg")


class FlakyLinks(Api):
    """The next-link pages of /linked fail once on the second page."""

    def __init__(self):
        super().__init__()
        self.fail_second_page = True

    def __call__(self, request):
        if request.url.path == "/linked" and "start=1" in str(request.url) and self.fail_second_page:
            self.fail_second_page = False
            return httpx.Response(502)
        return super().__call__(request)


def test_a_paged_request_carries_on_from_the_page_that_failed(store):
    api = FlakyLinks()
    engine = engine_for(store, api)
    api.fail_second_page = False
    engine.load_curl("curl 'https://api.test/linked?day=2026-11-04' -H 'Authorization: Bearer tok_123'", "linked", client=http(api))
    api.fail_second_page = True
    [r] = engine.reload_source("linked")
    assert r.status == "failed" and r.resume and r.resume["where"] == "page 2" and r.resume["rows"] == 1
    [r] = engine.resume("linked", r.resume)
    assert (r.status, r.rows) == ("succeeded", 2), r.error
    assert [(x["day"], x["n"]) for x in store.read_table("linked__data")] == [("2026-11-04", 0), ("2026-11-04", 1)]


def test_a_failure_on_the_first_page_keeps_nothing(store):
    api = Api()
    engine = engine_for(store, api)
    engine.load_curl(COMMAND, "invoices", client=http(api))
    add_set(store, "invoices", SIXTEEN)
    api.fail_on = "2026-11-01"
    [r] = engine.reload_source("invoices")
    assert r.status == "failed" and r.resume is None
    assert not store.table_exists("invoices__data__stg")


def test_the_queue_offers_the_retry_and_runs_it_on_request(store):
    api = Api()
    engine = engine_for(store, api)
    engine.load_curl(COMMAND, "invoices", client=http(api))
    add_set(store, "invoices", SIXTEEN)
    api.fail_on = "2026-11-15"
    client = TestClient(create_app(engine, TOKEN))
    queue: LoadQueue = client.app.state.load_queue
    item = client.post("/api/queue", headers=AUTH, json={"source": "invoices"}).json()["item"]
    failed = queue.wait(item["id"], 30)
    assert failed["status"] == "failed"
    assert failed["resumable"] == {"rows": 28, "pages": 14, "where": "statement 15 of 16", "table": "invoices__data"}
    assert "state" not in failed["resumable"]  # the bookmark itself stays on the server

    api.fail_on = None
    r = client.post(f"/api/queue/{item['id']}/retry", headers=AUTH)
    assert r.status_code == 202, r.text
    retry = queue.wait(r.json()["item"]["id"], 30)
    assert retry["status"] == "succeeded" and retry["by"] == "Retry from statement 15 of 16" and retry["retry_of"]
    assert retry["tables"][0]["rows"] == 32
    assert queue.get(item["id"])["retried_by"] == retry["id"]
    assert client.post(f"/api/queue/{item['id']}/retry", headers=AUTH).status_code == 409  # once
    assert len(store.read_table("invoices__data")) == 32
