"""An API source run as a set of day statements: a full load replaces the whole table; an
incremental load runs just its window (today-2 to today, say) and replaces only those days' rows."""

import datetime as dt

import httpx
import pytest
from fastapi.testclient import TestClient

from eyetable import curl
from eyetable.api import create_app
from eyetable.loadqueue import LoadQueue
from eyetable.statements import StatementError, normalize
from test_statements import AUTH, COMMAND, TOKEN, URL, Api, add_set, dates, engine_for, filter_of, http

WEEK = {"parts": [dates(**{"from": "today-6", "to": "today", "incremental_from": "today-2"})]}


def day(n):
    return (dt.date.today() - dt.timedelta(days=n)).isoformat()


class Versions(Api):
    """Each day's invoices carry the API's current version; `counts` changes how many a day has."""

    def __init__(self):
        super().__init__()
        self.version = 1
        self.counts = {}

    def __call__(self, request):
        answer = super().__call__(request)
        if answer.status_code != 200 or request.url.path != "/invoices":
            return answer
        d = filter_of(str(request.url)).split("ge ")[1][:10]
        rows = [{"invoice": f"{d}-{i}", "day": d, "version": self.version} for i in range(1, self.counts.get(d, 2) + 1)]
        return httpx.Response(200, json={"value": rows})


def seen_days(api):
    return [filter_of(u).split("ge ")[1][:10] for u in api.seen]


def by_day(store):
    out = {}
    for r in store.read_table("invoices__data"):
        out.setdefault(r["day"], []).append((r["invoice"], r["version"], r["ET_statement"]))
    return out


@pytest.fixture
def setup(store):
    api = Versions()
    engine = engine_for(store, api)
    engine.load_curl(COMMAND, "invoices", client=http(api))
    add_set(store, "invoices", WEEK)
    return engine, api


def test_a_full_load_tags_each_row_with_its_statement(store, setup):
    engine, api = setup
    [r] = engine.reload_source("invoices")
    assert (r.status, r.rows) == ("succeeded", 14), r.error
    got = by_day(store)
    assert sorted(got) == [day(n) for n in range(6, -1, -1)]
    assert all(stmt == d for d, rows in got.items() for _, _, stmt in rows)
    cols = [c["name"] for c in store.columns("invoices__data")]
    assert cols.index("ET_statement") < cols.index("ET_timestamp")  # with the data, before the bookkeeping


def test_an_incremental_load_replaces_just_its_days(store, setup):
    engine, api = setup
    engine.reload_source("invoices")
    api.version = 2
    api.counts = {day(2): 3, day(1): 1, day(5): 5}  # one window day grows, one shrinks; day 5 is outside it
    api.seen.clear()
    [r] = engine.reload_source("invoices", incremental=True)
    assert (r.status, r.mode, r.rows) == ("succeeded", "incremental", 6), r.error
    assert seen_days(api) == [day(2), day(1), day(0)]  # only the window was asked for
    assert "ran 3 statements" in r.note and "6 old rows were replaced" in r.note
    got = by_day(store)
    for n in (6, 5, 4, 3):  # outside the window: as the full load left them
        assert [(v, s) for _, v, s in got[day(n)]] == [(1, day(n))] * 2
    assert [(i, v) for i, v, _ in got[day(2)]] == [(f"{day(2)}-{i}", 2) for i in (1, 2, 3)]
    assert [(i, v) for i, v, _ in got[day(1)]] == [(f"{day(1)}-1", 2)]  # the row that went away is gone
    assert [(i, v) for i, v, _ in got[day(0)]] == [(f"{day(0)}-{i}", 2) for i in (1, 2)]
    assert len(store.read_table("invoices__data")) == 14
    assert not store.table_exists("invoices__data__incoming") and not store.table_exists("invoices__data__incoming__stg")


def test_a_full_load_still_replaces_everything(store, setup):
    engine, api = setup
    engine.reload_source("invoices")
    api.version = 2
    api.seen.clear()
    [r] = engine.reload_source("invoices")
    assert r.rows == 14 and len(api.seen) == 7
    assert {v for rows in by_day(store).values() for _, v, _ in rows} == {2}


def test_a_failed_incremental_load_changes_nothing(store, setup):
    engine, api = setup
    engine.reload_source("invoices")
    before = store.read_table("invoices__data")
    api.version = 2
    api.fail_on = day(1)
    [r] = engine.reload_source("invoices", incremental=True)
    assert r.status == "failed" and "statement 2 of 3" in r.error
    assert store.read_table("invoices__data") == before
    assert not store.table_exists("invoices__data__incoming")


def test_a_table_loaded_before_rows_were_tagged_loads_in_full_once(store, setup):
    engine, api = setup  # the table came from the single statement: no ET_statement column yet
    api.seen.clear()
    [r] = engine.reload_source("invoices", incremental=True)
    assert r.status == "succeeded" and r.rows == 14 and len(api.seen) == 7, r.error
    assert "ran in full" in r.note
    api.seen.clear()
    [r] = engine.reload_source("invoices", incremental=True)
    assert r.mode == "incremental" and len(api.seen) == 3


def test_auto_loads_by_the_window(store, setup):
    engine, api = setup
    engine.reload_source("invoices")
    api.seen.clear()
    [r] = engine.reload_source("invoices", incremental="auto")
    assert r.mode == "incremental" and len(api.seen) == 3


def test_the_window_needs_one_day_per_statement_and_some_days():
    with pytest.raises(StatementError, match="one day per statement"):
        normalize({"parts": [dates(days=2, incremental_from="2026-11-02")]}, URL, None)
    with pytest.raises(StatementError, match="after the last day"):
        normalize({"parts": [dates(incremental_from="2026-11-09")]}, URL, None)
    with pytest.raises(StatementError, match="isn't a date"):
        normalize({"parts": [dates(incremental_from="yesterday")]}, URL, None)
    assert "incremental_from" not in normalize({"parts": [dates(incremental_from=" ")]}, URL, None)["parts"][0]


def test_the_page_sees_the_window_and_the_queue_runs_it(store, setup):
    engine, api = setup
    client = TestClient(create_app(engine, TOKEN))
    src = next(s for s in client.get("/api/sources", headers=AUTH).json() if s["name"] == "invoices")
    assert src["statement_window"] == {"from": "today-2", "statements": 3}
    preview = curl.preview_statements(store.get_source("invoices").config, store.get_source("invoices").config["statements"])
    assert any("An incremental load runs 3 statements" in n for n in preview["notes"])

    queue: LoadQueue = client.app.state.load_queue
    engine.reload_source("invoices")
    api.seen.clear()
    item = client.post("/api/queue", headers=AUTH, json={"source": "invoices", "incremental": True})
    assert item.status_code == 202, item.text  # no incremental column needed
    done = queue.wait(item.json()["item"]["id"], 30)
    assert done["status"] == "succeeded" and done["tables"][0]["mode"] == "incremental", done
    assert len(api.seen) == 3
