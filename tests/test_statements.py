"""One curl source run as a set of statements, all landing in its one table."""

import datetime as dt
import time
import urllib.parse

import httpx
import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from eyetable import curl, statements
from eyetable.api import create_app
from eyetable.connectors import build_connector
from eyetable.connectors.curl_source import CurlConnector
from eyetable.engine import Engine
from eyetable.groups import GroupRunner
from eyetable.statements import StatementError, count, expand, normalize, parse_day, pieces, rewrite

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
KEY = Fernet.generate_key().decode()
TODAY = dt.date(2026, 11, 6)
FILTER = "status eq 'received, paid' and lastActionDate ge 2026-11-04 and lastActionDate le 2026-11-04 and buyerDUNS eq 108203241"
URL = "https://api.test/invoices?%24filter=" + urllib.parse.quote(FILTER) + "&%24top=100"
COMMAND = f"curl --location '{URL}' --header 'Authorization: Bearer tok_123'"


def dates(**extra):
    return {"target": "query", "name": "$filter", "find": "2026-11-04", "kind": "dates",
            "from": "2026-11-01", "to": "2026-11-03", **extra}


def filter_of(url):
    return dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))["$filter"]


def urls(plan):
    return [rewrite(URL, None, s.changes)[0] for s in expand(plan, TODAY)]


class Api:
    """Invoices by day from an OData-style filter, plus a page-numbered endpoint."""

    def __init__(self):
        self.seen = []
        self.fail_on = None

    def __call__(self, request):
        self.seen.append(str(request.url))
        if request.headers.get("authorization") != "Bearer tok_123":
            return httpx.Response(401)
        q = dict(urllib.parse.parse_qsl(request.url.query.decode()))
        if request.url.path == "/pages":
            page = int(q["page"])
            return httpx.Response(200, json={"data": [{"id": page}] if page <= 3 else []})
        if request.url.path == "/linked":
            day, start = q["day"], int(q.get("start", 0))
            body = {"value": [{"day": day, "n": start}]}
            if start == 0:
                body["nextLink"] = f"https://api.test/linked?day={day}&start=1"
            return httpx.Response(200, json=body)
        day = filter_of(str(request.url)).split("ge ")[1][:10]
        if day == self.fail_on:
            return httpx.Response(503)
        return httpx.Response(200, json={"value": [{"invoice": f"{day}-{i}", "day": day} for i in (1, 2)]})


@pytest.fixture
def api():
    return Api()


def engine_for(store, api):
    def factory(type_, config):
        if type_ == "curl":
            return CurlConnector(config, client=httpx.Client(transport=httpx.MockTransport(api)))
        return build_connector(type_, config)
    return Engine(store, factory, environ={"EYETABLE_SECRET_KEY": KEY})


def http(api):
    return httpx.Client(transport=httpx.MockTransport(api))


def add_set(store, name, plan):
    src = store.get_source(name)
    store.save_source(name, src.type, {**src.config, "statements": curl.clean_statements(src.config, plan)})


# -- building statements ---------------------------------------------------------------

def test_a_date_series_changes_every_mention():
    plan = normalize({"parts": [dates()]}, URL, None, TODAY)
    assert count(plan, TODAY) == 3
    first, _, last = urls(plan)
    assert "ge 2026-11-01 and lastActionDate le 2026-11-01" in filter_of(first)
    assert "ge 2026-11-03 and lastActionDate le 2026-11-03" in filter_of(last)
    assert "%20" in first and "+" not in first and "108203241" in filter_of(first)


def test_windows_put_the_last_day_in_the_later_mention():
    plan = normalize({"parts": [dates(**{"from": "2026-11-01", "to": "2026-11-10", "days": 4, "end_find": "2026-11-04"})]}, URL, None, TODAY)
    # "to" is capped at the last day, however the windows fall
    assert [s.label for s in expand(plan, TODAY)] == ["2026-11-01 to 2026-11-04", "2026-11-05 to 2026-11-08", "2026-11-09 to 2026-11-10"]
    assert "ge 2026-11-09 and lastActionDate le 2026-11-10" in filter_of(urls(plan)[2])


def test_a_window_between_two_different_dates():
    url = "https://api.test/x?from=2026-10-01&to=2026-10-31"
    part = {"target": "query", "name": "from", "find": "2026-10-01", "kind": "dates", "from": "2026-11-01", "to": "2026-11-14", "days": 7}
    # different parameters are different places, so this takes two parts; in one value it takes end_find
    url = "https://api.test/x?range=" + urllib.parse.quote("2026-10-01..2026-10-31")
    part = {**part, "name": "range", "end_find": "2026-10-31"}
    plan = normalize({"parts": [part]}, url, None, TODAY)
    got = [rewrite(url, None, s.changes)[0] for s in expand(plan, TODAY)]
    assert [urllib.parse.unquote(u).split("=")[1] for u in got] == ["2026-11-01..2026-11-07", "2026-11-08..2026-11-14"]


@pytest.mark.parametrize("text, written", [("11/04/2026", "11/01/2026"), ("20261104", "20261101"), ("2026-11-04", "2026-11-01")])
def test_dates_keep_their_format(text, written):
    url = f"https://api.test/x?d={urllib.parse.quote(text, safe='')}"
    part = {"target": "query", "name": "d", "find": text, "kind": "dates", "from": "2026-11-01", "to": "2026-11-01"}
    [statement] = expand(normalize({"parts": [part]}, url, None, TODAY), TODAY)
    assert urllib.parse.unquote(rewrite(url, None, statement.changes)[0]).endswith("d=" + written)


def test_rolling_dates_move_with_today():
    assert parse_day("today", TODAY) == TODAY
    assert parse_day("today-30", TODAY) == dt.date(2026, 10, 7)
    assert parse_day(" Today + 1 ", TODAY) == dt.date(2026, 11, 7)
    plan = normalize({"parts": [dates(**{"from": "today-2", "to": "today"})]}, URL, None, TODAY)
    assert [s.label for s in expand(plan, TODAY)] == ["2026-11-04", "2026-11-05", "2026-11-06"]
    assert [s.label for s in expand(plan, TODAY + dt.timedelta(days=1))][-1] == "2026-11-07"


def test_numbers_and_lists_and_every_combination():
    number = {"target": "query", "name": "$top", "find": "100", "kind": "numbers", "from": 10, "to": 30, "step": 10}
    listed = {"target": "query", "name": "$filter", "find": "received, paid", "kind": "list", "values": ["received", " paid ", "", "paid"]}
    plan = normalize({"parts": [listed, number]}, URL, None, TODAY)
    assert plan["parts"][0]["values"] == ["received", "paid"]
    assert [s.label for s in expand(plan, TODAY)] == [
        "received · 10", "received · 20", "received · 30", "paid · 10", "paid · 20", "paid · 30"]
    assert "status eq 'paid' and" in filter_of(urls(plan)[3])


def test_text_only_matches_on_its_own():
    url = "https://api.test/x?f=" + urllib.parse.quote("a eq 1 and b eq 2011 and c eq 1x and d eq 21")
    part = {"target": "query", "name": "f", "find": "1", "kind": "list", "values": ["9"]}
    [statement] = expand(normalize({"parts": [part]}, url, None, TODAY), TODAY)
    assert filter_of(url.replace("?f=", "?$filter=")) == "a eq 1 and b eq 2011 and c eq 1x and d eq 21"
    assert urllib.parse.unquote(rewrite(url, None, statement.changes)[0]).endswith("f=a eq 9 and b eq 2011 and c eq 9x and d eq 21")


def test_a_value_is_not_changed_again_by_another_part():
    url = "https://api.test/x?f=" + urllib.parse.quote("a eq 1 and b eq 2")
    parts = [{"target": "query", "name": "f", "find": "1", "kind": "list", "values": ["2"]},
             {"target": "query", "name": "f", "find": "2", "kind": "list", "values": ["3"]}]
    [statement] = expand(normalize({"parts": parts}, url, None, TODAY), TODAY)
    assert urllib.parse.unquote(rewrite(url, None, statement.changes)[0]).endswith("f=a eq 2 and b eq 3")


def test_path_and_body():
    url = "https://api.test/v1/2026-11-04/jobs"
    body = {"mode": "raw", "raw": '{"year": 2026, "status": "open"}'}
    parts = [{"target": "path", "find": "2026-11-04", "kind": "dates", "from": "2026-11-05", "to": "2026-11-05"},
             {"target": "body", "find": "open", "kind": "list", "values": ["closed"]}]
    [statement] = expand(normalize({"parts": parts}, url, body, TODAY), TODAY)
    got_url, got_body = rewrite(url, body, statement.changes)
    assert got_url == "https://api.test/v1/2026-11-05/jobs" and got_body["raw"] == '{"year": 2026, "status": "closed"}'
    assert body["raw"] == '{"year": 2026, "status": "open"}'


def test_a_blank_find_means_the_whole_value():
    url = "https://api.test/x?page=1"
    plan = normalize({"parts": [{"target": "query", "name": "page", "kind": "numbers", "from": 1, "to": 2}]}, url, None, TODAY)
    assert plan["parts"][0]["find"] == "1" and plan["parts"][0]["step"] == 1


@pytest.mark.parametrize("change, message", [
    ({"parts": []}, "choose something"),
    ({"parts": [dates()] * 6}, "at most 5"),
    ({"parts": [dates(name="nope")]}, "no nope parameter"),
    ({"parts": [dates(find="1999-01-01")]}, "isn't in"),
    ({"parts": [dates(find="108203241")]}, "isn't a date"),
    ({"parts": [dates(**{"from": "soon"})]}, "isn't a date"),
    ({"parts": [dates(**{"from": "2026-12-01"})]}, "none to run"),
    ({"parts": [dates(days=0)]}, "between 1 and 366"),
    ({"parts": [dates(end_find="2026-01-01")]}, "isn't a date|isn't in"),
    ({"parts": [dates(kind="other")]}, "dates, numbers or a list"),
    ({"parts": [dates(target="headers")]}, "query, the path or the body"),
    ({"parts": [{"target": "query", "name": "$top", "kind": "numbers", "from": 1, "to": 0}]}, "none to run"),
    ({"parts": [{"target": "query", "name": "$top", "kind": "numbers", "from": 1, "to": 5, "step": 0}]}, "at least 1"),
    ({"parts": [{"target": "query", "name": "$top", "kind": "numbers", "from": "a", "to": 5}]}, "whole number"),
    ({"parts": [{"target": "query", "name": "$top", "kind": "list", "values": [" "]}]}, "at least one value"),
    ({"parts": [{"target": "query", "name": "$top", "kind": "numbers", "from": 1, "to": 6000}]}, "more than 5,000"),
    ({"parts": [dates(**{"from": "1900-01-01", "to": "2026-01-01"})]}, "more than 5,000"),
    ({"parts": [dates()], "pause_seconds": 90}, "between 0 and 60"),
])
def test_bad_sets_are_explained(change, message):
    with pytest.raises(StatementError, match=message):
        normalize(change, URL, None, TODAY)


def test_too_many_combinations():
    number = {"target": "query", "name": "$top", "kind": "numbers", "from": 1, "to": 100}
    with pytest.raises(StatementError, match="10,000 statements"):
        normalize({"parts": [number, {**number, "to": 100, "name": "$top"}]}, URL, None, TODAY)


def test_pieces_of_a_value():
    found = {p["text"]: p for p in pieces(FILTER)}
    assert found["2026-11-04"] == {"text": "2026-11-04", "kind": "dates", "count": 2}
    assert found["108203241"]["kind"] == "numbers"
    assert found["received, paid"]["kind"] == "list"
    assert list(found) == ["received, paid", "2026-11-04", "108203241"]
    assert [p["text"] for p in pieces("from 2026-11-04T00:00:00Z")] == ["2026-11-04"]
    assert [p["kind"] for p in pieces("20261104")] == ["dates"]
    assert pieces("{{secret_1}}") == []


# -- a stored source -------------------------------------------------------------------

@pytest.mark.parametrize("flag", ["-d", "--data", "--data-raw"])
def test_a_json_body_is_sent_as_it_is(flag):
    req = curl.parse_curl(f"""curl https://api.test/x -H 'Content-Type: application/json' {flag} '{{"a": 1, "b": "c&d=e"}}'""")
    collection, _ = curl.build_collection(req)
    assert collection["item"][0]["request"]["body"]["raw"] == '{"a": 1, "b": "c&d=e"}'
    form = curl.build_collection(curl.parse_curl("curl https://api.test/x -d 'a=1&token=s3cret'"))[0]
    assert "s3cret" not in str(form)  # a real form still has its secrets taken out


def test_components_hide_secrets():
    config = curl.prepare("curl 'https://api.test/x/2026-11-04?day=2026-11-04&api_key=abc123' -H 'Authorization: Bearer tok_123' -d '{\"a\": 1}'", KEY)[1]
    parts = curl.components(config)
    assert parts["method"] == "POST" and parts["host"] == "api.test" and parts["path"] == "/x/2026-11-04"
    assert [p["text"] for p in parts["path_pieces"]] == ["2026-11-04"]
    day, key = parts["query"]
    assert day == {"name": "day", "value": "2026-11-04", "secret": False,
                   "pieces": [{"text": "2026-11-04", "kind": "dates", "count": 1}]}
    assert key["secret"] is True and key["pieces"] == [] and "abc123" not in str(parts) and "tok_123" not in str(parts)
    assert parts["body"] == '{"a": 1}'
    with pytest.raises(StatementError):  # a masked value can't be changed
        curl.clean_statements(config, {"parts": [{"target": "query", "name": "api_key", "kind": "list", "values": ["x"]}]})


def test_every_statement_lands_in_the_one_table(store, api):
    engine = engine_for(store, api)
    engine.load_curl(COMMAND, "invoices", client=http(api))
    assert len(store.read_table("invoices__data")) == 2
    add_set(store, "invoices", {"parts": [dates()]})

    api.seen.clear()
    [result] = engine.reload_source("invoices")
    assert (result.status, result.rows) == ("succeeded", 6), result.error
    assert len(api.seen) == 3
    table = store.read_table("invoices__data")
    assert [r["day"] for r in table] == ["2026-11-01"] * 2 + ["2026-11-02"] * 2 + ["2026-11-03"] * 2
    assert [r["ET_id"] for r in table] == [1, 2, 3, 4, 5, 6]
    assert store.get_source("invoices").config["variables"]["secret_1"].startswith("eyetable-enc:")  # still encrypted


def test_a_failing_statement_stops_the_run_and_keeps_the_table(store, api):
    engine = engine_for(store, api)
    engine.load_curl(COMMAND, "invoices", client=http(api))
    add_set(store, "invoices", {"parts": [dates()]})
    engine.reload_source("invoices")
    api.fail_on = "2026-11-02"
    [result] = engine.reload_source("invoices")
    assert result.status == "failed"
    assert "statement 2 of 3 (2026-11-02): the API answered 503" in result.error and "tok_123" not in result.error
    assert len(store.read_table("invoices__data")) == 6


def test_each_statement_pages_as_usual(store, api):
    engine = engine_for(store, api)
    engine.load_curl("curl 'https://api.test/linked?day=2026-11-04' -H 'Authorization: Bearer tok_123'", "linked", client=http(api))
    add_set(store, "linked", {"parts": [{"target": "query", "name": "day", "kind": "dates", "from": "2026-11-01", "to": "2026-11-02"}]})
    [result] = engine.reload_source("linked")
    assert result.rows == 4, result.error
    assert [(r["day"], r["n"]) for r in store.read_table("linked__data")] == [
        ("2026-11-01", 0), ("2026-11-01", 1), ("2026-11-02", 0), ("2026-11-02", 1)]


def test_a_set_over_the_paging_parameter_turns_paging_off_and_can_stop_early(store, api):
    engine = engine_for(store, api)
    engine.load_curl("curl 'https://api.test/pages?page=1' -H 'Authorization: Bearer tok_123'", "paged", client=http(api))
    src = store.get_source("paged")
    assert src.config["pagination"]["data"]["type"] == "page"
    plan = {"parts": [{"target": "query", "name": "page", "kind": "numbers", "from": 2, "to": 50}], "stop_when_empty": True}
    add_set(store, "paged", plan)
    api.seen.clear()
    [result] = engine.reload_source("paged")
    assert result.rows == 2, result.error  # pages 2 and 3
    assert [urllib.parse.urlsplit(u).query for u in api.seen] == ["page=2", "page=3", "page=4"]
    preview = curl.preview_statements(src.config, curl.clean_statements(src.config, plan))
    assert "Automatic paging is off" in preview["notes"][0]


def test_groups_run_the_set(store, api):
    engine = engine_for(store, api)
    engine.load_curl(COMMAND, "invoices", client=http(api))
    add_set(store, "invoices", {"parts": [dates()]})
    runner = GroupRunner(engine)
    group = runner.create("Invoices", ["invoices"])
    run = store.get_group_run(runner.run(group.id, wait=True))
    assert run.status == "succeeded" and run.results[0]["rows"] == 6


def test_a_pause_spaces_statements_out(store, api, monkeypatch):
    slept = []
    monkeypatch.setattr("eyetable.connectors.curl_source.time.sleep", slept.append)
    engine = engine_for(store, api)
    engine.load_curl(COMMAND, "invoices", client=http(api))
    add_set(store, "invoices", {"parts": [dates()], "pause_seconds": 1.5})
    engine.reload_source("invoices")
    assert slept == [1.5, 1.5]


def test_a_set_that_has_run_out_fails_rather_than_emptying_the_table(store, api):
    engine = engine_for(store, api)
    engine.load_curl(COMMAND, "invoices", client=http(api))
    src = store.get_source("invoices")
    stale = {"parts": [dates(**{"from": "today+1", "to": "2026-01-01", "days": 1})], "pause_seconds": 0, "stop_when_empty": False}
    store.save_source("invoices", "curl", {**src.config, "statements": stale})
    [result] = engine.reload_source("invoices")
    assert result.status == "failed" and "none to run" in result.error
    assert len(store.read_table("invoices__data")) == 2


# -- endpoints ---------------------------------------------------------------------------

def test_endpoints(store, api):
    engine = engine_for(store, api)
    web = TestClient(create_app(engine, TOKEN))
    engine.load_curl(COMMAND, "invoices", client=http(api))
    engine.ingest_upload("plain", "p.csv", b"a\n1\n", create=True)

    detail = web.get("/api/sources/invoices", headers=AUTH).json()
    assert detail["statements"] is None and "statement_count" not in detail
    assert [q["name"] for q in detail["request"]["query"]] == ["$filter", "$top"]
    assert detail["request"]["headers"] == [{"name": "Authorization", "value": "••••••"}]
    assert "request" not in web.get("/api/sources/plain", headers=AUTH).json()

    body = {"parts": [dates()], "pause_seconds": 0, "stop_when_empty": False}
    before = store.get_source("invoices").updated_at
    p = web.post("/api/sources/invoices/statements/preview", headers=AUTH, json=body).json()
    assert p["count"] == 3 and len(p["first"]) == 3 and p["last"] is None
    assert "ge 2026-11-01" in p["first"][0]["url"]
    assert store.get_source("invoices").updated_at == before  # a preview saves nothing

    long = {**body, "parts": [dates(**{"from": "2026-10-01", "to": "2026-10-31"})]}
    p = web.post("/api/sources/invoices/statements/preview", headers=AUTH, json=long).json()
    assert p["count"] == 31 and len(p["first"]) == 4 and "2026-10-31" in p["last"]["url"]

    r = web.post("/api/sources/invoices/statements/preview", headers=AUTH, json={"parts": []})
    assert r.status_code == 400 and "choose something" in r.json()["detail"]
    assert web.post("/api/sources/plain/statements/preview", headers=AUTH, json=body).status_code == 400
    assert web.put("/api/sources/invoices/statements", json=body).status_code == 401

    assert web.put("/api/sources/invoices/statements", headers=AUTH, json=body).json()["count"] == 3
    detail = web.get("/api/sources/invoices", headers=AUTH).json()
    assert detail["statement_count"] == 3 and detail["statements"]["parts"][0]["find"] == "2026-11-04"
    summary = {s["name"]: s for s in web.get("/api/sources", headers=AUTH).json()}
    assert summary["invoices"]["statement_count"] == 3

    job = web.post("/api/sources/invoices/run", headers=AUTH, json={}).json()["job_id"]
    for _ in range(100):
        state = web.get(f"/api/jobs/{job}", headers=AUTH).json()
        if state["status"] != "running":
            break
        time.sleep(0.05)
    assert state["status"] == "succeeded" and state["result"]["rows"] == 6, state["error"]
    assert state["result"]["source"]["rows"] == 6

    assert web.delete("/api/sources/invoices/statements", headers=AUTH).status_code == 200
    assert "statements" not in store.get_source("invoices").config
    [result] = engine.reload_source("invoices")
    assert result.rows == 2
    assert web.post("/api/sources/plain/run", headers=AUTH, json={}).status_code == 400
