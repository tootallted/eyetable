"""Next-link paging with an API's fixed page size, like a jobs API that returns
1,000 rows a page and a nextLink to the next one."""

import time
import urllib.parse

import httpx
import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from eyetable.api import create_app
from eyetable.connectors import build_connector
from eyetable.connectors.curl_source import CurlConnector
from eyetable.curl import CurlError, infer_paging, plan
from eyetable.engine import Engine

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
KEY = Fernet.generate_key().decode()
PAGE = 1000
JOBS = [{"jobId": i, "jobNumber": f"J-{i:05d}", "status": "open" if i % 3 else "closed"} for i in range(1, 2501)]
HOST = "https://jobs.test"
AUTHED = " --header 'Authorization: Bearer tok_123'"


class JobsApi:
    """2,500 jobs, 1,000 a page (fixed). Counts requests so stopping can be checked."""

    def __init__(self) -> None:
        self.requests: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(str(request.url))
        if request.headers.get("authorization") != "Bearer tok_123":
            return httpx.Response(401)
        q = dict(urllib.parse.parse_qsl(request.url.query.decode()))
        skip = int(q.get("$skip", 0))
        chunk = JOBS[skip: skip + PAGE]
        path = request.url.path
        if path == "/api/jobs":  # nextLink only while there is more
            body = {"value": chunk}
            if skip + PAGE < len(JOBS):
                body["nextLink"] = f"{HOST}/api/jobs?$skip={skip + PAGE}"
            return httpx.Response(200, json=body)
        if path == "/api/sticky":  # a nextLink on every page, even the last (and past it)
            return httpx.Response(200, json={"value": chunk, "nextLink": f"{HOST}/api/sticky?$skip={skip + PAGE}"})
        if path == "/api/hal":  # rows and a relative link in HAL style
            body = {"_embedded": {"jobs": chunk}, "_links": {"self": {"href": f"/api/hal?$skip={skip}"}}}
            if skip + PAGE < len(JOBS):
                body["_links"]["next"] = {"href": f"/api/hal?$skip={skip + PAGE}"}
            return httpx.Response(200, json=body)
        if path == "/api/odd":  # a link under a name eyetable doesn't guess, as a query only
            body = {"jobs": chunk, "paging": {"continueAt": f"?$skip={skip + PAGE}" if skip + PAGE < len(JOBS) else None}}
            return httpx.Response(200, json=body)
        return httpx.Response(404)


@pytest.fixture
def api():
    return JobsApi()


def engine_for(store, api):
    def factory(type_, config):
        if type_ == "curl":
            return CurlConnector(config, client=httpx.Client(transport=httpx.MockTransport(api)))
        return build_connector(type_, config)
    return Engine(store, factory, environ={"EYETABLE_SECRET_KEY": KEY})


def http(api):
    return httpx.Client(transport=httpx.MockTransport(api))


def cmd(path):
    return f"curl --location '{HOST}{path}' \\\n" + AUTHED


def test_link_names_are_found_in_their_usual_spellings():
    for body, path in [
        ({"value": [], "nextLink": "https://a.test/x?p=2"}, "nextLink"),
        ({"value": [], "next_link": "https://a.test/x?p=2"}, "next_link"),
        ({"value": [], "@odata.nextLink": "https://a.test/x?p=2"}, "@odata.nextLink"),
        ({"data": [], "links": {"next": "/x?p=2"}}, "links.next"),
        ({"data": [], "_links": {"next": {"href": "/x?p=2"}}}, "_links.next"),
        ({"data": [], "nextPageUrl": "?p=2"}, "nextPageUrl"),
    ]:
        spec, _ = infer_paging("https://a.test/x", None, body)
        assert spec and spec["next_path"] == path, body
    spec, _ = infer_paging("https://a.test/x", None, {"data": [], "next": 2})  # a number isn't a link
    assert spec is None


def test_every_page_loads_and_the_last_short_page_ends_it(store, api):
    engine = engine_for(store, api)
    [result] = engine.load_curl(cmd("/api/jobs"), "peak_jobs", client=http(api), page_size=PAGE)
    assert (result.status, result.rows) == ("succeeded", 2500), result.error
    rows = store.read_table("peak_jobs__data")
    assert [r["jobid"] for r in rows] == list(range(1, 2501))  # column names are lowercased
    assert [r["ET_id"] for r in rows[:3]] == [1, 2, 3] and rows[-1]["ET_id"] == 2500
    paging = store.get_source("peak_jobs").config["pagination"]["data"]
    assert paging == {"type": "next_link", "next_path": "nextLink", "page_size": PAGE, "max_pages": 1000}


def test_page_size_stops_an_api_that_always_returns_a_link(store, api):
    engine = engine_for(store, api)
    [result] = engine.load_curl(cmd("/api/sticky"), "sticky", client=http(api), page_size=PAGE)
    assert result.rows == 2500
    loads = api.requests[2:]  # after the check and the discovery requests
    assert loads == [f"{HOST}/api/sticky", f"{HOST}/api/sticky?$skip=1000", f"{HOST}/api/sticky?$skip=2000"]
    # 1,000 + 1,000 + 500, and no request past the short page

    # without the page size it asks once more, gets an empty page, and stops there
    api.requests.clear()
    [result] = engine.load_curl(cmd("/api/sticky"), "sticky2", client=http(api))
    assert result.rows == 2500 and len(api.requests[2:]) == 4


def test_reload_and_schedules_keep_the_settings(store, api):
    engine = engine_for(store, api)
    engine.load_curl(cmd("/api/sticky"), "sticky", client=http(api), page_size=PAGE)
    api.requests.clear()
    [again] = engine.reload_source("sticky")
    assert again.rows == 2500 and len(api.requests) == 3


def test_hal_rows_and_relative_links(store, api):
    p, _ = plan(cmd("/api/hal"), KEY, client=http(api), page_size=PAGE)
    assert p.next_field == "_links.next" and p.records_field == "_embedded.jobs" and p.rows == PAGE
    engine = engine_for(store, api)
    [result] = engine.load_curl(cmd("/api/hal"), "hal_jobs", client=http(api), page_size=PAGE)
    assert result.rows == 2500


def test_fields_eyetable_cant_guess_can_be_named(store, api):
    p, _ = plan(cmd("/api/odd"), KEY, client=http(api))
    assert p.pagination is None and p.records_field == "jobs"
    assert p.link_fields == [{"field": "paging.continueAt", "value": "?$skip=1000"}]

    p, _ = plan(cmd("/api/odd"), KEY, client=http(api), page_size=PAGE)
    assert any("no next-page link was found" in w for w in p.warnings)

    p, _ = plan(cmd("/api/odd"), KEY, client=http(api), page_size=PAGE, next_field="paging.continueAt")
    assert p.pagination["next_path"] == "paging.continueAt" and "fewer than 1,000 rows" in p.paging_text
    engine = engine_for(store, api)
    [result] = engine.load_curl(cmd("/api/odd"), "odd_jobs", client=http(api), page_size=PAGE,
                                next_field="paging.continueAt", records_field="jobs")
    assert result.rows == 2500
    assert store.get_source("odd_jobs").config["records_path"] == {"data": "jobs"}


def test_bad_settings_are_explained(api):
    with pytest.raises(CurlError, match="no nope field"):
        plan(cmd("/api/jobs"), KEY, client=http(api), records_field="nope")
    with pytest.raises(CurlError, match="isn't a link"):
        plan(cmd("/api/odd"), KEY, client=http(api), next_field="jobs.0.jobNumber")
    p, _ = plan(cmd("/api/jobs"), KEY, client=http(api), next_field="missing")
    assert any("no missing value" in w for w in p.warnings)
    p, _ = plan(cmd("/api/jobs"), KEY, client=http(api), page_size=10)
    assert any("more than the 10 page size" in w for w in p.warnings)


def test_endpoints_take_the_settings(store, api):
    web = TestClient(create_app(engine_for(store, api), TOKEN, curl_client=http(api)))
    p = web.post("/api/curl/inspect", headers=AUTH, json={"curl": cmd("/api/jobs"), "page_size": PAGE}).json()
    assert p["next_field"] == "nextLink" and p["records_field"] == "value" and p["page_size"] == PAGE
    assert p["link_fields"] == [{"field": "nextLink", "value": f"{HOST}/api/jobs?$skip=1000"}]
    assert p["list_fields"] == [{"field": "value", "rows": PAGE}]
    assert "fewer than 1,000 rows" in p["paging_text"]
    assert web.post("/api/curl/inspect", headers=AUTH, json={"curl": cmd("/api/jobs"), "page_size": 0}).status_code == 422

    job = web.post("/api/curl", headers=AUTH, json={"curl": cmd("/api/sticky"), "name": "jobs", "page_size": PAGE}).json()["job_id"]
    for _ in range(200):
        state = web.get(f"/api/jobs/{job}", headers=AUTH).json()
        if state["status"] != "running":
            break
        time.sleep(0.05)
    assert state["status"] == "succeeded", state["error"]
    assert state["result"]["rows"] == 2500
