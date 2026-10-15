from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from eyetable.api import create_app
from eyetable.engine import Engine

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
CSV = b"name,qty\nbolt,4\nnut,10\n"
PAGES = Path(__file__).resolve().parents[1] / "docs"


@pytest.fixture
def client(store):
    app = create_app(Engine(store), TOKEN, cors_origins=["https://tootallted.github.io"], max_upload_bytes=1024)
    return TestClient(app)


def post_csv(client, name, content=CSV, filename="parts.csv", **form):
    return client.post(
        f"/api/sources/{name}/uploads", headers=AUTH, files={"file": (filename, content, "text/csv")}, data=form
    )


def test_health_is_open_but_everything_else_needs_the_token(client):
    assert client.get("/api/health").json()["ok"] is True
    assert client.get("/api/sources").status_code == 401
    assert client.get("/api/sources", headers={"Authorization": "Bearer nope"}).status_code == 401
    assert client.get("/api/sources", headers=AUTH).json() == []


def test_drop_create_then_replace_and_append(client):
    r = post_csv(client, "parts", create="true")
    assert r.status_code == 200, r.text
    body = r.json()
    assert (body["rows"], body["table"], body["source"]["rows"]) == (2, "parts__data", 2)

    assert post_csv(client, "parts", filename="parts_v2.csv", mode="append").json()["source"]["rows"] == 4
    assert post_csv(client, "parts", filename="parts_v3.csv", mode="replace").json()["source"]["rows"] == 2

    [summary] = client.get("/api/sources", headers=AUTH).json()
    assert summary["name"] == "parts" and summary["type"] == "file"
    assert summary["last_upload"]["filename"] == "parts_v3.csv"

    detail = client.get("/api/sources/parts", headers=AUTH).json()
    assert [u["filename"] for u in detail["uploads"]] == ["parts_v3.csv", "parts_v2.csv", "parts.csv"]
    assert all(r["status"] == "succeeded" for r in detail["runs"])

    preview = client.get("/api/sources/parts/preview", headers=AUTH).json()
    assert preview["columns"][:4] == ["ET_id", "ET_timestamp", "ET_source", "ET_run_id"]
    assert [row[0] for row in preview["rows"]] == [1, 2]
    assert preview["rows"][0][2] == "parts - parts_v3.csv"


def test_upload_errors(client):
    assert post_csv(client, "Parts", create="true").status_code == 422
    assert post_csv(client, "parts", filename="parts.xlsx", create="true").status_code == 415
    assert post_csv(client, "parts", content=b"x" * 2048, create="true").status_code == 413
    assert post_csv(client, "parts", content=b"", create="true").status_code == 400
    assert post_csv(client, "missing").status_code == 404
    assert post_csv(client, "parts", create="true").status_code == 200
    assert post_csv(client, "parts", create="true").status_code == 409
    assert post_csv(client, "parts", mode="merge").status_code == 400


def test_cors_preflight_from_github_pages(client):
    r = client.options(
        "/api/sources",
        headers={
            "Origin": "https://tootallted.github.io",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "authorization",
            "Access-Control-Request-Private-Network": "true",
        },
    )
    assert r.status_code == 200
    assert r.headers["access-control-allow-origin"] == "https://tootallted.github.io"
    assert r.headers["access-control-allow-private-network"] == "true"

    other = client.options(
        "/api/sources", headers={"Origin": "https://evil.example", "Access-Control-Request-Method": "GET"}
    )
    assert "access-control-allow-origin" not in other.headers


def test_serves_the_page_locally(store):
    app = create_app(Engine(store), TOKEN, pages_dir=PAGES)
    client = TestClient(app)
    r = client.get("/source_management/")
    assert r.status_code == 200 and "Source management" in r.text
    assert client.get("/", follow_redirects=False).headers["location"] == "/source_management/"


def test_token_is_required():
    with pytest.raises(ValueError):
        create_app(None, "")
