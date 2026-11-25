"""HTTP API for the eyetable web pages: `eyetable serve`.

Every /api route except /api/health needs `Authorization: Bearer <token>`,
where the token is EYETABLE_API_TOKEN. The pages may be served from GitHub
Pages (another origin), so CORS is open to EYETABLE_CORS_ORIGINS and the
browser's private-network preflight is answered for http://localhost use.

The API can be exposed to the internet (e.g. through a Cloudflare tunnel), so
each client IP is rate limited, and an IP that keeps sending wrong tokens is
locked out for a while.
"""

import base64
import datetime as dt
import logging
import os
import re
import secrets
import threading
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

from eyetable import __version__
from eyetable.connectors.file import DATASET_KEY
from eyetable.engine import SINGLE_TABLE_TYPES, Engine
from eyetable.gsheets import SheetError, fetch_workbook, fetch_workbook_signed_in
from eyetable.identifiers import SOURCE_NAME, normalize_tag, table_name
from eyetable.query import MAX_LIMIT, QueryError, QueryRunner
from eyetable import dbconnect, entra, filesets, hooks, keys, targets
from eyetable.loadqueue import LoadQueue
from eyetable.curl import auth_scheme, clean_statements, components, parse_curl, preview_statements, secret_places, stored_request
from eyetable.curl import plan as curl_plan
from eyetable.google_auth import GoogleAuthError, GoogleConnection
from eyetable import notify
from eyetable.groups import KEEP, GroupRunner
from eyetable import msfiles
from eyetable.signin import SignIn, SignInError
from eyetable.ratelimit import SlidingWindow, client_ip
from eyetable.schedule import ScheduleError, describe, next_runs, normalize
from eyetable.statements import count as statement_count
from eyetable.credentials import decrypt_secrets
from eyetable.tls import client_cert, describe as describe_certificate
from eyetable.store import GroupNotFound
from eyetable.store import SourceNotFound

DEFAULT_CORS_ORIGINS = ["https://tootallted.github.io"]
DEFAULT_MAX_UPLOAD_BYTES = 50 * 1024 * 1024
log = logging.getLogger("eyetable.api")
PAGES_DIR = Path(__file__).resolve().parents[2] / "docs"  # the GitHub Pages site, in a source checkout


class GroupIn(BaseModel):
    name: str = Field(max_length=80)
    members: list[str] = Field(max_length=200)
    schedule: dict[str, Any] | None = None
    enabled: bool = True
    notify: str | None = Field(default=None, max_length=2000)  # emails for each run's result, comma-separated


class NotifyTestIn(BaseModel):
    to: str = Field(min_length=3, max_length=2000)


class ScheduleIn(BaseModel):
    schedule: dict[str, Any]


class DbIn(BaseModel):
    connection: str = Field(min_length=1, max_length=4000)


class DbItem(BaseModel):
    schema_: str = Field(alias="schema", max_length=256)
    name: str = Field(max_length=256)
    kind: str = "table"
    source: str = Field(max_length=63)


class DbLoadIn(DbIn):
    items: list[DbItem] = Field(min_length=1, max_length=500)
    mode: str = "replace"


class TagsIn(BaseModel):
    tags: list[str] = Field(max_length=30)


class StatementsIn(BaseModel):
    """A curl source's set of statements (see eyetable.statements, which checks it)."""
    parts: list[dict[str, Any]] = Field(max_length=20)
    pause_seconds: float = 0
    stop_when_empty: bool = False


class CertificateIn(BaseModel):
    """A client certificate, as Postman's certificate form has it; files are base64."""
    host: str | None = Field(default=None, max_length=255)
    port: int | None = Field(default=None, ge=1, le=65535)
    pfx: str | None = Field(default=None, max_length=300_000)
    crt: str | None = Field(default=None, max_length=300_000)
    key: str | None = Field(default=None, max_length=300_000)
    ca: str | None = Field(default=None, max_length=300_000)
    passphrase: str | None = Field(default=None, max_length=1000)

    def read(self) -> dict[str, Any]:
        def file(value: str | None) -> bytes | None:
            return base64.b64decode(value, validate=True) if value else None

        return client_cert(file(self.pfx), file(self.crt), file(self.key), file(self.ca),
                           self.passphrase, self.host, self.port)


class EntraSignInIn(BaseModel):
    username: str | None = Field(default=None, max_length=320)
    scope: str = Field(max_length=500)
    authority: str = Field(max_length=500)


class TableChoiceIn(BaseModel):
    schema_: str = Field(alias="schema", max_length=256)
    name: str = Field(max_length=256)
    kind: str = "table"


class TablesIn(BaseModel):
    tables: list[TableChoiceIn] = Field(max_length=2000)


class SettingsIn(BaseModel):
    """Changes to a source's connection; what's left out stays as it is."""
    model_config = {"populate_by_name": True}
    connection: str | None = Field(default=None, max_length=8000)  # database: a new connection string
    password: str | None = Field(default=None, max_length=1000)  # database: a new password
    schema_: str | None = Field(default=None, alias="schema", max_length=256)
    table: str | None = Field(default=None, max_length=256)
    command: str | None = Field(default=None, max_length=100_000)  # curl: a whole new command
    secrets: dict[str, str] | None = None  # curl: secret_N -> new value
    certificate: CertificateIn | None = None  # curl: a new client certificate
    remove_certificate: bool = False
    folder: str | None = Field(default=None, max_length=1000)  # file: the folder on the server holding its files
    pattern: str | None = Field(default=None, max_length=260)  # file: a file name or pattern (ledger_*.csv); "" = uploads
    name: str | None = Field(default=None, max_length=64)  # a new name for the source (and its table)
    test: bool = True  # check the connection before saving


class CurlIn(BaseModel):
    curl: str = Field(min_length=1, max_length=100_000)
    all_pages: bool = True
    page_size: int | None = Field(default=None, ge=1, le=1_000_000)  # the API's fixed rows per page
    next_field: str | None = Field(default=None, max_length=200)  # where the next-page link is
    records_field: str | None = Field(default=None, max_length=200)  # where the rows are
    certificate: CertificateIn | None = None

    def paging(self) -> dict[str, Any]:
        return {"all_pages": self.all_pages, "page_size": self.page_size,
                "next_field": self.next_field, "records_field": self.records_field}

    def client_certificate(self) -> dict[str, Any] | None:
        return self.certificate.read() if self.certificate else None


class CurlLoadIn(CurlIn):
    name: str = Field(max_length=63)
    mode: str = "replace"


class GoogleCodeIn(BaseModel):
    code: str = Field(min_length=1, max_length=2048)


class KeyIn(BaseModel):
    columns: list[str] = Field(default_factory=list, max_length=8)


class ReloadIn(BaseModel):
    # For a Google Sheet source whose sheet is private (see SheetIn.google_token).
    google_token: str | None = Field(default=None, max_length=4096)
    microsoft_token: str | None = Field(default=None, max_length=16384)  # an Excel / CSV file in OneDrive or SharePoint
    incremental: bool = False  # only the rows past each table's incremental column (incremental.py)
    table: str | None = Field(default=None, max_length=128)  # just this one of a database source's tables


class QueueIn(BaseModel):
    source: str = Field(min_length=1, max_length=64)
    table: str | None = Field(default=None, max_length=128)  # one of a database source's tables
    incremental: bool = False
    google_token: str | None = Field(default=None, max_length=4096)  # a private Google Sheet
    microsoft_token: str | None = Field(default=None, max_length=16384)  # a file in OneDrive or SharePoint


class HiddenIn(BaseModel):
    hidden: bool


class NamingIn(BaseModel):
    name: str | None = Field(default=None, max_length=64)  # the source's new name
    table: str | None = Field(default=None, max_length=200)  # which of its tables to rename (its table name now)
    new_table: str | None = Field(default=None, max_length=200)  # its new name: "table" or "schema.table"


class FilesIn(BaseModel):
    folder: str = Field(max_length=1000)
    pattern: str = Field(max_length=260)


class HistoryIn(BaseModel):
    on: bool


class SplitIn(BaseModel):
    column: str | None = Field(default=None, max_length=128)  # None: read in one query


class IncrementIn(BaseModel):
    column: str = Field(min_length=1, max_length=128)


class SheetIn(BaseModel):
    url: str = Field(max_length=2000)
    mode: str = "replace"
    # A short-lived Google access token from the page's "Sign in with Google", to read
    # private sheets as that person. Used for this request only; never stored or logged.
    google_token: str | None = Field(default=None, max_length=4096)
    # Likewise a Microsoft Graph token from "Sign in with Microsoft", for a OneDrive / SharePoint file.
    microsoft_token: str | None = Field(default=None, max_length=16384)


class SessionIn(BaseModel):
    provider: str = Field(pattern="^(google|microsoft)$")
    token: str = Field(min_length=10, max_length=16384)  # Google: an access token; Microsoft: an ID token


def sign_ins(body: Any) -> Any:
    """The caller's sign-ins for reading sheets and files: the Google token alone, or both."""
    if body is None:
        return None
    google, microsoft = getattr(body, "google_token", None), getattr(body, "microsoft_token", None)
    return {"google": google, "microsoft": microsoft} if microsoft else google


class QueryIn(BaseModel):
    sql: str = Field(max_length=100_000)
    limit: int = Field(default=1000, ge=1, le=MAX_LIMIT)


class SaveObjectIn(BaseModel):
    name: str = Field(max_length=63)
    sql: str = Field(max_length=100_000)
    replace: bool = False


class DeployIn(BaseModel):
    sql: str = Field(max_length=200_000)


class DestinationIn(BaseModel):
    name: str = Field(max_length=40)
    connection: str = Field(default="", max_length=4000)  # blank on a change: keep the stored one
    schema_: str | None = Field(default=None, max_length=63, alias="schema")


class DestinationTestIn(BaseModel):
    id: str | None = Field(default=None, max_length=40)
    connection: str = Field(default="", max_length=4000)
    schema_: str | None = Field(default=None, max_length=63, alias="schema")


def _json_safe(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    return value


def create_app(
    engine: Engine,
    token: str,
    cors_origins: list[str] | None = None,
    max_upload_bytes: int = DEFAULT_MAX_UPLOAD_BYTES,
    pages_dir: Path | None = PAGES_DIR,
    requests_per_minute: int = 240,
    uploads_per_hour: int = 120,
    auth_failures_per_15_minutes: int = 10,
    query_runner: QueryRunner | None = None,
    group_runner: GroupRunner | None = None,
    google: GoogleConnection | None = None,
    curl_client: Any = None,
    sheet_fetch: Callable[[str, str | None], Any] | None = None,
    hook_key: str | None = None,
    signin: SignIn | None = None,
    author: Any = None,
) -> FastAPI:
    if not token:
        raise ValueError("an API token is required")
    store = engine.store
    queries = query_runner or QueryRunner(store)
    google = google or GoogleConnection(store, None, None, None)
    signin = signin or SignIn(token)  # with no allow list, only the API token gets in
    if sheet_fetch is None:
        def sheet_fetch(url: str, google_token: Any = None):
            # `google_token`: the caller's Google token, or {"google": ..., "microsoft": ...} (sign_ins)
            tokens = google_token if isinstance(google_token, dict) else {"google": google_token}
            if msfiles.is_microsoft_url(url):
                return msfiles.fetch_workbook(url, tokens.get("microsoft"), max_bytes=max_upload_bytes)
            google_token = tokens.get("google")
            # Whose eyes read the sheet: the person using the page (their token), else the
            # Google account connected for scheduled runs, else nobody (link-shared only).
            if not google_token and google.is_connected():
                try:
                    google_token = google.access_token()
                except GoogleAuthError as exc:
                    raise SheetError(f"the Google account connected for scheduled runs can't be used: {exc}",
                                     needs_sign_in=True) from None
            if google_token:
                return fetch_workbook_signed_in(url, google_token, max_bytes=max_upload_bytes)
            return fetch_workbook(url, max_bytes=max_upload_bytes)
    app = FastAPI(title="eyetable", version=__version__, docs_url="/api/docs", openapi_url="/api/openapi.json")
    load_queue = LoadQueue(engine, sheet_fetch)
    app.state.load_queue = load_queue

    requests_limit = SlidingWindow(requests_per_minute, 60)
    uploads_limit = SlidingWindow(uploads_per_hour, 3600)
    auth_failures = SlidingWindow(auth_failures_per_15_minutes, 900)

    def too_many(wait: float, what: str) -> JSONResponse:
        seconds = max(int(wait) + 1, 1)
        return JSONResponse(
            {"detail": f"too many {what}; try again in {seconds} s"},
            status_code=429,
            headers={"Retry-After": str(seconds)},
        )

    # Added before CORS so CORS ends up outermost and 429s still carry CORS headers.
    @app.middleware("http")
    async def limit_and_harden(request: Request, call_next):
        ip = client_ip(request)
        if request.url.path.startswith("/api/") or request.url.path == "/hook":
            wait = auth_failures.blocked(ip)
            if wait and request.method != "OPTIONS":
                return too_many(wait, "failed sign-in attempts")
            wait = requests_limit.hit(ip)
            if wait:
                return too_many(wait, "requests")
            loads = request.url.path.endswith(("/uploads", "/reload", "/run")) or request.url.path in ("/api/gsheets", "/api/curl", "/api/db/load")
            if request.method == "POST" and loads:
                wait = uploads_limit.hit(ip)
                if wait:
                    return too_many(wait, "uploads")
        try:
            response = await call_next(request)
        except Exception as exc:
            # Answered here, inside CORS: a crash that reaches the server's own 500
            # has no CORS headers, and the page can only say the API is unreachable.
            log.exception("%s %s failed", request.method, request.url.path)
            response = JSONResponse({"detail": f"eyetable hit an unexpected error ({type(exc).__name__}); "
                                               "its log has the details"}, status_code=500)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        if request.url.path.startswith("/api/"):
            response.headers.setdefault("Cache-Control", "no-store")
        return response

    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins if cors_origins is not None else DEFAULT_CORS_ORIGINS,
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["Authorization", "Content-Type", "X-Hook-Key"],
        allow_private_network=True,  # Chrome asks before a public https page may call http://localhost
    )

    bearer = HTTPBearer(auto_error=False)

    def authorized(
        request: Request, credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)]
    ) -> None:
        given = credentials.credentials if credentials else ""
        if given and secrets.compare_digest(given.encode(), token.encode()):
            request.state.user = None  # the API token: no one in particular
            return
        person = signin.check_session(given) if given else None
        if person is None:
            auth_failures.hit(client_ip(request))
            raise HTTPException(401, "missing or wrong API token, or your sign-in has ended",
                                headers={"WWW-Authenticate": "Bearer"})
        request.state.user = person.email

    def user_of(request: Request) -> str | None:
        return getattr(request.state, "user", None)

    @app.get("/api/signin")
    def signin_options() -> dict[str, Any]:
        """Whether the page may offer signing in with Google or Microsoft (nothing here is secret)."""
        return signin.options()

    @app.post("/api/session")
    def start_session(body: SessionIn, request: Request) -> dict[str, Any]:
        """Sign in with a Google access token or a Microsoft ID token; someone on the allow list gets a
        session token to use in place of the API token."""
        try:
            return signin.sign_in(body.provider, body.token)
        except SignInError as exc:
            if exc.status in (401, 403):
                auth_failures.hit(client_ip(request))
            raise HTTPException(exc.status, str(exc)) from None

    @app.get("/api/session")
    def who_am_i(request: Request,
                 credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)]) -> dict[str, Any]:
        """Who the caller is: a signed-in person, or the API token."""
        authorized(request, credentials)
        person = signin.check_session(credentials.credentials)
        return {"email": person.email if person else None, "provider": person.provider if person else "token",
                "name": person.name if person else None}

    auth = [Depends(authorized)]

    def db_source_tables(src, datasets) -> list[dict[str, Any]]:
        """Each table a database source loads, with its rows and when it last loaded."""
        loads = store.table_loads(src.name)
        increments = store.increments()
        hidden = store.hidden()
        keyed, histories = store.keyed_tables(), store.histories()
        catalog = {d.key: d for d in datasets}
        out = []
        for item in dbconnect.db_tables(src.config):
            table = item.get("target") or table_name(src.name, item["name"])
            finished, written, mode = loads.get(item["key"], (None, None, None))
            # a replace writes the whole table, so its count is the table's; otherwise count it
            rows = written if mode == "replace" else store.count_rows(table)
            out.append({"key": item["key"], "schema": item["schema"], "name": item["table"], "kind": item.get("kind", "table"),
                        "table": table, "rows": rows, "last_loaded_at": finished, "incremental": increments.get(table),
                        "split": (item.get("split") or {}).get("column"), "hidden": table in hidden,
                        "key": table in keyed, "history": table in histories,
                        "columns": len(catalog[item["key"]].columns) if item["key"] in catalog else None})
        return out

    def main_table(src) -> str:
        """The table a single-table source's name stands for in queries."""
        if src.type == "dbtable":
            return targets.table_for(src, dbconnect.db_tables(src.config)[0]["name"])
        return targets.table_for(src)

    def source_summary(src, tags: dict[str, list[str]] | None = None) -> dict[str, Any]:
        datasets = [d for _, d in store.list_datasets(src.name)]
        summary: dict[str, Any] = {
            "name": src.name,
            "type": src.type,
            "tags": (tags if tags is not None else store.tags_by_source()).get(src.name, []),
            "created_at": src.created_at,
            "updated_at": src.updated_at,
            "last_loaded_at": store.last_loaded(src.name),
            "datasets": len(datasets),
            "destination": (src.config or {}).get("destination") or "eyetable",  # where its data loads (destinations.py)
        }
        if src.type == "dbtable":
            tables = db_source_tables(src, datasets)
            counted = [t["rows"] for t in tables if t["rows"] is not None]
            summary.update(
                table=tables[0]["table"],
                incremental=tables[0]["incremental"] if len(tables) == 1 else None,
                rows=sum(counted) if counted else None,
                columns=tables[0]["columns"] if len(tables) == 1 else None,
                last_upload=None,
                tables=tables,
            )
        elif src.type in SINGLE_TABLE_TYPES:
            table = targets.table_for(src)
            uploads = store.list_uploads(src.name, limit=1)
            if src.type == "file" and src.config.get("pattern"):
                summary["file_set"] = {"folder": src.config.get("folder"), "pattern": src.config["pattern"]}
            summary.update(
                table=table,
                hidden=table in store.hidden(),
                key=table in store.keyed_tables(),
                history=store.has_history(table),
                incremental=store.get_increment(table),
                provider=src.config.get("provider", "google") if src.type == "gsheet" else None,
                rows=store.count_rows(table),
                columns=len(datasets[0].columns) if datasets else None,
                last_upload=uploads[0] if uploads else None,
            )
        if src.type == "curl" and src.config.get("statements"):
            try:
                summary["statement_count"] = statement_count(src.config["statements"])
            except ValueError:  # e.g. a fixed date range that "today" has moved past
                summary["statement_count"] = 0
            window = targets.statement_window(src)
            if window:  # incremental loads run these statements and replace their rows
                try:
                    count = statement_count(window)
                except ValueError:
                    count = 0
                start = next((p["incremental_from"] for p in src.config["statements"]["parts"] if p.get("incremental_from")), None)
                summary["statement_window"] = {"from": start, "statements": count}
        summary["connection"] = connection_of(src)
        return summary

    def connection_of(src) -> dict[str, str] | None:
        """Where a source's data comes from, so the page can group sources that share it."""
        c = src.config
        if src.type == "dbtable" and c.get("label"):
            return {"key": "db:" + c["label"].lower(), "label": c["label"]}
        if src.type == "gsheet" and c.get("spreadsheet_id"):
            return {"key": "sheet:" + c["spreadsheet_id"],
                    "label": c.get("file_title") or ("Excel file" if c.get("provider") == "microsoft" else "Google Sheet")}
        if src.type == "curl":
            try:
                host = urllib.parse.urlsplit(stored_request(c)["url"]).hostname
            except (KeyError, IndexError, TypeError, ValueError):
                return None
            return {"key": "api:" + host, "label": host} if host else None
        return None

    def get_source(name: str):
        try:
            return store.get_source(name)
        except SourceNotFound:
            raise HTTPException(404, f"no source named {name!r}") from None

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "version": __version__}

    @app.get("/api/sources", dependencies=auth)
    def list_sources() -> list[dict[str, Any]]:
        tags = store.tags_by_source()
        return jsonable_encoder([source_summary(s, tags) for s in store.list_sources()])

    @app.put("/api/sources/{name}/tags", dependencies=auth)
    def set_tags(name: str, body: TagsIn) -> dict[str, Any]:
        """Replace a source's tags. "#Base", "base" and " base " all mean #base."""
        get_source(name)
        try:
            tags = [normalize_tag(t) for t in body.tags]
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None
        store.set_source_tags(name, tags)
        return jsonable_encoder(source_summary(store.get_source(name)))

    @app.get("/api/tags", dependencies=auth)
    def list_tags() -> list[dict[str, Any]]:
        """Every tag in use, with its sources."""
        by_tag: dict[str, list[str]] = {}
        for source, tags in store.tags_by_source().items():
            for tag in tags:
                by_tag.setdefault(tag, []).append(source)
        last_runs = store.last_tag_runs()
        return jsonable_encoder([
            {"tag": t, "sources": sorted(by_tag.get(t, [])), "running": groups.is_running(f"#{t}"), "last_run": last_runs.get(t)}
            for t, _ in store.all_tags()
        ])

    @app.post("/api/tags/{tag}/run", dependencies=auth, status_code=202)
    def run_tag(tag: str, body: ReloadIn | None = None) -> dict[str, Any]:
        """Start reloading every source with the tag; poll /api/group-runs/{run_id} for progress."""
        try:
            tag = normalize_tag(tag)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None
        try:
            run_id = groups.run_tag(tag, google_token=sign_ins(body),
                                    incremental=bool(body and body.incremental))
        except LookupError as exc:
            raise HTTPException(404, str(exc).strip("'\"")) from None
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from None
        return {"run_id": run_id}

    @app.get("/api/sources/{name}", dependencies=auth)
    def source_detail(name: str) -> dict[str, Any]:
        src = get_source(name)
        return jsonable_encoder({
            **source_summary(src),
            "catalog": [d.to_dict() for _, d in store.list_datasets(name)],
            "uploads": store.list_uploads(name, limit=25),
            "runs": store.list_runs(name, limit=25),
            **({"request": components(src.config), "statements": src.config.get("statements")} if src.type == "curl" else {}),
        })

    def settings_of(src) -> dict[str, Any]:
        """A source's connection settings for the page, secrets left out."""
        c = src.config
        out: dict[str, Any] = {"type": src.type}
        if src.type == "dbtable":
            out.update(format="odbc" if "odbc" in c else "url", connection=c.get("odbc") or c.get("url"),
                       has_password=bool(c.get("password")), schema=c.get("schema"), table=c.get("table"),
                       kind=c.get("kind"), label=c.get("label"), tables=len(dbconnect.db_tables(c)))
            if c.get("entra"):
                sign_in = c["entra"]
                out["entra"] = {**sign_in, "signed_in": entra.provider().signed_in(sign_in["username"], sign_in["authority"])}
        elif src.type == "curl":
            cert = c.get("client_cert")
            identity = None
            if cert:
                try:
                    identity = describe_certificate(decrypt_secrets(cert["pem"], engine.secret_key))
                except ValueError:
                    identity = {}
                identity.update(host=cert.get("host"), port=cert.get("port"), has_ca=bool(cert.get("ca")))
            secrets = secret_places(c)
            for secret in secrets:  # say when a "Bearer " goes in front, so just the token can be pasted
                try:
                    scheme = auth_scheme(decrypt_secrets(c["variables"][secret["name"]], engine.secret_key))
                except ValueError:
                    scheme = None
                secret["scheme"] = scheme.strip() if scheme else None
            out.update(secrets=secrets, certificate=identity, request=components(c))
        elif src.type == "gsheet":
            out.update(url=c.get("url"), tab=c.get("tab_title"), file=c.get("file_title"),
                       provider=c.get("provider", "google"))
        elif src.type == "file":
            uploads = store.list_uploads(src.name, limit=1)
            out.update(folder=c.get("folder"), pattern=c.get("pattern"),
                       filename=uploads[0].filename if uploads else None)
            if c.get("pattern"):
                try:
                    files = filesets.matching_files(c.get("folder", ""), c["pattern"])
                    out.update(files=filesets.describe_files(files), file_count=len(files), files_error=None)
                except filesets.FileSetProblem as exc:
                    out.update(files=[], file_count=0, files_error=str(exc))
        out["name"] = src.name
        return out

    @app.get("/api/sources/{name}/settings", dependencies=auth)
    def source_settings(name: str) -> dict[str, Any]:
        """How a source connects, with passwords, tokens and keys left out."""
        return jsonable_encoder(settings_of(get_source(name)))

    @app.put("/api/sources/{name}/settings", dependencies=auth)
    def change_settings(name: str, body: SettingsIn) -> dict[str, Any]:
        """Change how a source connects. With test (the default), nothing is saved unless it works."""
        src = get_source(name)
        try:
            if src.type == "dbtable":
                warnings = engine.update_db_source(name, body.connection, body.password, body.schema_, body.table, body.test)
            elif src.type == "curl":
                certificate = body.certificate.read() if body.certificate else None
                warnings = engine.update_curl_source(name, body.command, body.secrets, certificate,
                                                     body.remove_certificate, body.test)
            elif src.type == "file":
                name, warnings = engine.update_file_source(name, body.folder, body.pattern, body.name, body.test)
            else:
                raise HTTPException(400, f"a {src.type} source's settings can't be changed here")
        except ValueError as exc:
            raise problem(exc) from None
        return jsonable_encoder({**settings_of(store.get_source(name)), "warnings": warnings, "tested": body.test})

    def db_source(name: str):
        src = get_source(name)
        if src.type != "dbtable":
            raise HTTPException(400, f"{name} isn't a database source")
        return src

    @app.get("/api/sources/{name}/objects", dependencies=auth)
    def source_objects(name: str) -> dict[str, Any]:
        """Every table and view in a database source's database, and which the source loads."""
        src = db_source(name)
        try:
            with engine._open("dbtable", src.config) as conn:
                objects = conn.objects()
        except ValueError as exc:
            raise problem(exc) from None
        chosen = {(i["schema"], i["table"]) for i in dbconnect.db_tables(src.config)}
        for obj in objects:
            obj["chosen"] = (obj["schema"], obj["name"]) in chosen
        return jsonable_encoder({"label": src.config.get("label"), "objects": objects})

    @app.put("/api/sources/{name}/tables", dependencies=auth, status_code=202)
    def choose_source_tables(name: str, body: TablesIn) -> dict[str, Any]:
        """Load exactly these tables and views from the source's database, in the background."""
        db_source(name)
        chosen = [{"schema": t.schema_, "name": t.name, "kind": t.kind} for t in body.tables]
        if not chosen:
            raise HTTPException(400, "choose at least one table or view")

        def work() -> dict[str, Any]:
            done = engine.set_db_tables(name, chosen)
            failed = [r for r in done["results"] if r.status != "succeeded"]
            return {"added": done["added"], "removed": done["removed"],
                    "failed": [{"key": r.dataset_key, "error": r.error} for r in failed],
                    "rows": sum(r.rows for r in done["results"]),
                    "source": source_summary(store.get_source(name))}

        return {"job_id": start_job("tables", work)}

    def curl_source(name: str):
        src = get_source(name)
        if src.type != "curl":
            raise HTTPException(400, f"{name} isn't a curl source")
        return src

    def checked_statements(src, body: StatementsIn) -> dict[str, Any]:
        try:
            return clean_statements(src.config, body.model_dump())
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None

    @app.post("/api/sources/{name}/statements/preview", dependencies=auth)
    def preview_set(name: str, body: StatementsIn) -> dict[str, Any]:
        """What a set would send. Sends nothing and saves nothing."""
        src = curl_source(name)
        return preview_statements(src.config, checked_statements(src, body))

    @app.put("/api/sources/{name}/statements", dependencies=auth)
    def save_set(name: str, body: StatementsIn) -> dict[str, Any]:
        """Make the source run as this set from now on."""
        src = curl_source(name)
        plan = checked_statements(src, body)
        store.save_source(name, src.type, {**src.config, "statements": plan})
        return preview_statements(src.config, plan)

    @app.delete("/api/sources/{name}/statements", dependencies=auth)
    def remove_set(name: str) -> dict[str, Any]:
        """Back to the single statement."""
        src = curl_source(name)
        store.save_source(name, src.type, {k: v for k, v in src.config.items() if k != "statements"})
        return jsonable_encoder(source_summary(store.get_source(name)))

    @app.get("/api/sources/{name}/preview", dependencies=auth)
    def preview(name: str, limit: int = 20, table: str | None = None) -> dict[str, Any]:
        src = get_source(name)
        if src.type not in SINGLE_TABLE_TYPES:
            raise HTTPException(400, "preview is available for file and Google Sheet sources")
        if src.type == "dbtable" and table:
            if table not in targets.tables(src):
                raise HTTPException(404, f"{name} has no table {table}")
        else:
            table = main_table(src)
        if not store.table_exists(table):
            return {"table": table, "columns": [], "rows": []}
        rows = store.read_table(table, limit=max(1, min(limit, 500)))
        columns = list(rows[0]) if rows else []
        return jsonable_encoder({
            "table": table,
            "columns": columns,
            "rows": [[_json_safe(r[c]) for c in columns] for r in rows],
        })

    def file_aliases() -> dict[str, str]:
        return {s.name: main_table(s) for s in store.list_sources() if s.type in SINGLE_TABLE_TYPES}

    @app.get("/api/schema", dependencies=auth)
    def schema() -> dict[str, Any]:
        """What the query window can query: each source by name, and every table."""
        sources = []
        for src in store.list_sources():
            entry: dict[str, Any] = {"name": src.name, "type": src.type}
            if src.type in SINGLE_TABLE_TYPES:
                entry["table"] = main_table(src)
            if src.type == "dbtable":
                entry["tables"] = targets.tables(src)
            sources.append(entry)
        return {"sources": sources, "tables": queries.tables()}

    @app.post("/api/query", dependencies=auth)
    def query(body: QueryIn) -> dict[str, Any]:
        """Run one read-only query; file sources are queryable by their own names."""
        try:
            result = queries.run(body.sql, body.limit, file_aliases())
        except QueryError as exc:
            raise HTTPException(400, str(exc)) from None
        return jsonable_encoder({
            "columns": result.columns,
            "rows": [[_json_safe(v) for v in row] for row in result.rows],
            "row_count": len(result.rows),
            "truncated": result.truncated,
            "limit": body.limit,
            "elapsed_ms": result.elapsed_ms,
        })

    # -- destinations: the databases sources load into (see destinations.py) --------

    from eyetable.destinations import Destinations, DestinationError

    dests = Destinations(store, engine.secret_key)

    def dest_call(fn: Callable[[], Any]) -> Any:
        try:
            return jsonable_encoder(fn())
        except DestinationError as exc:
            raise HTTPException(exc.status, str(exc)) from None

    @app.get("/api/destinations", dependencies=auth)
    def list_destinations() -> list[dict[str, Any]]:
        """Every destination (eyetable's own first), with the sources routed to each."""
        return dest_call(dests.list)

    @app.post("/api/destinations/test", dependencies=auth)
    def test_destination(body: DestinationTestIn) -> dict[str, Any]:
        """Connect to a destination (saved, or a connection string not saved yet) and report what's there."""
        return dest_call(lambda: dests.test(body.id, body.connection, body.schema_))

    @app.post("/api/destinations", dependencies=auth)
    def add_destination(body: DestinationIn) -> dict[str, Any]:
        if not body.connection.strip():
            raise HTTPException(400, "paste the destination's connection string")
        return dest_call(lambda: dests.add(body.name, body.connection, body.schema_))

    @app.put("/api/destinations/{dest_id}", dependencies=auth)
    def update_destination(dest_id: str, body: DestinationIn) -> dict[str, Any]:
        return dest_call(lambda: dests.update(dest_id, body.name, body.connection, body.schema_))

    @app.delete("/api/destinations/{dest_id}", dependencies=auth)
    def remove_destination(dest_id: str) -> dict[str, Any]:
        return dest_call(lambda: {"removed": dests.remove(dest_id)})

    # -- views and procedures saved from the query window (see authoring.py) --------

    def authored(fn: Callable[[], Any]) -> Any:
        from eyetable.authoring import AuthorError

        if author is None:
            raise HTTPException(503, "saving views and procedures isn't set up: set EYETABLE_AUTHOR_DATABASE_URL in "
                                     ".env, run scripts/setup-postgres.ps1 as an administrator, and restart eyetable")
        try:
            return jsonable_encoder(fn())
        except AuthorError as exc:
            raise HTTPException(exc.status, str(exc)) from None

    @app.get("/api/objects", dependencies=auth)
    def list_objects() -> dict[str, Any]:
        """The saved views and procedures, or why saving isn't available."""
        if author is None or author.problem:
            return {"ready": False, "schema": author.schema if author else None, "objects": [],
                    "problem": author.problem if author else "saving views and procedures isn't set up (see the README)"}
        return authored(lambda: {"ready": True, "schema": author.schema, "problem": None,
                                 "objects": [o.as_dict() for o in author.objects()]})

    @app.post("/api/objects/kind", dependencies=auth)
    def object_kind(body: QueryIn) -> dict[str, Any]:
        """Whether the SQL would be saved as a view or a procedure (or is a CREATE statement to deploy)."""
        from eyetable.authoring import definition_target, kind_for

        target = definition_target(body.sql)
        return {"kind": kind_for(body.sql), "deploy": bool(target), "target": target[2] if target else None}

    @app.post("/api/objects", dependencies=auth)
    def save_object(body: SaveObjectIn) -> dict[str, Any]:
        """Save the SQL as a view, or a procedure when it can't be one."""
        return authored(lambda: author.save(body.name.strip(), body.sql, file_aliases(), body.replace))

    @app.post("/api/objects/deploy", dependencies=auth)
    def deploy_object(body: DeployIn) -> dict[str, Any]:
        """Run an edited CREATE [OR REPLACE] VIEW / PROCEDURE statement."""
        return authored(lambda: author.deploy(body.sql))

    @app.get("/api/objects/{name}", dependencies=auth)
    def get_object(name: str) -> dict[str, Any]:
        """A saved object's CREATE OR REPLACE statement, to edit in the query window."""
        return authored(lambda: author.definition(name))

    @app.post("/api/objects/{name}/call", dependencies=auth)
    def call_object(name: str) -> dict[str, Any]:
        """Run a saved procedure."""
        return authored(lambda: author.call(name))

    @app.delete("/api/objects/{name}", dependencies=auth)
    def drop_object(name: str) -> dict[str, Any]:
        return authored(lambda: {"dropped": author.drop(name).as_dict()})

    def sheet_error(exc: SheetError) -> HTTPException:
        # 403 tells the page that signing in with Google (again) may get past this.
        return HTTPException(403 if exc.needs_sign_in else 400, str(exc))

    @app.post("/api/gsheets/inspect", dependencies=auth)
    def inspect_gsheet(body: SheetIn) -> dict[str, Any]:
        """The tabs of a Google Sheet and the source each would become. Changes nothing."""
        try:
            load = engine.plan_gsheet(sheet_fetch(body.url, sign_ins(body)))
        except SheetError as exc:
            raise sheet_error(exc) from None
        return jsonable_encoder(load)

    entra.set_provider(entra.EntraTokens(store, engine.secret_key, os.environ.get("EYETABLE_ENTRA_CLIENT_ID")))

    def problem(exc: ValueError) -> HTTPException:
        """A refusal for the page: a needed Microsoft sign-in says for whom, so the page can offer it."""
        if isinstance(exc, entra.EntraSignInNeeded):
            return HTTPException(409, {"message": str(exc), "entra": exc.request})
        return HTTPException(400, str(exc).strip("'\""))

    @app.post("/api/entra/sign-in", dependencies=auth)
    def entra_sign_in(body: EntraSignInIn) -> dict[str, Any]:
        """Start a Microsoft sign-in: you enter the code it returns at its verification address."""
        if not re.fullmatch(r"https://login\.microsoftonline\.com/[A-Za-z0-9.\-]+", body.authority) \
                or not re.fullmatch(r"https://[A-Za-z0-9.\-]+/\.default", body.scope):
            raise HTTPException(400, "that isn't a Microsoft sign-in eyetable makes")
        try:
            return jsonable_encoder(entra.provider().start(body.username, body.scope, body.authority))
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None

    @app.get("/api/entra/sign-in/{flow_id}", dependencies=auth)
    def entra_sign_in_status(flow_id: str) -> dict[str, Any]:
        try:
            return jsonable_encoder(entra.provider().status(flow_id))
        except KeyError as exc:
            raise HTTPException(404, str(exc).strip("'\"")) from None

    groups = group_runner or GroupRunner(engine, sheet_fetch=lambda url, google_token=None: sheet_fetch(url, google_token))
    if groups.queue is None:
        groups.queue = load_queue  # group, tag and scheduled runs show in the page's Load queue

    def get_group(group_id: int):
        try:
            return store.get_group(group_id)
        except GroupNotFound:
            raise HTTPException(404, f"no group {group_id}") from None

    @app.get("/api/groups", dependencies=auth)
    def list_groups() -> list[dict[str, Any]]:
        return jsonable_encoder([groups.summary(g) for g in store.list_groups()])

    @app.post("/api/groups", dependencies=auth)
    def create_group(body: GroupIn) -> dict[str, Any]:
        try:
            group = groups.create(body.name, body.members, body.schedule, body.enabled, body.notify)
        except ValueError as exc:  # includes ScheduleError
            raise HTTPException(400, str(exc).strip("'\"")) from None
        return jsonable_encoder(groups.summary(group))

    @app.put("/api/groups/{group_id}", dependencies=auth)
    def update_group(group_id: int, body: GroupIn) -> dict[str, Any]:
        get_group(group_id)
        try:
            group = groups.update(group_id, body.name, body.members, body.schedule, body.enabled,
                                  body.notify if "notify" in body.model_fields_set else KEEP)
        except ValueError as exc:
            raise HTTPException(400, str(exc).strip("'\"")) from None
        return jsonable_encoder(groups.summary(group))

    @app.delete("/api/groups/{group_id}", dependencies=auth)
    def delete_group(group_id: int) -> dict[str, Any]:
        get_group(group_id)
        if groups.is_running(group_id):
            raise HTTPException(409, "the group is running; delete it when the run finishes")
        store.delete_group(group_id)
        return {"deleted": group_id}

    @app.post("/api/notify/test", dependencies=auth)
    def notify_test(body: NotifyTestIn) -> dict[str, Any]:
        """Send a test email to the addresses, so the Groups panel can check email works before a run."""
        try:
            to = notify.parse_recipients(body.to)
        except notify.NotifyError as exc:
            raise HTTPException(400, str(exc)) from None
        if not to:
            raise HTTPException(400, "give at least one email address")
        if groups.mailer is None:
            raise HTTPException(409, "email isn't set up on the eyetable server: set EYETABLE_SMTP_HOST (and the "
                                     "other EYETABLE_SMTP_ settings) in .env and restart it")
        try:
            groups.mailer.send(to, *notify.test_message())
        except Exception as exc:
            raise HTTPException(502, "not sent: " + notify.describe_error(exc)) from None
        return {"sent_to": to}

    @app.post("/api/groups/{group_id}/run", dependencies=auth, status_code=202)
    def run_group(group_id: int, body: ReloadIn | None = None) -> dict[str, Any]:
        """Start reloading every source in the group; poll /api/group-runs/{run_id} for progress."""
        get_group(group_id)
        try:
            run_id = groups.run(group_id, "manual", google_token=sign_ins(body),
                                incremental=bool(body and body.incremental))
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from None
        return {"run_id": run_id}

    @app.get("/api/group-runs/{run_id}", dependencies=auth)
    def group_run(run_id: int) -> dict[str, Any]:
        try:
            return jsonable_encoder(store.get_group_run(run_id))
        except LookupError:
            raise HTTPException(404, f"no group run {run_id}") from None

    @app.get("/api/groups/{group_id}/runs", dependencies=auth)
    def group_runs(group_id: int, limit: int = 10) -> list[dict[str, Any]]:
        get_group(group_id)
        return jsonable_encoder(store.list_group_runs(group_id, max(1, min(limit, 100))))

    # -- background jobs (loads that can outlast a web request) ------------------
    jobs: dict[str, dict[str, Any]] = {}
    jobs_lock = threading.Lock()
    job_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="eyetable-job")

    def start_job(kind: str, work: Callable[[], Any]) -> str:
        job_id = secrets.token_hex(8)
        with jobs_lock:
            for old in [k for k, j in jobs.items() if j["status"] != "running"][:-50]:  # keep the latest 50
                del jobs[old]
            jobs[job_id] = {"id": job_id, "kind": kind, "status": "running", "started_at": dt.datetime.now(dt.timezone.utc),
                            "finished_at": None, "result": None, "error": None}

        def run() -> None:
            try:
                result, status, error = work(), "succeeded", None
            except Exception as exc:
                result, status, error = None, "failed", str(exc).strip("'\"") or type(exc).__name__
            with jobs_lock:
                jobs[job_id].update(status=status, result=result, error=error, finished_at=dt.datetime.now(dt.timezone.utc))

        job_pool.submit(run)
        return job_id

    @app.get("/api/jobs/{job_id}", dependencies=auth)
    def job(job_id: str) -> dict[str, Any]:
        with jobs_lock:
            found = jobs.get(job_id)
            if found is None:
                raise HTTPException(404, "no such job (eyetable may have restarted)")
            return jsonable_encoder(dict(found))

    # -- the load queue (loadqueue.py): reloads in the background, with progress ----
    @app.post("/api/queue", dependencies=auth, status_code=202)
    def queue_load(body: QueueIn, request: Request) -> dict[str, Any]:
        """Queue a full or incremental reload of a source, or of one of its tables."""
        get_source(body.source)
        try:
            item, added = load_queue.add(body.source, body.table, body.incremental, sign_ins(body),
                                         by=user_of(request))
        except ValueError as exc:  # includes IncrementalProblem
            raise HTTPException(400, str(exc).strip("'\"")) from None
        return jsonable_encoder({"item": item, "added": added})

    @app.get("/api/queue", dependencies=auth)
    def list_queue() -> dict[str, Any]:
        return jsonable_encoder({"items": load_queue.list()})

    @app.get("/api/runs", dependencies=auth)
    def search_runs(source: str | None = None, since: dt.datetime | None = None, until: dt.datetime | None = None,
                    status: str | None = None, limit: int = 200) -> dict[str, Any]:
        """Past loads (every load is recorded, whatever started it), newest first: filtered by a
        source or table name (part of it), when they started (since/until, ISO times), and status."""
        if status and status not in ("succeeded", "failed", "running"):
            raise HTTPException(400, "status is succeeded, failed or running")
        limit = max(1, min(limit, 1000))
        runs = store.search_runs(source, since, until, status, limit + 1)
        return jsonable_encoder({"runs": runs[:limit], "more": len(runs) > limit})

    def hook_caller_allowed(request: Request) -> None:
        """The webhook takes its own key (EYETABLE_HOOK_KEY), or the API token: as key=..., an
        X-Hook-Key header or a Bearer token."""
        given = request.query_params.get("key") or request.headers.get("x-hook-key") or ""
        header = request.headers.get("authorization", "")
        if not given and header.lower().startswith("bearer "):
            given = header[7:].strip()
        keys = [k for k in (hook_key, token) if k]
        if not given or not any(secrets.compare_digest(given.encode(), k.encode()) for k in keys):
            auth_failures.hit(client_ip(request))
            raise HTTPException(401, "missing or wrong webhook key", headers={"WWW-Authenticate": "Bearer"})

    @app.api_route("/hook", methods=["GET", "POST"], status_code=201, include_in_schema=False)
    @app.api_route("/api/hook", methods=["GET", "POST"], status_code=201)
    async def run_hook(request: Request) -> dict[str, Any]:
        """Record a run of a process outside eyetable in eyetable.runs (see eyetable.hooks): one
        row per call, from query parameters or a JSON / form body."""
        hook_caller_allowed(request)
        values: dict[str, Any] = {k: v for k, v in request.query_params.items() if k != "key"}
        if request.method == "POST":
            kind = request.headers.get("content-type", "")
            try:
                if "json" in kind:
                    body = await request.json()
                    if not isinstance(body, dict):
                        raise HTTPException(400, "send the run as a JSON object")
                    values.update(body)
                elif "form" in kind:
                    values.update({k: v for k, v in (await request.form()).items() if k != "key"})
            except ValueError:
                raise HTTPException(400, "the body isn't valid JSON") from None
        try:
            row = hooks.run_from(values)
        except hooks.HookError as exc:
            raise HTTPException(400, str(exc)) from None
        run_id = store.add_run(**row)
        log.info("webhook recorded run %s for %s", run_id, row["source"])
        return jsonable_encoder({"recorded": store.get_run(run_id)})

    @app.get("/api/queue/{item_id}/log", dependencies=auth)
    def load_log(item_id: str, after: int = 0) -> dict[str, Any]:
        """A queued load's queries, requests and steps, from entry `after` on."""
        try:
            return jsonable_encoder(load_queue.log(item_id, after))
        except KeyError as exc:
            raise HTTPException(404, str(exc).strip("'\"")) from None

    @app.post("/api/queue/{item_id}/retry", dependencies=auth, status_code=202)
    def retry_load(item_id: str) -> dict[str, Any]:
        """Carry on a failed load from the complete pages it kept (a new queue item)."""
        try:
            return jsonable_encoder({"item": load_queue.retry(item_id)})
        except KeyError as exc:
            raise HTTPException(404, str(exc).strip("'\"")) from None
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from None

    @app.post("/api/queue/{item_id}/cancel", dependencies=auth)
    def cancel_load(item_id: str) -> dict[str, Any]:
        try:
            return jsonable_encoder(load_queue.cancel(item_id))
        except KeyError as exc:
            raise HTTPException(404, str(exc).strip("'\"")) from None

    @app.delete("/api/queue/{item_id}", dependencies=auth)
    def dismiss_load(item_id: str) -> dict[str, Any]:
        try:
            load_queue.dismiss(item_id)
        except KeyError as exc:
            raise HTTPException(404, str(exc).strip("'\"")) from None
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None
        return {"ok": True}

    @app.delete("/api/queue", dependencies=auth)
    def clear_queue() -> dict[str, Any]:
        """Take every finished load off the list."""
        return {"cleared": load_queue.clear_finished()}

    # -- key candidates and natural keys (keys.py) ------------------------------
    TABLE_NAME = re.compile(r"^([A-Za-z0-9_]{1,63}\.)?[A-Za-z0-9_]{1,63}$")  # a table, or schema.table

    def loaded_table(table: str) -> str:
        if not TABLE_NAME.match(table) or not store.table_exists(table):
            raise HTTPException(404, f"there is no loaded table named {table}")
        return table

    @app.get("/api/tables/{table}/keys", dependencies=auth)
    def table_keys(table: str) -> dict[str, Any]:
        """A table's latest key scan, its natural key, and whether the key still holds."""
        return jsonable_encoder(keys.key_info(store, loaded_table(table)))

    @app.post("/api/tables/{table}/keys/scan", dependencies=auth, status_code=202)
    def scan_table_keys(table: str) -> dict[str, Any]:
        """Scan a table again, in the background (a big table can take a while)."""
        loaded_table(table)
        return {"job_id": start_job("key-scan", lambda: jsonable_encoder(keys.scan_and_save(store, table)))}

    @app.put("/api/tables/{table}/key", dependencies=auth)
    def set_table_key(table: str, body: KeyIn) -> dict[str, Any]:
        """Make these columns the table's natural key; refused unless they identify every row."""
        try:
            return jsonable_encoder(keys.choose_key(store, loaded_table(table), body.columns))
        except keys.KeyProblem as exc:
            raise HTTPException(400, str(exc)) from None

    # -- file sets: a file source reading every matching file in a server folder ------
    @app.put("/api/sources/{name}/naming", dependencies=auth)
    def rename_source(name: str, body: NamingIn) -> dict[str, Any]:
        """Rename a source and/or one of its tables (schema.table). A renamed table's old copy is
        archived as <schema>_archived.<table>_<timestamp>; the next full load creates the new one."""
        get_source(name)
        try:
            done = engine.rename(name, body.name, body.table, body.new_table)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None
        return jsonable_encoder({**done, "summary": source_summary(store.get_source(done["source"]))})

    @app.post("/api/files/match", dependencies=auth)
    def match_files(body: FilesIn) -> dict[str, Any]:
        """The files in a folder on the server that match a pattern (before saving it to a source)."""
        try:
            files = filesets.matching_files(body.folder, body.pattern)
        except filesets.FileSetProblem as exc:
            raise HTTPException(400, str(exc)) from None
        return jsonable_encoder({"files": filesets.describe_files(files), "count": len(files),
                                 "suggested_name": filesets.name_for(body.pattern) if filesets.is_pattern(body.pattern) else None})

    @app.get("/api/sources/{name}/file-location", dependencies=auth)
    def file_location(name: str) -> dict[str, Any]:
        """Where a file source's files are: its folder and pattern, or, for an uploaded file, the folder
        it was found in on this machine (same name, size and contents), if it can be found."""
        src = get_source(name)
        if src.type != "file":
            raise HTTPException(400, f"{name} isn't a file source")
        if src.config.get("pattern"):
            return {"folder": src.config.get("folder"), "pattern": src.config["pattern"], "found": "set"}
        uploads = store.list_uploads(name, limit=1)
        if not uploads:
            return {"folder": None, "pattern": None, "found": False, "searched": []}
        up = uploads[0]
        path = filesets.find_upload(up.filename, up.size_bytes, up.sha256)
        return {"folder": str(path.parent) if path else None, "pattern": path.name if path else up.filename,
                "found": bool(path), "searched": [str(r) for r in filesets.search_roots()]}

    # -- hiding a table in Sources ("commenting it out"): every load skips it --------
    @app.put("/api/tables/{table}/hidden", dependencies=auth)
    def set_hidden(table: str, body: HiddenIn) -> dict[str, Any]:
        """Hide a source's table from Sources and from every load (groups, schedules, Reload), or bring it back."""
        for src in store.list_sources():
            tables = targets.tables(src) if src.type == "dbtable" or src.type in SINGLE_TABLE_TYPES else []
            if table in tables:
                store.set_hidden(table, body.hidden)
                return jsonable_encoder({"table": table, "source": src.name, "hidden": body.hidden})
        raise HTTPException(404, f"no source loads a table named {table}")

    # -- big tables read in parts (connectors/dbtable.py read_in_parts) ----------
    def db_table_item(table: str):
        """The database source and table entry that load `table`."""
        for src in store.list_sources():
            if src.type == "dbtable":
                for item in dbconnect.db_tables(src.config):
                    if (item.get("target") or table_name(src.name, item["name"])) == table:
                        return src, item
        raise HTTPException(404, f"{table} isn't loaded from a database, so it has no parts setting")

    def split_info(table: str) -> dict[str, Any]:
        from eyetable.connectors.dbtable import split_candidates

        src, item = db_table_item(table)
        catalog = {d.key: d for _, d in store.list_datasets(src.name)}
        columns = catalog[item["key"]].columns if item["key"] in catalog else []
        split = item.get("split") or {}
        return {"table": table, "source": src.name, "column": split.get("column"), "auto": bool(split.get("auto")),
                "candidates": split_candidates(columns)}

    @app.get("/api/tables/{table}/split", dependencies=auth)
    def get_split(table: str) -> dict[str, Any]:
        """Whether a database table loads in one query or in parts, and the columns it could be split by."""
        return split_info(table)

    @app.put("/api/tables/{table}/split", dependencies=auth)
    def put_split(table: str, body: SplitIn) -> dict[str, Any]:
        """Load this table in parts by `column` from now on, or (no column) in one query."""
        from eyetable.connectors.dbtable import _kind

        src, item = db_table_item(table)
        if body.column:
            catalog = {d.key: d for _, d in store.list_datasets(src.name)}
            types = {c.name: c.type for c in (catalog[item["key"]].columns if item["key"] in catalog else [])}
            if body.column not in types:
                raise HTTPException(400, f"{table} has no column {body.column}")
            if _kind(types[body.column]) is None:
                raise HTTPException(400, f"{body.column} holds {types[body.column]} values; choose a date, time or number column")
        engine.set_split(src.name, item["key"], body.column or None)
        return split_info(table)

    @app.put("/api/tables/{table}/incremental", dependencies=auth)
    def set_table_increment(table: str, body: IncrementIn) -> dict[str, Any]:
        """Make this column the one the table's incremental loads go by."""
        try:
            return jsonable_encoder(keys.choose_increment(store, loaded_table(table), body.column))
        except keys.KeyProblem as exc:
            raise HTTPException(400, str(exc)) from None

    @app.delete("/api/tables/{table}/incremental", dependencies=auth)
    def clear_table_increment(table: str) -> dict[str, Any]:
        return jsonable_encoder(keys.choose_increment(store, loaded_table(table), None))

    @app.put("/api/tables/{table}/history", dependencies=auth)
    def set_table_history(table: str, body: HistoryIn) -> dict[str, Any]:
        """Keep a table's history (merge loads by its natural key) or stop keeping it."""
        try:
            return jsonable_encoder(keys.choose_history(store, loaded_or_planned(table), body.on))
        except keys.KeyProblem as exc:
            raise HTTPException(400, str(exc)) from None

    def loaded_or_planned(table: str) -> str:
        if not TABLE_NAME.match(table) or not any(table in targets.tables(s) for s in store.list_sources()
                                                  if s.type == "dbtable" or s.type in SINGLE_TABLE_TYPES):
            raise HTTPException(404, f"no source loads a table named {table}")
        return table

    @app.delete("/api/tables/{table}/key", dependencies=auth)
    def clear_table_key(table: str) -> dict[str, Any]:
        try:
            return jsonable_encoder(keys.clear_key(store, loaded_table(table)))
        except keys.KeyProblem as exc:
            raise HTTPException(400, str(exc)) from None

    # -- curl commands -----------------------------------------------------------
    @app.post("/api/curl/inspect", dependencies=auth)
    def inspect_curl(body: CurlIn) -> dict[str, Any]:
        """Check a curl command: send it once, show its first page and how it pages. Saves nothing."""
        try:
            result, _ = curl_plan(body.curl, engine.secret_key, client=curl_client,
                                  certificate=body.client_certificate(), **body.paging())
        except (ValueError, KeyError) as exc:
            raise HTTPException(400, str(exc).strip("'\"")) from None
        out = jsonable_encoder(result)
        out["sample"] = [{k: _json_safe(v) for k, v in row.items()} for row in out["sample"]]
        return out

    @app.post("/api/curl", dependencies=auth, status_code=202)
    def load_curl(body: CurlLoadIn) -> dict[str, Any]:
        """Make a source from a curl command and load every page, in the background."""
        if body.mode not in ("replace", "append"):
            raise HTTPException(400, "mode must be replace or append")
        if not SOURCE_NAME.match(body.name):
            raise HTTPException(400, "source names are lowercase letters, digits and _, starting with a letter")
        if store.source_exists(body.name) and store.get_source(body.name).type != "curl":
            raise HTTPException(409, f"{body.name!r} is already a {store.get_source(body.name).type} source; pick another name")
        try:
            parse_curl(body.curl)
            certificate = body.client_certificate()
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None

        def work() -> dict[str, Any]:
            results = engine.load_curl(body.curl, body.name, body.mode, client=curl_client,
                                       certificate=certificate, **body.paging())
            failed = [r for r in results if r.status != "succeeded"]
            if failed:
                raise RuntimeError("; ".join(r.error or r.status for r in failed))
            return {"source": source_summary(store.get_source(body.name)), "rows": sum(r.rows for r in results)}

        return {"job_id": start_job("curl", work)}

    # -- database connection strings ---------------------------------------------
    @app.post("/api/db/inspect", dependencies=auth)
    def inspect_db(body: DbIn) -> dict[str, Any]:
        """Connect and list every table and view. Saves nothing; the password is never sent back."""
        try:
            prepared = dbconnect.prepare(body.connection, engine.secret_key)
            db_engine, warnings = dbconnect.make_engine(prepared.config, prepared.password)
            try:
                objects = dbconnect.list_objects(db_engine)
            finally:
                db_engine.dispose()
        except ValueError as exc:
            raise problem(exc) from None
        except KeyError as exc:
            raise HTTPException(400, str(exc).strip("'\"")) from None
        loaded = {
            (s.config.get("label"), item["schema"], item["table"]): s.name
            for s in store.list_sources() if s.type == "dbtable" for item in dbconnect.db_tables(s.config)
        }
        for obj in objects:
            obj["suggested_name"] = dbconnect.suggested_name(prepared.database, obj["schema"], obj["name"])
            obj["loaded_as"] = loaded.get((prepared.label, obj["schema"], obj["name"]))
        return jsonable_encoder({
            "label": prepared.label, "server": prepared.server, "database": prepared.database,
            "warnings": prepared.warnings + warnings, "objects": objects,
        })

    @app.post("/api/db/load", dependencies=auth, status_code=202)
    def load_db(body: DbLoadIn) -> dict[str, Any]:
        """Make a source for each chosen table/view and load them, in the background."""
        if body.mode not in ("replace", "append"):
            raise HTTPException(400, "mode must be replace or append")
        try:
            dbconnect.prepare(body.connection, engine.secret_key)  # fail fast on an unusable string
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None
        names = [i.source for i in body.items]
        if len(set(names)) != len(names):
            raise HTTPException(400, "two of the chosen tables have the same source name")
        items = [{"schema": i.schema_, "name": i.name, "kind": i.kind, "source": i.source} for i in body.items]

        def work() -> dict[str, Any]:
            results = engine.load_db_objects(body.connection, items, body.mode)
            return {"items": results, "loaded": sum(1 for r in results if r["status"] == "succeeded"),
                    "rows": sum(r["rows"] for r in results)}

        return {"job_id": start_job("db", work)}

    @app.get("/api/google", dependencies=auth)
    def google_status() -> dict[str, Any]:
        """The Google account scheduled runs read sheets as, if any."""
        return jsonable_encoder(google.status())

    @app.post("/api/google/connect", dependencies=auth)
    def google_connect(body: GoogleCodeIn) -> dict[str, Any]:
        """Swap the page's one-time authorization code for a stored, long-lived authorization."""
        if not google.configured:
            raise HTTPException(503, "Google access for scheduled runs isn't set up on the server: set "
                                + ", ".join(google.missing_settings()) + " in .env and restart eyetable")
        try:
            return jsonable_encoder(google.connect(body.code))
        except GoogleAuthError as exc:
            raise HTTPException(400, str(exc)) from None

    @app.delete("/api/google", dependencies=auth)
    def google_disconnect() -> dict[str, Any]:
        """Revoke the stored authorization at Google and forget it."""
        google.disconnect()
        return jsonable_encoder(google.status())

    @app.post("/api/schedule/preview", dependencies=auth)
    def schedule_preview(body: ScheduleIn) -> dict[str, Any]:
        """How a schedule reads, and its next few run times (the editor shows these as you type)."""
        try:
            schedule = normalize(body.schedule)
        except ScheduleError as exc:
            raise HTTPException(400, str(exc)) from None
        return jsonable_encoder({
            "schedule": schedule,
            "text": describe(schedule),
            "next_runs": next_runs(schedule, dt.datetime.now(dt.timezone.utc), 3),
        })

    @app.post("/api/sources/{name}/reload", dependencies=auth)
    def reload(name: str, body: ReloadIn | None = None) -> dict[str, Any]:
        """Load a source's data again: replay stored files, re-fetch a sheet tab, or re-pull."""
        get_source(name)
        token_ = sign_ins(body)
        try:
            results = engine.reload_source(name, sheet_fetch=lambda url: sheet_fetch(url, token_),
                                           incremental=bool(body and body.incremental), table=body.table if body else None)
        except SheetError as exc:
            raise sheet_error(exc) from None
        except (ValueError, KeyError) as exc:
            raise HTTPException(400, str(exc).strip("'\"")) from None
        failed = [r for r in results if r.status == "failed"]
        if failed:
            raise HTTPException(500, "; ".join(f"{r.dataset_key}: {r.error}" for r in failed))
        return jsonable_encoder({
            "source": source_summary(store.get_source(name)),
            "results": [r.__dict__ for r in results],
            "rows": sum(r.rows for r in results),
        })

    @app.post("/api/sources/{name}/run", dependencies=auth, status_code=202)
    def run_source(name: str, body: ReloadIn | None = None) -> dict[str, Any]:
        """Reload a source in the background, for a set that can outlast a web request."""
        curl_source(name)
        incremental_ = bool(body and body.incremental)

        def work() -> dict[str, Any]:
            results = engine.reload_source(name, incremental=incremental_)
            failed = [r for r in results if r.status == "failed"]
            if failed:
                raise RuntimeError("; ".join(r.error or r.status for r in failed))
            return jsonable_encoder({"source": source_summary(store.get_source(name)), "rows": sum(r.rows for r in results),
                                     "results": [r.__dict__ for r in results]})

        return {"job_id": start_job("reload", work)}

    @app.post("/api/gsheets", dependencies=auth)
    def load_gsheet(body: SheetIn) -> dict[str, Any]:
        """Load every tab of a Google Sheet into its own source "<file>_<tab>"."""
        if body.mode not in ("replace", "append"):
            raise HTTPException(400, "mode must be replace or append")
        try:
            load = engine.ingest_gsheet(body.url, body.mode, fetch=lambda url: sheet_fetch(url, sign_ins(body)))
        except SheetError as exc:
            raise sheet_error(exc) from None
        return jsonable_encoder(load)

    @app.post("/api/sources/{name}/uploads", dependencies=auth)
    def upload(
        name: str,
        file: Annotated[UploadFile, File()],
        mode: Annotated[str, Form()] = "replace",
        create: Annotated[bool, Form()] = False,
    ) -> dict[str, Any]:
        """Load a CSV into a file source: create it, or replace / append to its table."""
        if not SOURCE_NAME.match(name):
            raise HTTPException(422, "source names are lowercase letters, digits and _, starting with a letter")
        filename = Path(file.filename or "upload.csv").name
        if not filename.lower().endswith(".csv"):
            raise HTTPException(415, "only .csv files are accepted for now")
        content = file.file.read(max_upload_bytes + 1)
        if len(content) > max_upload_bytes:
            raise HTTPException(413, f"file is larger than {max_upload_bytes // (1024 * 1024)} MB")
        if create and store.source_exists(name):
            raise HTTPException(409, f"a source named {name!r} already exists")
        if not create:
            get_source(name)
        try:
            result = engine.ingest_upload(name, filename, content, mode=mode, create=create)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None
        if result.status != "succeeded":
            raise HTTPException(500, result.error or "ingestion failed")
        src = store.get_source(name)
        return jsonable_encoder({**result.__dict__, "filename": filename, "mode": mode, "source": source_summary(src)})

    if pages_dir is not None and (pages_dir / "source_management" / "index.html").exists():
        page = pages_dir / "source_management" / "index.html"

        @app.get("/", include_in_schema=False)
        def root() -> RedirectResponse:
            return RedirectResponse("/source_management/")

        @app.get("/source_management", include_in_schema=False)
        @app.get("/source_management/", include_in_schema=False)
        def source_management() -> FileResponse:
            return FileResponse(page)

        @app.get("/source_management/msal-redirect.html", include_in_schema=False)
        def msal_redirect() -> FileResponse:
            """Where Microsoft's sign-in popup returns (the page that opened it reads the result)."""
            return FileResponse(page.parent / "msal-redirect.html")

    app.state.group_runner = groups
    return app
