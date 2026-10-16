"""HTTP API for the eyetable web pages: `eyetable serve`.

Every /api route except /api/health needs `Authorization: Bearer <token>`,
where the token is EYETABLE_API_TOKEN. The pages may be served from GitHub
Pages (another origin), so CORS is open to EYETABLE_CORS_ORIGINS and the
browser's private-network preflight is answered for http://localhost use.

The API can be exposed to the internet (e.g. through a Cloudflare tunnel), so
each client IP is rate limited, and an IP that keeps sending wrong tokens is
locked out for a while.
"""

import secrets
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
from eyetable.identifiers import SOURCE_NAME, table_name
from eyetable.query import MAX_LIMIT, QueryError, QueryRunner
from eyetable.ratelimit import SlidingWindow, client_ip
from eyetable.store import SourceNotFound

DEFAULT_CORS_ORIGINS = ["https://tootallted.github.io"]
DEFAULT_MAX_UPLOAD_BYTES = 50 * 1024 * 1024
PAGES_DIR = Path(__file__).resolve().parents[2] / "docs"  # the GitHub Pages site, in a source checkout


class SheetIn(BaseModel):
    url: str = Field(max_length=2000)
    mode: str = "replace"
    # A short-lived Google access token from the page's "Sign in with Google", to read
    # private sheets as that person. Used for this request only; never stored or logged.
    google_token: str | None = Field(default=None, max_length=4096)


class QueryIn(BaseModel):
    sql: str = Field(max_length=100_000)
    limit: int = Field(default=1000, ge=1, le=MAX_LIMIT)


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
    sheet_fetch: Callable[[str, str | None], Any] | None = None,
) -> FastAPI:
    if not token:
        raise ValueError("an API token is required")
    store = engine.store
    queries = query_runner or QueryRunner(store)
    if sheet_fetch is None:
        def sheet_fetch(url: str, google_token: str | None = None):
            if google_token:
                return fetch_workbook_signed_in(url, google_token, max_bytes=max_upload_bytes)
            return fetch_workbook(url, max_bytes=max_upload_bytes)
    app = FastAPI(title="eyetable", version=__version__, docs_url="/api/docs", openapi_url="/api/openapi.json")

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
        if request.url.path.startswith("/api/"):
            wait = auth_failures.blocked(ip)
            if wait and request.method != "OPTIONS":
                return too_many(wait, "failed sign-in attempts")
            wait = requests_limit.hit(ip)
            if wait:
                return too_many(wait, "requests")
            if request.method == "POST" and (request.url.path.endswith("/uploads") or request.url.path == "/api/gsheets"):
                wait = uploads_limit.hit(ip)
                if wait:
                    return too_many(wait, "uploads")
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        if request.url.path.startswith("/api/"):
            response.headers.setdefault("Cache-Control", "no-store")
        return response

    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins if cors_origins is not None else DEFAULT_CORS_ORIGINS,
        allow_methods=["GET", "POST"],
        allow_headers=["Authorization", "Content-Type"],
        allow_private_network=True,  # Chrome asks before a public https page may call http://localhost
    )

    bearer = HTTPBearer(auto_error=False)

    def authorized(
        request: Request, credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)]
    ) -> None:
        if credentials is None or not secrets.compare_digest(credentials.credentials.encode(), token.encode()):
            auth_failures.hit(client_ip(request))
            raise HTTPException(401, "missing or wrong API token", headers={"WWW-Authenticate": "Bearer"})

    auth = [Depends(authorized)]

    def source_summary(src) -> dict[str, Any]:
        datasets = [d for _, d in store.list_datasets(src.name)]
        summary: dict[str, Any] = {
            "name": src.name,
            "type": src.type,
            "created_at": src.created_at,
            "updated_at": src.updated_at,
            "datasets": len(datasets),
        }
        if src.type in SINGLE_TABLE_TYPES:
            table = table_name(src.name, DATASET_KEY)
            uploads = store.list_uploads(src.name, limit=1)
            summary.update(
                table=table,
                rows=store.count_rows(table),
                columns=len(datasets[0].columns) if datasets else None,
                last_upload=uploads[0] if uploads else None,
            )
        return summary

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
        return jsonable_encoder([source_summary(s) for s in store.list_sources()])

    @app.get("/api/sources/{name}", dependencies=auth)
    def source_detail(name: str) -> dict[str, Any]:
        src = get_source(name)
        return jsonable_encoder({
            **source_summary(src),
            "catalog": [d.to_dict() for _, d in store.list_datasets(name)],
            "uploads": store.list_uploads(name, limit=25),
            "runs": store.list_runs(name, limit=25),
        })

    @app.get("/api/sources/{name}/preview", dependencies=auth)
    def preview(name: str, limit: int = 20) -> dict[str, Any]:
        src = get_source(name)
        if src.type not in SINGLE_TABLE_TYPES:
            raise HTTPException(400, "preview is available for file and Google Sheet sources")
        table = table_name(name, DATASET_KEY)
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
        return {s.name: table_name(s.name, DATASET_KEY) for s in store.list_sources() if s.type in SINGLE_TABLE_TYPES}

    @app.get("/api/schema", dependencies=auth)
    def schema() -> dict[str, Any]:
        """What the query window can query: each source by name, and every table."""
        sources = []
        for src in store.list_sources():
            entry: dict[str, Any] = {"name": src.name, "type": src.type}
            if src.type in SINGLE_TABLE_TYPES:
                entry["table"] = table_name(src.name, DATASET_KEY)
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

    def sheet_error(exc: SheetError) -> HTTPException:
        # 403 tells the page that signing in with Google (again) may get past this.
        return HTTPException(403 if exc.needs_sign_in else 400, str(exc))

    @app.post("/api/gsheets/inspect", dependencies=auth)
    def inspect_gsheet(body: SheetIn) -> dict[str, Any]:
        """The tabs of a Google Sheet and the source each would become. Changes nothing."""
        try:
            load = engine.plan_gsheet(sheet_fetch(body.url, body.google_token))
        except SheetError as exc:
            raise sheet_error(exc) from None
        return jsonable_encoder(load)

    @app.post("/api/gsheets", dependencies=auth)
    def load_gsheet(body: SheetIn) -> dict[str, Any]:
        """Load every tab of a Google Sheet into its own source "<file>_<tab>"."""
        if body.mode not in ("replace", "append"):
            raise HTTPException(400, "mode must be replace or append")
        try:
            load = engine.ingest_gsheet(body.url, body.mode, fetch=lambda url: sheet_fetch(url, body.google_token))
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

    return app
