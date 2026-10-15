"""HTTP API for the eyetable web pages: `eyetable serve`.

Every /api route except /api/health needs `Authorization: Bearer <token>`,
where the token is EYETABLE_API_TOKEN. The pages may be served from GitHub
Pages (another origin), so CORS is open to EYETABLE_CORS_ORIGINS and the
browser's private-network preflight is answered for http://localhost use.
"""

import secrets
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from eyetable import __version__
from eyetable.connectors.file import DATASET_KEY
from eyetable.engine import Engine
from eyetable.identifiers import SOURCE_NAME, table_name
from eyetable.store import SourceNotFound

DEFAULT_CORS_ORIGINS = ["https://tootallted.github.io"]
DEFAULT_MAX_UPLOAD_BYTES = 50 * 1024 * 1024
PAGES_DIR = Path(__file__).resolve().parents[2] / "docs"  # the GitHub Pages site, in a source checkout


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
) -> FastAPI:
    if not token:
        raise ValueError("an API token is required")
    store = engine.store
    app = FastAPI(title="eyetable", version=__version__, docs_url="/api/docs", openapi_url="/api/openapi.json")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins if cors_origins is not None else DEFAULT_CORS_ORIGINS,
        allow_methods=["GET", "POST"],
        allow_headers=["Authorization", "Content-Type"],
        allow_private_network=True,  # Chrome asks before a public https page may call http://localhost
    )

    bearer = HTTPBearer(auto_error=False)

    def authorized(credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)]) -> None:
        if credentials is None or not secrets.compare_digest(credentials.credentials.encode(), token.encode()):
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
        if src.type == "file":
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
        if src.type != "file":
            raise HTTPException(400, "preview is available for file sources")
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
