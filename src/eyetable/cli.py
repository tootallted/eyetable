"""Command line interface: `eyetable --help`."""

import json
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

from eyetable.config import load_settings
from eyetable.connectors.postman import import_postman, parse_pagination_option
from eyetable.credentials import env_ref
from eyetable.engine import Engine
from eyetable.store import CoreStore

app = typer.Typer(help="Capture data from any source into storage you own.", no_args_is_help=True)
source_app = typer.Typer(help="Register and manage data sources.", no_args_is_help=True)
app.add_typer(source_app, name="source")
console = Console()


def _engine() -> Engine:
    s = load_settings()
    return Engine(CoreStore(s.database_url, s.meta_schema, s.data_schema))


def _fail(message: str) -> None:
    console.print(f"[red]error:[/red] {message}")
    raise typer.Exit(1)


def _pairs(items: list[str] | None, what: str) -> dict[str, str]:
    out = {}
    for item in items or []:
        key, sep, value = item.partition("=")
        if not sep:
            _fail(f"{what} must look like KEY=VALUE, got {item!r}")
        out[key] = value
    return out


@app.command()
def init() -> None:
    """Create the core storage schemas and metadata tables."""
    s = load_settings()
    CoreStore(s.database_url, s.meta_schema, s.data_schema).init()
    console.print(f"core storage ready at [bold]{s.database_url.split('@')[-1]}[/bold]")


@source_app.command("add-db")
def add_db(
    name: str,
    url: Annotated[Optional[str], typer.Option(help="Full SQLAlchemy URL; may contain ${VAR} references.")] = None,
    driver: Annotated[Optional[str], typer.Option(help="e.g. postgresql+psycopg, mssql+pyodbc, mysql+pymysql")] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    username: Optional[str] = None,
    password_env: Annotated[Optional[str], typer.Option(help="Env var holding the password (recommended).")] = None,
    ask_password: Annotated[bool, typer.Option(help="Prompt for a password and store it in core storage.")] = False,
    schema: Annotated[Optional[list[str]], typer.Option(help="Only discover these schemas (repeatable).")] = None,
    query: Annotated[Optional[list[str]], typer.Option(help="Driver URL params KEY=VALUE (repeatable).")] = None,
    replace: bool = False,
) -> None:
    """Register a database source, with credentials, and verify the connection."""
    if not url and not driver:
        _fail("pass --url or --driver")
    config: dict = {"url": url} if url else {
        "driver": driver, "host": host, "port": port, "database": database, "username": username,
        "query": _pairs(query, "--query"),
    }
    if password_env:
        config["password"] = env_ref(password_env)
    elif ask_password:
        config["password"] = typer.prompt("password", hide_input=True)
        console.print("[yellow]note:[/yellow] password stored in core storage; --password-env keeps it out")
    if schema:
        config["schemas"] = schema
    try:
        _engine().add_source(name, "database", config, replace=replace)
    except Exception as exc:
        _fail(str(exc))
    console.print(f"connected and registered database source [bold]{name}[/bold]")


@source_app.command("add-postman")
def add_postman(
    name: str,
    collection: Annotated[Path, typer.Argument(help="Exported Postman collection (.json).", exists=True)],
    environment: Annotated[Optional[Path], typer.Option(help="Exported Postman environment (.json).", exists=True)] = None,
    var: Annotated[Optional[list[str]], typer.Option(help="Variable override KEY=VALUE (repeatable).")] = None,
    records_path: Annotated[Optional[list[str]], typer.Option(help="'Request key=dotted.path' to the rows (repeatable).")] = None,
    allow_method: Annotated[Optional[list[str]], typer.Option(help="Also ingest requests with this method, e.g. POST.")] = None,
    paginate: Annotated[Optional[list[str]], typer.Option(
        help="'Request key=type[,option=value,...]' with type page, offset, cursor or next_link, "
             "e.g. 'Users/List users=page,size_param=per_page,size=100' (repeatable).")] = None,
    replace: bool = False,
) -> None:
    """Register an API source from a Postman collection export."""
    env_data = json.loads(environment.read_text(encoding="utf-8")) if environment else None
    config, secrets = import_postman(
        name, json.loads(collection.read_text(encoding="utf-8")), env_data, _pairs(var, "--var")
    )
    if records_path:
        config["records_path"] = _pairs(records_path, "--records-path")
    if allow_method:
        config["allow_methods"] = [m.upper() for m in allow_method]
    if paginate:
        config["pagination"] = {}
        for item in paginate:
            try:
                key, settings = parse_pagination_option(item)
            except ValueError as exc:
                _fail(f"--paginate {item!r}: {exc}")
            config["pagination"][key] = settings
    try:
        _engine().add_source(name, "postman", config, replace=replace)
    except Exception as exc:
        if secrets:
            console.print("set these before registering: " + ", ".join(secrets))
        _fail(str(exc))
    console.print(f"registered Postman source [bold]{name}[/bold]")
    if secrets:
        console.print("secret environment values were not stored; keep these set: " + ", ".join(secrets))


@source_app.command("list")
def list_sources() -> None:
    """Show registered sources."""
    table = Table("name", "type", "updated")
    for s in _engine().store.list_sources():
        table.add_row(s.name, s.type, f"{s.updated_at:%Y-%m-%d %H:%M}")
    console.print(table)


@source_app.command("test")
def test_source(name: str) -> None:
    """Check a source can be reached with its stored config."""
    try:
        with _engine().connector(name) as conn:
            conn.test()
    except Exception as exc:
        _fail(str(exc))
    console.print(f"[green]ok[/green] {name}")


@source_app.command("remove")
def remove_source(name: str) -> None:
    """Forget a source and its catalog (ingested tables are kept)."""
    try:
        _engine().store.remove_source(name)
    except Exception as exc:
        _fail(str(exc))
    console.print(f"removed {name}")


def _dataset_table(rows) -> Table:
    table = Table("source", "dataset", "kind", "columns x rows", "note")
    for source, d in rows:
        note = d.detail.get("probe_error") or ("" if d.readable else f"{d.detail.get('method')} not ingested")
        table.add_row(source, d.key, d.kind, d.shape, note)
    return table


@app.command()
def discover(source: str) -> None:
    """Walk a source's metadata and catalog every dataset it exposes."""
    try:
        datasets = _engine().discover(source)
    except Exception as exc:
        _fail(str(exc))
    console.print(_dataset_table((source, d) for d in datasets))
    console.print(f"{len(datasets)} dataset(s) cataloged")


@app.command()
def datasets(source: Annotated[Optional[str], typer.Argument()] = None) -> None:
    """Show the dataset catalog."""
    console.print(_dataset_table(_engine().store.list_datasets(source)))


@app.command()
def ingest(
    source: str,
    dataset: Annotated[Optional[list[str]], typer.Option(help="Dataset key (repeatable); default all.")] = None,
    mode: Annotated[str, typer.Option(help="replace or append")] = "replace",
    batch_size: int = 1000,
) -> None:
    """Pull datasets from a source into core storage."""
    try:
        results = _engine().ingest(source, dataset, mode, batch_size)
    except Exception as exc:
        _fail(str(exc))
    table = Table("dataset", "table", "status", "rows", "error")
    for r in results:
        color = "green" if r.status == "succeeded" else "red"
        table.add_row(r.dataset_key, r.table, f"[{color}]{r.status}[/{color}]", str(r.rows), r.error or "")
    console.print(table)
    if any(r.status == "failed" for r in results):
        raise typer.Exit(1)


@app.command()
def runs(source: Annotated[Optional[str], typer.Argument()] = None, limit: int = 20) -> None:
    """Show recent ingestion runs."""
    table = Table("id", "source", "dataset", "status", "rows", "started", "error")
    for r in _engine().store.list_runs(source, limit):
        table.add_row(str(r.id), r.source, r.dataset_key, r.status, str(r.rows_written),
                      f"{r.started_at:%Y-%m-%d %H:%M:%S}", r.error or "")
    console.print(table)


@app.command()
def upload(
    source: str,
    file: Annotated[Path, typer.Argument(help="CSV file to load.", exists=True, dir_okay=False)],
    mode: Annotated[str, typer.Option(help="replace or append, for an existing file source")] = "replace",
    create: Annotated[bool, typer.Option(help="Register SOURCE as a new file source first.")] = False,
) -> None:
    """Load a CSV file into a file source (same as dropping it on the web page)."""
    try:
        result = _engine().ingest_upload(source, file.name, file.read_bytes(), mode=mode, create=create)
    except Exception as exc:
        _fail(str(exc))
    if result.status != "succeeded":
        _fail(result.error or "ingestion failed")
    console.print(f"{result.rows} row(s) from {file.name} -> [bold]{result.table}[/bold] ({mode})")


@app.command()
def serve(
    host: Annotated[str, typer.Option(help="Interface to listen on.")] = "127.0.0.1",
    port: int = 8765,
) -> None:
    """Run the web API and the source management page.

    Needs EYETABLE_API_TOKEN (any long random string); the page asks for it.
    EYETABLE_CORS_ORIGINS (comma-separated) lists the sites allowed to call the
    API; default https://tootallted.github.io.
    """
    import os

    import uvicorn

    from eyetable.api import DEFAULT_CORS_ORIGINS, DEFAULT_MAX_UPLOAD_BYTES, create_app
    from eyetable.query import QueryRunner

    s = load_settings()
    token = os.environ.get("EYETABLE_API_TOKEN", "")
    if len(token) < 16:
        _fail(
            "set EYETABLE_API_TOKEN to a random string of 16+ characters (in .env or the shell), e.g.\n"
            "  python -c \"import secrets; print(secrets.token_urlsafe(32))\""
        )
    origins = [o.strip() for o in os.environ.get("EYETABLE_CORS_ORIGINS", "").split(",") if o.strip()]
    max_mb = os.environ.get("EYETABLE_MAX_UPLOAD_MB")
    store = CoreStore(s.database_url, s.meta_schema, s.data_schema)
    store.init()
    query_url = os.environ.get("EYETABLE_QUERY_DATABASE_URL") or None
    if query_url is None and store.db.dialect.name == "postgresql":
        console.print(
            "[yellow]note:[/yellow] EYETABLE_QUERY_DATABASE_URL is not set, so the query window runs as the main "
            "database role (still read-only); scripts/setup-postgres.ps1 creates a SELECT-only role for it"
        )
    app_ = create_app(
        Engine(store),
        token,
        cors_origins=origins or DEFAULT_CORS_ORIGINS,
        max_upload_bytes=int(max_mb) * 1024 * 1024 if max_mb else DEFAULT_MAX_UPLOAD_BYTES,
        query_runner=QueryRunner(store, query_url, float(os.environ.get("EYETABLE_QUERY_TIMEOUT_S", "30"))),
    )
    console.print(f"source management: [bold]http://{host}:{port}/source_management/[/bold]")
    uvicorn.run(app_, host=host, port=port)
