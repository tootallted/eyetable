"""The eyetable engine: register sources, discover their datasets, ingest them."""

import csv
import datetime as dt
import io
import logging
import os
import threading
import traceback
from contextlib import contextmanager
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from eyetable import filesets, gsheets, incremental, keys, loadlog, targets
from eyetable.connectors import Connector, build_connector
from eyetable.connectors.file import DATASET_KEY, FileConnector
from eyetable.credentials import decrypt_secrets, resolve_env
from eyetable.identifiers import SOURCE_NAME, table_name
from eyetable.models import ColumnInfo, DatasetInfo
from eyetable.store import CoreStore, SourceNotFound
from eyetable.types import infer_columns

log = logging.getLogger(__name__)

# The progress callback of the load running on this thread (see Engine.reporting).
_local = threading.local()
Progress = Callable[[str, int], None]  # (table, rows written so far)

# Source types whose data is one table, <source>__data, loaded by pushing data in
# (a dropped file, a Google Sheet tab) rather than by a connector pulling it.
SINGLE_TABLE_TYPES = ("file", "gsheet", "curl", "dbtable")

ConnectorFactory = Callable[[str, dict[str, Any]], Connector]


@dataclass
class RunResult:
    run_id: int
    dataset_key: str
    table: str
    status: str  # "succeeded" | "failed" | "skipped" (the table is hidden in Sources)
    rows: int
    error: str | None = None
    upload_id: int | None = None
    mode: str = "replace"  # "replace" | "append" | "incremental"
    since: Any = None  # an incremental load's starting point: the largest value loaded before it
    replaced: int = 0  # older copies of rows an incremental load replaced (by natural key)
    detail: str | None = None  # a failure's full traceback, for the load queue's details
    note: str | None = None  # how it was read, when that's worth saying (in parts, say)


class UnknownDataset(KeyError):
    pass


@dataclass
class SheetTabPlan:
    tab: str
    index: int
    source: str
    table: str
    columns: int
    rows: int
    exists: bool  # this tab was loaded before; it will be replaced or appended to
    problem: str | None = None  # why this tab can't be loaded


@dataclass
class SheetLoad:
    title: str
    spreadsheet_id: str
    url: str
    tabs: list[SheetTabPlan] = field(default_factory=list)
    results: list[RunResult] = field(default_factory=list)


def _csv_snapshot(headers: list[str], records: list[dict[str, Any]]) -> bytes:
    out = io.StringIO()
    writer = csv.writer(out)  # csv's default line ending is \r\n
    writer.writerow(headers)
    for r in records:
        writer.writerow(["" if r[h] is None else (r[h].isoformat() if isinstance(r[h], (dt.date, dt.time)) else r[h]) for h in headers])
    return out.getvalue().encode("utf-8")


class Engine:
    def __init__(
        self,
        store: CoreStore,
        connector_factory: ConnectorFactory = build_connector,
        environ: Mapping[str, str] | None = None,
        scan_keys: str = "inline",
    ):
        """`scan_keys`: after each load, scan the table for key candidates "inline" (before
        the load returns), in the "background" (the API server, so a load isn't held up), or "off"."""
        if scan_keys not in ("inline", "background", "off"):
            raise ValueError(f"unknown scan_keys {scan_keys!r}")
        self.store = store
        self._factory = connector_factory
        self._environ = environ
        self._scan_keys = scan_keys
        self._scans: ThreadPoolExecutor | None = None

    @contextmanager
    def reporting(self, progress: "Progress | None"):
        """While inside, every load on this thread calls progress(table, rows so far): 0 as a
        table starts, then after each batch. The callback may raise to stop the load."""
        previous = getattr(_local, "progress", None)
        _local.progress = progress
        try:
            yield
        finally:
            _local.progress = previous

    @staticmethod
    def _counted(table: str, batches):
        progress = getattr(_local, "progress", None)
        if progress is None:
            return batches

        def counted():
            rows = 0
            progress(table, 0)
            for batch in batches:
                rows += len(batch)
                yield batch
                progress(table, rows)

        return counted()

    def scan_keys(self, table: str) -> None:
        """Scan a just-loaded table for the columns that could be its key (see keys.py)."""
        if self._scan_keys == "off":
            return
        if self._scan_keys == "background":
            if self._scans is None:
                self._scans = ThreadPoolExecutor(max_workers=1, thread_name_prefix="eyetable-keys")
            self._scans.submit(self._scan_quietly, table)
        else:
            self._scan_quietly(table)

    def _scan_quietly(self, table: str) -> None:
        # A scan only informs; it must never fail the load it follows.
        try:
            keys.scan_and_save(self.store, table)
        except Exception:
            log.exception("key scan of %s failed", table)

    @property
    def secret_key(self) -> str | None:
        """EYETABLE_SECRET_KEY: encrypts secrets kept in source configs."""
        return (self._environ if self._environ is not None else os.environ).get("EYETABLE_SECRET_KEY")

    def _open(self, type_: str, config: dict[str, Any]) -> Connector:
        return self._factory(type_, decrypt_secrets(resolve_env(config, self._environ), self.secret_key))

    def connector(self, source: str) -> Connector:
        src = self.store.get_source(source)
        return self._open(src.type, src.config)

    def add_source(
        self, name: str, type_: str, config: dict[str, Any], replace: bool = False, verify: bool = True
    ) -> None:
        """Register a source. `config` may hold `${NAME}` secret references."""
        if not SOURCE_NAME.match(name):
            raise ValueError("source names are lowercase letters, digits and _, starting with a letter")
        if not replace and self.store.source_exists(name):
            raise ValueError(f"source {name!r} already exists (pass replace=True to overwrite)")
        if verify:
            with self._open(type_, config) as conn:
                conn.test()
        self.store.save_source(name, type_, config)

    def ingest_upload(
        self,
        source: str,
        filename: str,
        content: bytes,
        mode: str = "replace",
        create: bool = False,
        keep_file: bool = True,
    ) -> RunResult:
        """Load an uploaded file into a file source's table.

        With `create`, the file source is registered first; otherwise it must
        already exist, and `mode` says whether this version of the file
        replaces the table's contents or is appended to them. The file is
        parsed before anything is registered or written, so a bad file
        changes nothing except being rejected. `keep_file=False` is for
        replaying a file that is already stored (a reload).
        """
        if mode not in ("replace", "append"):
            raise ValueError(f"mode must be replace or append, got {mode!r}")
        config = {"format": "csv"}
        if not create:
            src = self.store.get_source(source)
            if src.type != "file":
                raise ValueError(f"{source!r} is a {src.type} source; files can only be loaded into file sources")
            config = src.config
            if config.get("pattern") and keep_file:
                raise ValueError(f"{source} reads {config['pattern']} from {config.get('folder')} on the server; "
                                 "put the file in that folder and reload, rather than uploading it")
        conn = FileConnector(resolve_env(config, self._environ), content=content, filename=filename)
        [dataset] = conn.discover()  # parses, raising on an unreadable file
        if create:
            self.add_source(source, "file", config)

        upload_id = self.store.save_upload(source, filename, content, mode) if keep_file else None
        dataset.detail["upload_id"] = upload_id
        table = targets.table_for(self.store.get_source(source), dataset.name)
        run_id = self.store.start_run(source, DATASET_KEY, table, mode)
        if upload_id is not None:
            self.store.link_upload(upload_id, run_id)
        try:
            rows = self.store.write(
                table, self._counted(table, conn.read(DATASET_KEY)), mode, run_id, origin=f"{source} - {conn.origin(DATASET_KEY)}"
            )
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            self.store.finish_run(run_id, "failed", 0, error)
            return RunResult(run_id, DATASET_KEY, table, "failed", 0, error, upload_id, detail=traceback.format_exc())
        self.store.finish_run(run_id, "succeeded", rows)
        self.store.save_catalog(source, [dataset])
        self.scan_keys(table)
        return RunResult(run_id, DATASET_KEY, table, "succeeded", rows, upload_id=upload_id)

    def plan_gsheet(self, book: gsheets.Workbook) -> SheetLoad:
        """What loading this spreadsheet would do, tab by tab, without changing anything."""
        load = SheetLoad(book.title, book.spreadsheet_id, book.url)
        taken: set[str] = set()
        for tab in book.tabs:
            name = base = gsheets.source_name(book.title, tab.title)
            n = 2
            while name in taken:  # two tabs whose names differ only in punctuation
                name = gsheets.make_source_name(f"{base}_{n}")
                n += 1
            taken.add(name)
            plan = SheetTabPlan(tab.title, tab.index, name, self._table_of(name),
                                len(tab.headers), len(tab.records), exists=False)
            if not tab.headers:
                plan.problem = "the tab is empty"
            else:
                try:
                    src = self.store.get_source(name)
                except SourceNotFound:
                    pass
                else:
                    if src.type != "gsheet" or src.config.get("spreadsheet_id") != book.spreadsheet_id:
                        plan.problem = f"a different source is already named {name}"
                    else:
                        plan.exists = True
            load.tabs.append(plan)
        return load

    def ingest_gsheet(
        self,
        url: str,
        mode: str = "replace",
        fetch: Callable[[str], gsheets.Workbook] = gsheets.fetch_workbook,
    ) -> SheetLoad:
        """Load every tab of a Google Sheet, each into its own source "<file>_<tab>".

        Tabs seen for the first time become new sources; tabs loaded before are
        replaced or appended to, per `mode`. A tab that can't be loaded (empty,
        or its name is taken by another source) is reported and skipped.
        """
        if mode not in ("replace", "append"):
            raise ValueError(f"mode must be replace or append, got {mode!r}")
        book = fetch(url)
        load = self.plan_gsheet(book)
        tabs = {t.index: t for t in book.tabs}
        for plan in load.tabs:
            if plan.problem:
                load.results.append(RunResult(0, plan.tab, plan.table, "skipped", 0, plan.problem))
                continue
            tab = tabs[plan.index]
            tab_mode = mode if plan.exists else "replace"
            if not plan.exists:
                if not SOURCE_NAME.match(plan.source):
                    load.results.append(RunResult(0, plan.tab, plan.table, "skipped", 0, f"{plan.source} is not a valid source name"))
                    continue
                self.store.save_source(plan.source, "gsheet", {
                    "spreadsheet_id": book.spreadsheet_id, "published": book.published, "url": book.url,
                    "file_title": book.title, "tab_title": tab.title,
                })
            load.results.append(self._load_sheet_tab(book, tab, plan.source, plan.table, tab_mode))
        return load

    def _load_sheet_tab(self, book: gsheets.Workbook, tab: gsheets.Tab, source: str, table: str, mode: str) -> RunResult:
        label = f"{book.title} / {tab.title}"
        upload_id = self.store.save_upload(source, f"{label}.csv", _csv_snapshot(tab.headers, tab.records), mode)
        run_id = self.store.start_run(source, DATASET_KEY, table, mode)
        self.store.link_upload(upload_id, run_id)
        batches = (tab.records[i : i + 1000] for i in range(0, len(tab.records), 1000))
        try:
            rows = self.store.write(table, self._counted(table, batches), mode, run_id, origin=f"{source} - {label}")
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            self.store.finish_run(run_id, "failed", 0, error)
            return RunResult(run_id, tab.title, table, "failed", 0, error, upload_id, detail=traceback.format_exc())
        self.store.finish_run(run_id, "succeeded", rows)
        self.scan_keys(table)
        kinds = infer_columns(tab.records)
        self.store.save_catalog(source, [DatasetInfo(
            key=DATASET_KEY, name=DATASET_KEY, kind="sheet",
            columns=[ColumnInfo(h, kinds.get(h) or "text") for h in tab.headers],
            row_count=len(tab.records),
            detail={"spreadsheet_id": book.spreadsheet_id, "file_title": book.title, "tab_title": tab.title,
                    "url": book.url, "upload_id": upload_id},
        )])
        return RunResult(run_id, tab.title, table, "succeeded", rows, upload_id=upload_id)

    def load_curl(
        self,
        text: str,
        name: str,
        mode: str = "replace",
        all_pages: bool = True,
        client: Any = None,
        page_size: int | None = None,
        next_field: str | None = None,
        records_field: str | None = None,
        certificate: dict[str, Any] | None = None,
    ) -> list[RunResult]:
        """Make (or update) source `name` from a curl command and load it.

        The request is sent once to learn its paging (unless `all_pages` is off),
        the source is saved with its secrets encrypted, and every page is loaded.
        `page_size` (the API's fixed rows per page), `next_field` (where the
        next-page link is) and `records_field` (where the rows are) override
        what the first response suggests, and are kept for reloads, as is
        `certificate`, a client certificate (see eyetable.tls.client_cert).
        A source loaded before keeps its name and is replaced or appended to.
        """
        from eyetable import curl

        if mode not in ("replace", "append"):
            raise ValueError(f"mode must be replace or append, got {mode!r}")
        if not SOURCE_NAME.match(name or ""):
            raise ValueError("source names are lowercase letters, digits and _, starting with a letter")
        existing = self.store.get_source(name) if self.store.source_exists(name) else None
        if existing and existing.type != "curl":
            raise ValueError(f"{name!r} is already a {existing.type} source; pick another name")
        req, config, secrets = curl.prepare(text, self.secret_key, certificate)
        response, payload, _ = curl.probe(req, config, secrets, client, certificate)
        curl.settle(req, config, response, payload, all_pages=all_pages, page_size=page_size,
                    next_field=next_field, records_field=records_field)
        self.store.save_source(name, "curl", config)
        self.discover(name)
        return self.ingest(name, mode=mode if existing else "replace")

    def update_db_source(
        self,
        name: str,
        connection: str | None = None,
        password: str | None = None,
        schema: str | None = None,
        table: str | None = None,
        test: bool = True,
    ) -> list[str]:
        """Change a database table source's connection (or schema/table), keeping its name and table.

        A connection string without a password keeps the stored one; `password`
        replaces it. With `test`, nothing is saved unless the table can be read.
        """
        from eyetable import dbconnect
        from eyetable.credentials import encrypt_secret

        src = self.store.get_source(name)
        if src.type != "dbtable":
            raise ValueError(f"{name} isn't a database table source")
        config = dict(src.config)
        if connection and connection.strip():
            prepared = dbconnect.prepare(connection, self.secret_key)
            fresh = dict(prepared.config)
            if "password" not in fresh and config.get("password"):
                fresh["password"] = config["password"]
            kept = {k: config[k] for k in ("schema", "table", "kind", "tables") if k in config}
            config = {**fresh, **kept, "label": prepared.label}
        if password:
            config["password"] = encrypt_secret(password, self.secret_key)
        if (schema and schema.strip()) or (table and table.strip()):
            items = dbconnect.db_tables(config)
            if len(items) > 1:
                raise ValueError("this source loads several tables; choose them under Tables instead")
            item = {**items[0]}
            item["schema"] = schema.strip() if schema and schema.strip() else item["schema"]
            item["table"] = table.strip() if table and table.strip() else item["table"]
            config.update(schema=item["schema"], table=item["table"])
            if config.get("tables"):
                config["tables"] = [item]
        if test:
            with self._open("dbtable", config) as conn:
                conn.discover()
        self.store.save_source(name, "dbtable", config)
        warnings = []
        if test:
            self.discover(name)
        else:
            warnings.append("Saved without checking the connection; the next load will show whether it works.")
        return warnings

    def set_db_tables(self, name: str, chosen: list[dict[str, str]]) -> dict[str, Any]:
        """Make a database source load exactly the `chosen` tables and views ({schema, name, kind}).

        New ones are loaded now; ones no longer chosen are taken out, and their
        loaded tables deleted. Every chosen one must be readable before anything
        changes. Returns what was added and removed, and the new ones' load results.
        """
        from eyetable import dbconnect

        src = self.store.get_source(name)
        if src.type != "dbtable":
            raise ValueError(f"{name} isn't a database table source")
        if not chosen:
            raise ValueError("choose at least one table or view")
        current = dbconnect.db_tables(src.config)
        items = dbconnect.choose_tables(current, chosen)
        keys = {i["key"] for i in items}
        removed = [i for i in current if i["key"] not in keys]
        added = [i for i in items if i not in current]
        config = {**src.config, "tables": items, "schema": items[0]["schema"], "table": items[0]["table"],
                  "kind": items[0]["kind"]}
        with self._open("dbtable", config) as conn:
            datasets = conn.discover()  # all of them must be readable
        self.store.save_source(name, "dbtable", config)
        self.store.save_catalog(name, datasets)
        for item in removed:
            self.store.drop_table(item.get("target") or table_name(name, item["name"]))
        results = self.ingest(name, keys=[i["key"] for i in added]) if added else []
        return {"added": [i["key"] for i in added], "removed": [i["key"] for i in removed], "results": results}

    def update_curl_source(
        self,
        name: str,
        command: str | None = None,
        secrets: dict[str, str] | None = None,
        certificate: dict[str, Any] | None = None,
        remove_certificate: bool = False,
        test: bool = True,
    ) -> list[str]:
        """Change a curl source's request, secrets or client certificate, keeping its name and table.

        A new command keeps the source's paging, rows field, certificate and set
        of statements (the set is dropped if it no longer fits). With `test`, the
        request is sent once and nothing is saved unless it works.
        """
        import httpx

        from eyetable import curl
        from eyetable.credentials import encrypt_secret

        src = self.store.get_source(name)
        if src.type != "curl":
            raise ValueError(f"{name} isn't a curl source")
        config = {**src.config, "variables": dict(src.config.get("variables") or {})}
        warnings: list[str] = []
        if command and command.strip():
            _, fresh, _ = curl.prepare(command, self.secret_key)
            for key in ("pagination", "records_path", "client_cert", "timeout"):
                if key in config:
                    fresh[key] = config[key]
            plan = config.get("statements")
            if plan:
                try:
                    fresh["statements"] = curl.clean_statements(fresh, plan)
                except ValueError as exc:
                    warnings.append(f"The set of statements no longer fits the request ({exc}), so it was removed.")
            config = fresh
        for secret, value in (secrets or {}).items():
            if not value:
                continue
            if secret not in config["variables"]:
                raise ValueError(f"the request has no {secret}")
            # "Bearer abc" was stored whole; a new token on its own keeps the "Bearer "
            scheme = curl.auth_scheme(decrypt_secrets(config["variables"][secret], self.secret_key))
            if scheme and not curl.auth_scheme(value):
                value = scheme + value.strip()
            config["variables"][secret] = encrypt_secret(value, self.secret_key)
        url = curl.stored_request(config)["url"]
        if remove_certificate:
            config.pop("client_cert", None)
        if certificate:
            curl.attach_certificate(config, certificate, url, self.secret_key)
        elif config.get("client_cert") and not curl.covers(config["client_cert"], url):
            warnings.append(f"The client certificate is for {config['client_cert']['host']}, which this request "
                            "doesn't call, so it won't be sent.")
        if test:
            try:
                with self._open("curl", config) as conn:
                    conn.fetch(curl.REQUEST_NAME)
            except httpx.HTTPError as exc:
                raise ValueError(curl.http_problem(exc)) from None
        else:
            warnings.append("Saved without checking the connection; the next load will show whether it works.")
        self.store.save_source(name, "curl", config)
        return warnings

    def load_db_objects(
        self,
        connection: str,
        items: list[dict[str, str]],
        mode: str = "replace",
    ) -> list[dict[str, Any]]:
        """Make one source per chosen table/view of a database and load each.

        `items` are {"schema", "name", "kind", "source"}. A source loaded before
        (same name, database table type) is replaced or appended to; one name
        clashing with another kind of source is reported and skipped. Each item
        is loaded on its own, so one failing doesn't stop the rest.
        """
        from eyetable import dbconnect

        if mode not in ("replace", "append"):
            raise ValueError(f"mode must be replace or append, got {mode!r}")
        prepared = dbconnect.prepare(connection, self.secret_key)
        out = []
        for item in items:
            name = item.get("source", "")
            entry: dict[str, Any] = {"schema": item["schema"], "name": item["name"], "source": name,
                                     "status": "failed", "rows": 0, "error": None}
            out.append(entry)
            if not SOURCE_NAME.match(name):
                entry["error"] = "source names are lowercase letters, digits and _, starting with a letter"
                continue
            existing = self.store.get_source(name) if self.store.source_exists(name) else None
            if existing and existing.type != "dbtable":
                entry["error"] = f"{name} is already a {existing.type} source; pick another name"
                continue
            config = dbconnect.source_config(prepared, item["schema"], item["name"], item.get("kind", "table"))
            try:
                self.store.save_source(name, "dbtable", config)
                self.discover(name)
                results = self.ingest(name, mode=mode if existing else "replace")
            except Exception as exc:
                entry["error"] = str(exc).strip("'\"") or type(exc).__name__
                continue
            failed = [r for r in results if r.status != "succeeded"]
            entry.update(status="failed" if failed else "succeeded", rows=sum(r.rows for r in results),
                         error="; ".join(r.error or "" for r in failed) or None)
        return out

    def reload_source(
        self,
        source: str,
        sheet_fetch: Callable[[str], gsheets.Workbook] = gsheets.fetch_workbook,
        incremental: bool | str = False,
        table: str | None = None,
    ) -> list[RunResult]:
        """Load a source's data again, so its table (and last-loaded time) is fresh.

        * file: the stored files are replayed from the last replace onward, so a
          table built from a replace plus appends comes back exactly as it was.
        * gsheet: the tab is fetched again and replaces the table.
        * database / postman: every readable dataset is pulled again (replace).

        With `incremental`, only rows past each table's incremental column are
        loaded (see incremental.py); tables without one are left alone. With
        `incremental="auto"`, tables with an incremental column load incrementally and
        every other table (file sources included) loads in full. `table`
        loads just that one of a database source's tables.
        """
        src = self.store.get_source(source)
        auto = incremental == "auto"
        if auto:  # per table: incremental where set up, full elsewhere
            chosen = self.store.increments()
            incremental = src.type != "file" and any(t in chosen for t in targets.tables(src))
            if incremental and src.type != "dbtable":
                incremental = True  # its one table has an incremental column
            elif src.type == "dbtable" and incremental:
                incremental = "auto"
        loadlog.step(f"{'Incremental where set up, full elsewhere:' if incremental == 'auto' else 'Incremental' if incremental else 'Full'}"
                     f" load of {source} ({src.type} source)"
                     + (f", table {table}" if table else ""))
        if incremental and src.type == "file":
            raise incremental_problem("a file source gets new rows by loading a file with Append; there is nothing to fetch")
        keys_ = None
        if table is not None:
            if src.type == "dbtable":
                from eyetable import dbconnect

                keys_ = [e["key"] for e in targets.entries(src) if e["ref"] == table]
                if not keys_:
                    raise ValueError(f"{source} has no table {table}")
            elif table != targets.table_for(src):
                raise ValueError(f"{source} has no table {table}")
        # Tables hidden in Sources are skipped by every load: groups, schedules, the queue.
        hidden = self.store.hidden()
        skipped: list[RunResult] = []
        if src.type == "dbtable":
            from eyetable import dbconnect

            items = dbconnect.db_tables(src.config)
            wanted = keys_ if keys_ is not None else [i["key"] for i in items]
            for i in items:
                t = targets.table_for(src, i["name"])
                if i["key"] in wanted and t in hidden:
                    skipped.append(RunResult(0, i["key"], t, "skipped", 0, "hidden in Sources"))
            keys_ = [k for k in wanted if k not in {r.dataset_key for r in skipped}]
            if not keys_:
                return skipped
        elif targets.table_for(src) in hidden:
            return [RunResult(0, DATASET_KEY, targets.table_for(src), "skipped", 0, "hidden in Sources")]
        if src.type == "file" and src.config.get("pattern"):
            return [self._load_file_set(source, src.config)]
        if src.type == "file":
            history = [u for u, status in self.store.load_history(source) if status == "succeeded"]
            if not history:
                raise ValueError(f"no file has been loaded into {source!r} yet")
            start = max((i for i, u in enumerate(history) if u.mode == "replace"), default=0)
            results = []
            outer = getattr(_local, "progress", None)
            done = 0
            for i, upload in enumerate(history[start:]):
                # the files build one table, so progress counts across all of them
                inner = (lambda t, n, base=done: outer(t, base + n)) if outer else None
                with self.reporting(inner):
                    result = self.ingest_upload(
                        source, upload.filename, self.store.upload_content(upload.id),
                        mode="replace" if i == 0 else "append", keep_file=False,
                    )
                results.append(result)
                done += result.rows
                if result.status != "succeeded":
                    break
            return results
        if src.type == "gsheet":
            book = sheet_fetch(src.config["url"])
            wanted = src.config.get("tab_title")
            tab = next((t for t in book.tabs if t.title == wanted), None)
            if tab is None:
                raise ValueError(f'the tab "{wanted}" is no longer in "{book.title}"; it may have been renamed or deleted')
            if not tab.headers:
                raise ValueError(f'the tab "{wanted}" is empty now; nothing was reloaded')
            target = targets.table_for(src)
            if incremental:
                return [self._sheet_increment(book, tab, source, target)]
            return [self._load_sheet_tab(book, tab, source, target, "replace")]
        return skipped + self.ingest(source, keys=keys_, mode="auto" if incremental == "auto" else
                                     "incremental" if incremental else "replace")

    def _sheet_increment(self, book: gsheets.Workbook, tab: gsheets.Tab, source: str, table: str) -> RunResult:
        """Add the tab's rows past the incremental column (the whole tab is fetched; sheets can't be asked for less)."""
        p = incremental.plan(self.store, table)
        if p.since is None:
            return self._load_sheet_tab(book, tab, source, table, "replace")
        run_id = self.store.start_run(source, DATASET_KEY, table, "incremental")
        origin = f"{source} - {book.title} / {tab.title}"
        return self._write_increment(run_id, tab.title, table, origin, p,
                                     lambda split=None: incremental.newer([tab.records], p.column, p.since, p.inclusive))

    def _write_increment(self, run_id: int, key: str, table: str, origin: str, p: "incremental.Plan",
                         batches: Callable[..., Any], retry: tuple | None = None) -> RunResult:
        """Append an incremental load's rows; with a natural key, they replace their older copies."""
        def write(split):
            return self.store.write(table, self._counted(table, batches(split)), "append", run_id, origin=origin)

        note = None
        try:
            if retry:
                rows, note = self._write_or_split(*retry, write, undo=lambda: self.store.delete_run_rows(table, run_id))
            else:
                rows = write(None)
            replaced = self.store.replace_older(table, p.key, run_id) if p.key and rows else 0
        except Exception as exc:
            self.store.delete_run_rows(table, run_id)  # all or nothing, like a replace
            error = f"{type(exc).__name__}: {exc}"
            self.store.finish_run(run_id, "failed", 0, error)
            return RunResult(run_id, key, table, "failed", 0, error, mode="incremental", since=p.since,
                             detail=traceback.format_exc())
        self.store.finish_run(run_id, "succeeded", rows)
        if rows:
            self.scan_keys(table)
        return RunResult(run_id, key, table, "succeeded", rows, mode="incremental", since=p.since, replaced=replaced,
                         note=note)

    # -- file sets: every file matching a pattern in a server folder (filesets.py) -------------

    def _load_file_set(self, source: str, config: dict[str, Any]) -> RunResult:
        """Load every file in the folder that matches the pattern into the source's one table (a replace).
        A column in any file becomes a column; rows from files without it are NULL. Each row's
        ET_source names its file."""
        table = targets.table_for(self.store.get_source(source))
        files = filesets.matching_files(config.get("folder", ""), config["pattern"])
        loadlog.step(f"{len(files):,} file{'s' if len(files) != 1 else ''} in {config.get('folder')} match {config['pattern']}")
        if not files:
            raise filesets.FileSetProblem(f"no file in {config.get('folder')} matches {config['pattern']}")
        run_id = self.store.start_run(source, DATASET_KEY, table, "replace")
        current = {"origin": source}
        columns: dict[str, str] = {}  # every column seen, in order, with its kind
        canon: dict[str, str] = {}  # a column's table name -> the heading first seen for it

        def batches():
            from eyetable.identifiers import column_name

            for f in files:
                try:
                    conn = FileConnector(resolve_env(config, self._environ), content=f.read_bytes(), filename=f.name)
                    [dataset] = conn.discover()
                except Exception as exc:
                    raise ValueError(f"{f.name}: {exc}") from None
                for c in dataset.columns:
                    columns.setdefault(canon.setdefault(column_name(c.name), c.name), c.type)
                # the same heading in two files (even spelled Amount and amount) is one column
                rename = {c.name: canon[column_name(c.name)] for c in dataset.columns if canon[column_name(c.name)] != c.name}
                current["origin"] = f"{source} - {f.name}"
                loadlog.emit("file", f"{f}: {dataset.row_count:,} rows, {len(dataset.columns)} columns")
                for batch in conn.read(DATASET_KEY):
                    yield [{rename.get(k, k): v for k, v in r.items()} for r in batch] if rename else batch

        try:
            rows = self.store.write(table, self._counted(table, batches()), "replace", run_id,
                                    origin=lambda: current["origin"])
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            self.store.finish_run(run_id, "failed", 0, error)
            return RunResult(run_id, DATASET_KEY, table, "failed", 0, error, detail=traceback.format_exc())
        self.store.finish_run(run_id, "succeeded", rows)
        self.store.save_catalog(source, [DatasetInfo(
            key=DATASET_KEY, name=DATASET_KEY, kind="file", columns=[ColumnInfo(n, k) for n, k in columns.items()],
            row_count=rows, detail={"folder": config.get("folder"), "pattern": config["pattern"],
                                    "files": [f.name for f in files]})])
        self.scan_keys(table)
        return RunResult(run_id, DATASET_KEY, table, "succeeded", rows,
                         note=f"{len(files):,} file{'s' if len(files) != 1 else ''} matching {config['pattern']}")

    def update_file_source(self, name: str, folder: str | None, pattern: str | None, new_name: str | None,
                           test: bool = True) -> tuple[str, list[str]]:
        """Point a file source at the files matching `pattern` in `folder` on the server (an empty
        pattern goes back to uploaded files), and rename it if `new_name` differs. Returns
        (its name now, warnings). With `test`, nothing changes unless a matching file reads."""
        src = self.store.get_source(name)
        if src.type != "file":
            raise ValueError(f"{name} isn't a file source")
        config = dict(src.config)
        warnings: list[str] = []
        if pattern is not None:
            if pattern.strip():
                files = filesets.matching_files(folder or config.get("folder", ""), pattern)
                if test:
                    if not files:
                        raise filesets.FileSetProblem(f"no file in {folder or config.get('folder')} matches {pattern.strip()}")
                    first = files[0]
                    try:
                        FileConnector(resolve_env(config, self._environ), content=first.read_bytes(), filename=first.name).discover()
                    except Exception as exc:
                        raise ValueError(f"{first.name} can't be read: {exc}") from None
                elif not files:
                    warnings.append("No file matches yet; the next load fails until one does.")
                config.update(folder=str(filesets.check_folder(folder or config.get("folder", ""))), pattern=pattern.strip())
            else:
                config.pop("pattern", None)
                config.pop("folder", None)
        elif folder is not None and config.get("pattern"):
            config["folder"] = str(filesets.check_folder(folder))
            if test and not filesets.matching_files(config["folder"], config["pattern"]):
                raise filesets.FileSetProblem(f"no file in {config['folder']} matches {config['pattern']}")
        if config != src.config:
            self.store.save_source(name, "file", config)
        if new_name and new_name != name:
            self.rename_source(name, new_name)
            name = new_name
        return name, warnings

    def _table_of(self, source: str, name: str = DATASET_KEY) -> str:
        """The table a source's dataset loads into (named after the source if there's no source yet)."""
        try:
            return targets.table_for(self.store.get_source(source), name)
        except SourceNotFound:
            return table_name(source, name)

    def rename_source(self, old: str, new: str) -> None:
        """Rename a source; tables named after it follow (their old copies archived). See rename."""
        self.rename(old, new_name=new)

    def rename(self, source: str, new_name: str | None = None, table: str | None = None,
               new_table: str | None = None) -> dict[str, Any]:
        """Rename a source and/or one of its tables (`table`, given a new name `new_table`, which may
        name a schema: "finance.ledger").

        A table that changes name isn't renamed in place: the old table is archived (kept, as
        <schema>_archived.<table>_<yyyymmdd_hhmmss>), its settings (natural key, incremental
        column, hidden) move to the new name, and the next full load creates the new table.
        Tables still named after the source (<source>__x) follow a new source name the same way;
        a table with a name of its own keeps it. Returns what changed.
        """
        src = self.store.get_source(source)
        new_name = (new_name or source).strip()
        if new_name != source:
            if not SOURCE_NAME.match(new_name):
                raise ValueError("source names are lowercase letters, digits and _, starting with a letter")
            if self.store.source_exists(new_name):
                raise ValueError(f"there is already a source named {new_name}")
        current = targets.entries(src)
        chosen = None
        if new_table is not None:
            matches = [e for e in current if e["ref"] == table] if table else current[:1] if len(current) == 1 else []
            if not matches:
                raise ValueError(f"{source} has no table {table}" if table else f"say which of {source}'s tables to rename")
            chosen = matches[0]
            schema, name = targets.parse(new_table, self.store.data_schema, self.store.meta_schema)
            wanted = self.store.ref(schema, name)
        plan = []
        for e in current:
            default = table_name(new_name, e["name"])
            if chosen is not None and e is chosen:
                want = wanted
            elif e["target"]:
                want = e["target"]
            else:
                want = default
            plan.append((e, want, None if want == default else want))
        wants = [w for _, w, _ in plan]
        if len(set(wants)) != len(wants):
            raise ValueError("two of its tables would have the same name")
        for e, want, _ in plan:
            if want != e["ref"]:
                taken = [s.name for s in self.store.list_sources() if s.name != source and want in targets.tables(s)]
                if taken:
                    raise ValueError(f"{want} is already the table of {taken[0]}")
                if self.store.table_exists(want):
                    raise ValueError(f"a table named {want} already exists; choose another name")
        changes = []
        for e, want, _ in plan:
            if want == e["ref"]:
                continue
            archived = self.store.archive_table(e["ref"])
            self.store.move_table_settings(e["ref"], want)
            self.store.ensure_schema(self.store.split(want)[0])
            loadlog.step(f"{e['ref']} becomes {want}" + (f"; the old table is kept as {archived}" if archived else ""))
            changes.append({"key": e["key"], "from": e["ref"], "to": want, "archived_as": archived})
        config = dict(src.config)
        if src.type == "dbtable":
            from eyetable import dbconnect

            items = [dict(i) for i in dbconnect.db_tables(config)]
            for item, (_, _, target) in zip(items, plan):
                if target:
                    item["target"] = target
                else:
                    item.pop("target", None)
            config.update(tables=items, schema=items[0]["schema"], table=items[0]["table"], kind=items[0]["kind"])
        else:
            target = plan[0][2]
            if target:
                config["target"] = target
            else:
                config.pop("target", None)
        if config != src.config:
            self.store.save_source(source, src.type, config)
        if new_name != source:
            self.store.rename_source(source, new_name)
        return {"source": new_name, "renamed_from": source if new_name != source else None, "tables": changes}

    # -- big tables: reading in parts -----------------------------------------------------

    def _write_or_split(self, conn: Connector, source: str, key: str, dataset: DatasetInfo,
                        write: Callable[[str | None], int], undo: Callable[[], None] | None = None) -> tuple[int, str | None]:
        """Write a dataset; if reading it in one query times out, read it in parts instead.

        `write(split)` loads it (split: the column to read it in parts by, or None). A table
        that times out in one query is switched to parts by its best column (a creation time,
        say) and keeps loading that way. Returns (rows, a note on how it was read).
        """
        from eyetable.connectors.dbtable import is_timeout, split_candidates

        parts_by = None
        if getattr(conn, "reads_in_parts", False):
            parts_by = (conn._item(key).get("split") or {}).get("column")
        try:
            rows = write(None)
        except Exception as exc:
            if parts_by or not getattr(conn, "reads_in_parts", False) or not is_timeout(exc):
                raise
            choices = split_candidates(dataset.columns) or conn.split_columns(key)
            if not choices:
                raise
            if undo:
                undo()
            column = choices[0]
            loadlog.emit("warn", f"Reading it in one query timed out ({exc}); reading it again in parts by {column}")
            log.warning("%s/%s timed out in one query; reading it in parts by %s", source, key, column)
            rows = write(column)
            self.set_split(source, key, column, auto=True)
            return rows, (f"reading it in one query timed out, so it was read in {conn.parts_read.get(key, 0):,} parts "
                          f"by {column}; it loads in parts from now on")
        if parts_by:
            return rows, f"read in {conn.parts_read.get(key, 0):,} parts by {parts_by}"
        return rows, None

    def set_split(self, source: str, key: str, column: str | None, auto: bool = False) -> None:
        """Read a database source's table in parts by `column` from now on (None: in one query)."""
        from eyetable import dbconnect

        src = self.store.get_source(source)
        if src.type != "dbtable":
            raise ValueError(f"{source} isn't a database source")
        items = [dict(i) for i in dbconnect.db_tables(src.config)]
        found = [i for i in items if i["key"] == key]
        if not found:
            raise ValueError(f"{source} has no table {key}")
        if column:
            found[0]["split"] = {"column": column, "auto": auto}
        else:
            found[0].pop("split", None)
        config = {**src.config, "tables": items, "schema": items[0]["schema"], "table": items[0]["table"],
                  "kind": items[0]["kind"]}
        self.store.save_source(source, "dbtable", config)

    def discover(self, source: str) -> list[DatasetInfo]:
        """Walk the source's metadata, record every dataset found in the catalog."""
        with self.connector(source) as conn:
            datasets = conn.discover()
        self.store.save_catalog(source, datasets)
        return datasets

    def ingest(
        self,
        source: str,
        keys: list[str] | None = None,
        mode: str = "replace",
        batch_size: int = 1000,
    ) -> list[RunResult]:
        """Pull datasets from a source into core storage, one run per dataset.

        With no `keys`, every readable dataset in the catalog is ingested
        (discovering first if the source has never been discovered).
        """
        src = self.store.get_source(source)
        catalog = {d.key: d for _, d in self.store.list_datasets(source)}
        if not catalog:
            catalog = {d.key: d for d in self.discover(source)}
        if keys is None:
            keys = [k for k, d in catalog.items() if d.readable]
        unknown = [k for k in keys if k not in catalog]
        if unknown:
            raise UnknownDataset(f"not in {source!r} catalog (run discover?): {', '.join(unknown)}")
        if mode == "incremental":
            chosen = self.store.increments()
            keys = [k for k in keys if targets.table_for(src, catalog[k].name) in chosen]
            if not keys:
                raise incremental_problem("no incremental column is chosen for this table; choose one in Preview Data")

        chosen_columns = self.store.increments() if mode == "auto" else {}
        results = []
        with self.connector(source) as conn:
            for key in keys:
                table = targets.table_for(src, catalog[key].name)
                # "auto": this table incrementally if it has an incremental column, else in full
                key_mode = ("incremental" if table in chosen_columns else "replace") if mode == "auto" else mode
                if key_mode == "incremental":
                    p = incremental.plan(self.store, table)
                    loadlog.step(f"{table}: incremental by {p.column}; " + (
                        f"rows with {p.column} {'>=' if p.inclusive else '>'} {p.since}"
                        + (f", replacing older copies by the natural key {' + '.join(p.key)}" if p.key else ", appended (no natural key)")
                        if p.since is not None else "the table is empty, so it loads in full"))
                    if p.since is not None:
                        results.append(self._ingest_increment(conn, source, key, catalog[key], table, p, batch_size))
                        continue
                    # an empty (or missing) table: there is nothing to go on, so load it all
                run_id = self.store.start_run(source, key, table, "replace" if key_mode == "incremental" else key_mode)
                write_mode = "replace" if key_mode == "incremental" else key_mode

                def write(split, key=key, table=table, run_id=run_id, write_mode=write_mode):
                    loadlog.step(f"Reading {conn.origin(key)} into {table}" + (f" in parts by {split}" if split else ""))
                    read = conn.read(key, batch_size, split=split) if split else conn.read(key, batch_size)
                    return self.store.write(table, self._counted(table, read), write_mode, run_id,
                                            origin=f"{source} - {conn.origin(key)}")

                try:
                    rows, note = self._write_or_split(conn, source, key, catalog[key], write)
                except Exception as exc:
                    error = f"{type(exc).__name__}: {exc}"
                    self.store.finish_run(run_id, "failed", 0, error)
                    results.append(RunResult(run_id, key, table, "failed", 0, error, detail=traceback.format_exc()))
                else:
                    self.store.finish_run(run_id, "succeeded", rows)
                    results.append(RunResult(run_id, key, table, "succeeded", rows, note=note))
                    self.scan_keys(table)
        return results

    def _ingest_increment(self, conn: Connector, source: str, key: str, dataset: DatasetInfo, table: str,
                          p: "incremental.Plan", batch_size: int) -> RunResult:
        run_id = self.store.start_run(source, key, table, "incremental")
        origin = f"{source} - {conn.origin(key)}"
        column = incremental.source_key([c.name for c in dataset.columns], p.column) \
            if getattr(conn, "reads_since", False) else None

        def batches(split=None):
            if column is not None:  # the database is asked for just the newer rows
                more = {"split": split} if split else {}
                return conn.read(key, batch_size, since=(column, p.since, p.inclusive), **more)
            return incremental.newer(conn.read(key, batch_size), p.column, p.since, p.inclusive)

        return self._write_increment(run_id, key, table, origin, p, batches, retry=(conn, source, key, dataset))


def incremental_problem(message: str) -> Exception:
    return incremental.IncrementalProblem(message)
