"""The eyetable engine: register sources, discover their datasets, ingest them."""

import csv
import datetime as dt
import io
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from eyetable import gsheets
from eyetable.connectors import Connector, build_connector
from eyetable.connectors.file import DATASET_KEY, FileConnector
from eyetable.credentials import decrypt_secrets, resolve_env
from eyetable.identifiers import SOURCE_NAME, table_name
from eyetable.models import ColumnInfo, DatasetInfo
from eyetable.store import CoreStore, SourceNotFound
from eyetable.types import infer_columns

# Source types whose data is one table, <source>__data, loaded by pushing data in
# (a dropped file, a Google Sheet tab) rather than by a connector pulling it.
SINGLE_TABLE_TYPES = ("file", "gsheet", "curl")

ConnectorFactory = Callable[[str, dict[str, Any]], Connector]


@dataclass
class RunResult:
    run_id: int
    dataset_key: str
    table: str
    status: str  # "succeeded" | "failed"
    rows: int
    error: str | None = None
    upload_id: int | None = None


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
    ):
        self.store = store
        self._factory = connector_factory
        self._environ = environ

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
        conn = FileConnector(resolve_env(config, self._environ), content=content, filename=filename)
        [dataset] = conn.discover()  # parses, raising on an unreadable file
        if create:
            self.add_source(source, "file", config)

        upload_id = self.store.save_upload(source, filename, content, mode) if keep_file else None
        dataset.detail["upload_id"] = upload_id
        table = table_name(source, dataset.name)
        run_id = self.store.start_run(source, DATASET_KEY, table, mode)
        if upload_id is not None:
            self.store.link_upload(upload_id, run_id)
        try:
            rows = self.store.write(
                table, conn.read(DATASET_KEY), mode, run_id, origin=f"{source} - {conn.origin(DATASET_KEY)}"
            )
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            self.store.finish_run(run_id, "failed", 0, error)
            return RunResult(run_id, DATASET_KEY, table, "failed", 0, error, upload_id)
        self.store.finish_run(run_id, "succeeded", rows)
        self.store.save_catalog(source, [dataset])
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
            plan = SheetTabPlan(tab.title, tab.index, name, table_name(name, DATASET_KEY),
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
            rows = self.store.write(table, batches, mode, run_id, origin=f"{source} - {label}")
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            self.store.finish_run(run_id, "failed", 0, error)
            return RunResult(run_id, tab.title, table, "failed", 0, error, upload_id)
        self.store.finish_run(run_id, "succeeded", rows)
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
    ) -> list[RunResult]:
        """Make (or update) source `name` from a curl command and load it.

        The request is sent once to learn its paging (unless `all_pages` is off),
        the source is saved with its secrets encrypted, and every page is loaded.
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
        req, config, secrets = curl.prepare(text, self.secret_key)
        response, payload, _ = curl.probe(req, config, secrets, client)
        if all_pages:
            paging, _ = curl.infer_paging(req.url, response, payload)
            if paging:
                config["pagination"] = {curl.REQUEST_NAME: paging}
        self.store.save_source(name, "curl", config)
        self.discover(name)
        return self.ingest(name, mode=mode if existing else "replace")

    def reload_source(
        self,
        source: str,
        sheet_fetch: Callable[[str], gsheets.Workbook] = gsheets.fetch_workbook,
    ) -> list[RunResult]:
        """Load a source's data again, so its table (and last-loaded time) is fresh.

        * file: the stored files are replayed from the last replace onward, so a
          table built from a replace plus appends comes back exactly as it was.
        * gsheet: the tab is fetched again and replaces the table.
        * database / postman: every readable dataset is pulled again (replace).
        """
        src = self.store.get_source(source)
        if src.type == "file":
            history = [u for u, status in self.store.load_history(source) if status == "succeeded"]
            if not history:
                raise ValueError(f"no file has been loaded into {source!r} yet")
            start = max((i for i, u in enumerate(history) if u.mode == "replace"), default=0)
            results = []
            for i, upload in enumerate(history[start:]):
                result = self.ingest_upload(
                    source, upload.filename, self.store.upload_content(upload.id),
                    mode="replace" if i == 0 else "append", keep_file=False,
                )
                results.append(result)
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
            return [self._load_sheet_tab(book, tab, source, table_name(source, DATASET_KEY), "replace")]
        return self.ingest(source)

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
        catalog = {d.key: d for _, d in self.store.list_datasets(source)}
        if not catalog:
            catalog = {d.key: d for d in self.discover(source)}
        if keys is None:
            keys = [k for k, d in catalog.items() if d.readable]
        unknown = [k for k in keys if k not in catalog]
        if unknown:
            raise UnknownDataset(f"not in {source!r} catalog (run discover?): {', '.join(unknown)}")

        results = []
        with self.connector(source) as conn:
            for key in keys:
                table = table_name(source, catalog[key].name)
                run_id = self.store.start_run(source, key, table, mode)
                try:
                    origin = f"{source} - {conn.origin(key)}"
                    rows = self.store.write(table, conn.read(key, batch_size), mode, run_id, origin=origin)
                except Exception as exc:
                    error = f"{type(exc).__name__}: {exc}"
                    self.store.finish_run(run_id, "failed", 0, error)
                    results.append(RunResult(run_id, key, table, "failed", 0, error))
                else:
                    self.store.finish_run(run_id, "succeeded", rows)
                    results.append(RunResult(run_id, key, table, "succeeded", rows))
        return results
