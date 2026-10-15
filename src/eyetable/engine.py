"""The eyetable engine: register sources, discover their datasets, ingest them."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from eyetable.connectors import Connector, build_connector
from eyetable.credentials import resolve_env
from eyetable.identifiers import SOURCE_NAME, table_name
from eyetable.models import DatasetInfo
from eyetable.store import CoreStore

ConnectorFactory = Callable[[str, dict[str, Any]], Connector]


@dataclass
class RunResult:
    run_id: int
    dataset_key: str
    table: str
    status: str  # "succeeded" | "failed"
    rows: int
    error: str | None = None


class UnknownDataset(KeyError):
    pass


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

    def _open(self, type_: str, config: dict[str, Any]) -> Connector:
        return self._factory(type_, resolve_env(config, self._environ))

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
