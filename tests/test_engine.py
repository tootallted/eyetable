import datetime as dt

import httpx
import pytest

from eyetable.connectors import build_connector
from eyetable.connectors.postman import PostmanConnector, import_postman
from eyetable.credentials import MissingSecretError
from eyetable.engine import Engine


def test_database_source_end_to_end(store, source_db):
    engine = Engine(store)
    engine.add_source("crm", "database", {"url": source_db})

    datasets = engine.discover("crm")
    assert len(datasets) == 3
    assert {d.key for _, d in store.list_datasets("crm")} == {d.key for d in datasets}

    results = engine.ingest("crm")
    assert {(r.table, r.status, r.rows) for r in results} == {
        ("crm__customers", "succeeded", 3),
        ("crm__orders", "succeeded", 4),
        ("crm__big_orders", "succeeded", 2),
    }
    rows = store.read_table("crm__customers")
    assert [r["name"] for r in rows] == ["Ada", "Grace", "Linus"]
    assert [r["ET_id"] for r in rows] == [1, 2, 3]
    assert {r["ET_source"] for r in rows} == {"crm - main.customers"}
    assert all(isinstance(r["ET_timestamp"], dt.datetime) for r in rows)
    assert all(r["ET_run_id"] for r in rows)
    assert list(rows[0])[:4] == ["ET_id", "ET_timestamp", "ET_source", "ET_run_id"]

    # replace swaps in a fresh table, so ET_id starts over; append continues it
    engine.ingest("crm", ["main.customers"])
    assert [r["ET_id"] for r in store.read_table("crm__customers")] == [1, 2, 3]
    engine.ingest("crm", ["main.customers"], mode="append")
    assert [r["ET_id"] for r in store.read_table("crm__customers")] == [1, 2, 3, 4, 5, 6]

    runs = store.list_runs("crm")
    assert len(runs) == 5 and all(r.status == "succeeded" for r in runs)


def test_failed_replace_keeps_previous_table(store, source_db):
    engine = Engine(store)
    engine.add_source("crm", "database", {"url": source_db})
    engine.discover("crm")
    engine.ingest("crm", ["main.orders"])

    class Exploding:
        def __init__(self, inner):
            self.inner = inner

        def __getattr__(self, name):
            return getattr(self.inner, name)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.inner.close()

        def read(self, key, batch_size=1000):
            yield [{"id": 99}]
            raise ConnectionError("source went away")

    broken = Engine(store, lambda t, c: Exploding(build_connector(t, c)))
    [result] = broken.ingest("crm", ["main.orders"])
    assert result.status == "failed" and "source went away" in result.error
    assert len(store.read_table("crm__orders")) == 4
    assert store.list_runs("crm")[0].status == "failed"


def test_secrets_resolved_only_at_connect_time(store, source_db):
    engine = Engine(store, environ={"CRM_URL": source_db})
    engine.add_source("crm", "database", {"url": "${CRM_URL}"})
    assert store.get_source("crm").config == {"url": "${CRM_URL}"}
    assert len(engine.discover("crm")) == 3

    with pytest.raises(MissingSecretError, match="CRM_URL"):
        Engine(store, environ={}).discover("crm")


def test_schema_evolves_across_batches(store):
    batches = [
        [{"id": 1, "amount": 5}],
        [{"id": 2, "amount": 7.5, "tags": ["a"], "Note Text": "hi"}],
        [{"id": "three"}],
    ]
    assert store.write("t", iter(batches), origin="test - t") == 3
    rows = store.read_table("t")
    assert [r["ET_id"] for r in rows] == [1, 2, 3]
    assert rows[1]["amount"] == 7.5
    assert rows[1]["note_text"] == "hi"
    assert rows[2]["id"] == "three"


def test_postman_source_end_to_end(store, collection, environment):
    def handler(request):
        return httpx.Response(200, json={"data": [{"id": 1}, {"id": 2}]})

    def factory(type_, config):
        return PostmanConnector(config, client=httpx.Client(transport=httpx.MockTransport(handler)))

    config, secrets = import_postman("shop", collection, environment)
    env = {name: "secret" for name in secrets}
    engine = Engine(store, factory, environ=env)
    engine.add_source("shop", "postman", config)

    engine.discover("shop")
    results = engine.ingest("shop")
    assert {r.dataset_key for r in results} == {"Users/List users", "Users/Get user", "Reports/Sales (OData)"}
    assert all(r.status == "succeeded" for r in results)
    rows = store.read_table("shop__users_list_users")
    assert [r["ET_id"] for r in rows] == [1, 2]
    # the endpoint, with the query string (which can carry API keys) left off
    assert {r["ET_source"] for r in rows} == {"shop - https://api.example.test/v2/users"}
    user_rows = store.read_table("shop__users_get_user")
    assert {r["ET_source"] for r in user_rows} == {"shop - https://api.example.test/v2/users/7"}


def test_source_columns_cannot_take_et_names(store):
    store.write("t", iter([[{"ET_id": "theirs", "et_source": "x", "name": "a"}]]), origin="test - t")
    [row] = store.read_table("t")
    assert row["ET_id"] == 1 and row["ET_source"] == "test - t"
    assert row["et_id_src"] == "theirs" and row["et_source_src"] == "x"


def test_source_names_are_validated(store, source_db):
    with pytest.raises(ValueError, match="lowercase"):
        Engine(store).add_source("My CRM", "database", {"url": source_db})
