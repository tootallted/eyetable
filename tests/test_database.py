from eyetable.connectors.database import DatabaseConnector, build_url, is_system_schema


def test_discover_walks_tables_and_views(source_db):
    with DatabaseConnector({"url": source_db}) as conn:
        found = {d.key: d for d in conn.discover()}

    assert set(found) == {"main.customers", "main.orders", "main.big_orders"}
    customers = found["main.customers"]
    assert customers.kind == "table"
    assert customers.name == "customers"
    assert [c.name for c in customers.columns] == ["id", "name", "balance"]
    assert customers.columns[1].nullable is False
    assert customers.shape == "3 x 3"
    assert found["main.big_orders"].kind == "view"
    assert found["main.big_orders"].row_count == 2


def test_read_yields_batches(source_db):
    with DatabaseConnector({"url": source_db}) as conn:
        batches = list(conn.read("main.orders", batch_size=3))
    assert [len(b) for b in batches] == [3, 1]
    assert batches[0][0] == {"id": 1, "customer_id": 1, "total": 50.0, "placed_at": "2026-01-01"}


def test_url_from_parts_keeps_credentials_out_of_strings():
    url = build_url({
        "driver": "postgresql+psycopg", "host": "db", "port": 5432, "database": "crm",
        "username": "reader", "password": "p@ss:word",
    })
    assert url.password == "p@ss:word"
    assert url.render_as_string(hide_password=True) == "postgresql+psycopg://reader:***@db:5432/crm"


def test_system_schemas_are_skipped():
    assert is_system_schema("pg_catalog")
    assert is_system_schema("INFORMATION_SCHEMA")
    assert is_system_schema("db_owner")
    assert not is_system_schema("public")
    assert not is_system_schema("dbo")
