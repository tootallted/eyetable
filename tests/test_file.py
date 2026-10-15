import datetime as dt

import pytest

from eyetable.connectors.file import FileConnector, parse_csv
from eyetable.engine import Engine

SALES = b"region,units,price,launched,active,zip\nWest,3,9.5,2026-01-02,true,007\nEast,,10,2026-02-03,false,98101\n"


def test_columns_are_typed_from_all_their_values():
    headers, records = parse_csv(SALES)
    assert headers == ["region", "units", "price", "launched", "active", "zip"]
    assert records[0] == {
        "region": "West", "units": 3, "price": 9.5, "launched": dt.date(2026, 1, 2), "active": True, "zip": "007",
    }
    assert records[1]["units"] is None  # empty cell
    assert records[1]["price"] == 10.0  # the column has a decimal, so every value is a double
    assert records[1]["zip"] == "98101"  # leading zeros elsewhere keep the column text


def test_excel_style_files():
    content = "﻿Name;Amount;Name;\r\nAsa;1,5;x;\r\n\r\nBo;2;y;z\r\n".encode("utf-8")
    headers, records = parse_csv(content)
    assert headers == ["Name", "Amount", "Name_2", "column_4"]
    assert len(records) == 2  # blank line skipped
    assert records[0]["Amount"] == "1,5"  # comma decimals are not guessed at
    assert records[1]["column_4"] == "z"


def test_windows_1252_fallback():
    _, records = parse_csv("city\nMálaga\n".encode("cp1252"))
    assert records == [{"city": "Málaga"}]


def test_timestamps_and_impossible_dates():
    _, records = parse_csv(b"at,day\n2026-01-02T03:04:05Z,2026-02-30\n")
    assert records[0]["at"] == dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)
    assert records[0]["day"] == "2026-02-30"


def test_empty_file_is_rejected():
    with pytest.raises(ValueError, match="empty"):
        parse_csv(b"\n\n")


def test_connector_needs_an_upload():
    with pytest.raises(ValueError, match="uploading a file"):
        FileConnector({"format": "csv"}).discover()


def test_upload_create_replace_append(store):
    engine = Engine(store)
    first = engine.ingest_upload("sales", "sales_q1.csv", SALES, create=True)
    assert (first.status, first.rows, first.table) == ("succeeded", 2, "sales__data")
    assert store.get_source("sales").type == "file"

    rows = store.read_table("sales__data")
    assert [r["ET_id"] for r in rows] == [1, 2]
    assert {r["ET_source"] for r in rows} == {"sales - sales_q1.csv"}

    q2 = b"region,units,price,launched,active,zip\nNorth,7,1.25,2026-04-05,true,10001\n"
    engine.ingest_upload("sales", "sales_q2.csv", q2, mode="append")
    rows = store.read_table("sales__data")
    assert [(r["ET_id"], r["region"], r["ET_source"]) for r in rows] == [
        (1, "West", "sales - sales_q1.csv"),
        (2, "East", "sales - sales_q1.csv"),
        (3, "North", "sales - sales_q2.csv"),
    ]

    engine.ingest_upload("sales", "sales_q2.csv", q2, mode="replace")
    rows = store.read_table("sales__data")
    assert [(r["ET_id"], r["region"]) for r in rows] == [(1, "North")]

    uploads = store.list_uploads("sales")
    assert [(u.filename, u.mode) for u in uploads] == [
        ("sales_q2.csv", "replace"), ("sales_q2.csv", "append"), ("sales_q1.csv", "replace"),
    ]
    assert all(u.run_id for u in uploads)
    assert store.upload_content(uploads[-1].id) == SALES
    [dataset] = [d for _, d in store.list_datasets("sales")]
    assert dataset.detail["filename"] == "sales_q2.csv" and dataset.shape == "6 x 1"


def test_bad_file_changes_nothing(store):
    engine = Engine(store)
    with pytest.raises(ValueError):
        engine.ingest_upload("sales", "empty.csv", b"", create=True)
    assert not store.source_exists("sales")
    assert store.list_uploads() == []


def test_files_only_load_into_file_sources(store, source_db):
    engine = Engine(store)
    engine.add_source("crm", "database", {"url": source_db})
    with pytest.raises(ValueError, match="database source"):
        engine.ingest_upload("crm", "x.csv", SALES)
    with pytest.raises(ValueError, match="already exists"):
        engine.ingest_upload("crm", "x.csv", SALES, create=True)
