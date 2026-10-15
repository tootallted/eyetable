import json
import sqlite3
from pathlib import Path

import pytest

from eyetable.store import CoreStore

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def source_db(tmp_path) -> str:
    """A small SQLite database standing in for someone's operational database."""
    path = tmp_path / "source.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, balance NUMERIC);
        CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL, placed_at TEXT);
        CREATE VIEW big_orders AS SELECT * FROM orders WHERE total > 100;
        INSERT INTO customers VALUES (1, 'Ada', 10.5), (2, 'Grace', 0), (3, 'Linus', NULL);
        INSERT INTO orders VALUES
            (1, 1, 50.0, '2026-01-01'), (2, 1, 150.0, '2026-02-01'),
            (3, 2, 250.0, '2026-03-01'), (4, 3, 5.0, '2026-04-01');
        """
    )
    conn.commit()
    conn.close()
    return f"sqlite:///{path.as_posix()}"


@pytest.fixture
def store(tmp_path) -> CoreStore:
    s = CoreStore(f"sqlite:///{(tmp_path / 'core.db').as_posix()}")
    s.init()
    return s


@pytest.fixture
def collection() -> dict:
    return json.loads((FIXTURES / "collection.json").read_text())


@pytest.fixture
def environment() -> dict:
    return json.loads((FIXTURES / "environment.json").read_text())
