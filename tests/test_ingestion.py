"""
test_ingestion.py

Unit tests for ingest_csv.py and ingest_parquet.py
Uses an in-memory SQLite engine to avoid needing a real PostgreSQL instance.
"""

import os
import tempfile
import pytest
import sys
from unittest.mock import patch, MagicMock

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_sqlite_engine():
    """
    SQLite engine for testing without PostgreSQL.
    Uses a temp file so we can attach a 'raw' schema.
    """
    import tempfile
    db_file = tempfile.mktemp(suffix=".db")
    raw_file = tempfile.mktemp(suffix="_raw.db")
    engine = create_engine(f"sqlite:///{db_file}")
    with engine.connect() as conn:
        conn.execute(text(f"ATTACH DATABASE '{raw_file}' AS raw"))
        conn.commit()
    return engine


def _write_orders_csv(path: str, n_rows: int = 5) -> int:
    df = pd.DataFrame({
        "order_id": [f"ORD{i:03d}" for i in range(n_rows)],
        "customer_id": [f"CUST{i:03d}" for i in range(n_rows)],
        "order_date": ["2026-05-01"] * n_rows,
        "order_status": ["completed"] * n_rows,
        "currency": ["USD"] * n_rows,
        "total_amount": [100.0] * n_rows,
        "created_at": ["2026-05-01T00:00:00"] * n_rows,
        "updated_at": ["2026-05-01T00:00:00"] * n_rows,
    })
    df.to_csv(path, index=False)
    return n_rows


def _write_order_items_parquet(path: str, n_rows: int = 5) -> int:
    df = pd.DataFrame({
        "order_item_id": [f"ITEM{i:04d}" for i in range(n_rows)],
        "order_id": [f"ORD{i:03d}" for i in range(n_rows)],
        "product_id": [f"PROD{i:03d}" for i in range(n_rows)],
        "quantity": [1] * n_rows,
        "unit_price": [50.0] * n_rows,
        "discount_amount": [0.0] * n_rows,
        "tax_amount": [4.0] * n_rows,
        "line_total": [54.0] * n_rows,
    })
    df.to_parquet(path, index=False)
    return n_rows


# ---------------------------------------------------------------------------
# Tests — ingest_csv
# ---------------------------------------------------------------------------

def test_csv_file_loads_successfully():
    import ingest_csv

    f = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    f.close()
    n = _write_orders_csv(f.name)

    engine = _make_sqlite_engine()

    with patch("ingest_csv.get_engine", return_value=engine):
        rows = ingest_csv.ingest_csv(f.name, "orders")

    assert rows == n
    os.unlink(f.name)


def test_csv_row_count_is_correct():
    import ingest_csv

    f = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    f.close()
    expected = 12
    _write_orders_csv(f.name, n_rows=expected)

    engine = _make_sqlite_engine()

    with patch("ingest_csv.get_engine", return_value=engine):
        rows = ingest_csv.ingest_csv(f.name, "orders")

    assert rows == expected
    os.unlink(f.name)


def test_csv_unknown_source_raises_error():
    import ingest_csv

    f = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    f.close()

    try:
        with pytest.raises(ValueError, match="Unknown source_name"):
            ingest_csv.ingest_csv(f.name, "unknown_source")
    finally:
        os.unlink(f.name)


def test_csv_file_not_found_raises_error():
    import ingest_csv

    with pytest.raises(FileNotFoundError):
        ingest_csv.ingest_csv("/tmp/does_not_exist.csv", "orders")


def test_csv_metadata_columns_are_added():
    import ingest_csv

    f = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    f.close()
    _write_orders_csv(f.name)

    engine = _make_sqlite_engine()

    with patch("ingest_csv.get_engine", return_value=engine):
        ingest_csv.ingest_csv(f.name, "orders", ingestion_run_id=42)

    with engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM orders LIMIT 1")).fetchone()
        keys = conn.execute(text("SELECT * FROM orders LIMIT 1")).keys()
        result = dict(zip(keys, row))

    assert "_source_file_name" in result
    assert "_loaded_at" in result
    assert int(result["_ingestion_run_id"]) == 42

    os.unlink(f.name)


# ---------------------------------------------------------------------------
# Tests — ingest_parquet
# ---------------------------------------------------------------------------

def test_parquet_file_loads_successfully():
    import ingest_parquet

    f = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    f.close()
    n = _write_order_items_parquet(f.name)

    engine = _make_sqlite_engine()

    with patch("ingest_parquet.get_engine", return_value=engine):
        rows = ingest_parquet.ingest_parquet(f.name, "order_items")

    assert rows == n
    os.unlink(f.name)


def test_parquet_row_count_is_correct():
    import ingest_parquet

    f = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    f.close()
    expected = 8
    _write_order_items_parquet(f.name, n_rows=expected)

    engine = _make_sqlite_engine()

    with patch("ingest_parquet.get_engine", return_value=engine):
        rows = ingest_parquet.ingest_parquet(f.name, "order_items")

    assert rows == expected
    os.unlink(f.name)


def test_parquet_unknown_source_raises_error():
    import ingest_parquet

    f = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    f.close()

    try:
        with pytest.raises(ValueError, match="Unknown source_name"):
            ingest_parquet.ingest_parquet(f.name, "unknown_source")
    finally:
        os.unlink(f.name)


def test_parquet_file_not_found_raises_error():
    import ingest_parquet

    with pytest.raises(FileNotFoundError):
        ingest_parquet.ingest_parquet("/tmp/does_not_exist.parquet", "order_items")
