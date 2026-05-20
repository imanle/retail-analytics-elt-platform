"""
test_schema_validation.py

Unit tests for validate_schema.py
"""

import os
import tempfile
import pytest
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from validate_schema import validate_schema, REQUIRED_COLUMNS


def _write_csv(columns: list, rows: list = None) -> str:
    """Write a temp CSV with the given columns and optional rows."""
    df = pd.DataFrame(rows or [["val"] * len(columns)], columns=columns)
    f = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    df.to_csv(f.name, index=False)
    f.close()
    return f.name


def _write_parquet(columns: list) -> str:
    """Write a temp Parquet with the given columns."""
    df = pd.DataFrame([["val"] * len(columns)], columns=columns)
    f = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    df.to_parquet(f.name, index=False)
    f.close()
    return f.name


# ---------------------------------------------------------------------------
# Tests — orders CSV
# ---------------------------------------------------------------------------

def test_valid_orders_file_passes():
    path = _write_csv(REQUIRED_COLUMNS["orders"])
    try:
        result = validate_schema(path, "orders")
        assert result["is_valid"] is True
        assert result["missing_columns"] == []
        assert result["error"] is None
    finally:
        os.unlink(path)


def test_orders_file_missing_order_id_fails():
    cols = [c for c in REQUIRED_COLUMNS["orders"] if c != "order_id"]
    path = _write_csv(cols)
    try:
        result = validate_schema(path, "orders")
        assert result["is_valid"] is False
        assert "order_id" in result["missing_columns"]
    finally:
        os.unlink(path)


def test_extra_columns_do_not_break_validation():
    cols = REQUIRED_COLUMNS["orders"] + ["extra_col_1", "extra_col_2"]
    path = _write_csv(cols)
    try:
        result = validate_schema(path, "orders")
        assert result["is_valid"] is True
    finally:
        os.unlink(path)


# ---------------------------------------------------------------------------
# Tests — order_items Parquet
# ---------------------------------------------------------------------------

def test_valid_order_items_parquet_passes():
    path = _write_parquet(REQUIRED_COLUMNS["order_items"])
    try:
        result = validate_schema(path, "order_items")
        assert result["is_valid"] is True
    finally:
        os.unlink(path)


def test_order_items_missing_product_id_fails():
    cols = [c for c in REQUIRED_COLUMNS["order_items"] if c != "product_id"]
    path = _write_parquet(cols)
    try:
        result = validate_schema(path, "order_items")
        assert result["is_valid"] is False
        assert "product_id" in result["missing_columns"]
    finally:
        os.unlink(path)


# ---------------------------------------------------------------------------
# Tests — error cases
# ---------------------------------------------------------------------------

def test_unsupported_file_type_fails():
    f = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    f.write(b"{}")
    f.close()
    try:
        result = validate_schema(f.name, "orders")
        assert result["is_valid"] is False
        assert "Unsupported" in result["error"]
    finally:
        os.unlink(f.name)


def test_unknown_source_name_fails():
    path = _write_csv(["col1", "col2"])
    try:
        result = validate_schema(path, "unknown_source")
        assert result["is_valid"] is False
        assert "Unknown source" in result["error"]
    finally:
        os.unlink(path)


def test_file_not_found_fails():
    result = validate_schema("/tmp/nonexistent_file.csv", "orders")
    assert result["is_valid"] is False
    assert "not found" in result["error"]


def test_empty_file_fails():
    f = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    f.close()
    try:
        result = validate_schema(f.name, "orders")
        assert result["is_valid"] is False
    finally:
        os.unlink(f.name)


# ---------------------------------------------------------------------------
# Tests — all source types have required columns defined
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("source_name", list(REQUIRED_COLUMNS.keys()))
def test_all_sources_have_required_columns_defined(source_name):
    assert len(REQUIRED_COLUMNS[source_name]) > 0
