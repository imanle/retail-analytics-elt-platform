"""
test_checksum.py

Unit tests for calculate_checksum.py
"""

import os
import tempfile
import pytest
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from calculate_checksum import calculate_checksum


def _write_temp_file(content: bytes) -> str:
    """Write bytes to a temp file and return its path."""
    f = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    f.write(content)
    f.close()
    return f.name


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_same_file_returns_same_checksum():
    path = _write_temp_file(b"order_id,customer_id\nORD001,CUST001\n")
    try:
        assert calculate_checksum(path) == calculate_checksum(path)
    finally:
        os.unlink(path)


def test_different_files_return_different_checksums():
    path1 = _write_temp_file(b"order_id,customer_id\nORD001,CUST001\n")
    path2 = _write_temp_file(b"order_id,customer_id\nORD002,CUST002\n")
    try:
        assert calculate_checksum(path1) != calculate_checksum(path2)
    finally:
        os.unlink(path1)
        os.unlink(path2)


def test_checksum_is_64_char_hex_string():
    path = _write_temp_file(b"some,data\n1,2\n")
    try:
        result = calculate_checksum(path)
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)
    finally:
        os.unlink(path)


def test_file_not_found_raises_error():
    with pytest.raises(FileNotFoundError):
        calculate_checksum("/tmp/this_file_does_not_exist_xyz.csv")


def test_empty_file_raises_error():
    path = _write_temp_file(b"")
    try:
        with pytest.raises(ValueError, match="empty"):
            calculate_checksum(path)
    finally:
        os.unlink(path)


def test_modifying_file_changes_checksum():
    path = _write_temp_file(b"order_id\nORD001\n")
    try:
        checksum_before = calculate_checksum(path)
        with open(path, "ab") as f:
            f.write(b"ORD002\n")
        checksum_after = calculate_checksum(path)
        assert checksum_before != checksum_after
    finally:
        os.unlink(path)
