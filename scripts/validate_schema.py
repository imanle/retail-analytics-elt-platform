"""
validate_schema.py

Validates that a source file contains the required columns
before it is loaded into PostgreSQL.
"""

import os
import pandas as pd


# Expected columns for each source type
REQUIRED_COLUMNS = {
    "orders": [
        "order_id",
        "customer_id",
        "order_date",
        "order_status",
        "currency",
        "total_amount",
        "created_at",
        "updated_at",
    ],
    "order_items": [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
        "tax_amount",
        "line_total",
    ],
    "customers": [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "country",
        "city",
        "created_at",
    ],
    "products": [
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "cost_price",
        "sale_price",
        "is_active",
    ],
    "shipments": [
        "shipment_id",
        "order_id",
        "carrier",
        "shipping_method",
        "shipping_cost",
        "shipped_at",
        "shipment_status",
    ],
    "payments": [
        "payment_id",
        "order_id",
        "payment_method",
        "payment_status",
        "payment_amount",
        "paid_at",
    ],
}

SUPPORTED_EXTENSIONS = {".csv", ".parquet"}


def _read_file_columns(file_path: str) -> list:
    """
    Reads the column headers from a CSV or Parquet file.
    """
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        df = pd.read_csv(file_path, nrows=0)
    elif ext == ".parquet":
        import pyarrow.parquet as pq

        schema = pq.read_schema(file_path)
        return schema.names
    else:
        raise ValueError(f"Unsupported file type: {ext}")

    return list(df.columns)


def validate_schema(file_path: str, source_name: str) -> dict:
    """
    Validate that a file contains the required columns for its source type.

    Args:
        file_path:   Path to the file.
        source_name: Source type key (e.g. 'orders', 'customers').

    Returns:
        dict with keys:
            is_valid (bool)
            missing_columns (list)
            file_columns (list)
            error (str or None)
    """
    result = {
        "is_valid": False,
        "missing_columns": [],
        "file_columns": [],
        "error": None,
    }

    # Check file exists
    if not os.path.exists(file_path):
        result["error"] = f"File not found: {file_path}"
        return result

    # Check file type
    ext = os.path.splitext(file_path)[1].lower()
    if ext not in SUPPORTED_EXTENSIONS:
        result["error"] = (
            f"Unsupported file type '{ext}'. Supported: {SUPPORTED_EXTENSIONS}"
        )
        return result

    # Check source_name is known
    if source_name not in REQUIRED_COLUMNS:
        result["error"] = (
            f"Unknown source name '{source_name}'. Known: {list(REQUIRED_COLUMNS.keys())}"
        )
        return result

    # Check file is not empty
    if os.path.getsize(file_path) == 0:
        result["error"] = f"File is empty: {file_path}"
        return result

    # Read columns
    try:
        file_columns = _read_file_columns(file_path)
    except Exception as e:
        result["error"] = f"Could not read file columns: {e}"
        return result

    result["file_columns"] = file_columns

    # Check required columns
    required = REQUIRED_COLUMNS[source_name]
    missing = [col for col in required if col not in file_columns]
    result["missing_columns"] = missing

    if missing:
        result["error"] = f"Missing required columns: {missing}"
    else:
        result["is_valid"] = True

    return result


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python3 validate_schema.py <file_path> <source_name>")
        print(f"Known sources: {list(REQUIRED_COLUMNS.keys())}")
        sys.exit(1)

    path = sys.argv[1]
    source = sys.argv[2]
    result = validate_schema(path, source)

    if result["is_valid"]:
        print(f"[OK] Schema valid for source '{source}'")
        print(f"     Columns: {result['file_columns']}")
    else:
        print(f"[FAIL] Schema invalid for source '{source}'")
        print(f"       Error: {result['error']}")
        sys.exit(1)
