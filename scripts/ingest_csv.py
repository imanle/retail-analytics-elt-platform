"""
ingest_csv.py

Loads a CSV file into a raw PostgreSQL table.
Adds ingestion metadata columns to every row.
"""

import os
from datetime import datetime

import pandas as pd

from db import get_engine


# Maps source_name -> raw schema table name
TABLE_MAP = {
    "orders": "raw.orders",
    "customers": "raw.customers",
    "products": "raw.products",
    "shipments": "raw.shipments",
    "payments": "raw.payments",
}


def ingest_csv(
    file_path: str,
    source_name: str,
    ingestion_run_id: int = None,
) -> int:
    """
    Args:
        file_path:        Path to the CSV file.
        source_name:      Source type key (e.g. 'orders').
        ingestion_run_id: ID from metadata.ingestion_runs (optional).

    Returns:
        Number of rows loaded.

    Raises:
        ValueError: If source_name is not recognised.
        FileNotFoundError: If the file does not exist.
    """
    if source_name not in TABLE_MAP:
        raise ValueError(
            f"Unknown source_name '{source_name}'. Known: {list(TABLE_MAP.keys())}"
        )

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    table = TABLE_MAP[source_name]
    schema, table_name = table.split(".")

    print(f"[ingest_csv] Loading '{os.path.basename(file_path)}' -> {table}")

    df = pd.read_csv(file_path, dtype=str)  # read everything as str for raw layer

    # Add ingestion metadata columns
    df["_source_file_name"] = os.path.basename(file_path)
    df["_source_file_path"] = os.path.abspath(file_path)
    df["_loaded_at"] = datetime.utcnow().isoformat()
    df["_ingestion_run_id"] = ingestion_run_id

    engine = get_engine()
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    rows_loaded = len(df)
    print(f"[ingest_csv] Loaded {rows_loaded:,} rows into {table}")
    return rows_loaded


if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv
    load_dotenv()

    if len(sys.argv) < 3:
        print("Usage: python3 ingest_csv.py <file_path> <source_name>")
        sys.exit(1)

    rows = ingest_csv(
        file_path=sys.argv[1],
        source_name=sys.argv[2],
    )
    print(f"Done. {rows:,} rows loaded.")
