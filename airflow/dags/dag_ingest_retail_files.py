"""
dag_ingest_retail_files.py

DAG 1: File Ingestion

Scans raw_data folders, validates new files, loads them into
PostgreSQL raw tables, updates metadata, and archives loaded files.

Schedule: daily at 06:00 UTC
"""

import os
import shutil
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# ---------------------------------------------------------------------------
# Paths — these match the volumes mounted in docker-compose.yml
# ---------------------------------------------------------------------------
RAW_DATA_DIR = "/opt/airflow/raw_data"
ARCHIVE_LOADED_DIR = "/opt/airflow/archive/loaded"
ARCHIVE_FAILED_DIR = "/opt/airflow/archive/failed"

# Maps folder name -> source_name used by ingestion scripts
SOURCE_FOLDERS = {
    "orders": "orders",
    "order_items": "order_items",
    "customers": "customers",
    "products": "products",
    "shipments": "shipments",
    "payments": "payments",
}

# Maps source_name -> file extension
SOURCE_EXTENSIONS = {
    "orders": ".csv",
    "order_items": ".parquet",
    "customers": ".csv",
    "products": ".csv",
    "shipments": ".csv",
    "payments": ".csv",
}

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def scan_raw_data_folders(**context):
    """
    Scan all raw_data subfolders and push discovered file paths to XCom.
    """
    discovered = []

    for folder, source_name in SOURCE_FOLDERS.items():
        folder_path = os.path.join(RAW_DATA_DIR, folder)
        if not os.path.exists(folder_path):
            print(f"[scan] Folder not found, skipping: {folder_path}")
            continue

        expected_ext = SOURCE_EXTENSIONS[source_name]
        for file_name in sorted(os.listdir(folder_path)):
            if file_name.startswith("."):
                continue
            if not file_name.endswith(expected_ext):
                continue
            file_path = os.path.join(folder_path, file_name)
            discovered.append({
                "source_name": source_name,
                "file_name": file_name,
                "file_path": file_path,
            })
            print(f"[scan] Discovered: {file_path}")

    print(f"[scan] Total files discovered: {len(discovered)}")
    context["ti"].xcom_push(key="discovered_files", value=discovered)


def filter_unloaded_files(**context):
    """
    Filter out files that have already been loaded (by checksum).
    Pushes only new files to XCom.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/scripts")
    from calculate_checksum import calculate_checksum
    from metadata_writer import is_file_already_loaded, register_source_file

    discovered = context["ti"].xcom_pull(key="discovered_files", task_ids="scan_raw_data_folders")
    if not discovered:
        print("[filter] No files to process.")
        context["ti"].xcom_push(key="new_files", value=[])
        return

    new_files = []
    for file_info in discovered:
        file_path = file_info["file_path"]
        source_name = file_info["source_name"]
        file_name = file_info["file_name"]

        try:
            checksum = calculate_checksum(file_path)
        except Exception as e:
            print(f"[filter] Could not checksum {file_name}: {e}")
            continue

        if is_file_already_loaded(checksum):
            print(f"[filter] Skipping (already loaded): {file_name}")
            continue

        # Register in metadata as discovered
        file_size = os.path.getsize(file_path)
        ext = os.path.splitext(file_name)[1].lower()

        # Extract date from filename (e.g. orders_2026-05-01.csv -> 2026-05-01)
        file_date = None
        parts = os.path.splitext(file_name)[0].split("_")
        for part in parts:
            try:
                file_date = datetime.strptime(part, "%Y-%m-%d").date()
                break
            except ValueError:
                continue

        source_file_id = register_source_file(
            source_name=source_name,
            file_name=file_name,
            file_path=file_path,
            file_type=ext.lstrip("."),
            file_size_bytes=file_size,
            file_date=file_date,
            checksum=checksum,
        )

        new_files.append({
            **file_info,
            "checksum": checksum,
            "source_file_id": source_file_id,
        })
        print(f"[filter] New file queued: {file_name}")

    print(f"[filter] Files to load: {len(new_files)}")
    context["ti"].xcom_push(key="new_files", value=new_files)


def validate_and_load_files(**context):
    """
    Validate schema and load each new file into the raw PostgreSQL table.
    Updates metadata after each file. Moves files to archive when done.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/scripts")
    from validate_schema import validate_schema
    from ingest_csv import ingest_csv
    from ingest_parquet import ingest_parquet
    from metadata_writer import (
        update_source_file_status,
        start_ingestion_run,
        finish_ingestion_run,
    )

    new_files = context["ti"].xcom_pull(key="new_files", task_ids="filter_unloaded_files")
    if not new_files:
        print("[load] No new files to load.")
        return

    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id

    os.makedirs(ARCHIVE_LOADED_DIR, exist_ok=True)
    os.makedirs(ARCHIVE_FAILED_DIR, exist_ok=True)

    results = []
    for file_info in new_files:
        file_path = file_info["file_path"]
        file_name = file_info["file_name"]
        source_name = file_info["source_name"]
        source_file_id = file_info["source_file_id"]

        print(f"\n[load] Processing: {file_name}")

        # --- Schema validation ---
        validation = validate_schema(file_path, source_name)
        if not validation["is_valid"]:
            print(f"[load] FAILED schema validation: {validation['error']}")
            update_source_file_status(
                source_file_id=source_file_id,
                status="failed",
                error_message=validation["error"],
            )
            _move_to_archive(file_path, ARCHIVE_FAILED_DIR)
            results.append({"file_name": file_name, "status": "failed"})
            continue

        update_source_file_status(source_file_id=source_file_id, status="validated")

        # --- Ingestion run tracking ---
        ingestion_run_id = start_ingestion_run(
            source_name=source_name,
            file_name=file_name,
            dag_id=dag_id,
            task_id=task_id,
        )

        # --- Load file ---
        try:
            ext = os.path.splitext(file_name)[1].lower()
            if ext == ".csv":
                rows = ingest_csv(file_path, source_name, ingestion_run_id)
            elif ext == ".parquet":
                rows = ingest_parquet(file_path, source_name, ingestion_run_id)
            else:
                raise ValueError(f"Unsupported file type: {ext}")

            finish_ingestion_run(ingestion_run_id, status="success", rows_loaded=rows)
            update_source_file_status(source_file_id=source_file_id, status="loaded")
            _move_to_archive(file_path, ARCHIVE_LOADED_DIR)
            results.append({"file_name": file_name, "status": "success", "rows": rows})
            print(f"[load] SUCCESS: {file_name} ({rows:,} rows)")

        except Exception as e:
            error_msg = str(e)
            print(f"[load] FAILED loading {file_name}: {error_msg}")
            finish_ingestion_run(ingestion_run_id, status="failed", error_message=error_msg)
            update_source_file_status(
                source_file_id=source_file_id,
                status="failed",
                error_message=error_msg,
            )
            _move_to_archive(file_path, ARCHIVE_FAILED_DIR)
            results.append({"file_name": file_name, "status": "failed"})

    # Summary
    success = sum(1 for r in results if r["status"] == "success")
    failed = sum(1 for r in results if r["status"] == "failed")
    print(f"\n[load] Done. Success: {success} | Failed: {failed}")

    if failed > 0 and success == 0:
        raise Exception(f"All {failed} file(s) failed to load. Check logs above.")


def _move_to_archive(file_path: str, archive_dir: str) -> None:
    """Move a file to the given archive directory."""
    os.makedirs(archive_dir, exist_ok=True)
    dest = os.path.join(archive_dir, os.path.basename(file_path))
    if os.path.exists(dest):
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        base, ext = os.path.splitext(os.path.basename(file_path))
        dest = os.path.join(archive_dir, f"{base}_{ts}{ext}")
    shutil.move(file_path, dest)
    print(f"[archive] Moved to: {dest}")

with DAG(
    dag_id="dag_ingest_retail_files",
    description="Scan, validate, and load raw retail files into PostgreSQL",
    start_date=datetime(2026, 5, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "retail"],
) as dag:

    start = EmptyOperator(task_id="start")

    scan = PythonOperator(
        task_id="scan_raw_data_folders",
        python_callable=scan_raw_data_folders,
    )

    filter_files = PythonOperator(
        task_id="filter_unloaded_files",
        python_callable=filter_unloaded_files,
    )

    load_files = PythonOperator(
        task_id="validate_and_load_files",
        python_callable=validate_and_load_files,
    )

    end = EmptyOperator(task_id="end")

    start >> scan >> filter_files >> load_files >> end
