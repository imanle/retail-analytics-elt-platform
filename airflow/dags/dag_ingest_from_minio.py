"""
dag_ingest_from_minio.py

DAG 4: MinIO Ingestion

Scans MinIO (S3-compatible) for new retail files, downloads them
to a temp directory, validates schemas, loads into PostgreSQL raw
tables, updates metadata, and archives processed files in MinIO.

Schedule: daily at 06:30 UTC
"""

import os
import shutil
import tempfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# ---------------------------------------------------------------------------
# MinIO / S3 configuration — read from Airflow environment
# ---------------------------------------------------------------------------
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET", "retail-data")

# Prefixes inside the bucket
RAW_PREFIX      = "raw_data"
ARCHIVE_LOADED  = "archive/loaded"
ARCHIVE_FAILED  = "archive/failed"

# Maps S3 folder -> source_name
SOURCE_FOLDERS = {
    "orders":      "orders",
    "order_items": "order_items",
    "customers":   "customers",
    "products":    "products",
    "shipments":   "shipments",
    "payments":    "payments",
}

SOURCE_EXTENSIONS = {
    "orders":      ".csv",
    "order_items": ".parquet",
    "customers":   ".csv",
    "products":    ".csv",
    "shipments":   ".csv",
    "payments":    ".csv",
}

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ---------------------------------------------------------------------------
# Helper — get boto3 S3 client pointed at MinIO
# ---------------------------------------------------------------------------
def _get_s3_client():
    import boto3
    from botocore.client import Config
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _move_in_minio(s3, source_key: str, dest_prefix: str) -> None:
    """Copy an object to archive prefix then delete the original."""
    file_name = os.path.basename(source_key)
    dest_key  = f"{dest_prefix}/{file_name}"

    # Avoid overwrite collision
    try:
        s3.head_object(Bucket=MINIO_BUCKET, Key=dest_key)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        base, ext = os.path.splitext(file_name)
        dest_key = f"{dest_prefix}/{base}_{ts}{ext}"
    except Exception:
        pass  # key does not exist — safe to use as-is

    s3.copy_object(
        Bucket=MINIO_BUCKET,
        CopySource={"Bucket": MINIO_BUCKET, "Key": source_key},
        Key=dest_key,
    )
    s3.delete_object(Bucket=MINIO_BUCKET, Key=source_key)
    print(f"[minio] Archived: {source_key} -> {dest_key}")


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def scan_minio_for_files(**context):
    """
    List all files under raw_data/ in MinIO and push them to XCom.
    """
    s3 = _get_s3_client()
    discovered = []

    for folder, source_name in SOURCE_FOLDERS.items():
        prefix = f"{RAW_PREFIX}/{folder}/"
        expected_ext = SOURCE_EXTENSIONS[source_name]

        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix)

        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                file_name = os.path.basename(key)

                if not file_name or file_name.startswith("."):
                    continue
                if not file_name.endswith(expected_ext):
                    continue

                discovered.append({
                    "source_name": source_name,
                    "file_name":   file_name,
                    "s3_key":      key,
                    "file_size":   obj["Size"],
                })
                print(f"[scan] Found: s3://{MINIO_BUCKET}/{key}")

    print(f"[scan] Total files discovered: {len(discovered)}")
    context["ti"].xcom_push(key="discovered_files", value=discovered)


def filter_unloaded_minio_files(**context):
    """
    Download each discovered file to a temp dir, calculate checksum,
    skip already-loaded files, register new ones in metadata.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/scripts")
    from calculate_checksum import calculate_checksum
    from metadata_writer import is_file_already_loaded, register_source_file

    discovered = context["ti"].xcom_pull(
        key="discovered_files", task_ids="scan_minio_for_files"
    )
    if not discovered:
        print("[filter] No files found in MinIO.")
        context["ti"].xcom_push(key="new_files", value=[])
        return

    s3 = _get_s3_client()
    tmp_dir = tempfile.mkdtemp(prefix="minio_ingest_")
    print(f"[filter] Using temp dir: {tmp_dir}")

    new_files = []
    for file_info in discovered:
        s3_key    = file_info["s3_key"]
        file_name = file_info["file_name"]
        source_name = file_info["source_name"]

        # Download to temp
        local_path = os.path.join(tmp_dir, file_name)
        s3.download_file(MINIO_BUCKET, s3_key, local_path)

        # Checksum deduplication
        try:
            checksum = calculate_checksum(local_path)
        except Exception as e:
            print(f"[filter] Could not checksum {file_name}: {e}")
            os.unlink(local_path)
            continue

        if is_file_already_loaded(checksum):
            print(f"[filter] Skipping (already loaded): {file_name}")
            os.unlink(local_path)
            continue

        # Extract date from filename
        file_date = None
        for part in os.path.splitext(file_name)[0].split("_"):
            try:
                file_date = datetime.strptime(part, "%Y-%m-%d").date()
                break
            except ValueError:
                continue

        ext = os.path.splitext(file_name)[1].lower()
        source_file_id = register_source_file(
            source_name=source_name,
            file_name=file_name,
            file_path=f"s3://{MINIO_BUCKET}/{s3_key}",
            file_type=ext.lstrip("."),
            file_size_bytes=file_info["file_size"],
            file_date=file_date,
            checksum=checksum,
        )

        new_files.append({
            **file_info,
            "local_path":     local_path,
            "checksum":       checksum,
            "source_file_id": source_file_id,
        })
        print(f"[filter] Queued: {file_name}")

    print(f"[filter] Files to load: {len(new_files)}")
    context["ti"].xcom_push(key="new_files", value=new_files)


def validate_and_load_minio_files(**context):
    """
    Validate schema and load each new file into the raw PostgreSQL table.
    Archives files in MinIO after processing.
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

    new_files = context["ti"].xcom_pull(
        key="new_files", task_ids="filter_unloaded_minio_files"
    )
    if not new_files:
        print("[load] No new files to load.")
        return

    s3 = _get_s3_client()
    dag_id  = context["dag"].dag_id
    task_id = context["task"].task_id
    results = []

    for file_info in new_files:
        local_path     = file_info["local_path"]
        file_name      = file_info["file_name"]
        source_name    = file_info["source_name"]
        source_file_id = file_info["source_file_id"]
        s3_key         = file_info["s3_key"]

        print(f"\n[load] Processing: {file_name}")

        # Schema validation
        validation = validate_schema(local_path, source_name)
        if not validation["is_valid"]:
            print(f"[load] FAILED schema validation: {validation['error']}")
            update_source_file_status(
                source_file_id=source_file_id,
                status="failed",
                error_message=validation["error"],
            )
            _move_in_minio(s3, s3_key, ARCHIVE_FAILED)
            if os.path.exists(local_path):
                os.unlink(local_path)
            results.append({"file_name": file_name, "status": "failed"})
            continue

        update_source_file_status(source_file_id=source_file_id, status="validated")

        # Start ingestion run
        ingestion_run_id = start_ingestion_run(
            source_name=source_name,
            file_name=file_name,
            dag_id=dag_id,
            task_id=task_id,
        )

        # Load file
        try:
            ext = os.path.splitext(file_name)[1].lower()
            if ext == ".csv":
                rows = ingest_csv(local_path, source_name, ingestion_run_id)
            elif ext == ".parquet":
                rows = ingest_parquet(local_path, source_name, ingestion_run_id)
            else:
                raise ValueError(f"Unsupported file type: {ext}")

            finish_ingestion_run(ingestion_run_id, status="success", rows_loaded=rows)
            update_source_file_status(source_file_id=source_file_id, status="loaded")
            _move_in_minio(s3, s3_key, ARCHIVE_LOADED)
            results.append({"file_name": file_name, "status": "success", "rows": rows})
            print(f"[load] SUCCESS: {file_name} ({rows:,} rows)")

        except Exception as e:
            error_msg = str(e)
            print(f"[load] FAILED: {file_name}: {error_msg}")
            finish_ingestion_run(ingestion_run_id, status="failed", error_message=error_msg)
            update_source_file_status(
                source_file_id=source_file_id,
                status="failed",
                error_message=error_msg,
            )
            _move_in_minio(s3, s3_key, ARCHIVE_FAILED)
            results.append({"file_name": file_name, "status": "failed"})

        finally:
            if os.path.exists(local_path):
                os.unlink(local_path)

    # Cleanup temp dir
    tmp_dir = os.path.dirname(new_files[0]["local_path"])
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir, ignore_errors=True)

    success = sum(1 for r in results if r["status"] == "success")
    failed  = sum(1 for r in results if r["status"] == "failed")
    print(f"\n[load] Done. Success: {success} | Failed: {failed}")

    if failed > 0 and success == 0:
        raise Exception(f"All {failed} file(s) failed to load.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dag_ingest_from_minio",
    description="Scan MinIO for new retail files and load into PostgreSQL",
    start_date=datetime(2026, 5, 1),
    schedule_interval="30 6 * * *",
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "minio", "retail"],
) as dag:

    start = EmptyOperator(task_id="start")

    scan = PythonOperator(
        task_id="scan_minio_for_files",
        python_callable=scan_minio_for_files,
    )

    filter_files = PythonOperator(
        task_id="filter_unloaded_minio_files",
        python_callable=filter_unloaded_minio_files,
    )

    load_files = PythonOperator(
        task_id="validate_and_load_minio_files",
        python_callable=validate_and_load_minio_files,
    )

    end = EmptyOperator(task_id="end")

    start >> scan >> filter_files >> load_files >> end
