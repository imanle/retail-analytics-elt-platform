"""
metadata_writer.py

Writes and updates records in the metadata schema tables.
Used by ingestion scripts to track file discovery, validation,
loading status, and ingestion run history.
"""

from datetime import datetime

from sqlalchemy import text

from db import get_engine


# ---------------------------------------------------------------------------
# metadata.source_files
# ---------------------------------------------------------------------------

def register_source_file(
    source_name: str,
    file_name: str,
    file_path: str,
    file_type: str,
    file_size_bytes: int,
    file_date,
    checksum: str,
) -> int:
    """
    Insert a new record into metadata.source_files with status 'discovered'.

    Returns:
        The new source_file_id.
    """
    engine = get_engine()
    sql = text("""
        INSERT INTO metadata.source_files (
            source_name, file_name, file_path, file_type,
            file_size_bytes, file_date, checksum,
            discovered_at, status
        ) VALUES (
            :source_name, :file_name, :file_path, :file_type,
            :file_size_bytes, :file_date, :checksum,
            NOW(), 'discovered'
        )
        RETURNING source_file_id
    """)

    with engine.begin() as conn:
        row = conn.execute(sql, {
            "source_name": source_name,
            "file_name": file_name,
            "file_path": file_path,
            "file_type": file_type,
            "file_size_bytes": file_size_bytes,
            "file_date": file_date,
            "checksum": checksum,
        }).fetchone()

    source_file_id = row[0]
    print(f"[metadata] Registered source file id={source_file_id} -> {file_name}")
    return source_file_id


def update_source_file_status(
    source_file_id: int,
    status: str,
    error_message: str = None,
) -> None:
    """
    Update the status of a source file record.

    Valid statuses: discovered | validated | loaded | failed | skipped
    """
    engine = get_engine()
    loaded_at = datetime.utcnow() if status == "loaded" else None

    sql = text("""
        UPDATE metadata.source_files
        SET status        = :status,
            error_message = :error_message,
            loaded_at     = COALESCE(:loaded_at, loaded_at)
        WHERE source_file_id = :source_file_id
    """)

    with engine.begin() as conn:
        conn.execute(sql, {
            "status": status,
            "error_message": error_message,
            "loaded_at": loaded_at,
            "source_file_id": source_file_id,
        })

    print(f"[metadata] source_file_id={source_file_id} status -> '{status}'")


def is_file_already_loaded(checksum: str) -> bool:
    """
    Return True if a file with this checksum was already successfully loaded.
    Used for deduplication.
    """
    engine = get_engine()
    sql = text("""
        SELECT COUNT(*)
        FROM metadata.source_files
        WHERE checksum = :checksum
          AND status = 'loaded'
    """)

    with engine.connect() as conn:
        count = conn.execute(sql, {"checksum": checksum}).scalar()

    return count > 0


# ---------------------------------------------------------------------------
# metadata.ingestion_runs
# ---------------------------------------------------------------------------

def start_ingestion_run(
    source_name: str,
    file_name: str,
    dag_id: str = None,
    task_id: str = None,
) -> int:
    """
    Insert a new ingestion run record with status 'running'.

    Returns:
        The new ingestion_run_id.
    """
    engine = get_engine()
    sql = text("""
        INSERT INTO metadata.ingestion_runs (
            dag_id, task_id, source_name, file_name,
            started_at, status
        ) VALUES (
            :dag_id, :task_id, :source_name, :file_name,
            NOW(), 'running'
        )
        RETURNING ingestion_run_id
    """)

    with engine.begin() as conn:
        row = conn.execute(sql, {
            "dag_id": dag_id,
            "task_id": task_id,
            "source_name": source_name,
            "file_name": file_name,
        }).fetchone()

    ingestion_run_id = row[0]
    print(f"[metadata] Started ingestion run id={ingestion_run_id} for '{file_name}'")
    return ingestion_run_id


def finish_ingestion_run(
    ingestion_run_id: int,
    status: str,
    rows_loaded: int = 0,
    error_message: str = None,
) -> None:
    """
    Mark an ingestion run as finished.

    Valid statuses: success | failed
    """
    engine = get_engine()
    sql = text("""
        UPDATE metadata.ingestion_runs
        SET finished_at   = NOW(),
            status        = :status,
            rows_loaded   = :rows_loaded,
            error_message = :error_message
        WHERE ingestion_run_id = :ingestion_run_id
    """)

    with engine.begin() as conn:
        conn.execute(sql, {
            "status": status,
            "rows_loaded": rows_loaded,
            "error_message": error_message,
            "ingestion_run_id": ingestion_run_id,
        })

    print(f"[metadata] Finished ingestion run id={ingestion_run_id} status='{status}' rows={rows_loaded}")
