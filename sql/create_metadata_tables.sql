-- =============================================================
-- create_metadata_tables.sql
-- Creates pipeline metadata and quality tracking tables
-- =============================================================

-- -------------------------------------------------------------
-- metadata.source_files
-- Tracks every source file discovered by the pipeline
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata.source_files (
    source_file_id      SERIAL          PRIMARY KEY,
    source_name         VARCHAR(100)    NOT NULL,
    file_name           VARCHAR(255)    NOT NULL,
    file_path           TEXT            NOT NULL,
    file_type           VARCHAR(20)     NOT NULL,
    file_size_bytes     BIGINT,
    file_date           DATE,
    checksum            VARCHAR(64),
    discovered_at       TIMESTAMP       NOT NULL DEFAULT NOW(),
    loaded_at           TIMESTAMP,
    status              VARCHAR(20)     NOT NULL DEFAULT 'discovered',
    error_message       TEXT,

    CONSTRAINT chk_source_files_status
        CHECK (status IN ('discovered', 'validated', 'loaded', 'failed', 'skipped'))
);

CREATE INDEX IF NOT EXISTS idx_source_files_checksum
    ON metadata.source_files (checksum);

CREATE INDEX IF NOT EXISTS idx_source_files_status
    ON metadata.source_files (status);

CREATE INDEX IF NOT EXISTS idx_source_files_source_name
    ON metadata.source_files (source_name);

-- -------------------------------------------------------------
-- metadata.ingestion_runs
-- Tracks each individual file ingestion run
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata.ingestion_runs (
    ingestion_run_id    SERIAL          PRIMARY KEY,
    dag_id              VARCHAR(255),
    task_id             VARCHAR(255),
    source_name         VARCHAR(100)    NOT NULL,
    file_name           VARCHAR(255)    NOT NULL,
    started_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMP,
    rows_loaded         INTEGER,
    status              VARCHAR(20)     NOT NULL DEFAULT 'running',
    error_message       TEXT,

    CONSTRAINT chk_ingestion_runs_status
        CHECK (status IN ('running', 'success', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_ingestion_runs_source_name
    ON metadata.ingestion_runs (source_name);

CREATE INDEX IF NOT EXISTS idx_ingestion_runs_started_at
    ON metadata.ingestion_runs (started_at);

-- -------------------------------------------------------------
-- metadata.pipeline_runs
-- Tracks each full DAG run
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    pipeline_run_id     SERIAL          PRIMARY KEY,
    dag_id              VARCHAR(255)    NOT NULL,
    run_id              VARCHAR(255),
    started_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMP,
    status              VARCHAR(20)     NOT NULL DEFAULT 'running',
    files_discovered    INTEGER         DEFAULT 0,
    files_loaded        INTEGER         DEFAULT 0,
    files_failed        INTEGER         DEFAULT 0,
    files_skipped       INTEGER         DEFAULT 0,
    error_message       TEXT,

    CONSTRAINT chk_pipeline_runs_status
        CHECK (status IN ('running', 'success', 'failed', 'partial'))
);

-- -------------------------------------------------------------
-- quality.data_quality_results
-- Stores results of post-ingestion quality checks
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quality.data_quality_results (
    quality_check_id    SERIAL          PRIMARY KEY,
    check_name          VARCHAR(255)    NOT NULL,
    table_name          VARCHAR(255)    NOT NULL,
    column_name         VARCHAR(255),
    check_type          VARCHAR(100)    NOT NULL,
    check_status        VARCHAR(20)     NOT NULL,
    failed_row_count    INTEGER         DEFAULT 0,
    checked_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    details             TEXT,

    CONSTRAINT chk_quality_status
        CHECK (check_status IN ('passed', 'failed', 'warning'))
);

CREATE INDEX IF NOT EXISTS idx_quality_results_table_name
    ON quality.data_quality_results (table_name);

CREATE INDEX IF NOT EXISTS idx_quality_results_checked_at
    ON quality.data_quality_results (checked_at);

CREATE INDEX IF NOT EXISTS idx_quality_results_status
    ON quality.data_quality_results (check_status);

-- -------------------------------------------------------------
-- quality.table_freshness
-- Tracks when each table was last updated
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quality.table_freshness (
    freshness_id        SERIAL          PRIMARY KEY,
    schema_name         VARCHAR(100)    NOT NULL,
    table_name          VARCHAR(255)    NOT NULL,
    row_count           BIGINT,
    max_loaded_at       TIMESTAMP,
    checked_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    is_fresh            BOOLEAN         DEFAULT TRUE,
    staleness_hours     NUMERIC(10, 2)
);

-- -------------------------------------------------------------
-- quality.row_count_checks
-- Tracks row count history per table per day
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quality.row_count_checks (
    row_count_check_id  SERIAL          PRIMARY KEY,
    table_name          VARCHAR(255)    NOT NULL,
    check_date          DATE            NOT NULL DEFAULT CURRENT_DATE,
    expected_min_rows   INTEGER,
    actual_row_count    BIGINT,
    check_status        VARCHAR(20)     NOT NULL,
    checked_at          TIMESTAMP       NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_row_count_status
        CHECK (check_status IN ('passed', 'failed', 'warning'))
);
