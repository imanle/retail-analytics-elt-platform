# Airflow DAGs

## Overview

The platform uses four Airflow DAGs. The MinIO ingestion DAG is the primary ingestion path.

| DAG | Schedule | Purpose |
|---|---|---|
| `dag_ingest_from_minio` | Daily 06:30 UTC | Scan MinIO, validate, load into raw tables |
| `dag_ingest_retail_files` | Daily 06:00 UTC | Alternative: ingest from local raw_data/ folders |
| `dag_run_dbt_transformations` | Daily 07:00 UTC | Run dbt staging and mart models |
| `dag_data_quality_checks` | Daily 08:00 UTC | Run quality checks and write results |

---

## DAG 1 — dag_ingest_from_minio

**Purpose:** Scan MinIO for new retail files, download them, validate, load into PostgreSQL, archive in MinIO.

**Task flow:**
```
start
  └── scan_minio_for_files
        └── filter_unloaded_minio_files
              └── validate_and_load_minio_files
                    └── end
```

**Task descriptions:**

`scan_minio_for_files`
Lists all files under `raw_data/` in the MinIO bucket and pushes them to XCom.

`filter_unloaded_minio_files`
Downloads each file to a temp directory, calculates SHA-256 checksum, checks `metadata.source_files`. Files already loaded are skipped. New files are registered in metadata with status `discovered`.

`validate_and_load_minio_files`
For each new file:
1. Validates required columns are present
2. Starts an ingestion run record
3. Loads the file into the correct `raw.*` table
4. Updates metadata to `loaded`
5. Moves the file in MinIO to `archive/loaded/`

Failed files are moved to `archive/failed/` and marked as `failed` in metadata.

---

## DAG 2 — dag_ingest_retail_files

**Purpose:** Alternative ingestion from local `raw_data/` folders. Useful for testing without MinIO.

**Task flow:**
```
start
  └── scan_raw_data_folders
        └── filter_unloaded_files
              └── validate_and_load_files
                    └── end
```

---

## DAG 3 — dag_run_dbt_transformations

**Purpose:** Run dbt staging and mart models after raw data is loaded.

**Task flow:**
```
start
  └── dbt_run_staging
        └── dbt_run_marts
              └── dbt_test
                    └── end
```

**Task descriptions:**

`dbt_run_staging`
Runs `dbt run --select staging` — builds all 6 staging views on top of raw tables.

`dbt_run_marts`
Runs `dbt run --select marts` — builds all 7 mart tables on top of staging views.

`dbt_test`
Runs `dbt test` — executes all 42 schema tests across staging and mart models.

---

## DAG 4 — dag_data_quality_checks

**Purpose:** Run post-ingestion quality checks and write results to the quality schema.

**Task flow:**
```
start
  ├── check_raw_row_counts
  └── check_null_primary_keys
        └── check_duplicate_orders
              └── check_negative_amounts
                    └── check_invalid_dates
                          └── check_table_freshness
                                └── end
```

**Checks performed:**

| Check | Table | Description |
|---|---|---|
| Raw row counts | All raw tables | Each table must have at least one row |
| Null primary keys | All raw tables | PKs must not be null |
| Duplicate orders | marts.fact_orders | order_id must be unique |
| Negative amounts | marts.fact_orders, fact_order_items | Amounts must not be negative |
| Invalid dates | staging.stg_shipments | delivered_at must be after shipped_at |
| Future order dates | staging.stg_orders | order_date must not be in the future |
| Table freshness | Key tables | Tables must have been loaded within 48 hours |

All results are written to `quality.data_quality_results` and visible in the Pipeline Health dashboard.

---

## Running DAGs Manually

Open Airflow at http://localhost:8080 (admin / admin).

Recommended trigger order:
1. `dag_ingest_from_minio` — load raw data from MinIO
2. `dag_run_dbt_transformations` — transform raw into staging and marts
3. `dag_data_quality_checks` — validate the loaded and transformed data

To trigger a DAG manually:
1. Find the DAG in the list
2. Toggle it on if it is paused
3. Click the play button → **Trigger DAG**

To view logs:
1. Click on a DAG run
2. Click on a task
3. Click **Logs**