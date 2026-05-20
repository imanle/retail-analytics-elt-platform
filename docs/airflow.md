# Airflow DAGs

## Overview

The platform uses three Airflow DAGs that run in sequence each day.

| DAG | Schedule | Depends On |
|---|---|---|
| `dag_ingest_retail_files` | 06:00 UTC | — |
| `dag_run_dbt_transformations` | 07:00 UTC | Ingestion complete |
| `dag_data_quality_checks` | 08:00 UTC | dbt complete |

---

## DAG 1 — dag_ingest_retail_files

**Purpose:** Detect new files, validate them, load into PostgreSQL, update metadata.

**Task flow:**
```
start
  └── scan_raw_data_folders
        └── filter_unloaded_files
              └── validate_and_load_files
                    └── end
```

**Task descriptions:**

`scan_raw_data_folders`
Scans all subfolders under `raw_data/` and pushes a list of discovered file paths to XCom.

`filter_unloaded_files`
Calculates the SHA-256 checksum of each file and checks `metadata.source_files`. Files with a matching checksum and status `loaded` are skipped. New files are registered in metadata with status `discovered` and passed forward via XCom.

`validate_and_load_files`
For each new file:
1. Validates that required columns are present
2. Starts an ingestion run record
3. Loads the file into the correct `raw.*` table
4. Updates metadata to `loaded`
5. Moves the file to `archive/loaded/`

If schema validation fails, the file is moved to `archive/failed/` and marked as `failed` in metadata. Other files continue processing.

---

## DAG 2 — dag_run_dbt_transformations

**Purpose:** Run dbt staging and mart models after raw data is loaded.

**Task flow:**
```
start
  └── dbt_debug
        └── dbt_deps
              └── dbt_run_staging
                    └── dbt_run_marts
                          └── dbt_test
                                └── end
```

---

## DAG 3 — dag_data_quality_checks

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

To trigger a DAG manually:
1. Find the DAG in the list
2. Toggle it on if it is paused
3. Click the play button → **Trigger DAG**

To view logs:
1. Click on a DAG run
2. Click on a task
3. Click **Logs**
