# Architecture

## High-Level Overview

The platform follows a standard ELT pattern — Extract, Load, Transform.

```
raw_data/ (local)
  orders/          (.csv, daily)
  order_items/     (.parquet, daily)
  customers/       (.csv, static)
  products/        (.csv, static)
  shipments/       (.csv, daily)
  payments/        (.csv, daily)
        │
        ▼
upload_to_minio.py
        │
        ▼
MinIO — S3-compatible object storage
  retail-data/
    raw_data/orders/
    raw_data/order_items/
    raw_data/customers/
    raw_data/products/
    raw_data/shipments/
    raw_data/payments/
    archive/loaded/
    archive/failed/
        │
        ▼
Airflow — dag_ingest_from_minio
  ├── Scan MinIO raw_data/ prefix
  ├── Download files to temp directory
  ├── Calculate SHA-256 checksum (deduplication)
  ├── Validate file schema
  ├── Load into PostgreSQL raw schema
  ├── Update metadata.source_files
  └── Move file to archive/loaded/ or archive/failed/ in MinIO
        │
        ▼
PostgreSQL — raw schema
  (all columns stored as VARCHAR, metadata columns added)
        │
        ▼
Airflow — dag_run_dbt_transformations
  ├── dbt run staging models (views)
  └── dbt run mart models (tables)
        │
        ▼
PostgreSQL — staging schema       PostgreSQL — marts schema
  stg_orders                        fact_orders
  stg_order_items                   fact_order_items
  stg_customers                     fact_daily_sales
  stg_products                      fact_customer_revenue
  stg_shipments                     fact_shipping_performance
  stg_payments                      dim_customers
                                    dim_products
        │
        ▼
Airflow — dag_data_quality_checks
  ├── Check raw row counts
  ├── Check null primary keys
  ├── Check duplicate orders
  ├── Check negative amounts
  ├── Check invalid dates
  └── Check table freshness
        │
        ▼
PostgreSQL — quality schema
  data_quality_results
  table_freshness
        │
        ▼
Metabase Dashboards
  Revenue Overview
  Customer Analytics
  Shipping Performance
  Pipeline Health
```

---

## Infrastructure

All services run locally via Docker Compose.

| Service | Image | Port | Purpose |
|---|---|---|---|
| postgres | postgres:15 | 5432 | Data warehouse + Airflow metadata |
| minio | minio/minio:latest | 9000 (API), 9001 (UI) | S3-compatible file storage |
| minio-init | minio/mc:latest | — | Creates bucket and folder structure |
| airflow-init | apache/airflow:2.8.1 | — | Initializes Airflow DB and admin user |
| airflow-webserver | apache/airflow:2.8.1 | 8080 | Airflow UI |
| airflow-scheduler | apache/airflow:2.8.1 | — | Runs scheduled DAG tasks |
| metabase | metabase/metabase:latest | 3000 | Business dashboards |

---

## Key Design Decisions

**MinIO for S3-compatible storage**
Raw files are uploaded to MinIO before ingestion. This simulates how real companies store files in S3 or GCS before processing. The ingestion DAG downloads files from MinIO to a temp directory, processes them, then moves them to `archive/loaded/` or `archive/failed/` — mirroring a real S3-based pipeline.

**Raw layer uses VARCHAR columns**
The raw schema stores all data as VARCHAR. This prevents load failures caused by unexpected values or format changes in source files. Type casting happens in dbt staging models where it is easier to handle and test.

**SHA-256 checksum deduplication**
Each file is fingerprinted with SHA-256 before loading. If the checksum already exists in `metadata.source_files` with status `loaded`, the file is skipped. This is more reliable than filename-based deduplication because it catches renamed duplicates and detects file content changes.

**Metadata columns on every raw row**
Every row loaded into the raw schema includes `_source_file_name`, `_source_file_path`, `_loaded_at`, and `_ingestion_run_id`. This makes debugging easier — you can always trace a row back to its exact source file and ingestion run.

**dbt staging models deduplicate**
Each staging model uses `ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY _loaded_at DESC)` to keep only the latest version of each record. This means re-running ingestion is safe — duplicates are handled at the transformation layer.

**Separate metadata and quality schemas**
Pipeline health data is stored separately from business data. This makes it possible to build a Pipeline Health dashboard that monitors the platform itself — a sign of data engineering maturity.