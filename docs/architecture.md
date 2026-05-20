# Architecture

## High-Level Overview

The platform follows a standard ELT pattern — Extract, Load, Transform.

```
raw_data/
  orders/          (.csv, daily)
  order_items/     (.parquet, daily)
  customers/       (.csv, static)
  products/        (.csv, static)
  shipments/       (.csv, daily)
  payments/        (.csv, daily)
        │
        ▼
Airflow — dag_ingest_retail_files
  ├── Scan raw_data folders
  ├── Calculate checksum (deduplication)
  ├── Validate file schema
  ├── Load into PostgreSQL raw schema
  ├── Update metadata.source_files
  └── Archive loaded files
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
| airflow-init | apache/airflow:2.8.1 | — | Initializes Airflow DB and admin user |
| airflow-webserver | apache/airflow:2.8.1 | 8080 | Airflow UI |
| airflow-scheduler | apache/airflow:2.8.1 | — | Runs scheduled DAG tasks |
| metabase | metabase/metabase:latest | 3000 | Business dashboards |

---

## Key Design Decisions

**Raw layer uses VARCHAR columns**
The raw schema stores all data as VARCHAR. This prevents load failures caused by unexpected values or format changes in source files. Type casting happens in dbt staging models where it is easier to handle and test.

**Checksum-based deduplication**
Each file is fingerprinted with SHA-256 before loading. If the checksum already exists in `metadata.source_files` with status `loaded`, the file is skipped. This is more reliable than filename-based deduplication because it catches renamed duplicates.

**Metadata columns on every raw row**
Every row loaded into the raw schema includes `_source_file_name`, `_source_file_path`, `_loaded_at`, and `_ingestion_run_id`. This makes debugging easier — you can always trace a row back to its exact source file and ingestion run.

**dbt staging models deduplicate**
Each staging model uses `ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY _loaded_at DESC)` to keep only the latest version of each record. This means re-running ingestion is safe — duplicates are handled at the transformation layer.

**Separate metadata and quality schemas**
Pipeline health data is stored separately from business data. This makes it possible to build a Pipeline Health dashboard that monitors the platform itself — a sign of data engineering maturity.
