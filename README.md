# Retail Analytics ELT Platform

A Dockerized local data engineering project that ingests daily CSV and Parquet retail files using Airflow, loads them into PostgreSQL, transforms them with dbt, validates data quality, and visualizes revenue, customer, shipping, and pipeline-health metrics in Metabase.

---

## Overview

This project simulates a real-world data engineering platform for a retail / e-commerce company. It covers the full data lifecycle — from raw file ingestion to dashboard-ready analytics tables — using only open-source tools running locally via Docker Compose.

It is designed to demonstrate practical data engineering skills without requiring paid cloud services.

---

## Architecture

```
CSV / Parquet Files
        │
        ▼
MinIO (S3-compatible storage)
        │
        ▼
Airflow DAG — scan, validate, deduplicate, load
        │
        ▼
PostgreSQL — raw schema (source data preserved as-is)
        │
        ▼
dbt — staging models (clean, cast, deduplicate)
        │
        ▼
dbt — mart models (fact + dimension tables)
        │
        ▼
Metabase Dashboards
```

For a detailed architecture diagram see [docs/architecture.md](docs/architecture.md).

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Docker Compose | Run the full platform locally with one command |
| PostgreSQL 15 | Local data warehouse (raw, staging, marts, metadata, quality schemas) |
| Apache Airflow 2.8 | Orchestrate ingestion, transformation, and quality check pipelines |
| dbt Core 1.8 | Transform raw data into staging and mart models |
| MinIO | S3-compatible local object storage for raw files |
| Metabase | Business dashboards connected directly to PostgreSQL |
| Python 3.8+ | Ingestion scripts, schema validation, checksum calculation |
| GitHub Actions | CI — linting, unit tests, dbt parse, docker validation |

---

## Dataset

The project uses generated retail data representing an e-commerce company.

| Source | Format | Frequency | Description |
|---|---|---|---|
| orders | CSV | Daily | Customer orders |
| order_items | Parquet | Daily | Line items per order |
| customers | CSV | Static | Customer profiles |
| products | CSV | Static | Product catalogue |
| shipments | CSV | Daily | Shipment and delivery info |
| payments | CSV | Daily | Payment transactions |

Generate sample data:
```bash
python3 scripts/generate_sample_data.py --days 7 --start-date 2026-05-01
```

---

## How to Run Locally

**Prerequisites:** Docker Desktop, Python 3.8+

```bash
# 1. Clone the repo
git clone https://github.com/your-username/retail-analytics-elt-platform.git
cd retail-analytics-elt-platform

# 2. Create your .env file
cp .env.example .env
# Edit .env with your own values

# 3. Generate sample data
pip3 install pandas pyarrow faker
python3 scripts/generate_sample_data.py --days 7 --start-date 2026-05-01

# 4. Start all services
docker compose up -d

# 5. Initialize the database schemas
export $(grep -v '^#' .env | xargs)
docker exec -i retail_postgres psql -U $POSTGRES_USER -d $POSTGRES_DB < sql/create_schemas.sql
docker exec -i retail_postgres psql -U $POSTGRES_USER -d $POSTGRES_DB < sql/init_db.sql
docker exec -i retail_postgres psql -U $POSTGRES_USER -d $POSTGRES_DB < sql/create_metadata_tables.sql

# 6. Upload raw data files to MinIO
pip3 install boto3
python3 scripts/upload_to_minio.py

# 7. Open Airflow and trigger the DAGs in order
# http://localhost:8080  (admin / admin)
# 1. dag_ingest_from_minio
# 2. dag_run_dbt_transformations
# 3. dag_data_quality_checks

# 8. Open Metabase
# http://localhost:3000
# Connect to PostgreSQL: host=postgres, db=retail_warehouse

# 9. Open MinIO UI
# http://localhost:9001
# Login with MINIO_ACCESS_KEY and MINIO_SECRET_KEY from .env
```

---

## Services

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Metabase | http://localhost:3000 | set on first login |
| MinIO UI | http://localhost:9001 | from .env |
| PostgreSQL | localhost:5432 | from .env |

---

## Airflow DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `dag_ingest_from_minio` | Daily 06:30 UTC | Scan MinIO, validate schemas, load files into raw tables |
| `dag_ingest_retail_files` | Daily 06:00 UTC | Alternative: ingest from local raw_data/ folders |
| `dag_run_dbt_transformations` | Daily 07:00 UTC | Run dbt staging and mart models |
| `dag_data_quality_checks` | Daily 08:00 UTC | Run quality checks and write results to quality schema |

See [docs/airflow.md](docs/airflow.md) for full DAG documentation.

---

## Database Design

The database is split into five schemas:

| Schema | Purpose |
|---|---|
| `raw` | Source data loaded as-is (all columns as VARCHAR) |
| `staging` | Cleaned, cast, and deduplicated views built by dbt |
| `marts` | Analytics-ready fact and dimension tables built by dbt |
| `metadata` | Pipeline metadata — file tracking, ingestion run history |
| `quality` | Data quality check results and table freshness |

See [docs/data_model.md](docs/data_model.md) for full schema documentation.

---

## dbt Models

**Staging models** (materialized as views):

| Model | Source |
|---|---|
| `stg_orders` | `raw.orders` |
| `stg_order_items` | `raw.order_items` |
| `stg_customers` | `raw.customers` |
| `stg_products` | `raw.products` |
| `stg_shipments` | `raw.shipments` |
| `stg_payments` | `raw.payments` |

**Mart models** (materialized as tables):

| Model | Description |
|---|---|
| `fact_orders` | One row per order with payment and shipment info |
| `fact_order_items` | One row per order item with estimated profit |
| `fact_daily_sales` | Aggregated sales metrics per day |
| `fact_customer_revenue` | Revenue and order stats per customer |
| `fact_shipping_performance` | Shipment details with late delivery flag |
| `dim_customers` | Customer dimension with segment classification |
| `dim_products` | Product dimension with gross margin |

Run dbt manually:
```bash
cd dbt
export $(grep -v '^#' ../.env | xargs)
dbt run       # build all models
dbt test      # run all schema tests
dbt docs generate && dbt docs serve  # view lineage graph
```

---

## Data Quality Checks

The platform runs two levels of data quality:

**Pre-ingestion (ingestion DAG):**
- File exists and is not empty
- File type is supported (.csv or .parquet)
- Required columns are present
- File has not already been loaded (SHA-256 checksum deduplication)

**Post-ingestion (data quality DAG):**
- Raw tables have rows
- Primary keys are not null
- No duplicate order IDs in fact tables
- No negative amounts
- Delivered date is not before shipped date
- Order dates are not in the future
- Tables are fresh (loaded within 48 hours)

Results are stored in `quality.data_quality_results` and visible in the Pipeline Health dashboard.

---

## Dashboards

| Dashboard | Key Metrics |
|---|---|
| Revenue Overview | Total revenue, orders by day, revenue by category, top products |
| Customer Analytics | Customer segments, top customers, revenue by country |
| Shipping Performance | Avg delivery days, late shipments by carrier, shipment status |
| Pipeline Health | Files ingested, failed files, quality check results, table freshness |

See [docs/dashboards.md](docs/dashboards.md) for dashboard details and screenshots.

---

## GitHub Actions CI

Every push to `main` runs four automated checks:

| Job | Checks |
|---|---|
| Code Quality | `ruff` linting and formatting on `scripts/` and `tests/` |
| Unit Tests | 30 pytest tests for checksum, schema validation, and ingestion |
| dbt Checks | `dbt deps` and `dbt parse` to validate all SQL models |
| Docker Validation | `docker compose config` to validate the compose file |

---

## Repository Structure

```
retail-analytics-elt-platform/
├── docker-compose.yml
├── .env.example
├── .gitignore
├── README.md
├── requirements-dev.txt
├── airflow/
│   ├── dags/
│   │   ├── dag_ingest_retail_files.py
│   │   ├── dag_ingest_from_minio.py
│   │   ├── dag_run_dbt_transformations.py
│   │   └── dag_data_quality_checks.py
│   ├── logs/
│   └── requirements.txt
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   └── macros/
├── scripts/
│   ├── generate_sample_data.py
│   ├── upload_to_minio.py
│   ├── ingest_csv.py
│   ├── ingest_parquet.py
│   ├── validate_schema.py
│   ├── calculate_checksum.py
│   ├── metadata_writer.py
│   └── db.py
├── sql/
│   ├── create_schemas.sql
│   ├── init_db.sql
│   └── create_metadata_tables.sql
├── tests/
│   ├── test_checksum.py
│   ├── test_schema_validation.py
│   └── test_ingestion.py
├── raw_data/
│   ├── orders/
│   ├── order_items/
│   ├── customers/
│   ├── products/
│   ├── shipments/
│   └── payments/
├── archive/
│   ├── loaded/
│   └── failed/
├── docs/
│   ├── architecture.md
│   ├── data_model.md
│   ├── airflow.md
│   ├── dashboards.md
│   └── screenshots/
└── .github/
    └── workflows/
        └── ci.yml
```

---

## Future Improvements

- Add Kafka or Redpanda for streaming ingestion
- Add Great Expectations for advanced data validation
- Add Slack alerting on failed DAG runs
- Add incremental dbt models
- Add slowly changing dimensions (SCD Type 2) for customers
- Add a Makefile for common commands
- Add role-based PostgreSQL users