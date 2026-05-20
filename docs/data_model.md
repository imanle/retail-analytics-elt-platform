# Data Model

## Schemas

| Schema | Materialization | Purpose |
|---|---|---|
| `raw` | Tables | Source data loaded as-is |
| `staging` | Views (dbt) | Cleaned and typed data |
| `marts` | Tables (dbt) | Analytics-ready models |
| `metadata` | Tables | Pipeline tracking |
| `quality` | Tables | Data quality results |

---

## Raw Schema

All columns are VARCHAR. Ingestion metadata columns are added to every table.

**Shared metadata columns on all raw tables:**

| Column | Description |
|---|---|
| `_source_file_name` | Name of the source file |
| `_source_file_path` | Full path to the source file |
| `_loaded_at` | Timestamp when the row was loaded |
| `_ingestion_run_id` | FK to metadata.ingestion_runs |

---

## Staging Schema

Built by dbt as views on top of raw tables. Each model:
- Casts columns to correct data types
- Standardises string values (lowercase, initcap)
- Deduplicates by primary key (latest record wins)
- Filters out null primary keys

---

## Marts Schema

### Dimension Tables

**`dim_customers`**

| Column | Type | Description |
|---|---|---|
| customer_id | VARCHAR | Primary key |
| full_name | VARCHAR | First + last name |
| email | VARCHAR | Email address |
| country | VARCHAR | Country |
| city | VARCHAR | City |
| customer_created_at | TIMESTAMP | Account creation date |
| first_order_date | DATE | Date of first completed order |
| last_order_date | DATE | Date of most recent completed order |
| total_orders | INTEGER | Total completed orders |
| total_revenue | NUMERIC | Total revenue from completed orders |
| customer_segment | VARCHAR | new / returning / high_value / inactive |

**`dim_products`**

| Column | Type | Description |
|---|---|---|
| product_id | VARCHAR | Primary key |
| product_name | VARCHAR | Product name |
| category | VARCHAR | Top-level category |
| subcategory | VARCHAR | Subcategory |
| brand | VARCHAR | Brand name |
| cost_price | NUMERIC | Cost price |
| sale_price | NUMERIC | Sale price |
| is_active | BOOLEAN | Whether the product is active |
| gross_margin_pct | NUMERIC | (sale - cost) / sale * 100 |

---

### Fact Tables

**`fact_orders`**

| Column | Type | Description |
|---|---|---|
| order_id | VARCHAR | Primary key |
| customer_id | VARCHAR | FK to dim_customers |
| order_date | DATE | Date of order |
| order_status | VARCHAR | completed / cancelled / refunded / pending |
| currency | VARCHAR | Order currency |
| total_amount | NUMERIC | Order total |
| payment_method | VARCHAR | Payment method |
| payment_status | VARCHAR | paid / refunded / pending |
| payment_amount | NUMERIC | Amount paid |
| carrier | VARCHAR | Shipping carrier |
| shipping_method | VARCHAR | standard / express / overnight |
| shipping_cost | NUMERIC | Shipping cost |
| shipment_status | VARCHAR | delivered / in_transit / pending / failed |
| total_items | INTEGER | Number of items in the order |
| is_completed | BOOLEAN | True if order status is completed |
| is_cancelled | BOOLEAN | True if order status is cancelled |
| is_refunded | BOOLEAN | True if order status is refunded |

**`fact_order_items`**

| Column | Type | Description |
|---|---|---|
| order_item_id | VARCHAR | Primary key |
| order_id | VARCHAR | FK to fact_orders |
| product_id | VARCHAR | FK to dim_products |
| quantity | INTEGER | Units ordered |
| unit_price | NUMERIC | Price per unit |
| discount_amount | NUMERIC | Discount applied |
| tax_amount | NUMERIC | Tax applied |
| line_total | NUMERIC | Final line total |
| estimated_cost | NUMERIC | cost_price × quantity |
| estimated_profit | NUMERIC | line_total − estimated_cost |

**`fact_daily_sales`**

| Column | Type | Description |
|---|---|---|
| order_date | DATE | Primary key |
| total_orders | INTEGER | All orders on this date |
| completed_orders | INTEGER | Completed orders |
| cancelled_orders | INTEGER | Cancelled orders |
| refunded_orders | INTEGER | Refunded orders |
| total_revenue | NUMERIC | Revenue from completed orders |
| total_items_sold | INTEGER | Total items across all orders |
| average_order_value | NUMERIC | Avg order value for completed orders |
| total_shipping_cost | NUMERIC | Total shipping cost |

**`fact_customer_revenue`**

| Column | Type | Description |
|---|---|---|
| customer_id | VARCHAR | Primary key |
| full_name | VARCHAR | Customer name |
| email | VARCHAR | Email |
| country | VARCHAR | Country |
| city | VARCHAR | City |
| customer_segment | VARCHAR | Segment from dim_customers |
| total_orders | INTEGER | All orders |
| completed_orders | INTEGER | Completed orders |
| total_revenue | NUMERIC | Revenue from completed orders |
| avg_order_value | NUMERIC | Average order value |
| first_order_date | DATE | First order date |
| last_order_date | DATE | Most recent order date |

**`fact_shipping_performance`**

| Column | Type | Description |
|---|---|---|
| shipment_id | VARCHAR | Primary key |
| order_id | VARCHAR | FK to fact_orders |
| order_date | DATE | Order date |
| carrier | VARCHAR | Carrier name |
| shipping_method | VARCHAR | standard / express / overnight |
| shipping_cost | NUMERIC | Shipping cost |
| shipped_at | TIMESTAMP | When shipment left warehouse |
| delivered_at | TIMESTAMP | When shipment was delivered |
| shipment_status | VARCHAR | delivered / in_transit / pending / failed |
| delivery_days | NUMERIC | Days between shipped and delivered |
| is_late | BOOLEAN | True if delivery exceeded method SLA |

---

## Metadata Schema

**`metadata.source_files`** — tracks every file discovered by the pipeline

**`metadata.ingestion_runs`** — tracks each individual file load attempt

**`metadata.pipeline_runs`** — tracks each full DAG execution

---

## Quality Schema

**`quality.data_quality_results`** — stores pass/fail results for each quality check

**`quality.table_freshness`** — tracks when each table was last loaded

**`quality.row_count_checks`** — tracks row count history per table per day
