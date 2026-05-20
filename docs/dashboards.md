# Dashboards

All dashboards are built in Metabase at http://localhost:3000 and connect directly to PostgreSQL.

---

## Dashboard 1 — Revenue Overview

**Purpose:** Show how the business is performing financially.

**Source tables:** `marts.fact_daily_sales`, `marts.fact_orders`, `marts.fact_order_items`, `marts.dim_products`

| Chart | Type | Description |
|---|---|---|
| Total Revenue | Single number | Sum of total_revenue from fact_daily_sales |
| Total Orders | Single number | Count of orders from fact_orders |
| Average Order Value | Single number | Avg total_amount for completed orders |
| Revenue by Day | Line chart | Daily revenue trend |
| Orders by Day | Bar chart | Daily order count |
| Revenue by Category | Bar chart | Revenue grouped by product category |
| Top 10 Products | Table | Products ranked by revenue |

---

## Dashboard 2 — Customer Analytics

**Purpose:** Understand customer behaviour and value.

**Source tables:** `marts.dim_customers`, `marts.fact_customer_revenue`

| Chart | Type | Description |
|---|---|---|
| Total Customers | Single number | Count of customers |
| Customers by Segment | Pie chart | new / returning / high_value / inactive |
| Top 10 Customers | Table | Customers ranked by total revenue |
| Revenue by Country | Bar chart | Revenue grouped by country |
| New Customers by Month | Bar chart | Monthly customer acquisition |

---

## Dashboard 3 — Shipping Performance

**Purpose:** Analyse delivery speed and carrier performance.

**Source tables:** `marts.fact_shipping_performance`

| Chart | Type | Description |
|---|---|---|
| Average Delivery Days | Single number | Avg days from shipped to delivered |
| Late Shipments | Single number | Count of shipments that exceeded SLA |
| Avg Delivery Days by Carrier | Bar chart | Carrier speed comparison |
| Late Shipments by Carrier | Bar chart | Late shipment count per carrier |
| Shipment Status Breakdown | Pie chart | delivered / in_transit / pending / failed |
| Shipping Cost by Day | Line chart | Daily shipping cost trend |

Late shipment SLAs:
- Overnight: > 1 day
- Express: > 3 days
- Standard: > 7 days

---

## Dashboard 4 — Pipeline Health

**Purpose:** Monitor the data platform itself.

**Source tables:** `metadata.source_files`, `metadata.ingestion_runs`, `quality.data_quality_results`

| Chart | Type | Description |
|---|---|---|
| Files Ingested Today | Single number | Files loaded today |
| Failed Files | Single number | Files with failed status |
| Files Ingested by Day | Bar chart | Daily ingestion history |
| Rows Loaded by Source | Bar chart | Total rows per source type |
| Quality Checks by Status | Pie chart | passed / failed / warning |
| Failed Ingestion Runs | Table | Failed runs with error messages |

This dashboard is important because it shows data engineering maturity — the platform monitors itself, not just the business data.
