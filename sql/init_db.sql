-- =============================================================
-- init_db.sql
-- Creates all raw schema tables for the retail analytics platform
-- Run this once after the database and schemas are created.
-- =============================================================

-- -------------------------------------------------------------
-- raw.customers
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id         VARCHAR(20),
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    email               VARCHAR(255),
    country             VARCHAR(100),
    city                VARCHAR(100),
    created_at          VARCHAR(50),

    -- ingestion metadata columns
    _source_file_name   VARCHAR(255),
    _source_file_path   TEXT,
    _loaded_at          TIMESTAMP       DEFAULT NOW(),
    _ingestion_run_id   INTEGER
);

-- -------------------------------------------------------------
-- raw.products
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.products (
    product_id          VARCHAR(20),
    product_name        VARCHAR(255),
    category            VARCHAR(100),
    subcategory         VARCHAR(100),
    brand               VARCHAR(100),
    cost_price          VARCHAR(50),
    sale_price          VARCHAR(50),
    is_active           VARCHAR(10),

    -- ingestion metadata columns
    _source_file_name   VARCHAR(255),
    _source_file_path   TEXT,
    _loaded_at          TIMESTAMP       DEFAULT NOW(),
    _ingestion_run_id   INTEGER
);

-- -------------------------------------------------------------
-- raw.orders
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id            VARCHAR(20),
    customer_id         VARCHAR(20),
    order_date          VARCHAR(20),
    order_status        VARCHAR(50),
    currency            VARCHAR(10),
    total_amount        VARCHAR(50),
    created_at          VARCHAR(50),
    updated_at          VARCHAR(50),

    -- ingestion metadata columns
    _source_file_name   VARCHAR(255),
    _source_file_path   TEXT,
    _loaded_at          TIMESTAMP       DEFAULT NOW(),
    _ingestion_run_id   INTEGER
);

-- -------------------------------------------------------------
-- raw.order_items
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id       VARCHAR(20),
    order_id            VARCHAR(20),
    product_id          VARCHAR(20),
    quantity            VARCHAR(20),
    unit_price          VARCHAR(50),
    discount_amount     VARCHAR(50),
    tax_amount          VARCHAR(50),
    line_total          VARCHAR(50),

    -- ingestion metadata columns
    _source_file_name   VARCHAR(255),
    _source_file_path   TEXT,
    _loaded_at          TIMESTAMP       DEFAULT NOW(),
    _ingestion_run_id   INTEGER
);

-- -------------------------------------------------------------
-- raw.shipments
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.shipments (
    shipment_id         VARCHAR(20),
    order_id            VARCHAR(20),
    carrier             VARCHAR(100),
    shipping_method     VARCHAR(50),
    shipping_cost       VARCHAR(50),
    shipped_at          VARCHAR(50),
    delivered_at        VARCHAR(50),
    shipment_status     VARCHAR(50),

    -- ingestion metadata columns
    _source_file_name   VARCHAR(255),
    _source_file_path   TEXT,
    _loaded_at          TIMESTAMP       DEFAULT NOW(),
    _ingestion_run_id   INTEGER
);

-- -------------------------------------------------------------
-- raw.payments
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.payments (
    payment_id          VARCHAR(20),
    order_id            VARCHAR(20),
    payment_method      VARCHAR(50),
    payment_status      VARCHAR(50),
    payment_amount      VARCHAR(50),
    paid_at             VARCHAR(50),

    -- ingestion metadata columns
    _source_file_name   VARCHAR(255),
    _source_file_path   TEXT,
    _loaded_at          TIMESTAMP       DEFAULT NOW(),
    _ingestion_run_id   INTEGER
);

-- -------------------------------------------------------------
-- Indexes on raw tables for faster deduplication lookups
-- -------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_raw_orders_order_id
    ON raw.orders (order_id);

CREATE INDEX IF NOT EXISTS idx_raw_orders_loaded_at
    ON raw.orders (_loaded_at);

CREATE INDEX IF NOT EXISTS idx_raw_order_items_order_id
    ON raw.order_items (order_id);

CREATE INDEX IF NOT EXISTS idx_raw_shipments_order_id
    ON raw.shipments (order_id);

CREATE INDEX IF NOT EXISTS idx_raw_payments_order_id
    ON raw.payments (order_id);
