-- DIMENSION: Customers
CREATE TABLE IF NOT EXISTS `your_project.commercepulse.dim_customer` (
    customer_id STRING NOT NULL,
    name STRING,
    email STRING,
    country STRING,
    created_at TIMESTAMP
);

-- DIMENSION: Products
CREATE TABLE IF NOT EXISTS `your_project.commercepulse.dim_product` (
    product_id STRING NOT NULL,
    name STRING,
    category STRING,
    vendor_id STRING,
    unit_price FLOAT64
);

-- DIMENSION: Dates (pre-populated calendar table)
CREATE TABLE IF NOT EXISTS `your_project.commercepulse.dim_date` (
    date_id DATE NOT NULL,
    day_of_week INT64,
    month INT64,
    quarter INT64,
    year INT64,
    is_weekend BOOL
);

-- FACT: Orders (current state — upserted)
CREATE TABLE IF NOT EXISTS `your_project.commercepulse.fact_orders` (
    order_id STRING NOT NULL,
    customer_id STRING,
    vendor_id STRING,
    status STRING,          -- created, paid, shipped, delivered, cancelled
    gross_amount FLOAT64,
    currency STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _ingested_at TIMESTAMP  -- when this row arrived in BigQuery
);

-- FACT: Payments (append-only — never update, just add rows)
CREATE TABLE IF NOT EXISTS `your_project.commercepulse.fact_payments` (
    payment_id STRING NOT NULL,
    order_id STRING,
    vendor_id STRING,
    status STRING,          -- success, failed, pending
    amount FLOAT64,
    currency STRING,
    payment_method STRING,
    attempted_at TIMESTAMP,
    _ingested_at TIMESTAMP
);

-- FACT: Refunds (append-only)
CREATE TABLE IF NOT EXISTS `your_project.commercepulse.fact_refunds` (
    refund_id STRING NOT NULL,
    order_id STRING,
    payment_id STRING,
    vendor_id STRING,
    amount FLOAT64,
    reason STRING,
    refunded_at TIMESTAMP,
    _ingested_at TIMESTAMP
);

-- FACT: Shipments
CREATE TABLE IF NOT EXISTS `your_project.commercepulse.fact_shipments` (
    shipment_id STRING NOT NULL,
    order_id STRING,
    vendor_id STRING,
    status STRING,          -- dispatched, in_transit, delivered, failed
    dispatched_at TIMESTAMP,
    delivered_at TIMESTAMP,
    _ingested_at TIMESTAMP
);

-- AGGREGATE: Daily order summary (pre-computed for dashboards)
CREATE TABLE IF NOT EXISTS `your_project.commercepulse.fact_order_daily` (
    report_date DATE NOT NULL,
    vendor_id STRING,
    total_orders INT64,
    gross_revenue FLOAT64,
    total_refunds FLOAT64,
    net_revenue FLOAT64,
    successful_payments INT64,
    failed_payments INT64,
    _computed_at TIMESTAMP
);