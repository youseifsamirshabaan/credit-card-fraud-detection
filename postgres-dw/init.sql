-- ===========================================================================
-- Serving Database (postgres-dw) for Credit Card Fraud Detection
-- ===========================================================================
-- Holds streaming predictions, alerts, deduplicated views, and analytical
-- views for Superset dashboards.

-- 1. Streaming Predictions Table (populated by Spark streaming job)
CREATE TABLE IF NOT EXISTS predictions (
    transaction_id      VARCHAR(64),
    event_time          TIMESTAMP WITHOUT TIME ZONE,
    amount              DOUBLE PRECISION,
    merchant_id         VARCHAR(32),
    merchant_category   VARCHAR(64),
    merchant_location   VARCHAR(64),
    card_type           VARCHAR(32),
    country             VARCHAR(8),
    city                VARCHAR(64),
    fraud_probability   DOUBLE PRECISION,
    predicted_label     INTEGER,
    actual_label        INTEGER,
    processed_at        TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
);

-- Ensure merchant_location column exists if table already existed without it
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'predictions' AND column_name = 'merchant_location'
    ) THEN
        ALTER TABLE predictions ADD COLUMN merchant_location VARCHAR(64);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'predictions' AND column_name = 'city'
    ) THEN
        ALTER TABLE predictions ADD COLUMN city VARCHAR(64);
    END IF;
END $$;

-- 2. Streaming Alerts Table (populated by Spark streaming job for high-risk transactions)
CREATE TABLE IF NOT EXISTS alerts (
    transaction_id      VARCHAR(64),
    event_time          TIMESTAMP WITHOUT TIME ZONE,
    risk_level          VARCHAR(16),
    fraud_probability   DOUBLE PRECISION,
    amount              DOUBLE PRECISION,
    merchant_location   VARCHAR(64),
    country             VARCHAR(8),
    created_at          TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'alerts' AND column_name = 'merchant_location'
    ) THEN
        ALTER TABLE alerts ADD COLUMN merchant_location VARCHAR(64);
    END IF;
END $$;

-- 3. High Performance Indexes
CREATE INDEX IF NOT EXISTS idx_predictions_event_time ON predictions (event_time);
CREATE INDEX IF NOT EXISTS idx_predictions_txn_id ON predictions (transaction_id);
CREATE INDEX IF NOT EXISTS idx_predictions_merchant_cat ON predictions (merchant_category);
CREATE INDEX IF NOT EXISTS idx_predictions_predicted_label ON predictions (predicted_label);
CREATE INDEX IF NOT EXISTS idx_alerts_event_time ON alerts (event_time);
CREATE INDEX IF NOT EXISTS idx_alerts_risk_level ON alerts (risk_level);

-- 4. Deduplicated Streaming Predictions View
-- Guarantees exactly-once semantics per transaction_id even if micro-batches replay
CREATE OR REPLACE VIEW vw_demo_predictions AS
SELECT DISTINCT ON (predictions.transaction_id)
    predictions.transaction_id,
    predictions.event_time,
    predictions.amount,
    predictions.merchant_id,
    predictions.merchant_category,
    predictions.card_type,
    predictions.country,
    predictions.city,
    predictions.merchant_location,
    predictions.fraud_probability,
    predictions.predicted_label,
    predictions.actual_label,
    predictions.processed_at
FROM predictions
ORDER BY predictions.transaction_id, predictions.processed_at DESC;

-- 5. Transactions Dynamic View for Superset
-- Maps deduplicated predictions to full analytical transaction schema
-- Accurately preserves merchant_location and risk scores
CREATE OR REPLACE VIEW transactions AS
SELECT
    transaction_id,
    event_time,
    event_time::date AS txn_date,
    amount,
    amount AS transaction_amount,
    CASE
        WHEN amount < 50::double precision THEN '0-50'::text
        WHEN amount < 100::double precision THEN '50-100'::text
        WHEN amount < 500::double precision THEN '100-500'::text
        ELSE '500+'::text
    END AS transaction_amount_bin,
    merchant_id,
    merchant_category,
    card_type,
    country,
    city,
    COALESCE(NULLIF(merchant_location, ''), country, 'unknown') AS merchant_location,
    CASE
        WHEN fraud_probability >= 0.90::double precision THEN 'Critical'::text
        WHEN fraud_probability >= 0.70::double precision THEN 'High'::text
        WHEN fraud_probability >= 0.40::double precision THEN 'Medium'::text
        ELSE 'Low'::text
    END AS merchant_risk_score,
    fraud_probability,
    predicted_label,
    actual_label,
    processed_at,
    CASE
        WHEN predicted_label = 1 THEN 1
        ELSE 0
    END AS is_fraud
FROM vw_demo_predictions;

-- 6. Top Suspicious Transactions View (Deduplicated & High Risk First)
CREATE OR REPLACE VIEW vw_top_suspicious_transactions AS
SELECT
    transaction_id,
    event_time,
    txn_date,
    transaction_amount,
    transaction_amount_bin,
    amount,
    merchant_id,
    merchant_category,
    card_type,
    country,
    city,
    merchant_location,
    merchant_risk_score,
    fraud_probability,
    predicted_label,
    actual_label,
    is_fraud
FROM transactions
WHERE predicted_label = 1
ORDER BY fraud_probability DESC, transaction_amount DESC
LIMIT 100;

-- 7. KPI Overview Metrics View
CREATE OR REPLACE VIEW vw_fraud_kpis AS
SELECT
    count(*) AS total_transactions,
    sum(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    count(*) - sum(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END) AS legitimate_transactions,
    round(100.0 * sum(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(count(*), 0)::numeric, 2) AS fraud_rate_pct,
    COALESCE(round(sum(amount)::numeric, 2), 0) AS total_amount,
    COALESCE(round(sum(CASE WHEN predicted_label = 1 THEN amount ELSE 0 END)::numeric, 2), 0) AS fraud_amount,
    COALESCE(round(sum(CASE WHEN predicted_label = 0 THEN amount ELSE 0 END)::numeric, 2), 0) AS legitimate_amount,
    COALESCE(round(avg(amount)::numeric, 2), 0) AS avg_transaction_amount
FROM vw_demo_predictions;

-- Compatible demo KPI view
CREATE OR REPLACE VIEW vw_demo_kpis AS
SELECT
    count(*) AS total_transactions,
    sum(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    round(100.0 * sum(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(count(*), 0)::numeric, 2) AS fraud_rate_pct,
    COALESCE(sum(amount), 0::double precision) AS total_amount,
    COALESCE(sum(CASE WHEN predicted_label = 1 THEN amount ELSE 0 END), 0::double precision) AS fraud_amount
FROM vw_demo_predictions;

-- 8. Time-series Trends View (Hourly & Daily Aggregation)
CREATE OR REPLACE VIEW vw_fraud_over_time AS
SELECT
    date_trunc('hour', event_time) AS time_bucket,
    date_trunc('day', event_time)::date AS txn_date,
    count(*) AS total_transactions,
    sum(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    round(100.0 * sum(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(count(*), 0)::numeric, 2) AS fraud_rate_pct,
    COALESCE(round(sum(amount)::numeric, 2), 0) AS total_amount,
    COALESCE(round(sum(CASE WHEN predicted_label = 1 THEN amount ELSE 0 END)::numeric, 2), 0) AS fraud_amount
FROM vw_demo_predictions
GROUP BY 1, 2
ORDER BY 1;

-- 9. Fraud by Merchant Category View
CREATE OR REPLACE VIEW vw_fraud_by_merchant_category AS
SELECT
    merchant_category,
    count(*) AS total_transactions,
    sum(is_fraud) AS fraud_transactions,
    round(100.0 * sum(is_fraud) / NULLIF(count(*), 0), 2) AS fraud_rate_pct,
    COALESCE(round(sum(CASE WHEN is_fraud = 1 THEN transaction_amount ELSE 0 END)::numeric, 2), 0) AS fraud_amount
FROM transactions
GROUP BY merchant_category
ORDER BY fraud_transactions DESC;

-- 10. Fraud by Merchant Location View
CREATE OR REPLACE VIEW vw_fraud_by_merchant_location AS
SELECT
    merchant_location,
    count(*) AS total_transactions,
    sum(is_fraud) AS fraud_transactions,
    round(100.0 * sum(is_fraud) / NULLIF(count(*), 0), 2) AS fraud_rate_pct,
    COALESCE(round(sum(CASE WHEN is_fraud = 1 THEN transaction_amount ELSE 0 END)::numeric, 2), 0) AS fraud_amount
FROM transactions
GROUP BY merchant_location
ORDER BY fraud_transactions DESC;
