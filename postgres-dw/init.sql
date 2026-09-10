-- Serving Database (postgres-dw) for Credit Card Fraud Detection
-- Holds full transactions dataset (5M real records), streaming predictions, alerts, and analytical views for Superset.

-- 1. Raw / Sampled 5M Transactions Table
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id                      VARCHAR(64) PRIMARY KEY,
    customer_id                         VARCHAR(64),
    time_of_transaction                 VARCHAR(32),
    event_time                          TIMESTAMP WITHOUT TIME ZONE,
    transaction_amount                  DOUBLE PRECISION,
    transaction_amount_bin              VARCHAR(32),
    avg_amount_deviation_sigma          DOUBLE PRECISION,
    merchant_category                   VARCHAR(64),
    merchant_risk_score                 VARCHAR(32),
    merchant_category_vs_history        VARCHAR(64),
    geo_velocity_kmh                    DOUBLE PRECISION,
    geo_velocity_bin                    VARCHAR(32),
    geo_distance_km                     DOUBLE PRECISION,
    geo_distance_bin                    VARCHAR(32),
    merchant_location                   VARCHAR(64),
    country_consistency                 VARCHAR(32),
    ip_address_type                     VARCHAR(32),
    device_fingerprint_match            VARCHAR(32),
    session_duration_sec                DOUBLE PRECISION,
    session_duration_bin                VARCHAR(32),
    network_carrier_type                VARCHAR(64),
    cards_on_device_30d                 INTEGER,
    cvv_match_status                    VARCHAR(32),
    three_ds_auth_result                VARCHAR(32),
    tokenization_used                   VARCHAR(32),
    failed_attempts_before_success      INTEGER,
    transaction_velocity_1h             VARCHAR(32),
    account_credential_change_recency   VARCHAR(64),
    card_present_cnp                    VARCHAR(32),
    order_shipping_speed                VARCHAR(32),
    account_age_days                    INTEGER,
    chargeback_history_count            INTEGER,
    segment                             VARCHAR(64),
    customer_type                       VARCHAR(64),
    is_fraud                            INTEGER
);

-- 2. Streaming Predictions Table (populated by Spark streaming job)
CREATE TABLE IF NOT EXISTS predictions (
    transaction_id      VARCHAR(64),
    event_time          TIMESTAMP WITHOUT TIME ZONE,
    amount              DOUBLE PRECISION,
    merchant_id         VARCHAR(32),
    merchant_category   VARCHAR(64),
    merchant_location   VARCHAR(64),
    card_type           VARCHAR(32),
    country             VARCHAR(8),
    fraud_probability   DOUBLE PRECISION,
    predicted_label     INTEGER,
    actual_label        INTEGER,
    processed_at        TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
);

-- 3. Streaming Alerts Table
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

-- 4. High Performance Indexes for Analytical Queries
CREATE INDEX IF NOT EXISTS idx_transactions_event_time ON transactions (event_time);
CREATE INDEX IF NOT EXISTS idx_transactions_is_fraud ON transactions (is_fraud);
CREATE INDEX IF NOT EXISTS idx_transactions_merchant_cat ON transactions (merchant_category);
CREATE INDEX IF NOT EXISTS idx_transactions_merchant_loc ON transactions (merchant_location);
CREATE INDEX IF NOT EXISTS idx_transactions_segment ON transactions (segment);
CREATE INDEX IF NOT EXISTS idx_transactions_customer_type ON transactions (customer_type);
CREATE INDEX IF NOT EXISTS idx_transactions_card_present ON transactions (card_present_cnp);
CREATE INDEX IF NOT EXISTS idx_transactions_risk_score ON transactions (merchant_risk_score);
CREATE INDEX IF NOT EXISTS idx_transactions_three_ds ON transactions (three_ds_auth_result);
CREATE INDEX IF NOT EXISTS idx_transactions_cvv ON transactions (cvv_match_status);

CREATE INDEX IF NOT EXISTS idx_predictions_event_time ON predictions (event_time);
CREATE INDEX IF NOT EXISTS idx_alerts_event_time ON alerts (event_time);

-- 5. Analytical Views for Superset Dashboards

-- View: KPI Overview Metrics
CREATE OR REPLACE VIEW vw_fraud_kpis AS
SELECT
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_transactions,
    COUNT(*) - SUM(is_fraud) AS legitimate_transactions,
    ROUND(100.0 * SUM(is_fraud) / NULLIF(COUNT(*), 0), 2) AS fraud_rate_pct,
    ROUND(SUM(transaction_amount)::numeric, 2) AS total_amount,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN transaction_amount ELSE 0 END)::numeric, 2) AS fraud_amount,
    ROUND(SUM(CASE WHEN is_fraud = 0 THEN transaction_amount ELSE 0 END)::numeric, 2) AS legitimate_amount,
    ROUND(AVG(transaction_amount)::numeric, 2) AS avg_transaction_amount
FROM transactions;

-- View: Transactions & Fraud Over Time (Daily Aggregation)
CREATE OR REPLACE VIEW vw_fraud_over_time AS
SELECT
    date_trunc('day', event_time) AS txn_date,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_transactions,
    ROUND(100.0 * SUM(is_fraud) / NULLIF(COUNT(*), 0), 2) AS fraud_rate_pct,
    ROUND(SUM(transaction_amount)::numeric, 2) AS total_amount,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN transaction_amount ELSE 0 END)::numeric, 2) AS fraud_amount
FROM transactions
GROUP BY 1
ORDER BY 1;

-- View: Fraud by Merchant Category
CREATE OR REPLACE VIEW vw_fraud_by_merchant_category AS
SELECT
    merchant_category,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_transactions,
    ROUND(100.0 * SUM(is_fraud) / NULLIF(COUNT(*), 0), 2) AS fraud_rate_pct,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN transaction_amount ELSE 0 END)::numeric, 2) AS fraud_amount
FROM transactions
GROUP BY merchant_category
ORDER BY fraud_transactions DESC;

-- View: Fraud by Merchant Location
CREATE OR REPLACE VIEW vw_fraud_by_merchant_location AS
SELECT
    merchant_location,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_transactions,
    ROUND(100.0 * SUM(is_fraud) / NULLIF(COUNT(*), 0), 2) AS fraud_rate_pct,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN transaction_amount ELSE 0 END)::numeric, 2) AS fraud_amount
FROM transactions
GROUP BY merchant_location
ORDER BY fraud_transactions DESC;

-- View: Fraud by Customer Segment & Customer Type
CREATE OR REPLACE VIEW vw_fraud_by_customer_segment AS
SELECT
    segment,
    customer_type,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_transactions,
    ROUND(100.0 * SUM(is_fraud) / NULLIF(COUNT(*), 0), 2) AS fraud_rate_pct,
    ROUND(SUM(transaction_amount)::numeric, 2) AS total_amount
FROM transactions
GROUP BY segment, customer_type
ORDER BY fraud_transactions DESC;

-- View: Risk & Security Analysis
CREATE OR REPLACE VIEW vw_risk_and_security_analysis AS
SELECT
    merchant_risk_score,
    card_present_cnp,
    cvv_match_status,
    three_ds_auth_result,
    device_fingerprint_match,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_transactions,
    ROUND(100.0 * SUM(is_fraud) / NULLIF(COUNT(*), 0), 2) AS fraud_rate_pct
FROM transactions
GROUP BY merchant_risk_score, card_present_cnp, cvv_match_status, three_ds_auth_result, device_fingerprint_match;

-- View: Top Suspicious Transactions
CREATE OR REPLACE VIEW vw_top_suspicious_transactions AS
SELECT
    transaction_id,
    customer_id,
    event_time,
    transaction_amount,
    merchant_category,
    merchant_location,
    merchant_risk_score,
    three_ds_auth_result,
    cvv_match_status,
    card_present_cnp,
    failed_attempts_before_success,
    segment,
    customer_type,
    is_fraud
FROM transactions
WHERE is_fraud = 1 OR merchant_risk_score IN ('high_risk', 'very_high_risk')
ORDER BY transaction_amount DESC
LIMIT 500;
