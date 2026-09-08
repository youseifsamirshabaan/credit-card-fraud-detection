-- Serving Database (proposal: "4B. Serving Database (PostgreSQL)")
-- Holds final predictions + aggregated views that feed Superset dashboards.
-- Populated by spark/apps/streaming_fraud_pipeline.py via JDBC.

-- No PRIMARY KEY on transaction_id: the streaming job appends via plain
-- JDBC batch inserts (no upsert support), so a replayed micro-batch after a
-- checkpoint hiccup should not blow up the whole pipeline with a PK
-- violation. Dedup in Superset with COUNT(DISTINCT transaction_id) if needed.
CREATE TABLE IF NOT EXISTS predictions (
    transaction_id      VARCHAR(64),
    event_time          TIMESTAMP,
    amount              DOUBLE PRECISION,
    merchant_id         VARCHAR(32),
    merchant_category   VARCHAR(64),
    card_type           VARCHAR(32),
    country             VARCHAR(8),
    city                VARCHAR(64),
    fraud_probability   DOUBLE PRECISION,
    predicted_label     INTEGER,
    actual_label        INTEGER,
    processed_at        TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_predictions_event_time ON predictions (event_time);
CREATE INDEX IF NOT EXISTS idx_predictions_country ON predictions (country);
CREATE INDEX IF NOT EXISTS idx_predictions_merchant ON predictions (merchant_category);

CREATE TABLE IF NOT EXISTS alerts (
    transaction_id      VARCHAR(64),
    event_time          TIMESTAMP,
    risk_level          VARCHAR(16),
    fraud_probability   DOUBLE PRECISION,
    amount              DOUBLE PRECISION,
    country             VARCHAR(8),
    created_at          TIMESTAMP DEFAULT now()
);

-- "Fraud Rate Over Time" panel
CREATE OR REPLACE VIEW fraud_rate_over_time AS
SELECT date_trunc('minute', event_time)                                   AS bucket,
       COUNT(*)                                                           AS total_txn,
       SUM(predicted_label)                                               AS fraud_txn,
       ROUND(100.0 * SUM(predicted_label) / NULLIF(COUNT(*), 0), 2)       AS fraud_rate_pct
FROM predictions
GROUP BY 1
ORDER BY 1;

-- "Top Fraudulent Merchants" panel
CREATE OR REPLACE VIEW top_fraudulent_merchants AS
SELECT merchant_category,
       COUNT(*) FILTER (WHERE predicted_label = 1) AS fraud_txn,
       COUNT(*)                                     AS total_txn
FROM predictions
GROUP BY merchant_category
ORDER BY fraud_txn DESC;

-- "Fraud by Country" panel
CREATE OR REPLACE VIEW fraud_by_country AS
SELECT country,
       COUNT(*) FILTER (WHERE predicted_label = 1) AS fraud_txn,
       COUNT(*)                                     AS total_txn,
       ROUND(100.0 * COUNT(*) FILTER (WHERE predicted_label = 1) / NULLIF(COUNT(*), 0), 2) AS fraud_rate_pct
FROM predictions
GROUP BY country
ORDER BY fraud_txn DESC;

-- "Card Type Analysis" panel
CREATE OR REPLACE VIEW fraud_by_card_type AS
SELECT card_type,
       COUNT(*) FILTER (WHERE predicted_label = 1) AS fraud_txn,
       COUNT(*)                                     AS total_txn
FROM predictions
GROUP BY card_type
ORDER BY fraud_txn DESC;
