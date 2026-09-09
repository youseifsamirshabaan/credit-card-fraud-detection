# Processing Layer — Output Schema Reference

Documentation for Person 4 (ML) and Person 5 (Storage/Visualization) on the
exact shape of every dataset produced by the Processing layer
(`spark/apps/streaming_fraud_pipeline.py`, `feature_transformation.py`,
`train_fraud_model.py`).

---

## 1. `hdfs:///stream` — cleaned + feature-engineered stream

Written by the main streaming query, one row per valid transaction.

| Column | Type | Notes |
|---|---|---|
| transaction_id | string | unique id, deduplicated |
| timestamp | string | original raw timestamp (ISO string) |
| amount | double | validated: >= 0 and <= `MAX_REASONABLE_AMOUNT` (20000) |
| merchant_id | string | required, never null |
| merchant_category | string | filled with `"unknown"` if missing |
| card_type | string | filled with `"unknown"` if missing |
| country | string | filled with `"unknown"` if missing |
| city | string | filled with `"unknown"` if missing |
| device_id | string | required, never null |
| ip_address | string | |
| fraud_label | int | ground-truth label (0/1), only present on historical/synthetic data |
| event_time | timestamp | parsed from `timestamp`, used for windowing/watermarking |
| txn_hour | int | hour of day (0-23) extracted from `event_time` |
| is_foreign | int | 1 if `country != "EG"`, else 0 |

---

## 2. `hdfs:///processed` — enriched transactions

Same as `/stream` above, **plus**:

| Column | Type | Notes |
|---|---|---|
| fraud_probability | double | model's predicted probability of fraud (0-1) |
| predicted_label | int | model's predicted class (0/1) |

---

## 3. `hdfs:///predictions` — prediction records (also mirrored to `postgres-dw.predictions`)

| Column | Type | Notes |
|---|---|---|
| transaction_id | string | |
| event_time | timestamp | |
| amount | double | |
| merchant_id | string | |
| merchant_category | string | |
| card_type | string | |
| country | string | |
| city | string | |
| fraud_probability | double | |
| predicted_label | int | |
| actual_label | int | renamed from `fraud_label` |

---

## 4. `hdfs:///features/device_velocity` — 10-minute velocity aggregation

A separate stateful streaming query, refreshed every 60s, one row per
(device_id, 10-minute window).

| Column | Type | Notes |
|---|---|---|
| device_id | string | |
| window_start | timestamp | start of the trailing 10-min window |
| window_end | timestamp | end of the trailing 10-min window |
| txn_count_10min | long | number of transactions by this device in the window |
| amount_sum_10min | double | sum of transaction amounts in the window |

Use this to join against a transaction's `device_id` + `event_time` for a
velocity-based fraud signal.

---

## 5. `hdfs:///features/feature_pipeline` — fitted feature-transformation pipeline

**Not a dataframe** — this is a fitted Spark ML `PipelineModel` (StringIndexer
+ OneHotEncoder + VectorAssembler), saved by `feature_transformation.py`.

Load it like this:
```python
from pyspark.ml import PipelineModel
pipeline = PipelineModel.load("hdfs://namenode:9000/features/feature_pipeline")
transformed_df = pipeline.transform(cleaned_df)
# transformed_df now has a "features" column (Vector) ready for a classifier
```

Input columns it expects: `merchant_category`, `card_type`, `country`
(categorical) and `amount`, `txn_hour`, `is_foreign` (numeric) — i.e. exactly
the columns produced in section 1 above.

Output: adds a `features` column (dense/sparse Vector) via
`VectorAssembler`. **Note:** numeric columns are not currently scaled
(no `StandardScaler`) — keep this in mind if using a distance/gradient-based
model like Logistic Regression.

---

## 6. `hdfs:///models/fraud_model` — full trained model (used by the streaming job)

Fitted by `train_fraud_model.py`. Same feature list as above, plus a final
classifier stage (`LogisticRegression` or `RandomForestClassifier`). Loaded
directly by `streaming_fraud_pipeline.py` for real-time inference — you
normally don't need to touch this unless retraining.

---

## 7. `hdfs:///features/feature_importance` (Random Forest only)

CSV with two columns: `feature`, `importance` — sorted descending.

## 8. `hdfs:///features/metrics_lr` / `metrics_rf`

Single-line JSON text file: `{"model": ..., "auc": ..., "accuracy": ...,
"weightedPrecision": ..., "weightedRecall": ..., "f1": ...}`

---

## Feature list (kept in sync across all 3 scripts)

```python
CATEGORICAL_COLS = ["merchant_category", "card_type", "country"]
NUMERIC_COLS = ["amount", "txn_hour", "is_foreign"]
```
If you add/remove a feature, update it in all three: `streaming_fraud_pipeline.py`,
`feature_transformation.py`, `train_fraud_model.py`.
