"""
Streaming compatibility test:
- Load the saved PipelineModel
- Simulate streaming batch with records from HDFS raw CSV
- Apply numeric casts (same as streaming_fraud_pipeline does)
- Call model.transform()
- Confirm predictions land correctly
"""
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour
from pyspark.sql.types import DoubleType, IntegerType

MODEL_PATH = "hdfs://namenode:9000/models/fraud_model"
RAW_PATH = "hdfs://namenode:9000/raw/credit_card_fraud.csv"

MODEL_NUMERIC_COLS = [
    "transaction_amount",
    "avg_amount_deviation_sigma",
    "geo_velocity_kmh",
    "geo_distance_km",
    "session_duration_sec",
    "cards_on_device_30d",
    "failed_attempts_before_success",
    "account_age_days",
    "chargeback_history_count",
]

def main():
    spark = SparkSession.builder.appName("StreamingCompatibilityTest").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ---- Step 1: Load model ----
    print(f"[1] Loading model from {MODEL_PATH}...")
    model = PipelineModel.load(MODEL_PATH)
    print(f"    Model loaded. Stages: {[type(s).__name__ for s in model.stages]}")

    # ---- Step 2: Simulate streaming batch ----
    print(f"\n[2] Reading 20 records from HDFS raw CSV (simulates Kafka micro-batch)...")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH).limit(20)

    # Simulate streaming cleaning (same as streaming_fraud_pipeline.py)
    cleaned = (
        df
        .filter(col("transaction_id").isNotNull())
        .withColumn("amount", col("transaction_amount").cast(DoubleType()))
        .withColumn("fraud_label", col("is_fraud").cast(IntegerType()))
        .filter(col("amount").isNotNull())
        .filter(col("amount") >= 0)
    )

    # ---- Step 3: Cast numeric cols to Double (same as streaming pipeline fix) ----
    print("\n[3] Applying numeric casts (streaming pipeline logic)...")
    batch_model_input = cleaned
    for c in MODEL_NUMERIC_COLS:
        batch_model_input = batch_model_input.withColumn(c, col(c).cast(DoubleType()))

    # ---- Step 4: Run model.transform() ----
    print("\n[4] Running model.transform() on simulated batch...")
    predicted = model.transform(batch_model_input)
    predicted = predicted.withColumn(
        "fraud_probability", vector_to_array(col("probability"))[1]
    ).withColumnRenamed("prediction", "predicted_label")

    result = predicted.select(
        "transaction_id",
        "transaction_amount",
        "merchant_category",
        "is_fraud",
        "predicted_label",
        "fraud_probability",
    )

    print("\n[5] Inference results on 20 simulated streaming records:")
    result.show(20, truncate=False)

    actual = result.select("is_fraud").rdd.flatMap(lambda x: x).collect()
    predicted_vals = result.select("predicted_label").rdd.flatMap(lambda x: x).collect()
    print(f"    Actual labels    : {actual}")
    print(f"    Predicted labels : {[int(p) for p in predicted_vals]}")

    # ---- Verify schema columns are present ----
    print("\n[6] Schema compatibility check:")
    model_input_cols = set(batch_model_input.columns)
    expected_categorical = [
        "transaction_amount_bin", "merchant_category", "merchant_risk_score",
        "merchant_category_vs_history", "geo_velocity_bin", "geo_distance_bin",
        "merchant_location", "country_consistency", "ip_address_type",
        "device_fingerprint_match", "session_duration_bin", "network_carrier_type",
        "cvv_match_status", "three_ds_auth_result", "tokenization_used",
        "transaction_velocity_1h", "time_of_transaction",
        "account_credential_change_recency", "card_present_cnp", "order_shipping_speed",
    ]
    expected_numeric = MODEL_NUMERIC_COLS

    missing_cat = [c for c in expected_categorical if c not in model_input_cols]
    missing_num = [c for c in expected_numeric if c not in model_input_cols]

    if missing_cat:
        print(f"    MISSING CATEGORICAL COLUMNS: {missing_cat}")
    else:
        print(f"    All 20 categorical feature columns: PRESENT")

    if missing_num:
        print(f"    MISSING NUMERIC COLUMNS: {missing_num}")
    else:
        print(f"    All 9 numeric feature columns: PRESENT")

    if not missing_cat and not missing_num:
        print("\n=== STREAMING COMPATIBILITY TEST: PASS ===")
    else:
        print("\n=== STREAMING COMPATIBILITY TEST: FAIL ===")

    spark.stop()

if __name__ == "__main__":
    main()
