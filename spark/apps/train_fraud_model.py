"""
Lab: Spark MLlib - train the fraud classifier
------------------------------------------------
Proposal mapping: "5A. Machine Learning Models (Spark MLlib)"

Reads the historical batch from HDFS /raw (written once by the `producer`
container), does type casting + feature engineering, trains a Random Forest
classifier, evaluates it, and saves:

  hdfs:///models/fraud_model         <- the fitted Pipeline (used for
                                         real-time inference by
                                         streaming_fraud_pipeline.py)
  hdfs:///features/metrics.json      <- evaluation metrics

Real CSV schema (34 columns, from producer/producer.py):
  transaction_amount, transaction_amount_bin, avg_amount_deviation_sigma,
  merchant_category, merchant_risk_score, merchant_category_vs_history,
  geo_velocity_kmh, geo_velocity_bin, geo_distance_km, geo_distance_bin,
  merchant_location, country_consistency, ip_address_type,
  device_fingerprint_match, session_duration_sec, session_duration_bin,
  network_carrier_type, cards_on_device_30d, cvv_match_status,
  three_ds_auth_result, tokenization_used, failed_attempts_before_success,
  transaction_velocity_1h, time_of_transaction,
  account_credential_change_recency, card_present_cnp, order_shipping_speed,
  account_age_days, chargeback_history_count, customer_id, transaction_id,
  segment, customer_type, is_fraud

Run:
    docker exec -it spark-master /spark/bin/spark-submit \\
        --master spark://spark-master:7077 \\
        --driver-memory 2g --executor-memory 2g \\
        /opt/spark-apps/train_fraud_model.py
"""
import json
import time

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

RAW_PATH = "hdfs://namenode:9000/raw/creditcard_transactions_historical.csv"
MODEL_PATH = "hdfs://namenode:9000/models/fraud_model"
FEATURES_DIR = "hdfs://namenode:9000/features"

# Categorical features: low-cardinality string columns (excluding identifiers)
CATEGORICAL_COLS = [
    "transaction_amount_bin",
    "merchant_category",
    "merchant_risk_score",
    "merchant_category_vs_history",
    "geo_velocity_bin",
    "geo_distance_bin",
    "merchant_location",
    "country_consistency",
    "ip_address_type",
    "device_fingerprint_match",
    "session_duration_bin",
    "network_carrier_type",
    "cvv_match_status",
    "three_ds_auth_result",
    "tokenization_used",
    "transaction_velocity_1h",
    "time_of_transaction",
    "account_credential_change_recency",
    "card_present_cnp",
    "order_shipping_speed",
]

# Numeric features (continuous/count)
NUMERIC_COLS = [
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

# Excluded: customer_id, transaction_id -> identifiers (leakage/cardinality)
# Excluded: segment, customer_type -> zero variance in sample (1 distinct value)
# Excluded: is_fraud -> label
# Excluded: event_timestamp -> not in CSV (added by producer at runtime)

LABEL_COL = "is_fraud"


def build_pipeline() -> Pipeline:
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in CATEGORICAL_COLS
    ]
    encoders = [
        OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec", handleInvalid="keep")
        for c in CATEGORICAL_COLS
    ]
    assembler = VectorAssembler(
        inputCols=[f"{c}_vec" for c in CATEGORICAL_COLS] + NUMERIC_COLS,
        outputCol="features",
        handleInvalid="keep",
    )
    classifier = RandomForestClassifier(
        labelCol=LABEL_COL,
        featuresCol="features",
        weightCol="class_weight",
        numTrees=100,
        maxDepth=8,
        seed=42,
    )
    return Pipeline(stages=indexers + encoders + [assembler, classifier])


def main():
    start_time = time.time()

    spark = SparkSession.builder \
        .appName("TrainFraudModel") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading dataset from {RAW_PATH}...")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH).limit(1000000)  # Limit to 1M rows for faster training in this example

    # Cast numeric columns to double
    for c in NUMERIC_COLS:
        df = df.withColumn(c, col(c).cast("double"))
    df = df.withColumn(LABEL_COL, col(LABEL_COL).cast("double"))

    total_count = df.count()
    fraud_count = df.filter(col(LABEL_COL) == 1.0).count()
    normal_count = df.filter(col(LABEL_COL) == 0.0).count()
    fraud_pct = (fraud_count / total_count) * 100.0 if total_count > 0 else 0

    print(f"Dataset stats: Total={total_count}, Normal={normal_count}, "
          f"Fraud={fraud_count}, Fraud %={fraud_pct:.4f}%")

    # Class weighting for imbalance
    weight_fraud = total_count / (2.0 * fraud_count) if fraud_count > 0 else 1.0
    weight_normal = total_count / (2.0 * normal_count) if normal_count > 0 else 1.0
    df = df.withColumn("class_weight",
                       when(col(LABEL_COL) == 1.0, weight_fraud).otherwise(weight_normal))

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    pipeline = build_pipeline()
    print("Fitting Random Forest pipeline...")
    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)

    # Evaluation
    auc = BinaryClassificationEvaluator(
        labelCol=LABEL_COL, rawPredictionCol="rawPrediction",
        metricName="areaUnderROC").evaluate(predictions)
    pr_auc = BinaryClassificationEvaluator(
        labelCol=LABEL_COL, rawPredictionCol="rawPrediction",
        metricName="areaUnderPR").evaluate(predictions)
    accuracy = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL, metricName="accuracy").evaluate(predictions)
    precision = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL, metricName="weightedPrecision").evaluate(predictions)
    recall = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL, metricName="weightedRecall").evaluate(predictions)
    f1 = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL, metricName="f1").evaluate(predictions)

    tp = predictions.filter((col(LABEL_COL) == 1.0) & (col("prediction") == 1.0)).count()
    fp = predictions.filter((col(LABEL_COL) == 0.0) & (col("prediction") == 1.0)).count()
    fn = predictions.filter((col(LABEL_COL) == 1.0) & (col("prediction") == 0.0)).count()
    tn = predictions.filter((col(LABEL_COL) == 0.0) & (col("prediction") == 0.0)).count()
    fraud_precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    fraud_recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0

    metrics = {
        "model": "rf",
        "total_rows": total_count,
        "train_rows": train_df.count(),
        "test_rows": test_df.count(),
        "normal_count": normal_count,
        "fraud_count": fraud_count,
        "fraud_pct": fraud_pct,
        "accuracy": accuracy,
        "weightedPrecision": precision,
        "weightedRecall": recall,
        "f1": f1,
        "auc": auc,
        "pr_auc": pr_auc,
        "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "fraud_precision": fraud_precision,
        "fraud_recall": fraud_recall,
    }

    print("=== Model evaluation ===")
    for k, v in metrics.items():
        print(f"  {k}: {v}")

    model.write().overwrite().save(MODEL_PATH)
    print(f"Model saved to {MODEL_PATH}")

    spark.sparkContext.parallelize([json.dumps(metrics)]).coalesce(1).saveAsTextFile(
        f"{FEATURES_DIR}/metrics_rf"
    )

    spark.stop()
    print(f"Total time: {time.time() - start_time:.2f}s")


if __name__ == "__main__":
    main()
