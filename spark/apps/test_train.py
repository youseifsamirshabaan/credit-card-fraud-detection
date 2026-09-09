import argparse
import json
import time

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

RAW_PATH = "hdfs://namenode:9000/raw/credit_card_fraud.csv"
MODEL_PATH = "hdfs://namenode:9000/models/fraud_model"
FEATURES_DIR = "hdfs://namenode:9000/features"

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

LABEL_COL = "is_fraud"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample_fraction", type=float, default=1.0)
    args = parser.parse_args()

    start_time = time.time()

    spark = SparkSession.builder \
        .appName("TrainFraudModelRF") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading dataset from {RAW_PATH}...")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH)

    # Cast numeric columns properly
    for c in NUMERIC_COLS:
        df = df.withColumn(c, col(c).cast("double"))
    df = df.withColumn(LABEL_COL, col(LABEL_COL).cast("double"))

    if args.sample_fraction < 1.0:
        print(f"Sampling dataset with fraction {args.sample_fraction}...")
        df = df.sample(withReplacement=False, fraction=args.sample_fraction, seed=42)

    df = df.cache()
    total_count = df.count()
    print(f"Total dataset count used: {total_count}")

    # Compute class weights for imbalance handling
    fraud_count = df.filter(col(LABEL_COL) == 1.0).count()
    normal_count = df.filter(col(LABEL_COL) == 0.0).count()
    fraud_pct = (fraud_count / total_count) * 100.0 if total_count > 0 else 0

    print(f"Dataset stats: Normal={normal_count}, Fraud={fraud_count}, Fraud %={fraud_pct:.4f}%")

    weight_fraud = total_count / (2.0 * fraud_count) if fraud_count > 0 else 1.0
    weight_normal = total_count / (2.0 * normal_count) if normal_count > 0 else 1.0
    print(f"Computed weights: Normal={weight_normal:.4f}, Fraud={weight_fraud:.4f}")

    df = df.withColumn("class_weight", when(col(LABEL_COL) == 1.0, weight_fraud).otherwise(weight_normal))

    # Pipeline stages
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

    pipeline = Pipeline(stages=indexers + encoders + [assembler, classifier])

    print("Splitting train/test (80/20)...")
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    train_count = train_df.count()
    test_count = test_df.count()
    print(f"Train rows: {train_count}, Test rows: {test_count}")

    print("Fitting Random Forest pipeline...")
    fit_start = time.time()
    model = pipeline.fit(train_df)
    fit_duration = time.time() - fit_start
    print(f"Model fitting completed in {fit_duration:.2f} seconds.")

    print("Transforming test data and evaluating...")
    predictions = model.transform(test_df).cache()

    # Evaluation
    roc_eval = BinaryClassificationEvaluator(labelCol=LABEL_COL, rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    pr_eval = BinaryClassificationEvaluator(labelCol=LABEL_COL, rawPredictionCol="rawPrediction", metricName="areaUnderPR")

    roc_auc = roc_eval.evaluate(predictions)
    pr_auc = pr_eval.evaluate(predictions)

    acc_eval = MulticlassClassificationEvaluator(labelCol=LABEL_COL, metricName="accuracy")
    prec_eval = MulticlassClassificationEvaluator(labelCol=LABEL_COL, metricName="weightedPrecision")
    rec_eval = MulticlassClassificationEvaluator(labelCol=LABEL_COL, metricName="weightedRecall")
    f1_eval = MulticlassClassificationEvaluator(labelCol=LABEL_COL, metricName="f1")

    accuracy = acc_eval.evaluate(predictions)
    precision = prec_eval.evaluate(predictions)
    recall = rec_eval.evaluate(predictions)
    f1 = f1_eval.evaluate(predictions)

    # Confusion Matrix & Fraud-specific Precision/Recall
    tp = predictions.filter((col(LABEL_COL) == 1.0) & (col("prediction") == 1.0)).count()
    fp = predictions.filter((col(LABEL_COL) == 0.0) & (col("prediction") == 1.0)).count()
    fn = predictions.filter((col(LABEL_COL) == 1.0) & (col("prediction") == 0.0)).count()
    tn = predictions.filter((col(LABEL_COL) == 0.0) & (col("prediction") == 0.0)).count()

    fraud_precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    fraud_recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0

    print("\n=================== EVALUATION RESULTS ===================")
    print(f"Total Rows Used     : {total_count}")
    print(f"Train Rows          : {train_count}")
    print(f"Test Rows           : {test_count}")
    print(f"Normal Rows         : {normal_count}")
    print(f"Fraud Rows          : {fraud_count}")
    print(f"Fraud Percentage    : {fraud_pct:.4f}%")
    print(f"Accuracy            : {accuracy:.4f}")
    print(f"Weighted Precision  : {precision:.4f}")
    print(f"Weighted Recall     : {recall:.4f}")
    print(f"F1-Score            : {f1:.4f}")
    print(f"ROC-AUC             : {roc_auc:.4f}")
    print(f"PR-AUC              : {pr_auc:.4f}")
    print(f"Confusion Matrix    : TP={tp}, FP={fp}, FN={fn}, TN={tn}")
    print(f"Fraud Precision     : {fraud_precision:.4f}")
    print(f"Fraud Recall        : {fraud_recall:.4f}")
    print("==========================================================")

    # Feature Importance
    rf_model = model.stages[-1]
    assembler_stage = model.stages[-2]

    # Map assembler output vector names to importance
    # Note: OneHotEncoder expands categorical features, so we write to HDFS
    print(f"Saving model to {MODEL_PATH}...")
    model.write().overwrite().save(MODEL_PATH)
    print("Model save verified!")

    total_duration = time.time() - start_time
    print(f"Total script execution time: {total_duration:.2f} seconds.")

    spark.stop()

if __name__ == "__main__":
    main()
