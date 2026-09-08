"""
Lab: Spark MLlib - train the fraud classifier
------------------------------------------------
Proposal mapping: "5A. Machine Learning Models (Spark MLlib)"

Reads the historical batch from HDFS /raw (written once by the `producer`
container), does light cleaning + feature engineering, trains a
classification model (Logistic Regression by default, Random Forest with
--model rf), evaluates it, and saves:

  hdfs:///models/fraud_model         <- the fitted Pipeline (used for
                                         real-time inference by
                                         streaming_fraud_pipeline.py)
  hdfs:///features/feature_importance.csv  <- feature importances (rf only)
  hdfs:///features/metrics.json      <- accuracy / precision / recall / F1

Run:
    docker exec -it spark-master spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-apps/train_fraud_model.py

    # Random Forest instead of Logistic Regression:
    docker exec -it spark-master spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-apps/train_fraud_model.py --model rf

This is also what the weekly `train_or_update_model` task in
airflow/dags/pipeline_fraud_detection.py re-implements inline (Airflow's
image doesn't have this file mounted, only pyspark) - keep the two in sync
if you change the feature list.
"""
import argparse
import json

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp

RAW_PATH = "hdfs://namenode:9000/raw/creditcard_transactions_historical.csv"
MODEL_PATH = "hdfs://namenode:9000/models/fraud_model"
FEATURES_DIR = "hdfs://namenode:9000/features"

CATEGORICAL_COLS = ["merchant_category", "card_type", "country"]
NUMERIC_COLS = ["amount", "txn_hour", "is_foreign"]
LABEL_COL = "fraud_label"


def build_pipeline(model_type: str) -> Pipeline:
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in CATEGORICAL_COLS
    ]
    encoders = [
        OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec")
        for c in CATEGORICAL_COLS
    ]
    assembler = VectorAssembler(
        inputCols=[f"{c}_vec" for c in CATEGORICAL_COLS] + NUMERIC_COLS,
        outputCol="features",
        handleInvalid="skip",
    )

    if model_type == "rf":
        classifier = RandomForestClassifier(
            labelCol=LABEL_COL, featuresCol="features", numTrees=100, maxDepth=8, seed=42
        )
    else:
        classifier = LogisticRegression(labelCol=LABEL_COL, featuresCol="features", maxIter=50)

    return Pipeline(stages=indexers + encoders + [assembler, classifier])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", choices=["lr", "rf"], default="lr")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("TrainFraudModel").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH)
    )

    # --- Data Cleaning & Validation ---
    df = df.dropna(subset=["amount", "merchant_category", "card_type", "country", LABEL_COL])
    df = df.filter(col("amount") >= 0)

    # --- Feature Engineering ---
    df = df.withColumn("txn_hour", hour(to_timestamp(col("timestamp"))))
    df = df.withColumn("is_foreign", (col("country") != "EG").cast("int"))
    df = df.withColumn(LABEL_COL, col(LABEL_COL).cast("double"))

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    pipeline = build_pipeline(args.model)
    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)

    # --- Evaluation ---
    auc = BinaryClassificationEvaluator(labelCol=LABEL_COL, metricName="areaUnderROC").evaluate(predictions)
    metrics = {"model": args.model, "auc": auc}
    for metric_name in ["accuracy", "weightedPrecision", "weightedRecall", "f1"]:
        evaluator = MulticlassClassificationEvaluator(labelCol=LABEL_COL, metricName=metric_name)
        metrics[metric_name] = evaluator.evaluate(predictions)

    print("=== Model evaluation ===")
    for k, v in metrics.items():
        print(f"{k}: {v}")

    # --- Feature importance (Random Forest only) ---
    if args.model == "rf":
        rf_model = model.stages[-1]
        assembler_stage = model.stages[-2]
        importances = list(zip(assembler_stage.getInputCols(), rf_model.featureImportances.toArray()))
        importances.sort(key=lambda x: x[1], reverse=True)
        fi_df = spark.createDataFrame(importances, ["feature", "importance"])
        fi_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            f"{FEATURES_DIR}/feature_importance"
        )

    # --- Save model + metrics ---
    model.write().overwrite().save(MODEL_PATH)
    spark.sparkContext.parallelize([json.dumps(metrics)]).coalesce(1).saveAsTextFile(
        f"{FEATURES_DIR}/metrics_{args.model}"
    )

    print(f"Model saved to {MODEL_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
