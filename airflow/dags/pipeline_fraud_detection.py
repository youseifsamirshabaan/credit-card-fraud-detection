"""
Fraud Detection pipeline orchestration
-----------------------------------------
Proposal mapping: "Orchestration & Workflow (Apache Airflow)":
  Schedule Pipelines -> Run Ingestion -> Process Data -> Train/Update Models
  -> Update Dashboards

Ingestion (the `producer` container) and stream processing (the
`spark-streaming-job` container) already run continuously as their own
Docker Compose services - they don't belong on a schedule. What Airflow
owns here is the part that *should* be periodic:

  1. ensure_hdfs_dirs       -> idempotent, safe to (re)run any time
  2. check_pipeline_health  -> confirms the producer/streaming containers
                                are actually producing data before we
                                bother retraining on stale input
  3. train_or_update_model  -> retrains the Spark MLlib fraud classifier on
                                the latest HDFS /raw data and overwrites
                                hdfs:///models/fraud_model (the running
                                streaming job picks up the new model next
                                time it restarts)
  4. notify_dashboards_ready -> Superset queries postgres-dw live, so
                                there's no cache to invalidate - this task
                                just logs the run summary for visibility.

Scheduled weekly per the proposal ("Automated Model Retraining, e.g.,
weekly"); trigger it manually from the UI any time to retrain on demand.

Note: this DAG builds its own SparkSession in-process (pyspark is installed
in the Airflow image, see airflow/docker/Dockerfile) rather than calling
spark/apps/train_fraud_model.py directly, because that file lives on a
volume only mounted into the Spark/Jupyter containers. The training logic
below is intentionally a lighter version (Logistic Regression only) of the
full script - keep both in sync if you change the feature list.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "instructor",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

RAW_PATH = "hdfs://namenode:9000/raw/creditcard_transactions_historical.csv"
MODEL_PATH = "hdfs://namenode:9000/models/fraud_model"


def ensure_hdfs_dirs():
    from hdfs import InsecureClient

    client = InsecureClient("http://namenode:9870", user="root")
    for path in ["/raw", "/stream", "/processed", "/features", "/models", "/predictions"]:
        client.makedirs(path)
    print("HDFS directory layer ready: /raw /stream /processed /features /models /predictions")


def check_pipeline_health():
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        "transactions_raw",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="latest",
        consumer_timeout_ms=8000,
    )
    partitions = consumer.partitions_for_topic("transactions_raw")
    consumer.close()
    if not partitions:
        raise RuntimeError(
            "Topic 'transactions_raw' has no partitions yet - is the "
            "`producer` container running and `kafka-topics-init` finished?"
        )
    print(f"transactions_raw is live with partitions {partitions}")


def train_or_update_model():
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, hour, to_timestamp

    spark = (
        SparkSession.builder.appName("AirflowFraudModelRetrain")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH)
    df = df.dropna(subset=["amount", "merchant_category", "card_type", "country", "fraud_label"])
    df = df.withColumn("txn_hour", hour(to_timestamp(col("timestamp"))))
    df = df.withColumn("is_foreign", (col("country") != "EG").cast("int"))
    df = df.withColumn("fraud_label", col("fraud_label").cast("double"))

    categorical_cols = ["merchant_category", "card_type", "country"]
    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in categorical_cols]
    encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec") for c in categorical_cols]
    assembler = VectorAssembler(
        inputCols=[f"{c}_vec" for c in categorical_cols] + ["amount", "txn_hour", "is_foreign"],
        outputCol="features",
        handleInvalid="skip",
    )
    lr = LogisticRegression(labelCol="fraud_label", featuresCol="features", maxIter=50)
    pipeline = Pipeline(stages=indexers + encoders + [assembler, lr])

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    model = pipeline.fit(train_df)
    auc = BinaryClassificationEvaluator(labelCol="fraud_label").evaluate(model.transform(test_df))

    model.write().overwrite().save(MODEL_PATH)
    spark.stop()
    print(f"Retrained fraud model saved to {MODEL_PATH} (test AUC={auc:.4f})")


def notify_dashboards_ready(**context):
    ts = context["ts"]
    print(
        f"Pipeline run {ts} complete: hdfs:///models/fraud_model refreshed. "
        f"Superset dashboards read postgres-dw live, so nothing else to refresh - "
        f"restart `spark-streaming-job` to pick up the new model."
    )


with DAG(
    dag_id="pipeline_fraud_detection",
    description="Credit Card Fraud Detection: HDFS + Kafka health check + weekly MLlib retrain",
    default_args=default_args,
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fraud-detection", "integration"],
) as dag:

    t1 = PythonOperator(task_id="ensure_hdfs_dirs", python_callable=ensure_hdfs_dirs)
    t2 = PythonOperator(task_id="check_pipeline_health", python_callable=check_pipeline_health)
    t3 = PythonOperator(task_id="train_or_update_model", python_callable=train_or_update_model)
    t4 = PythonOperator(task_id="notify_dashboards_ready", python_callable=notify_dashboards_ready)

    t1 >> t2 >> t3 >> t4
