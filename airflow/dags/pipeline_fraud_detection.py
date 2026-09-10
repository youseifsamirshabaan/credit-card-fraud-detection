"""
Fraud Detection pipeline orchestration
--------------------------------------

Airflow owns the periodic orchestration layer:

1. Ensure HDFS directories
2. Check Kafka pipeline health
3. Run the official Spark Random Forest trainer
4. Notify that the model was refreshed

The producer and Spark Streaming services run continuously
through Docker Compose and are not started by this DAG.

The training task uses the same trainer as the main Spark pipeline:
    /opt/airflow/spark-apps/train_fraud_model.py

This keeps Airflow training consistent with the current 34-column
dataset schema and the Random Forest model used by Spark Streaming.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "instructor",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def ensure_hdfs_dirs():
    from hdfs import InsecureClient

    client = InsecureClient(
        "http://namenode:9870",
        user="root",
    )

    for path in [
        "/raw",
        "/stream",
        "/processed",
        "/features",
        "/models",
        "/predictions",
        "/checkpoints",
    ]:
        client.makedirs(path)

    print(
        "HDFS directory layer ready: "
        "/raw /stream /processed /features /models "
        "/predictions /checkpoints"
    )


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
            "Topic 'transactions_raw' has no partitions. "
            "Check Kafka and the producer."
        )

    print(
        f"transactions_raw is live with partitions: {partitions}"
    )


def notify_dashboards_ready(**context):
    ts = context["ts"]

    print(
        f"Pipeline run {ts} complete. "
        "The Random Forest fraud model was refreshed at "
        "hdfs://namenode:9000/models/fraud_model. "
        "Superset reads PostgreSQL live. "
        "Restart spark-streaming-job if the running streaming "
        "process needs to load the newly trained model."
    )


with DAG(
    dag_id="pipeline_fraud_detection",
    description=(
        "Credit Card Fraud Detection: "
        "HDFS + Kafka health check + Spark Random Forest retraining"
    ),
    default_args=default_args,
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fraud-detection", "integration"],
) as dag:

    t1 = PythonOperator(
        task_id="ensure_hdfs_dirs",
        python_callable=ensure_hdfs_dirs,
    )

    t2 = PythonOperator(
        task_id="check_pipeline_health",
        python_callable=check_pipeline_health,
    )

    t3 = BashOperator(
        task_id="train_or_update_model",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/airflow/spark-apps/train_fraud_model.py"
        ),
    )

    t4 = PythonOperator(
        task_id="notify_dashboards_ready",
        python_callable=notify_dashboards_ready,
    )

    t1 >> t2 >> t3 >> t4