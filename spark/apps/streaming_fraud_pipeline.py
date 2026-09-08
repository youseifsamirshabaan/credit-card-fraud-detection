"""
Lab: Real-time fraud detection pipeline (Spark Structured Streaming + MLlib)
------------------------------------------------------------------------------
Proposal mapping: "3. PROCESSING LAYER" end-to-end, in one continuously
running job (runs as its own container - see `spark-streaming-job` in
docker-compose.yml, no manual submission needed):

  transactions_raw (Kafka)
        |  clean + validate + feature engineer
        v
  transactions_stream (Kafka) + hdfs:///stream        <- cleaned/featurized
        |  Spark MLlib inference (loads hdfs:///models/fraud_model,
        |  trained by train_fraud_model.py)
        v
  fraud_predictions (Kafka) + hdfs:///predictions + postgres-dw.predictions
        |  join prediction back onto the full record
        v
  enriched_transactions (Kafka) + hdfs:///processed
        |  filter high-risk predictions
        v
  alerts (Kafka) + postgres-dw.alerts

Requires the fraud model to already exist at hdfs:///models/fraud_model -
run train_fraud_model.py at least once first (the `airflow` weekly DAG does
this automatically; you can also trigger it manually, see README).

Run (this is what the `spark-streaming-job` compose service already does
for you automatically on `docker compose up`):
    docker exec -it spark-master spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.7.3 \
        /opt/spark-apps/streaming_fraud_pipeline.py
"""
import os

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, hour, lit, struct, to_json, to_timestamp, when
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
RAW_TOPIC = os.environ.get("RAW_TOPIC", "transactions_raw")
STREAM_TOPIC = os.environ.get("STREAM_TOPIC", "transactions_stream")
PREDICTIONS_TOPIC = os.environ.get("PREDICTIONS_TOPIC", "fraud_predictions")
ENRICHED_TOPIC = os.environ.get("ENRICHED_TOPIC", "enriched_transactions")
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "alerts")

MODEL_PATH = "hdfs://namenode:9000/models/fraud_model"
STREAM_HDFS_PATH = "hdfs://namenode:9000/stream"
PROCESSED_HDFS_PATH = "hdfs://namenode:9000/processed"
PREDICTIONS_HDFS_PATH = "hdfs://namenode:9000/predictions"
CHECKPOINT_BASE = "hdfs://namenode:9000/checkpoints/fraud_pipeline"

POSTGRES_DW_DB = os.environ.get("POSTGRES_DW_DB", "frauddb")
POSTGRES_DW_USER = os.environ.get("POSTGRES_DW_USER", "fraud_etl")
POSTGRES_DW_PASSWORD = os.environ.get("POSTGRES_DW_PASSWORD", "fraud_etl")
JDBC_URL = f"jdbc:postgresql://postgres-dw:5432/{POSTGRES_DW_DB}"
JDBC_PROPS = {
    "user": POSTGRES_DW_USER,
    "password": POSTGRES_DW_PASSWORD,
    "driver": "org.postgresql.Driver",
}

ALERT_THRESHOLD = float(os.environ.get("ALERT_THRESHOLD", "0.7"))

SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_category", StringType()),
    StructField("card_type", StringType()),
    StructField("country", StringType()),
    StructField("city", StringType()),
    StructField("device_id", StringType()),
    StructField("ip_address", StringType()),
    StructField("fraud_label", IntegerType()),
])


def to_kafka(df, topic):
    (
        df.select(to_json(struct(*df.columns)).alias("value"))
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", topic)
        .save()
    )


def main():
    spark = SparkSession.builder.appName("FraudStreamingPipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    model = PipelineModel.load(MODEL_PATH)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), SCHEMA).alias("d"))
        .select("d.*")
    )

    # --- Data Cleaning & Validation + Feature Engineering -> "transactions_stream" ---
    cleaned = (
        parsed.filter(col("amount").isNotNull() & (col("amount") >= 0))
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withColumn("txn_hour", hour(col("event_time")))
        .withColumn("is_foreign", (col("country") != lit("EG")).cast("int"))
    )

    def process_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            print(f"[batch {batch_id}] empty, skipping")
            return
        batch_df.persist()
        print(f"[batch {batch_id}] {batch_df.count()} transactions")

        # publish the cleaned/featurized stream + land it in the data lake
        to_kafka(batch_df, STREAM_TOPIC)
        batch_df.write.mode("append").parquet(STREAM_HDFS_PATH)

        # --- Real-time Inference (Spark MLlib) ---
        predicted = model.transform(batch_df)
        predicted = predicted.withColumn(
            "fraud_probability", vector_to_array(col("probability"))[1]
        ).withColumnRenamed("prediction", "predicted_label")

        result = (
            predicted.select(
                "transaction_id", "event_time", "amount", "merchant_id",
                "merchant_category", "card_type", "country", "city",
                "fraud_probability", "predicted_label", "fraud_label",
            )
            .withColumnRenamed("fraud_label", "actual_label")
            .withColumn("predicted_label", col("predicted_label").cast("int"))
        )
        result.persist()

        result.write.mode("append").parquet(PREDICTIONS_HDFS_PATH)
        to_kafka(result, PREDICTIONS_TOPIC)
        (
            result.write.format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", "predictions")
            .options(**JDBC_PROPS)
            .mode("append")
            .save()
        )

        # --- enriched_transactions: original fields + prediction ---
        enriched = batch_df.join(
            result.select("transaction_id", "fraud_probability", "predicted_label"),
            "transaction_id",
        )
        to_kafka(enriched, ENRICHED_TOPIC)
        enriched.write.mode("append").parquet(PROCESSED_HDFS_PATH)

        # --- Alert Generation for High Risk transactions ---
        alerts = (
            result.filter(col("fraud_probability") >= ALERT_THRESHOLD)
            .withColumn(
                "risk_level",
                when(col("fraud_probability") >= 0.9, "CRITICAL").otherwise("HIGH"),
            )
            .select("transaction_id", "event_time", "risk_level", "fraud_probability", "amount", "country")
        )
        if not alerts.rdd.isEmpty():
            to_kafka(alerts, ALERTS_TOPIC)
            (
                alerts.write.format("jdbc")
                .option("url", JDBC_URL)
                .option("dbtable", "alerts")
                .options(**JDBC_PROPS)
                .mode("append")
                .save()
            )

        result.unpersist()
        batch_df.unpersist()

    query = (
        cleaned.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_BASE)
        .trigger(processingTime="15 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
