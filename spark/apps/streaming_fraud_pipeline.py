"""
Lab: Real-time fraud detection pipeline
Spark Structured Streaming + MLlib

Flow:
Kafka transactions_raw
        |
        v
New 34-column schema
        |
        v
Cleaning + Feature Engineering
        |
        v
Compatibility layer for Hania's ML model
        |
        v
fraud model inference
        |
        +--> transactions_stream
        +--> fraud_predictions
        +--> enriched_transactions
        +--> alerts
        +--> HDFS
        +--> PostgreSQL
"""

import os

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    hour,
    struct,
    to_json,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Numeric columns that must be cast to DoubleType for the model
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


# ============================================================
# CONFIG
# ============================================================

KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP",
    "kafka:9092",
)

RAW_TOPIC = os.environ.get(
    "RAW_TOPIC",
    "transactions_raw",
)

STREAM_TOPIC = os.environ.get(
    "STREAM_TOPIC",
    "transactions_stream",
)

PREDICTIONS_TOPIC = os.environ.get(
    "PREDICTIONS_TOPIC",
    "fraud_predictions",
)

ENRICHED_TOPIC = os.environ.get(
    "ENRICHED_TOPIC",
    "enriched_transactions",
)

ALERTS_TOPIC = os.environ.get(
    "ALERTS_TOPIC",
    "alerts",
)

MODEL_PATH = (
    "hdfs://namenode:9000/models/fraud_model"
)

STREAM_HDFS_PATH = (
    "hdfs://namenode:9000/stream"
)

PROCESSED_HDFS_PATH = (
    "hdfs://namenode:9000/processed"
)

PREDICTIONS_HDFS_PATH = (
    "hdfs://namenode:9000/predictions"
)

CHECKPOINT_BASE = (
    "hdfs://namenode:9000/checkpoints/fraud_pipeline"
)

POSTGRES_DW_DB = os.environ.get(
    "POSTGRES_DW_DB",
    "frauddb",
)

POSTGRES_DW_USER = os.environ.get(
    "POSTGRES_DW_USER",
    "fraud_etl",
)

POSTGRES_DW_PASSWORD = os.environ.get(
    "POSTGRES_DW_PASSWORD",
    "fraud_etl",
)

JDBC_URL = (
    f"jdbc:postgresql://postgres-dw:5432/{POSTGRES_DW_DB}"
)

JDBC_PROPS = {
    "user": POSTGRES_DW_USER,
    "password": POSTGRES_DW_PASSWORD,
    "driver": "org.postgresql.Driver",
}

ALERT_THRESHOLD = float(
    os.environ.get("ALERT_THRESHOLD", "0.7")
)

MAX_REASONABLE_AMOUNT = float(
    os.environ.get("MAX_REASONABLE_AMOUNT", "20000")
)


# ============================================================
# NEW DATASET SCHEMA
# ============================================================

NEW_SCHEMA = StructType([
    StructField("transaction_amount", StringType()),
    StructField("transaction_amount_bin", StringType()),
    StructField("avg_amount_deviation_sigma", StringType()),
    StructField("merchant_category", StringType()),
    StructField("merchant_risk_score", StringType()),
    StructField("merchant_category_vs_history", StringType()),
    StructField("geo_velocity_kmh", StringType()),
    StructField("geo_velocity_bin", StringType()),
    StructField("geo_distance_km", StringType()),
    StructField("geo_distance_bin", StringType()),
    StructField("merchant_location", StringType()),
    StructField("country_consistency", StringType()),
    StructField("ip_address_type", StringType()),
    StructField("device_fingerprint_match", StringType()),
    StructField("session_duration_sec", StringType()),
    StructField("session_duration_bin", StringType()),
    StructField("network_carrier_type", StringType()),
    StructField("cards_on_device_30d", StringType()),
    StructField("cvv_match_status", StringType()),
    StructField("three_ds_auth_result", StringType()),
    StructField("tokenization_used", StringType()),
    StructField("failed_attempts_before_success", StringType()),
    StructField("transaction_velocity_1h", StringType()),
    StructField("time_of_transaction", StringType()),
    StructField("account_credential_change_recency", StringType()),
    StructField("card_present_cnp", StringType()),
    StructField("order_shipping_speed", StringType()),
    StructField("account_age_days", StringType()),
    StructField("chargeback_history_count", StringType()),
    StructField("customer_id", StringType()),
    StructField("transaction_id", StringType()),
    StructField("segment", StringType()),
    StructField("customer_type", StringType()),
    StructField("is_fraud", StringType()),
    StructField("event_timestamp", StringType()),
])


# ============================================================
# KAFKA OUTPUT
# ============================================================

def to_kafka(df, topic):

    (
        df.select(
            to_json(
                struct(*df.columns)
            ).alias("value")
        )
        .write
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP,
        )
        .option(
            "topic",
            topic,
        )
        .save()
    )


# ============================================================
# MAIN
# ============================================================

def main():

    spark = (
        SparkSession.builder
        .appName("FraudStreamingPipeline")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("Loading fraud model...")

    model = PipelineModel.load(
        MODEL_PATH
    )

    print(
        f"Fraud model loaded from {MODEL_PATH}"
    )


    # ========================================================
    # READ FROM KAFKA
    # ========================================================

    raw = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP,
        )
        .option(
            "subscribe",
            RAW_TOPIC,
        )
        .option(
            "startingOffsets",
            "latest",
        )
        .load()
    )


    # ========================================================
    # PARSE NEW 34-COLUMN SCHEMA
    # ========================================================

    parsed = (
        raw
        .selectExpr(
            "CAST(value AS STRING) AS json_str"
        )
        .select(
            from_json(
                col("json_str"),
                NEW_SCHEMA,
            ).alias("d")
        )
        .select("d.*")
    )


    # ========================================================
    # CLEANING + TYPE CONVERSION
    # ========================================================

    cleaned = (
        parsed

        .filter(
            col("transaction_id").isNotNull()
        )

        .withColumn(
            "amount",
            col("transaction_amount").cast(
                DoubleType()
            )
        )

        .withColumn(
            "fraud_label",
            col("is_fraud").cast(
                IntegerType()
            )
        )

        .withColumn(
            "event_time",
            to_timestamp(
                col("event_timestamp")
            )
        )

        .filter(
            col("amount").isNotNull()
        )

        .filter(
            col("amount") >= 0
        )

        .filter(
            col("amount") <= MAX_REASONABLE_AMOUNT
        )

        .filter(
            col("event_time").isNotNull()
        )

        .dropDuplicates(
            ["transaction_id"]
        )

        .withColumn(
            "txn_hour",
            hour(col("event_time"))
        )
    )


    # ========================================================
    # The trained model reads the real 34-column schema
    # directly. No compatibility mapping needed.
    # Numeric columns must be explicitly cast to DoubleType.
    # ========================================================
    # (model_input is now just cleaned with numeric casts,
    #  applied per-batch inside process_batch)


    # ========================================================
    # PROCESS EACH MICRO-BATCH
    # ========================================================

    def process_batch(batch_df, batch_id):

        if batch_df.rdd.isEmpty():

            print(
                f"[batch {batch_id}] empty, skipping"
            )

            return


        batch_df.persist()

        count = batch_df.count()

        print(
            f"[batch {batch_id}] "
            f"{count} transactions"
        )


        # ====================================================
        # CLEANED / FEATURE-ENGINEERED STREAM
        # ====================================================

        to_kafka(
            batch_df,
            STREAM_TOPIC,
        )

        (
            batch_df
            .write
            .mode("append")
            .parquet(
                STREAM_HDFS_PATH
            )
        )


        # ====================================================
        # ML INFERENCE
        # Cast numeric columns to DoubleType to match training
        # schema, then run the PipelineModel transform.
        # ====================================================

        batch_model_input = batch_df
        for _c in MODEL_NUMERIC_COLS:
            batch_model_input = batch_model_input.withColumn(
                _c, col(_c).cast(DoubleType())
            )

        predicted = model.transform(batch_model_input)

        predicted = (
            predicted
            .withColumn(
                "fraud_probability",
                vector_to_array(
                    col("probability")
                )[1]
            )
            .withColumnRenamed(
                "prediction",
                "predicted_label"
            )
        )

        # ====================================================
        # PREDICTION RESULT
        # ====================================================

        result = (
            predicted.select(
                "transaction_id",
                "event_time",
                "amount",
                "merchant_category",
                "merchant_location",
                "fraud_probability",
                "predicted_label",
                "fraud_label",
            )

            .withColumnRenamed(
                "fraud_label",
                "actual_label"
            )

            .withColumn(
                "predicted_label",
                col("predicted_label").cast(
                    "int"
                )
            )
        )


        result.persist()


        # ====================================================
        # HDFS PREDICTIONS
        # ====================================================

        (
            result
            .write
            .mode("append")
            .parquet(
                PREDICTIONS_HDFS_PATH
            )
        )


        # ====================================================
        # KAFKA PREDICTIONS
        # ====================================================

        to_kafka(
            result,
            PREDICTIONS_TOPIC,
        )


        # ====================================================
        # POSTGRES PREDICTIONS
        # ====================================================

        (
            result
            .write
            .format("jdbc")
            .option(
                "url",
                JDBC_URL,
            )
            .option(
                "dbtable",
                "predictions",
            )
            .options(
                **JDBC_PROPS
            )
            .mode("append")
            .save()
        )


        # ====================================================
        # ENRICHED TRANSACTIONS
        # ====================================================

        enriched = (
            batch_df.join(
                result.select(
                    "transaction_id",
                    "fraud_probability",
                    "predicted_label",
                ),
                "transaction_id",
            )
        )


        to_kafka(
            enriched,
            ENRICHED_TOPIC,
        )


        (
            enriched
            .write
            .mode("append")
            .parquet(
                PROCESSED_HDFS_PATH
            )
        )


        # ====================================================
        # HIGH-RISK ALERTS
        # ====================================================

        alerts = (
            result

            .filter(
                col("fraud_probability")
                >= ALERT_THRESHOLD
            )

            .withColumn(
                "risk_level",
                when(
                    col("fraud_probability")
                    >= 0.9,
                    "CRITICAL",
                ).otherwise(
                    "HIGH"
                ),
            )

            .select(
                "transaction_id",
                "event_time",
                "risk_level",
                "fraud_probability",
                "amount",
                "merchant_location",
            )
        )


        if not alerts.rdd.isEmpty():

            to_kafka(
                alerts,
                ALERTS_TOPIC,
            )


            (
                alerts
                .write
                .format("jdbc")
                .option(
                    "url",
                    JDBC_URL,
                )
                .option(
                    "dbtable",
                    "alerts",
                )
                .options(
                    **JDBC_PROPS
                )
                .mode("append")
                .save()
            )


        result.unpersist()
        batch_df.unpersist()


    # ========================================================
    # START STREAMING QUERY
    # ========================================================

    query = (
        cleaned.writeStream
        .foreachBatch(
            process_batch
        )
        .option(
            "checkpointLocation",
            CHECKPOINT_BASE,
        )
        .trigger(
            processingTime="15 seconds"
        )
        .start()
    )


    print(
        "Fraud streaming pipeline started."
    )

    query.awaitTermination()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()

