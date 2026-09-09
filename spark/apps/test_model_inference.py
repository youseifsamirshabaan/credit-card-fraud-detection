from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

MODEL_PATH = "hdfs://namenode:9000/models/fraud_model"
RAW_PATH = "hdfs://namenode:9000/raw/credit_card_fraud.csv"

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

def main():
    spark = SparkSession.builder.appName("TestModelInference").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Loading trained model from {MODEL_PATH}...")
    model = PipelineModel.load(MODEL_PATH)
    print("Model loaded successfully!")

    print(f"Reading sample data from {RAW_PATH}...")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH).limit(10)

    for c in NUMERIC_COLS:
        df = df.withColumn(c, col(c).cast("double"))

    print("Running inference transform...")
    predictions = model.transform(df)

    results = predictions.withColumn("fraud_probability", vector_to_array(col("probability"))[1]) \
        .select("transaction_id", "transaction_amount", "merchant_category", "is_fraud", "prediction", "fraud_probability")

    print("=== INFERENCE TEST RESULTS ===")
    results.show(truncate=False)

    print("Inference test passed cleanly!")
    spark.stop()

if __name__ == "__main__":
    main()
