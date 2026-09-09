from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.appName("InspectFraudData").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    RAW_PATH = "hdfs://namenode:9000/raw/credit_card_fraud.csv"

    print("Reading data...")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH)

    print("\n=== PRINT SCHEMA ===")
    df.printSchema()

    print("\n=== SAMPLE ROWS ===")
    rows = df.take(2)
    for i, r in enumerate(rows):
        print(f"Row {i}:")
        for k, v in r.asDict().items():
            print(f"  {k:35s}: {v}")

    print("\n=== CARDINALITY & NULLS (on sample) ===")
    sample_df = df.sample(withReplacement=False, fraction=0.01, seed=42).limit(50000).cache()
    sample_count = sample_df.count()
    print(f"Sample count: {sample_count}")

    for c in df.columns:
        dt = str(df.schema[c].dataType)
        null_cnt = sample_df.filter(col(c).isNull()).count()
        dist_cnt = sample_df.select(c).distinct().count()
        print(f"{c:35s} | {dt:15s} | nulls: {null_cnt:6d} | distinct_sample: {dist_cnt:6d}")

    print("\n=== LABEL DISTRIBUTION (FULL DATASET) ===")
    df.groupBy("is_fraud").count().show()

    spark.stop()

if __name__ == "__main__":
    main()
