"""
Lab: Feature Transformation Pipeline (Person 3's deliverable)
------------------------------------------------------------------------------
Builds and fits ONLY the feature-transformation stages (indexing,
one-hot encoding, vectorization, scaling) - no ML classifier included.

This is intentionally separate from train_fraud_model.py: Person 3 (data
processing) owns this pipeline and its feature list; Person 4 (ML) loads it
and appends a classifier as the final stage, instead of re-writing feature
engineering logic.

Output: hdfs:///features/feature_pipeline
    -> a fitted PipelineModel (StringIndexer + OneHotEncoder + VectorAssembler + StandardScaler)
       that turns raw cleaned columns into a single "features" vector column.

Run:
    docker exec -it spark-master spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-apps/feature_transformation.py
"""
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp

RAW_PATH = "hdfs://namenode:9000/raw/creditcard_transactions_historical.csv"
FEATURE_PIPELINE_PATH = "hdfs://namenode:9000/features/feature_pipeline"

# Same feature list used by both the batch trainer and the streaming job -
# keep this list in sync if you add/remove a feature anywhere.
CATEGORICAL_COLS = ["merchant_category", "card_type", "country"]
NUMERIC_COLS = ["amount", "txn_hour", "is_foreign"]


def build_feature_pipeline() -> Pipeline:
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in CATEGORICAL_COLS
    ]
    encoders = [
        OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec")
        for c in CATEGORICAL_COLS
    ]
    # 1. Assemble encoded categorical and numeric features into an unscaled vector
    assembler = VectorAssembler(
        inputCols=[f"{c}_vec" for c in CATEGORICAL_COLS] + NUMERIC_COLS,
        outputCol="unscaled_features",
        handleInvalid="skip",
    )
    
    # 2. Scale features to standardize variance
    # Note: withMean=False is required for Spark Streaming compatibility
    scaler = StandardScaler(
        inputCol="unscaled_features",
        outputCol="features",
        withMean=False,
        withStd=True
    )
    
    return Pipeline(stages=indexers + encoders + [assembler, scaler])


def main():
    spark = SparkSession.builder.appName("FeatureTransformationPipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH)

    # Same cleaning + feature engineering as the streaming job, kept
    # consistent so batch-trained features match real-time features.
    df = df.dropna(subset=["amount", "merchant_category", "card_type", "country"])
    df = df.filter(col("amount") >= 0)
    df = df.withColumn("txn_hour", hour(to_timestamp(col("timestamp"))))
    df = df.withColumn("is_foreign", (col("country") != "EG").cast("int"))

    pipeline = build_feature_pipeline()
    fitted = pipeline.fit(df)

    fitted.write().overwrite().save(FEATURE_PIPELINE_PATH)
    print(f"Feature transformation pipeline saved to {FEATURE_PIPELINE_PATH}")

    # Quick sanity check: show a few transformed rows
    fitted.transform(df).select("unscaled_features", "features").show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()