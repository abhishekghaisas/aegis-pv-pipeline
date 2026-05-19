import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import mlflow

# Enforce local path assignments
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
GOLD_DELTA_PATH = f"file://{PROJECT_ROOT}/storage/gold/enriched_adverse_events"
INFERENCE_ALERT_PATH = f"file://{PROJECT_ROOT}/storage/gold/live_inference_alerts"
CHECKPOINT_PATH = f"file://{PROJECT_ROOT}/storage/checkpoints/stream_inference"

def init_streaming_spark_session():
    """Initializes a local PySpark session bound to Kafka and Delta Lake utilities."""
    return SparkSession.builder \
        .appName("AegisPV-Unified-Streaming-Inference") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def execute_streaming_inference():
    print("🚀 Initializing Unified Streaming Inference Engine...")
    spark = init_streaming_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Point to your local MLflow tracking vault directory
    mlflow.set_tracking_uri(f"file://{PROJECT_ROOT}/mlruns")
    
    print("📥 Loading signature-locked Version 4 model from local MLflow Registry...")
    model_name = "AegisPV_XGB_Core"
    model_version = 4
    model_uri = "models:/AegisPV_XGB_Core/4"
    
    try:
        # Load the registered tracking binary as a native Spark UDF
        predict_risk_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type=DoubleType())
    except Exception as e:
        print(f"❌ Failed to locate MLflow model asset at {model_uri}. Ensure Version 4 is registered.")
        print(f"Error Details: {e}")
        sys.exit(1)

    print("🔌 Binding consumer stream to local KRaft broker (localhost:9092)...")
    # Read the live raw clinical feed passing through the message broker
    raw_kafka_stream = spark.readStream \
        .format("kafka") \
        .options(**{
            "kafka.bootstrap.servers": "localhost:9092",
            "subscribe": "raw-clinical-events",
            "startingOffsets": "latest"
        }) \
        .load()

    # Define the base JSON decoding schema from your Gold structure
    clinical_payload_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("systolic_bp", DoubleType(), True),
        StructField("diastolic_bp", DoubleType(), True),
        StructField("rxnorm_id", IntegerType(), True),
        StructField("snomed_code", IntegerType(), True)
    ])

    print("🧬 Parsing inbound metrics and extracting contract variables...")
    # Deserialize the message value and apply structured vector isolation
    parsed_stream = raw_kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), clinical_payload_schema).alias("data")) \
        .select("data.*")
    print("🛡️ Applying Null Imputation & Strict Contract Type Enforcement...")

    # Handle missing upstream flags dynamically by mapping structural fallbacks
    aligned_features = parsed_stream \
        .na.fill({"rxnorm_id": 0, "snomed_code": 0, "systolic_bp": 0, "diastolic_bp": 0}) \
        .withColumn("rxnorm_id", col("rxnorm_id").cast("long")) \
        .withColumn("snomed_code", col("snomed_code").cast("long")) \
        .withColumn("systolic_bp", col("systolic_bp").cast("long")) \
        .withColumn("diastolic_bp", col("diastolic_bp").cast("long"))

    print("⚖️ Executing real-time inference scoring loops across stream records...")
    # Wrap your signature feature structures directly into the input contract array
    scored_stream = aligned_features \
        .withColumn(
            "adverse_event_risk_score", 
            predict_risk_udf(struct("rxnorm_id", "snomed_code", "systolic_bp", "diastolic_bp"))
        ) \
        .withColumn("evaluated_at", current_timestamp())

    print(f"💾 Persisting scored analytics trail to local Delta directory: {INFERENCE_ALERT_PATH}")
    # Write the calculated outputs safely back into a physical transactional Delta layer
    query = scored_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .start(INFERENCE_ALERT_PATH)

    # Alternate mirror target: Print evaluations directly to terminal window for monitoring
    console_query = scored_stream.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    query.awaitTermination()
    console_query.awaitTermination()

if __name__ == "__main__":
    execute_streaming_inference()