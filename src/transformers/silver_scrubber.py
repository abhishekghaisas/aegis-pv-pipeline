import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_replace, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# --- CONFIGURATION & PATHING ---
# Dynamically locate root storage directory relative to this script
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DELTA_SILVER_PATH = os.path.join(BASE_DIR, "storage", "silver", "sanitized_clinical")
CHECKPOINT_PATH = os.path.join(BASE_DIR, "storage", "silver", "_checkpoints", "sanitized_clinical")

# Explicit incoming JSON schema matching the mock generator payload
CLINICAL_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("provider_id", StringType(), True),
    StructField("patient_name", StringType(), True),  # PHI: To be completely dropped
    StructField("patient_ssn", StringType(), True),   # PHI: To be fully masked via Regex
    StructField("medication_prescribed", StringType(), True),
    StructField("clinical_notes", StringType(), True),
    StructField("systolic_bp", IntegerType(), True),
    StructField("diastolic_bp", IntegerType(), True)
])

def init_local_spark_session():
    """Initializes PySpark with native Maven coordinates for Kafka and Delta Lake integration."""
    return SparkSession.builder \
        .appName("AegisPV-SilverScrubber") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

def execute_streaming_pipeline():
    spark = init_local_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Reduces terminal log spamming
    
    print("🔌 Connecting PySpark Stream to local Kafka broker (localhost:9092)...")
    
    # 1. Consume raw binary stream from Kafka broker
    raw_kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "raw-clinical-events") \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Extract binary payload, parse JSON, and flatten fields
    parsed_stream = raw_kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), CLINICAL_EVENT_SCHEMA).alias("data")) \
        .select("data.*")

    # 3. COMPLIANCE / TRANFORMATION LAYER (Silver Cleansing)
    # Rule A: Completely drop explicit identifiers (patient_name) to fulfill HIPAA minimal data standard
    # Rule B: Redact SSN data elements via regular expression masking (XXX-XX-XXXX format)
    sanitized_stream = parsed_stream \
        .drop("patient_name") \
        .withColumn("patient_ssn", regexp_replace(col("patient_ssn"), r"\d{3}-\d{2}-\d{4}", "XXX-XX-XXXX")) \
        .withColumn("ingested_at", current_timestamp())  # Metadata audit trail tracing

    print("🔒 Silver Layer HIPAA sanitization rules actively compiled.")
    print("💾 Streaming clean data into local Delta Lake transaction sink...")

    # 4. Write pristine stream transactions to local disk as a Delta Lake Table
    query = sanitized_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .start(DELTA_SILVER_PATH)

    # Keep terminal open and processing indefinitely
    query.awaitTermination()

if __name__ == "__main__":
    try:
        execute_streaming_pipeline()
    except KeyboardInterrupt:
        print("\n🛑 Silver Layer streaming consumer safely shut down.")