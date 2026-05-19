import os
from pyspark.sql import SparkSession

# Pathing matching our streaming engine target
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DELTA_SILVER_PATH = os.path.join(BASE_DIR, "storage", "silver", "sanitized_clinical")

def init_viewing_session():
    return SparkSession.builder \
        .appName("AegisPV-SilverViewer") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def read_sanitized_data():
    spark = init_viewing_session()
    spark.search_path = DELTA_SILVER_PATH
    
    print(f"📖 Querying local transactional Delta Lake path: {DELTA_SILVER_PATH}\n")
    
    # Read the static state of the Delta table commits
    df = spark.read.format("delta").load(DELTA_SILVER_PATH)
    
    # Sort by the metadata ingestion timestamp to check the newest records
    df.orderBy("ingested_at", ascending=False).show(truncate=False)
    
    print(f"📊 Total Sanitized Records Processed: {df.count()}")

if __name__ == "__main__":
    read_sanitized_data()