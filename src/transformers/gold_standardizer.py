import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, StructType, StructField

def init_local_spark_session():
    """Initializes local Spark session with Delta Lake extensions."""
    return SparkSession.builder \
        .appName("AegisPV_Gold_Advanced_NLP") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

# ==============================================================================
# PRODUCTION-GRADE CLINICAL ENTITY PARSING DICTIONARIES
# Enforces advanced lookups with regex patterns to catch clinical mutations.
# ==============================================================================
RXNORM_NER_PATTERNS = {
    r"\b(amlodipin[e]?|norvasc)\b": "17767",
    r"\b(lisinopril|lisinosyn|prinivil|zestril)\b": "29302",
    r"\b(simvastatin|zocor)\b": "json_36561", # Maps brand/generic variants uniformly
    r"\b(metformin|glucophage)\b": "311654",
    r"\b(ibuprofen|advil|motrin)\b": "5640"
}

SNOMED_NER_PATTERNS = {
    r"\b(kidney|renal|creatinine|nephro)\b.*(injury|damage|elevated|failure)\b": "709044004", # Acute Kidney Injury
    r"\b(muscle pain|myalgia|rhabdo|muscle ache)\b": "450428004",                             # Myalgia / Muscle Pain
    r"\b(rash|swelling|urticaria|injection site)\b": "266395002",                           # Dermatological Adverse Reaction
    r"\b(dizzy|dizziness|lightheaded|vertigo)\b": "404640003",                              # Dizziness / Syncope
    r"\b(cough|dry cough|persistent cough)\b": "450428004"
}

@udf(returnType=StringType())
def clinical_ner_rxnorm(medication_string):
    """Programmatic Clinical NER to resolve drug variations to canonical RxNorm IDs."""
    if not medication_string:
        return "00000"
    normalized_target = medication_string.lower().strip()
    
    for pattern, rxnorm_id in RXNORM_NER_PATTERNS.items():
        if re.search(pattern, normalized_target):
            return rxnorm_id
    return "00000" # Safe default for unknown compounds

@udf(returnType=StringType())
def clinical_ner_snomed(clinical_notes_string):
    """Parses unstructured clinical symptoms to identify standardized SNOMED CT codes."""
    if not clinical_notes_string:
        return "000000000"
    normalized_target = clinical_notes_string.lower().strip()
    
    for pattern, snomed_code in SNOMED_NER_PATTERNS.items():
        if re.search(pattern, normalized_target):
            return snomed_code
    return "000000000" # Safe default for routine visits without localized signals

def execute_gold_standardization():
    spark = init_local_spark_session()
    
    silver_path = "storage/silver/sanitized_clinical"
    gold_path = "storage/gold/enriched_adverse_events"
    
    print(f"📖 Reading sanitized records from Silver Lake: {silver_path}")
    df_silver = spark.read.format("delta").load(silver_path)
    
    print("🧬 Extracting named entities and mapping text to clinical vocabularies...")
    # Apply our advanced UDF regex-parsers to build structured gold metrics
    df_gold = df_silver \
        .withColumn("rxnorm_id", clinical_ner_rxnorm(col("medication_prescribed"))) \
        .withColumn("snomed_code", clinical_ner_snomed(col("clinical_notes"))) \
        .withColumn("enriched_at", col("ingested_at")) # Add trackability token
        
    print(f"💾 Saving advanced enriched assets to Gold Lake: {gold_path}")
    df_gold.write.format("delta").mode("overwrite").save(gold_path)
    
    # Render preview output to confirm typing maps unblocked cleanly
    df_gold.select("event_id", "medication_prescribed", "rxnorm_id", "snomed_code").show(5, truncate=False)
    print("✅ Advanced Clinical NLP Gold processing successfully completed.")

if __name__ == "__main__":
    execute_gold_standardization()