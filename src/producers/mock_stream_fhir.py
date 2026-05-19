import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Initialize Kafka Producer pointing to localhost
# Note: Ensure your local Kafka broker is running on port 9092 before execution
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'raw-clinical-events'

# Mock pools to simulate realistic clinical text and data combinations
PATIENTS = [
    {"name": "John Doe", "ssn": "000-12-3456"},
    {"name": "Alice Smith", "ssn": "111-23-4567"},
    {"name": "Bob Johnson", "ssn": "222-34-5678"},
    {"name": "Eleanor Vance", "ssn": "333-45-6789"},
    {"name": "Marcus Aurelius", "ssn": "444-56-7890"}
]

DRUGS = ["Lisinosyn", "Simvastatin", "Metformin", "Ibuprofen", "Amlodipine"]
SYMPTOMS = [
    "Patient reports acute muscle pain and dark urine after starting lipid regimen.",
    "Presented with mild rash and localized swelling near the injection site.",
    "Routine follow-up. Expressing sudden dizziness and occasional shortness of breath.",
    "Complains of persistent dry cough and lightheadedness since dosage adjustments.",
    "Lab results indicate elevated serum creatinine levels. Patient feels completely asymptomatic."
]

def generate_medical_event():
    """Generates a raw, un-sanitized clinical event containing PHI."""
    patient = random.choice(PATIENTS)
    return {
        "event_id": f"evt_{random.randint(100000, 999999)}",
        "timestamp": datetime.utcnow().isoformat(),
        "provider_id": f"prov_{random.randint(10, 99)}",
        # Explicit PHI fields to test our downstream Silver Layer Scrubber
        "patient_name": patient["name"],
        "patient_ssn": patient["ssn"],
        "medication_prescribed": random.choice(DRUGS),
        "clinical_notes": random.choice(SYMPTOMS),
        "systolic_bp": random.randint(110, 160),
        "diastolic_bp": random.randint(70, 100)
    }

if __name__ == "__main__":
    print(f"🚀 Starting local clinical data stream into Kafka topic: '{TOPIC_NAME}'...")
    print("Press Ctrl+C to terminate the stream.")
    
    try:
        while True:
            payload = generate_medical_event()
            
            # Send message to local Kafka broker
            producer.send(TOPIC_NAME, payload)
            print(f"📦 Sent Event {payload['event_id']} | Prescribed: {payload['medication_prescribed']}")
            
            # Flush to ensure network delivery
            producer.flush()
            
            # Stream dynamically every 1 to 3 seconds to mimic realistic network ingestion
            time.sleep(random.uniform(1.0, 3.0))
            
    except KeyboardInterrupt:
        print("\n🛑 Local data stream safely halted.")