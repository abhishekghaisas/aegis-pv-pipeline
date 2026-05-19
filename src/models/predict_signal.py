import os
import pandas as pd
import mlflow.pyfunc

# 1. Point to your local tracking vault
mlflow.set_tracking_uri("file://" + os.path.abspath("./mlruns"))

def evaluate_inbound_patient_risk():
    # Simulate a new, incoming clinical profile from the pipeline
    # Features match our explicit schema: [systolic_bp, diastolic_bp, amlodipine, kidney_injury]
    new_patient_data = pd.DataFrame([{
        "systolic_bp": 145.0,
        "diastolic_bp": 92.0,
        "rxnorm_17767_amlodipine": 1,
        "snomed_709044004_kidney_injury": 1
    }])
    
    print("📖 Querying Model Registry for 'AegisPV_XGB_Core' (Version 1)...")
    
    # Load the asset natively as a PyFunc model for decoupled production inference
    model_uri = "models:/AegisPV_XGB_Core/1"
    loaded_model = mlflow.pyfunc.load_model(model_uri)
    
    # Generate live evaluation risk vector
    probabilities = loaded_model.predict(new_patient_data)
    risk_probability = probabilities[0]
    
    print("-" * 50)
    print(f"🎯 Inbound Patient Risk Score: {risk_probability * 100:.2f}%")
    if risk_probability > 0.5:
        print("⚠️ ALERT: High Adverse Drug Event Signal Isolated. Dispatching to Safety Triage.")
    else:
        print("✅ Signal Normal: Continuous Monitoring Stable.")
    print("-" * 50)

if __name__ == "__main__":
    evaluate_inbound_patient_risk()