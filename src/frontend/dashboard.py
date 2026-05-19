import os
import glob
import pandas as pd
import streamlit as st
import time

# Configure enterprise styling layout
st.set_page_config(
    page_title="AegisPV - Real-Time Adverse Event Governance",
    page_icon="🛡️",
    layout="wide"
)

# Dynamically locate local storage paths
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ALERTS_PATH = os.path.join(PROJECT_ROOT, "storage", "gold", "live_inference_alerts")

def load_live_inference_stream():
    """Directly reads active parquet file chunks on disk to bypass JVM overhead."""
    search_pattern = os.path.join(ALERTS_PATH, "*.parquet")
    parquet_files = glob.glob(search_pattern)
    
    if not parquet_files:
        return pd.DataFrame()
    
    try:
        # Consolidate streaming file slices emitted by the PySpark engine
        dataframes = [pd.read_parquet(fp) for fp in parquet_files]
        combined_df = pd.concat(dataframes, ignore_index=True)
        
        if not combined_df.empty and "evaluated_at" in combined_df.columns:
            combined_df["evaluated_at"] = pd.to_datetime(combined_df["evaluated_at"])
            combined_df = combined_df.sort_values(by="evaluated_at", ascending=False)
            
        return combined_df
    except Exception:
        # Gracefully catch mid-write file locks during active stream operations
        return pd.DataFrame()

# --- HEADER INTERFACE ---
st.title("🛡️ AegisPV: Real-Time Pharmacovigilance & ADE Scoring")
st.markdown("""
This executive interface monitors real-time Electronic Health Record stream telemetry. Inbound medical vectors 
are standardized, checked for nulls, evaluated against signature-locked **Model Version 4 (AegisPV_XGB_Core)**, 
and committed to an immutable local Delta Lake path with a full compliance audit trail.
""")

# Setup auto-refresh loop interval placeholder
refresh_rate = st.sidebar.slider("Streaming Refresh Rate (seconds)", min_value=1, max_value=10, value=2)
st.sidebar.markdown("---")
st.sidebar.info("💡 **System Status**: Active\n\nMonitoring local KRaft topic: `raw-clinical-events`")

# Initialize real-time visualization container blocks
kpi_placeholder = st.empty()
chart_placeholder = st.empty()
table_placeholder = st.empty()

# --- LIVE REFRESH LOOP ---
while True:
    df = load_live_inference_stream()
    
    if df.empty:
        with kpi_placeholder.container():
            st.warning("⏳ Waiting for streaming inference engine records... Ensure your Kafka producers are running.")
    else:
        # 1. Compute Analytics Metrics
        total_evaluations = len(df)
        high_risk_alerts = int(df["adverse_event_risk_score"].sum())
        high_risk_ratio = (high_risk_alerts / total_evaluations) * 100 if total_evaluations > 0 else 0.0
        
        # 2. Render Scorecard Widgets
        with kpi_placeholder.container():
            col1, col2, col3 = st.columns(3)
            col1.metric(label="📊 Total Inbound Messages Evaluated", value=f"{total_evaluations:,} records")
            col2.metric(label="🚨 High-Risk ADE Signals Identified", value=f"{high_risk_alerts:,} events", delta=f"{high_risk_ratio:.1f}% ratio", delta_color="inverse")
            col3.metric(label="⚡ Local Compute Ingestion Overhead", value="$0.00 (Zero Cloud Overhead)")
            st.markdown("---")
        
        # 3. Render Decision Boundary Visualizations
        with chart_placeholder.container():
            left_chart, right_chart = st.columns(2)
            
            with left_chart:
                st.subheader("⚖️ Model Risk Decisions by Blood Pressure Profile")
                # Scatter mapping to showcase how the XGBoost boundary separates risk
                st.scatter_chart(
                    data=df,
                    x="diastolic_bp",
                    y="systolic_bp",
                    color="adverse_event_risk_score",
                    use_container_width=True
                )
                
            with right_chart:
                st.subheader("📈 Chronological Ingest Velocity Tracker")
                # Group timestamps by seconds to map current throughput rates
                df['time_sec'] = df['evaluated_at'].dt.strftime('%H:%M:%S')
                velocity_df = df.groupby('time_sec').size().reset_index(name='Message Count')
                st.line_chart(data=velocity_df, x='time_sec', y='Message Count', use_container_width=True)
                
        # 4. Render Live Audit Ledger Table
        with table_placeholder.container():
            st.subheader("📜 Compliance Audit Trail (Latest Streaming Events)")
            # Clean column formatting to mimic standard FDA safety logs
            display_df = df[["event_id", "evaluated_at", "systolic_bp", "diastolic_bp", "rxnorm_id", "snomed_code", "adverse_event_risk_score"]].copy()
            display_df.columns = ["Event Identifier", "Evaluation Timestamp", "Systolic BP", "Diastolic BP", "RxNorm ID", "SNOMED Code", "Assessed ADE Risk Score"]
            st.dataframe(display_df.head(15), use_container_width=True, hide_index=True)
            
    # Hold the process execution for the duration of the slider setting before rerunning loop
    time.sleep(refresh_rate)