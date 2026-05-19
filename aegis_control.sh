#!/bin/bash

# ==============================================================================
# 🛡️ AegisPV Pipeline Master Orchestration Control Utility
# Description: Automates zero-cost local architecture deployment blocks.
# ==============================================================================

# Terminal text color variables
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${CYAN}====================================================================${NC}"
echo -e "${CYAN}   🛡️  INITIALIZING AEGIS-PV AUTOMATED ORCHESTRATION INTERFACE   ${NC}"
echo -e "${CYAN}====================================================================${NC}"

# 1. PRE-FLIGHT SYSTEM VERIFICATION ENV CHECKS
echo -e "\n${YELLOW}[Step 1/5] Verifying local development environment states...${NC}"
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo -e "${RED}❌ ERROR: Local virtual environment (venv) not active.${NC}"
    echo -e "Please run 'source venv/bin/activate' before invoking this orchestrator."
    exit 1
else
    echo -e "${GREEN}✅ Virtual Environment Verified Active: $VIRTUAL_ENV${NC}"
fi

# 2. CONTAINER CORE LAUNCHPASS
echo -e "\n${YELLOW}[Step 2/5] Spinning up containerized message broker backbone...${NC}"
if [ -f "docker/docker-compose.yml" ]; then
    # Spin up KRaft Kafka node cleanly in detached mode
    docker compose -f docker/docker-compose.yml up -d
    echo -e "${GREEN}🐳 Single-node KRaft Kafka instance successfully targeted.${NC}"
else
    echo -e "${RED}❌ ERROR: Could not locate docker/docker-compose.yml config file.${NC}"
    exit 1
fi

# 3. KAFKA BROKER PORT READINESS PROBE
echo -e "\n${YELLOW}[Step 3/5] Waiting for Kafka Broker loopback bindings to settle...${NC}"
until nc -z localhost 9092; do
    echo -e "⏳ Broker port 9092 still stabilizing... pooling next slot."
    sleep 3
done
echo -e "${GREEN}🔌 Connection established! Local stream broker active on port 9092.${NC}"

# 4. DEPLOY STREAMING INGESTION PROCESSES BACKGROUND
echo -e "\n${YELLOW}[Step 4/5] Deploying background data engineering pipelines...${NC}"

# Boot up the raw clinical FHIR JSON data generator
echo -e "🚀 Launching active clinical stream generator (mock_stream_fhir.py)..."
python src/producers/mock_stream_fhir.py > logs_mock_stream.txt 2>&1 &
PRODUCER_PID=$!

# Boot up the Spark Structured Streaming Silver Scrubber
echo -e "🚀 Deploying real-time PySpark HIPAA Scrubber engine (silver_scrubber.py)..."
python src/transformers/silver_scrubber.py > logs_silver_scrubber.txt 2>&1 &
SCRUBBER_PID=$!

echo -e "${GREEN}✅ Streaming engines successfully spun out into background PIDs.${NC}"
echo -e "📄 Telemetry traces writing to local root text files 'logs_mock_stream.txt' and 'logs_silver_scrubber.txt'."

# 5. INITIALIZE AUDIT AND GOVERNANCE UI
echo -e "\n${YELLOW}[Step 5/5] Booting up local MLOps Governance Server interface...${NC}"
echo -e "📊 Starting loopback tracking instance via port 5000..."
mlflow ui --port 5000 > logs_mlflow_server.txt 2>&1 &
MLFLOW_PID=$!

sleep 2
echo -e "${GREEN}🎯 System Initialization Complete. AegisPV Architecture Active.${NC}"
echo -e "${CYAN}--------------------------------------------------------------------${NC}"
echo -e "👉 Open independent browser context: http://127.0.0.1:5000"
echo -e "${CYAN}--------------------------------------------------------------------${NC}"
echo -e "${YELLOW}Press [CTRL+C] at any time to halt background PIDs and tear down infrastructure.${NC}\n"

# Maintain active listening hook to intercept termination commands safely
cleanup_architecture_components() {
    echo -e "\n\n${RED}🛑 Intercepted Shutdown Command. Cleaning up local pipeline assets...${NC}"
    
    echo -e "Stopping clinical stream producer (PID: $PRODUCER_PID)..."
    kill $PRODUCER_PID 2>/dev/null
    
    echo -e "Halting PySpark streaming worker execution (PID: $SCRUBBER_PID)..."
    kill $SCRUBBER_PID 2>/dev/null
    
    echo -e "Terminating local MLflow graphical tracking dashboard (PID: $MLFLOW_PID)..."
    kill $MLFLOW_PID 2>/dev/null
    
    echo -e "Tearing down containerized KRaft network layout..."
    docker compose -f docker/docker-compose.yml down
    
    echo -e "${GREEN}🎯 System resources successfully reclaimed. Pipeline offline.${NC}"
    exit 0
}

# Trap terminal break commands to trigger absolute graceful shutdown sequence
trap cleanup_architecture_components SIGINT SIGTERM

# Infinite loop sleep process to keep shell listener responsive
while true; do
    sleep 1
done