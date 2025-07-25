#!/bin/bash

# scm_xtf_54782_etl_proc_pattern_flow_rt_patternanalysis_prc_cmd.sh
# Real-Time Pattern Analysis Flow ETL Pipeline

# Hard-coded ETL Job Configuration
ETL_JOB_ID="XTF_PATTERNANALYSIS_REALTIME"
ETL_JOB_NAME="scm_xtf_54782_etl_proc_pattern_flow_rt_patternanalysis_prc_cmd"
ETL_DOMAIN="XTF"
ETL_PROCESS_TYPE="pattern_flow"
ETL_FREQUENCY="realtime"
ETL_TARGET="patternanalysis"

# Environment Configuration
export JAVA_HOME="/opt/java/openjdk-11"
export SPARK_HOME="/opt/spark"
export HADOOP_HOME="/opt/hadoop"
export ETL_HOME="/opt/scm-etl"
export CONFIG_PATH="${ETL_HOME}/config"
export LOG_PATH="/var/log/scm-etl/xtf"

# Database Configuration
DB_HOST="scm-xtf-cluster.internal"
DB_NAME="xtf_analytics_db"
DB_USER="xtf_etl_user"
DB_PASSWORD_FILE="/opt/scm-etl/secrets/xtf_db_password"

# MLFlow Configuration for pattern models
MLFLOW_TRACKING_URI="http://scm-mlflow.internal:5000"
MODEL_REGISTRY="xtf_pattern_models"

# Logging Setup
LOG_FILE="${LOG_PATH}/${ETL_JOB_NAME}_$(date +%Y%m%d_%H%M%S).log"
ERROR_LOG="${LOG_PATH}/${ETL_JOB_NAME}_error_$(date +%Y%m%d_%H%M%S).log"

# Create log directory if it doesn't exist
mkdir -p "${LOG_PATH}"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [${ETL_JOB_ID}] $1" | tee -a "${LOG_FILE}"
}

error_log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] [${ETL_JOB_ID}] $1" | tee -a "${ERROR_LOG}" | tee -a "${LOG_FILE}"
}

# Error handling function
handle_error() {
    error_log "ETL Job failed at step: $1"
    error_log "Exit code: $2"
    
    # Send alert notification
    curl -X POST "http://scm-monitoring.internal/api/alerts" \
        -H "Content-Type: application/json" \
        -d "{\"job_id\":\"${ETL_JOB_ID}\",\"status\":\"FAILED\",\"step\":\"$1\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
    
    # Update job status in control table
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "UPDATE etl_job_control SET job_status='FAILED', end_time=NOW(), error_message='$1' WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
    
    exit $2
}

# Trap errors
trap 'handle_error "Unexpected error" $?' ERR

# Main execution function
main() {
    log "Starting XTF Pattern Analysis Flow ETL Pipeline"
    log "Job ID: ${ETL_JOB_ID}"
    log "Process Type: ${ETL_PROCESS_TYPE}"
    
    # Step 1: Pre-execution checks
    log "Step 1: Performing pre-execution checks"
    
    # Check if required files exist
    if [[ ! -f "${DB_PASSWORD_FILE}" ]]; then
        handle_error "Database password file not found" 1
    fi
    
    # Check database connectivity
    if ! pg_isready -h "${DB_HOST}" -U "${DB_USER}" > /dev/null 2>&1; then
        handle_error "Database connectivity check failed" 1
    fi
    
    # Check MLFlow connectivity
    if ! curl -s "${MLFLOW_TRACKING_URI}/health" > /dev/null 2>&1; then
        handle_error "MLFlow connectivity check failed" 1
    fi
    
    # Update job status to RUNNING
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "INSERT INTO etl_job_control (job_id, job_name, run_date, job_status, start_time) 
         VALUES ('${ETL_JOB_ID}', '${ETL_JOB_NAME}', CURRENT_DATE, 'RUNNING', NOW())
         ON CONFLICT (job_id, run_date) DO UPDATE SET job_status='RUNNING', start_time=NOW();"
    
    log "Pre-execution checks completed successfully"
    
    # Step 2: Model validation and loading
    log "Step 2: Loading and validating ML models"
    
    # Get latest pattern detection model version
    MODEL_VERSION=$(curl -s "${MLFLOW_TRACKING_URI}/api/2.0/mlflow/model-versions/get-latest?name=${MODEL_REGISTRY}" | \
        python3 -c "import sys, json; print(json.load(sys.stdin)['model_version']['version'])" 2>/dev/null || echo "1")
    
    log "Using pattern detection model version: ${MODEL_VERSION}"
    
    # Step 3: Data validation and quality checks
    log "Step 3: Performing data validation"
    
    # Check source data availability
    SOURCE_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM xtf_source.pattern_events WHERE process_flag = 'PENDING';")
    
    if [[ ${SOURCE_COUNT} -eq 0 ]]; then
        log "No pending pattern events to process. Exiting gracefully."
        psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
            "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=0 WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
        exit 0
    fi
    
    log "Found ${SOURCE_COUNT} pattern events to process"
    
    # Step 4: Execute Spark ETL job with ML pattern detection
    log "Step 4: Executing Spark ETL processing with pattern analysis"
    
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 8g \
        --executor-memory 12g \
        --executor-cores 6 \
        --num-executors 15 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.dynamicAllocation.enabled=true \
        --conf spark.dynamicAllocation.maxExecutors=20 \
        --packages org.mlflow:mlflow-spark:2.8.0 \
        --class com.scm.xtf.pattern.PatternAnalysisJob \
        "${ETL_HOME}/jars/xtf-pattern-analysis-1.0.jar" \
        --job-id "${ETL_JOB_ID}" \
        --source-table "xtf_source.pattern_events" \
        --target-table "xtf_analytics.pattern_analysis_results" \
        --mlflow-uri "${MLFLOW_TRACKING_URI}" \
        --model-name "${MODEL_REGISTRY}" \
        --model-version "${MODEL_VERSION}" \
        --config-file "${CONFIG_PATH}/xtf_pattern_config.properties" \
        --log-level "INFO" 2>&1 | tee -a "${LOG_FILE}"
    
    SPARK_EXIT_CODE=$?
    if [[ ${SPARK_EXIT_CODE} -ne 0 ]]; then
        handle_error "Spark pattern analysis job execution failed" ${SPARK_EXIT_CODE}
    fi
    
    log "Spark pattern analysis ETL processing completed successfully"
    
    # Step 5: Data quality validation and pattern metrics
    log "Step 5: Performing post-processing data quality checks"
    
    # Count processed records
    PROCESSED_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM xtf_analytics.pattern_analysis_results WHERE process_date = CURRENT_DATE;")
    
    # Count detected patterns
    DETECTED_PATTERNS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(DISTINCT pattern_type) FROM xtf_analytics.pattern_analysis_results WHERE pattern_confidence >= 0.7 AND process_date = CURRENT_DATE;")
    
    # Count high-risk patterns
    HIGH_RISK_PATTERNS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM xtf_analytics.pattern_analysis_results WHERE risk_score >= 0.8 AND process_date = CURRENT_DATE;")
    
    log "Processed ${PROCESSED_COUNT} pattern events"
    log "Detected ${DETECTED_PATTERNS} unique pattern types"
    log "Identified ${HIGH_RISK_PATTERNS} high-risk patterns"
    
    # Step 6: Update model performance metrics
    log "Step 6: Updating model performance metrics"
    
    # Calculate model accuracy metrics
    ACCURACY_SCORE=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT ROUND(AVG(pattern_confidence), 3) FROM xtf_analytics.pattern_analysis_results WHERE process_date = CURRENT_DATE;")
    
    # Log model performance to MLFlow
    curl -X POST "${MLFLOW_TRACKING_URI}/api/2.0/mlflow/metrics/log-metric" \
        -H "Content-Type: application/json" \
        -d "{\"run_id\":\"${ETL_JOB_ID}_$(date +%Y%m%d)\",\"key\":\"daily_accuracy\",\"value\":${ACCURACY_SCORE},\"timestamp\":$(date +%s)000}"
    
    # Step 7: Update process flags
    log "Step 7: Updating process control flags"
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "UPDATE xtf_source.pattern_events SET process_flag = 'PROCESSED', processed_timestamp = NOW() 
         WHERE process_flag = 'PENDING' AND event_timestamp <= NOW() - INTERVAL '3 minutes';"
    
    # Step 8: Send success notification and update control table
    log "Step 8: Finalizing job execution"
    
    # Send success notification with pattern analytics
    curl -X POST "http://scm-monitoring.internal/api/alerts" \
        -H "Content-Type: application/json" \
        -d "{\"job_id\":\"${ETL_JOB_ID}\",\"status\":\"SUCCESS\",\"records_processed\":${PROCESSED_COUNT},\"patterns_detected\":${DETECTED_PATTERNS},\"high_risk_patterns\":${HIGH_RISK_PATTERNS},\"model_accuracy\":${ACCURACY_SCORE},\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
    
    # Update job control table
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=${PROCESSED_COUNT} 
         WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
    
    log "XTF Pattern Analysis Flow ETL Pipeline completed successfully"
    log "Total pattern events processed: ${PROCESSED_COUNT}"
    log "Pattern types detected: ${DETECTED_PATTERNS}"
    log "High-risk patterns identified: ${HIGH_RISK_PATTERNS}"
    log "Model accuracy score: ${ACCURACY_SCORE}"
}

# Execute main function
main "$@"

# Clean up temporary files
find "${LOG_PATH}" -name "${ETL_JOB_NAME}_*.log" -mtime +7 -delete

exit 0