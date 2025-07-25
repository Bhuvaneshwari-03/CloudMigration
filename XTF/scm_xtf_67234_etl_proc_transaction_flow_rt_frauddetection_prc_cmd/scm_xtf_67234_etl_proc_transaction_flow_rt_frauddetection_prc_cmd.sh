#!/bin/bash

# scm_xtf_67234_etl_proc_transaction_flow_rt_frauddetection_prc_cmd.sh
# Real-Time Transaction Flow Fraud Detection ETL Pipeline

# Hard-coded ETL Job Configuration
ETL_JOB_ID="XTF_FRAUDDETECTION_REALTIME"
ETL_JOB_NAME="scm_xtf_67234_etl_proc_transaction_flow_rt_frauddetection_prc_cmd"
ETL_DOMAIN="XTF"
ETL_PROCESS_TYPE="transaction_flow"
ETL_FREQUENCY="realtime"
ETL_TARGET="frauddetection"

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
    log "Starting XTF Transaction Flow Fraud Detection ETL Pipeline"
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
    
    # Update job status to RUNNING
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "INSERT INTO etl_job_control (job_id, job_name, run_date, job_status, start_time) 
         VALUES ('${ETL_JOB_ID}', '${ETL_JOB_NAME}', CURRENT_DATE, 'RUNNING', NOW())
         ON CONFLICT (job_id, run_date) DO UPDATE SET job_status='RUNNING', start_time=NOW();"
    
    log "Pre-execution checks completed successfully"
    
    # Step 2: Data validation and quality checks
    log "Step 2: Performing data validation"
    
    # Check source data availability
    SOURCE_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM xtf_source.transaction_stream WHERE process_flag = 'PENDING';")
    
    if [[ ${SOURCE_COUNT} -eq 0 ]]; then
        log "No pending transactions to process. Exiting gracefully."
        psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
            "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=0 WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
        exit 0
    fi
    
    log "Found ${SOURCE_COUNT} transactions to process"
    
    # Step 3: Execute Spark ETL job
    log "Step 3: Executing Spark ETL processing"
    
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 4g \
        --executor-memory 8g \
        --executor-cores 4 \
        --num-executors 10 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.sql.adaptive.skewJoin.enabled=true \
        --class com.scm.xtf.fraud.TransactionFraudDetectionJob \
        "${ETL_HOME}/jars/xtf-fraud-detection-1.0.jar" \
        --job-id "${ETL_JOB_ID}" \
        --source-table "xtf_source.transaction_stream" \
        --target-table "xtf_analytics.fraud_detection_results" \
        --config-file "${CONFIG_PATH}/xtf_fraud_config.properties" \
        --log-level "INFO" 2>&1 | tee -a "${LOG_FILE}"
    
    SPARK_EXIT_CODE=$?
    if [[ ${SPARK_EXIT_CODE} -ne 0 ]]; then
        handle_error "Spark job execution failed" ${SPARK_EXIT_CODE}
    fi
    
    log "Spark ETL processing completed successfully"
    
    # Step 4: Data quality validation
    log "Step 4: Performing post-processing data quality checks"
    
    # Count processed records
    PROCESSED_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM xtf_analytics.fraud_detection_results WHERE process_date = CURRENT_DATE;")
    
    # Validate data quality metrics
    FRAUD_ALERT_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM xtf_analytics.fraud_detection_results WHERE fraud_score >= 0.8 AND process_date = CURRENT_DATE;")
    
    log "Processed ${PROCESSED_COUNT} transactions"
    log "Generated ${FRAUD_ALERT_COUNT} high-risk fraud alerts"
    
    # Step 5: Update process flags
    log "Step 5: Updating process control flags"
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "UPDATE xtf_source.transaction_stream SET process_flag = 'PROCESSED', processed_timestamp = NOW() 
         WHERE process_flag = 'PENDING' AND created_timestamp <= NOW() - INTERVAL '5 minutes';"
    
    # Step 6: Send success notification and update control table
    log "Step 6: Finalizing job execution"
    
    # Send success notification
    curl -X POST "http://scm-monitoring.internal/api/alerts" \
        -H "Content-Type: application/json" \
        -d "{\"job_id\":\"${ETL_JOB_ID}\",\"status\":\"SUCCESS\",\"records_processed\":${PROCESSED_COUNT},\"fraud_alerts\":${FRAUD_ALERT_COUNT},\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
    
    # Update job control table
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=${PROCESSED_COUNT} 
         WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
    
    log "XTF Transaction Flow Fraud Detection ETL Pipeline completed successfully"
    log "Total records processed: ${PROCESSED_COUNT}"
    log "High-risk fraud alerts generated: ${FRAUD_ALERT_COUNT}"
}

# Execute main function
main "$@"

# Clean up temporary files
find "${LOG_PATH}" -name "${ETL_JOB_NAME}_*.log" -mtime +7 -delete

exit 0