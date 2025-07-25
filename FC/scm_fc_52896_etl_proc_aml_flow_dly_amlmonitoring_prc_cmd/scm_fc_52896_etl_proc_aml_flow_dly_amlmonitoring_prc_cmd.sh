#!/bin/bash

# scm_fc_52896_etl_proc_aml_flow_dly_amlmonitoring_prc_cmd.sh
# Daily AML Monitoring and Compliance ETL Pipeline

# Hard-coded ETL Job Configuration
ETL_JOB_ID="fc_AMLMONITORING_DAILY"
ETL_JOB_NAME="scm_fc_52896_etl_proc_aml_flow_dly_amlmonitoring_prc_cmd"
ETL_DOMAIN="fc"
ETL_PROCESS_TYPE="aml_flow"
ETL_FREQUENCY="daily"
ETL_TARGET="amlmonitoring"

# Environment Configuration
export JAVA_HOME="/opt/java/openjdk-11"
export SPARK_HOME="/opt/spark"
export HADOOP_HOME="/opt/hadoop"
export ETL_HOME="/opt/scm-etl"
export CONFIG_PATH="${ETL_HOME}/config"
export LOG_PATH="/var/log/scm-etl/fc"

# Database Configuration
DB_HOST="scm-fc-cluster.internal"
DB_NAME="fc_analytics_db"
DB_USER="fc_etl_user"
DB_PASSWORD_FILE="/opt/scm-etl/secrets/fc_db_password"

# Regulatory Configuration
STRAC_API="https://api.strac.gov."
STRAC_API_KEY_FILE="/opt/scm-etl/secrets/strac_api_key"

# Logging Setup
LOG_FILE="${LOG_PATH}/${ETL_JOB_NAME}_$(date +%Y%m%d_%H%M%S).log"
ERROR_LOG="${LOG_PATH}/${ETL_JOB_NAME}_error_$(date +%Y%m%d_%H%M%S).log"

mkdir -p "${LOG_PATH}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [${ETL_JOB_ID}] $1" | tee -a "${LOG_FILE}"
}

error_log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] [${ETL_JOB_ID}] $1" | tee -a "${ERROR_LOG}" | tee -a "${LOG_FILE}"
}

handle_error() {
    error_log "ETL Job failed at step: $1"
    error_log "Exit code: $2"
    
    curl -X POST "http://scm-monitoring.internal/api/alerts" \
        -H "Content-Type: application/json" \
        -d "{\"job_id\":\"${ETL_JOB_ID}\",\"status\":\"FAILED\",\"step\":\"$1\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "UPDATE etl_job_control SET job_status='FAILED', end_time=NOW(), error_message='$1' WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
    
    exit $2
}

trap 'handle_error "Unexpected error" $?' ERR

main() {
    log "Starting fc AML Monitoring ETL Pipeline"
    log "Job ID: ${ETL_JOB_ID}"
    
    # Pre-execution checks
    if [[ ! -f "${DB_PASSWORD_FILE}" ]]; then
        handle_error "Database password file not found" 1
    fi
    
    # Check regulatory compliance database connectivity
    if ! pg_isready -h "${DB_HOST}" -U "${DB_USER}" > /dev/null 2>&1; then
        handle_error "Database connectivity check failed" 1
    fi
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "INSERT INTO etl_job_control (job_id, job_name, run_date, job_status, start_time) 
         VALUES ('${ETL_JOB_ID}', '${ETL_JOB_NAME}', CURRENT_DATE, 'RUNNING', NOW())
         ON CONFLICT (job_id, run_date) DO UPDATE SET job_status='RUNNING', start_time=NOW();"
    
    # Data validation
    PROCESS_DATE=$(date -d "yesterday" +%Y-%m-%d)
    TRANSACTION_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_source.transaction_records WHERE DATE(timestamp) = '${PROCESS_DATE}';")
    
    CUSTOMER_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(DISTINCT customer_id) FROM fc_source.customer_profiles WHERE last_kyc_update >= '${PROCESS_DATE}' - INTERVAL '30 days';")
    
    log "Found ${TRANSACTION_COUNT} transactions and ${CUSTOMER_COUNT} active customers for ${PROCESS_DATE}"
    
    if [[ ${TRANSACTION_COUNT} -eq 0 ]]; then
        log "No transactions found for ${PROCESS_DATE}. Exiting gracefully."
        psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
            "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=0 WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
        exit 0
    fi
    
    # Execute Spark ETL with AML rules engine
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 8g \
        --executor-memory 16g \
        --executor-cores 8 \
        --num-executors 25 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.dynamicAllocation.enabled=true \
        --conf spark.dynamicAllocation.maxExecutors=35 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
        --class com.scm.fc.aml.AMLMonitoringJob \
        "${ETL_HOME}/jars/fc-aml-monitoring-1.0.jar" \
        --job-id "${ETL_JOB_ID}" \
        --process-date "${PROCESS_DATE}" \
        --source-schema "fc_source" \
        --target-schema "fc_analytics" \
        --config-file "${CONFIG_PATH}/fc_aml_config.properties" \
        --log-level "INFO" 2>&1 | tee -a "${LOG_FILE}"
    
    SPARK_EXIT_CODE=$?
    if [[ ${SPARK_EXIT_CODE} -ne 0 ]]; then
        handle_error "Spark AML monitoring job execution failed" ${SPARK_EXIT_CODE}
    fi
    
    # Validation and AML metrics
    PROCESSED_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_analytics.daily_aml_alerts WHERE process_date = '${PROCESS_DATE}';")
    
    HIGH_RISK_ALERTS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_analytics.daily_aml_alerts WHERE alert_severity = 'HIGH' AND process_date = '${PROCESS_DATE}';")
    
    REGULATORY_REPORTABLE=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_analytics.daily_aml_alerts WHERE regulatory_reportable = true AND process_date = '${PROCESS_DATE}';")
    
    STRUCTURING_ALERTS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_analytics.daily_aml_alerts WHERE alert_type = 'STRUCTURING' AND process_date = '${PROCESS_DATE}';")
    
    # Generate regulatory reports
    THRESHOLD_REPORTS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "INSERT INTO fc_analytics.regulatory_reports (report_type, customer_id, transaction_amount, report_date, due_date, status) 
         SELECT 'TTR', customer_id, MAX(transaction_amount), CURRENT_DATE, CURRENT_DATE + INTERVAL '10 days', 'PENDING'
         FROM fc_analytics.daily_aml_alerts 
         WHERE alert_type = 'THRESHOLD' AND transaction_amount >= 10000 AND process_date = '${PROCESS_DATE}'
         GROUP BY customer_id
         RETURNING 1;" | wc -l)
    
    # Calculate AML effectiveness metrics
    FALSE_POSITIVE_RATE=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT ROUND(
            (SELECT COUNT(*) FROM fc_analytics.daily_aml_alerts WHERE investigation_status = 'FALSE_POSITIVE' AND process_date >= '${PROCESS_DATE}' - INTERVAL '30 days')::DECIMAL /