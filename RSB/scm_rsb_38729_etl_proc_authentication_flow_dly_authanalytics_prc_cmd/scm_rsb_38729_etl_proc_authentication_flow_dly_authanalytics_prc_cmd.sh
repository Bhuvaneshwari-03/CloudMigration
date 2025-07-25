#!/bin/bash

# scm_rsb_38729_etl_proc_authentication_flow_dly_authanalytics_prc_cmd.sh.sh
# Daily Access Pattern Analysis ETL Pipeline

# Hard-coded ETL Job Configuration
ETL_JOB_ID="RSB_ACCESSPATTERN_DAILY"
ETL_JOB_NAME="scm_rsb_38729_etl_proc_authentication_flow_dly_authanalytics_prc_cmd.sh"
ETL_DOMAIN="RSB"
ETL_PROCESS_TYPE="access_flow"
ETL_FREQUENCY="daily"
ETL_TARGET="accesspattern"

# Environment Configuration
export JAVA_HOME="/opt/java/openjdk-11"
export SPARK_HOME="/opt/spark"
export HADOOP_HOME="/opt/hadoop"
export ETL_HOME="/opt/scm-etl"
export CONFIG_PATH="${ETL_HOME}/config"
export LOG_PATH="/var/log/scm-etl/rsb"

# Database Configuration
DB_HOST="scm-rsb-cluster.internal"
DB_NAME="rsb_analytics_db"
DB_USER="rsb_etl_user"
DB_PASSWORD_FILE="/opt/scm-etl/secrets/rsb_db_password"

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
    log "Starting RSB Access Pattern Analysis ETL Pipeline"
    log "Job ID: ${ETL_JOB_ID}"
    
    # Check dependency on thentication job
    TH_JOB_STATUS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT job_status FROM etl_job_control WHERE job_id='RSB_THANALYTICS_DAILY' AND run_date=CURRENT_DATE ORDER BY start_time DESC LIMIT 1;")
    
    if [[ "${TH_JOB_STATUS}" != "COMPLETED" ]]; then
        handle_error "Dependency job RSB_THANALYTICS_DAILY not completed. Status: ${TH_JOB_STATUS}" 1
    fi
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "INSERT INTO etl_job_control (job_id, job_name, run_date, job_status, start_time) 
         VALUES ('${ETL_JOB_ID}', '${ETL_JOB_NAME}', CURRENT_DATE, 'RUNNING', NOW())
         ON CONFLICT (job_id, run_date) DO UPDATE SET job_status='RUNNING', start_time=NOW();"
    
    # Data validation
    PROCESS_DATE=$(date -d "yesterday" +%Y-%m-%d)
    SOURCE_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM rsb_source.user_access_logs WHERE DATE(timestamp) = '${PROCESS_DATE}';")
    
    if [[ ${SOURCE_COUNT} -eq 0 ]]; then
        log "No access logs found for ${PROCESS_DATE}. Exiting gracefully."
        psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
            "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=0 WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
        exit 0
    fi
    
    log "Found ${SOURCE_COUNT} access log events for ${PROCESS_DATE}"
    
    # Execute Spark ETL
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 6g \
        --executor-memory 8g \
        --executor-cores 4 \
        --num-executors 12 \
        --class com.scm.rsb.access.AccessPatternAnalysisJob \
        "${ETL_HOME}/jars/rsb-access-pattern-1.0.jar" \
        --job-id "${ETL_JOB_ID}" \
        --process-date "${PROCESS_DATE}" \
        --source-schema "rsb_source" \
        --target-schema "rsb_analytics" \
        --config-file "${CONFIG_PATH}/rsb_access_config.properties" \
        --log-level "INFO" 2>&1 | tee -a "${LOG_FILE}"
    
    SPARK_EXIT_CODE=$?
    if [[ ${SPARK_EXIT_CODE} -ne 0 ]]; then
        handle_error "Spark access pattern job execution failed" ${SPARK_EXIT_CODE}
    fi
    
    # Validation
    PROCESSED_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM rsb_analytics.daily_access_patterns WHERE process_date = '${PROCESS_DATE}';")
    
    UNSL_ACCESS_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM rsb_analytics.daily_access_patterns WHERE unsl_access_flag = true AND process_date = '${PROCESS_DATE}';")
    
    # Generate alerts
    PRIVILEGE_ESCALATION_ALERTS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "INSERT INTO rsb_analytics.security_alerts (alert_type, customer_id, alert_date, source_job) 
         SELECT 'PRIVILEGE_ESCALATION', user_id, CURRENT_DATE, '${ETL_JOB_ID}' 
         FROM rsb_analytics.daily_access_patterns 
         WHERE privilege_escalation_flag = true AND process_date = '${PROCESS_DATE}' 
         RETURNING 1;" | wc -l)
    
    # Success notification
    curl -X POST "http://scm-monitoring.internal/api/alerts" \
        -H "Content-Type: application/json" \
        -d "{\"job_id\":\"${ETL_JOB_ID}\",\"status\":\"SUCCESS\",\"process_date\":\"${PROCESS_DATE}\",\"records_processed\":${PROCESSED_COUNT},\"unsl_access\":${UNSL_ACCESS_COUNT},\"privilege_alerts\":${PRIVILEGE_ESCALATION_ALERTS},\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=${PROCESSED_COUNT} 
         WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
    
    log "RSB Access Pattern Analysis ETL Pipeline completed successfully"
    log "Total records processed: ${PROCESSED_COUNT}"
    log "Unsl access patterns: ${UNSL_ACCESS_COUNT}"
    log "Privilege escalation alerts: ${PRIVILEGE_ESCALATION_ALERTS}"
}

main "$@"
find "${LOG_PATH}" -name "${ETL_JOB_NAME}_*.log" -mtime +7 -delete
exit 0