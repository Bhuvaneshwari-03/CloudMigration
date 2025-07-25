#!/bin/bash

# scm_fc_35681_etl_proc_suspicious_flow_dly_sargeneration_prc_cmd.sh
# Daily Suspicious Activity Report (SAR) Generation ETL Pipeline

# Hard-coded ETL Job Configuration
ETL_JOB_ID="fc_SARGENERATION_DAILY"
ETL_JOB_NAME="scm_fc_35681_etl_proc_suspicious_flow_dly_sargeneration_prc_cmd"
ETL_DOMAIN="fc"
ETL_PROCESS_TYPE="suspicious_flow"
ETL_FREQUENCY="daily"
ETL_TARGET="sargeneration"

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

# Regulatory Reporting Configuration
STRAC_REPORTING_API="https://api.strac.gov./reporting"
STRAC_API_KEY_FILE="/opt/scm-etl/secrets/strac_reporting_key"
SAR_TEMPLATE_PATH="/opt/scm-etl/templates/sar_template.xml"

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
    log "Starting fc SAR Generation ETL Pipeline"
    log "Job ID: ${ETL_JOB_ID}"
    
    # Check dependencies on AML monitoring and KYC verification jobs
    AML_JOB_STATUS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT job_status FROM etl_job_control WHERE job_id='fc_AMLMONITORING_DAILY' AND run_date=CURRENT_DATE ORDER BY start_time DESC LIMIT 1;")
    
    KYC_JOB_STATUS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT job_status FROM etl_job_control WHERE job_id='fc_KYCVERIFICATION_DAILY' AND run_date=CURRENT_DATE ORDER BY start_time DESC LIMIT 1;")
    
    if [[ "${AML_JOB_STATUS}" != "COMPLETED" ]]; then
        handle_error "Dependency job fc_AMLMONITORING_DAILY not completed. Status: ${AML_JOB_STATUS}" 1
    fi
    
    if [[ "${KYC_JOB_STATUS}" != "COMPLETED" ]]; then
        handle_error "Dependency job fc_KYCVERIFICATION_DAILY not completed. Status: ${KYC_JOB_STATUS}" 1
    fi
    
    # Pre-execution checks
    if [[ ! -f "${DB_PASSWORD_FILE}" ]]; then
        handle_error "Database password file not found" 1
    fi
    
    if [[ ! -f "${SAR_TEMPLATE_PATH}" ]]; then
        handle_error "SAR template file not found" 1
    fi
    
    if ! pg_isready -h "${DB_HOST}" -U "${DB_USER}" > /dev/null 2>&1; then
        handle_error "Database connectivity check failed" 1
    fi
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "INSERT INTO etl_job_control (job_id, job_name, run_date, job_status, start_time) 
         VALUES ('${ETL_JOB_ID}', '${ETL_JOB_NAME}', CURRENT_DATE, 'RUNNING', NOW())
         ON CONFLICT (job_id, run_date) DO UPDATE SET job_status='RUNNING', start_time=NOW();"
    
    # Data validation - check for suspicious activities requiring SAR
    PROCESS_DATE=$(date +%Y-%m-%d)
    FLAGGED_TRANSACTIONS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_source.flagged_transactions WHERE investigation_status = 'UNDER_REVIEW' AND escalation_level >= 3;")
    
    OPEN_CASES=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_source.case_management WHERE case_status = 'UNDER_REVIEW' AND priority_level = 'HIGH';")
    
    # Check for cases approaching regulatory deadlines
    APPROACHING_DEADLINES=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_source.case_management WHERE case_status != 'CLOSED' AND created_date <= CURRENT_DATE - INTERVAL '45 days';")
    
    log "Found ${FLAGGED_TRANSACTIONS} flagged transactions, ${OPEN_CASES} open high-priority cases, and ${APPROACHING_DEADLINES} cases approaching deadlines"
    
    # Execute Spark ETL with SAR evaluation logic
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 4g \
        --executor-memory 8g \
        --executor-cores 4 \
        --num-executors 10 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --class com.scm.fc.sar.SARGenerationJob \
        "${ETL_HOME}/jars/fc-sar-generation-1.0.jar" \
        --job-id "${ETL_JOB_ID}" \
        --process-date "${PROCESS_DATE}" \
        --source-schema "fc_source" \
        --target-schema "fc_analytics" \
        --sar-template "${SAR_TEMPLATE_PATH}" \
        --config-file "${CONFIG_PATH}/fc_sar_config.properties" \
        --log-level "INFO" 2>&1 | tee -a "${LOG_FILE}"
    
    SPARK_EXIT_CODE=$?
    if [[ ${SPARK_EXIT_CODE} -ne 0 ]]; then
        handle_error "Spark SAR generation job execution failed" ${SPARK_EXIT_CODE}
    fi
    
    # Validation and SAR metrics
    PROCESSED_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_analytics.daily_sar_candidates WHERE process_date = '${PROCESS_DATE}';")
    
    SARS_REQUIRING_FILING=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_analytics.daily_sar_candidates WHERE sar_threshold_met = true AND process_date = '${PROCESS_DATE}';")
    
    URGENT_SARS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_analytics.daily_sar_candidates WHERE filing_priority = 'URGENT' AND process_date = '${PROCESS_DATE}';")
    
    APPROACHING_DEADLINES_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_analytics.daily_sar_candidates WHERE regulatory_deadline <= CURRENT_DATE + INTERVAL '7 days' AND process_date = '${PROCESS_DATE}';")
    
    # Generate SAR packages for filing
    SAR_PACKAGES_CREATED=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "INSERT INTO fc_analytics.sar_packages (customer_id, case_id, package_date, sar_type, filing_status, regulatory_deadline, package_content) 
         SELECT customer_id, case_id, CURRENT_DATE, suspicious_activity_type, 'READY_FOR_FILING', regulatory_deadline, 
                json_build_object(
                    'customer_id', customer_id,
                    'case_id', case_id,
                    'activity_type', suspicious_activity_type,
                    'documentation', required_documentation,
                    'priority', filing_priority,
                    'deadline', regulatory_deadline
                )
         FROM fc_analytics.daily_sar_candidates 
         WHERE sar_threshold_met = true AND sar_status = 'READY' AND process_date = '${PROCESS_DATE}'
         RETURNING 1;" | wc -l)
    
    # Update case management with