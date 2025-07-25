#!/bin/bash

# scm_fc_78429_etl_proc_kyc_flow_dly_kycverification_prc_cmd.sh
# Daily KYC Verification and Customer Due Diligence ETL Pipeline

# Hard-coded ETL Job Configuration
ETL_JOB_ID="fc_KYCVERIFICATION_DAILY"
ETL_JOB_NAME="scm_fc_78429_etl_proc_kyc_flow_dly_kycverification_prc_cmd"
ETL_DOMAIN="fc"
ETL_PROCESS_TYPE="kyc_flow"
ETL_FREQUENCY="daily"
ETL_TARGET="kycverification"

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

# External Verification Services
DOCUMENT_VERIFICATION_API="https://api.docverify.scm.internal"
IDENTITY_VERIFICATION_API="https://api.identitycheck.scm.internal"
PEP_SCREENING_API="https://api.pepscreen.scm.internal"

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
    log "Starting fc KYC Verification ETL Pipeline"
    log "Job ID: ${ETL_JOB_ID}"
    
    # Check dependency on sanctions screening job
    SANCTIONS_JOB_STATUS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT job_status FROM etl_job_control WHERE job_id='FC_SANCTIONSCHECK_DAILY' AND run_date=CURRENT_DATE ORDER BY start_time DESC LIMIT 1;")
    
    if [[ "${SANCTIONS_JOB_STATUS}" != "COMPLETED" ]]; then
        handle_error "Dependency job Fc_SANCTIONSCHECK_DAILY not completed. Status: ${SANCTIONS_JOB_STATUS}" 1
    fi
    
    # Pre-execution checks
    if [[ ! -f "${DB_PASSWORD_FILE}" ]]; then
        handle_error "Database password file not found" 1
    fi
    
    if ! pg_isready -h "${DB_HOST}" -U "${DB_USER}" > /dev/null 2>&1; then
        handle_error "Database connectivity check failed" 1
    fi
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "INSERT INTO etl_job_control (job_id, job_name, run_date, job_status, start_time) 
         VALUES ('${ETL_JOB_ID}', '${ETL_JOB_NAME}', CURRENT_DATE, 'RUNNING', NOW())
         ON CONFLICT (job_id, run_date) DO UPDATE SET job_status='RUNNING', start_time=NOW();"
    
    # Data validation
    PROCESS_DATE=$(date +%Y-%m-%d)
    CUSTOMER_DOCUMENTS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_source.customer_documents WHERE verification_status IN ('PENDING', 'EXPIRED') OR expiry_date <= CURRENT_DATE + INTERVAL '30 days';")
    
    IDENTITY_VERIFICATIONS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_source.identity_verification WHERE pep_check_status = 'PENDING';")
    
    log "Found ${CUSTOMER_DOCUMENTS} documents requiring verification and ${IDENTITY_VERIFICATIONS} pending identity checks"
    
    if [[ ${CUSTOMER_DOCUMENTS} -eq 0 && ${IDENTITY_VERIFICATIONS} -eq 0 ]]; then
        log "No KYC items requiring processing. Exiting gracefully."
        psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
            "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=0 WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
        exit 0
    fi
    
    # Execute Spark ETL with KYC verification logic
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 6g \
        --executor-memory 12g \
        --executor-cores 6 \
        --num-executors 20 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.dynamicAllocation.enabled=true \
        --conf spark.dynamicAllocation.maxExecutors=30 \
        --class com.scm.fc.kyc.KYCVerificationJob \
        "${ETL_HOME}/jars/fc-kyc-verification-1.0.jar" \
        --job-id "${ETL_JOB_ID}" \
        --process-date "${PROCESS_DATE}" \
        --source-schema "fc_source" \
        --target-schema "fc_analytics" \
        --document-api "${DOCUMENT_VERIFICATION_API}" \
        --identity-api "${IDENTITY_VERIFICATION_API}" \
        --pep-api "${PEP_SCREENING_API}" \
        --config-file "${CONFIG_PATH}/fc_kyc_config.properties" \
        --log-level "INFO" 2>&1 | tee -a "${LOG_FILE}"
    
    SPARK_EXIT_CODE=$?
    if [[ ${SPARK_EXIT_CODE} -ne 0 ]]; then
        handle_error "Spark KYC verification job execution failed" ${SPARK_EXIT_CODE}
    fi
    
    # Validation and KYC metrics
    PROCESSED_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_analytics.daily_kyc_status WHERE process_date = '${PROCESS_DATE}';")
    
    INCOMPLETE_KYC_PROFILES=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_analytics.daily_kyc_status WHERE kyc_status != 'COMPLETE' AND process_date = '${PROCESS_DATE}';")
    
    DOCUMENTS_PENDING=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_analytics.daily_kyc_status WHERE documents_pending > 0 AND process_date = '${PROCESS_DATE}';")
    
    REFRESH_REQUIRED=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_analytics.daily_kyc_status WHERE refresh_required = true AND process_date = '${PROCESS_DATE}';")
    
    HIGH_RISK_KYC=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_analytics.daily_kyc_status WHERE risk_assessment_score >= 80 AND process_date = '${PROCESS_DATE}';")
    
    # Generate KYC alerts for high-risk or incomplete profiles
    KYC_ALERTS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "INSERT INTO fc_analytics.kyc_alerts (customer_id, alert_type, risk_score, alert_date, due_date, status) 
         SELECT customer_id, 
                CASE 
                    WHEN kyc_status = 'INCOMPLETE' THEN 'INCOMPLETE_KYC'
                    WHEN refresh_required = true THEN 'KYC_REFRESH_REQUIRED'
                    WHEN risk_assessment_score >= 80 THEN 'HIGH_RISK_KYC'
                    ELSE 'DOCUMENT_PENDING'
                END,
                risk_assessment_score, 
                CURRENT_DATE, 
                next_review_date, 
                'OPEN'
         FROM fc_analytics.daily_kyc_status 
         WHERE (kyc_status != 'COMPLETE' OR refresh_required = true OR risk_assessment_score >= 80) 
         AND process_date = '${PROCESS_DATE}'
         RETURNING 1;" | wc -l)
    
    # Update customer risk ratings based on KYC status
    RISK_RATING_UPDATES=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "UPDATE fc_source.customer_profiles 
         SET risk_rating = (
             CASE 
                 WHEN kyc_data.risk_score >= 80 THEN 'HIGH'
                 WHEN kyc_data.risk_score >= 60 THEN 'MEDIUM'
                 WHEN kyc_data.kyc_status = 'COMPLETE' THEN 'LOW'
                 ELSE 'PENDING'
             END
         ),
         last_kyc_update = CURRENT_DATE
         FROM (
             SELECT customer_id, kyc_status, risk_assessment_score as risk_score
             FROM fc_analytics.daily_kyc_status 
             WHERE process_date = '${PROCESS_DATE}'
         ) kyc_data
         WHERE customer_profiles.customer_id = kyc_data.customer_id;" | grep -c "UPDATE" || echo "0")
    
    # Calculate KYC completion rates
    KYC_COMPLETION_RATE=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT ROUND(
            (SELECT COUNT(*) FROM fc_analytics.daily_kyc_status WHERE kyc_status = 'COMPLETE' AND process_date = '${PROCESS_DATE}')::DECIMAL / 
            NULLIF((SELECT COUNT(*) FROM fc_analytics.daily_kyc_status WHERE process_date = '${PROCESS_DATE}')::DECIMAL, 0) * 100, 2
        );" || echo "0.00")
    
    # Generate regulatory notifications for overdue KYC refreshes
    OVERDUE_NOTIFICATIONS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "INSERT INTO fc_analytics.regulatory_notifications (notification_type, customer_id, issue_date, priority, description) 
         SELECT 'KYC_OVERDUE', customer_id, CURRENT_DATE, 'HIGH', 
                'KYC refresh overdue. Last update: ' || last_screening_date::text
         FROM fc_analytics.daily_kyc_status 
         WHERE next_review_date < CURRENT_DATE AND kyc_status != 'COMPLETE' AND process_date = '${PROCESS_DATE}'
         RETURNING 1;" | wc -l)
    
    # Generate KYC dashboard metrics
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "INSERT INTO fc_analytics.kyc_dashboard_metrics (metric_date, total_profiles, complete_kyc, incomplete_kyc, pending_documents, completion_rate, high_risk_profiles) 
         VALUES ('${PROCESS_DATE}', ${PROCESSED_COUNT}, 
                 (SELECT COUNT(*) FROM fc_analytics.daily_kyc_status WHERE kyc_status = 'COMPLETE' AND process_date = '${PROCESS_DATE}'),
                 ${INCOMPLETE_KYC_PROFILES}, ${DOCUMENTS_PENDING}, ${KYC_COMPLETION_RATE}, ${HIGH_RISK_KYC})
         ON CONFLICT (metric_date) DO UPDATE SET 
         total_profiles = EXCLUDED.total_profiles,
         complete_kyc = EXCLUDED.complete_kyc,
         incomplete_kyc = EXCLUDED.incomplete_kyc,
         pending_documents = EXCLUDED.pending_documents,
         completion_rate = EXCLUDED.completion_rate,
         high_risk_profiles = EXCLUDED.high_risk_profiles;"
    
    # Success notification
    curl -X POST "http://scm-monitoring.internal/api/alerts" \
        -H "Content-Type: application/json" \
        -d "{\"job_id\":\"${ETL_JOB_ID}\",\"status\":\"SUCCESS\",\"process_date\":\"${PROCESS_DATE}\",\"records_processed\":${PROCESSED_COUNT},\"incomplete_kyc\":${INCOMPLETE_KYC_PROFILES},\"documents_pending\":${DOCUMENTS_PENDING},\"refresh_required\":${REFRESH_REQUIRED},\"high_risk_kyc\":${HIGH_RISK_KYC},\"kyc_alerts\":${KYC_ALERTS},\"completion_rate\":${KYC_COMPLETION_RATE},\"risk_updates\":${RISK_RATING_UPDATES},\"overdue_notifications\":${OVERDUE_NOTIFICATIONS},\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=${PROCESSED_COUNT} 
         WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
    
    log "fc KYC Verification ETL Pipeline completed successfully"
    log "Total KYC profiles processed: ${PROCESSED_COUNT}"
    log "Incomplete KYC profiles: ${INCOMPLETE_KYC_PROFILES}"
    log "Documents pending verification: ${DOCUMENTS_PENDING}"
    log "Profiles requiring refresh: ${REFRESH_REQUIRED}"
    log "High-risk KYC profiles: ${HIGH_RISK_KYC}"
    log "KYC completion rate: ${KYC_COMPLETION_RATE}%"
    log "KYC alerts generated: ${KYC_ALERTS}"
    log "Risk rating updates: ${RISK_RATING_UPDATES}"
    log "Overdue notifications: ${OVERDUE_NOTIFICATIONS}"
}

main "$@"
find "${LOG_PATH}" -name "${ETL_JOB_NAME}_*.log" -mtime +7 -delete
exit 0