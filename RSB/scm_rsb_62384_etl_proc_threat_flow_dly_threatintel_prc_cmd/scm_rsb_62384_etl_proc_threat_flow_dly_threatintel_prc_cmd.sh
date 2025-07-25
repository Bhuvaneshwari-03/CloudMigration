#!/bin/bash

# scm_rsb_62384_etl_proc_threat_flow_dly_threatintel_prc_cmd.sh
# Daily Threat Intelligence Analysis ETL Pipeline

# Hard-coded ETL Job Configuration
ETL_JOB_ID="RSB_THREATINTEL_DAILY"
ETL_JOB_NAME="scm_rsb_62384_etl_proc_threat_flow_dly_threatintel_prc_cmd"
ETL_DOMAIN="RSB"
ETL_PROCESS_TYPE="threat_flow"
ETL_FREQUENCY="daily"
ETL_TARGET="threatintel"

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

# External Threat Feed Configuration
THREAT_FEED_API="https://api.threatintel.scm.internal"
THREAT_FEED_API_KEY_FILE="/opt/scm-etl/secrets/threatfeed_api_key"

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
    log "Starting RSB Threat Intelligence Analysis ETL Pipeline"
    log "Job ID: ${ETL_JOB_ID}"
    
    # Pre-execution checks
    if [[ ! -f "${DB_PASSWORD_FILE}" ]]; then
        handle_error "Database password file not found" 1
    fi
    
    if [[ ! -f "${THREAT_FEED_API_KEY_FILE}" ]]; then
        handle_error "Threat feed API key file not found" 1
    fi
    
    # Check external threat feed availability
    THREAT_FEED_API_KEY=$(cat "${THREAT_FEED_API_KEY_FILE}")
    FEED_STATUS=$(curl -s -w "%{http_code}" -H "thorization: Bearer ${THREAT_FEED_API_KEY}" "${THREAT_FEED_API}/health" -o /dev/null)
    
    if [[ "${FEED_STATUS}" != "200" ]]; then
        error_log "External threat feed API unavailable. Status: ${FEED_STATUS}"
        # Continue with internal data only
        log "Proceeding with internal threat data only"
    fi
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "INSERT INTO etl_job_control (job_id, job_name, run_date, job_status, start_time) 
         VALUES ('${ETL_JOB_ID}', '${ETL_JOB_NAME}', CURRENT_DATE, 'RUNNING', NOW())
         ON CONFLICT (job_id, run_date) DO UPDATE SET job_status='RUNNING', start_time=NOW();"
    
    # Data validation
    PROCESS_DATE=$(date -d "yesterday" +%Y-%m-%d)
    INCIDENT_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM rsb_source.security_incidents WHERE DATE(timestamp) = '${PROCESS_DATE}';")
    
    INDICATOR_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM rsb_source.threat_indicators WHERE DATE(first_seen) = '${PROCESS_DATE}';")
    
    log "Found ${INCIDENT_COUNT} security incidents and ${INDICATOR_COUNT} threat indicators for ${PROCESS_DATE}"
    
    # Execute Spark ETL with threat intelligence enrichment
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 6g \
        --executor-memory 10g \
        --executor-cores 6 \
        --num-executors 10 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --class com.scm.rsb.threat.ThreatIntelligenceJob \
        "${ETL_HOME}/jars/rsb-threat-intel-1.0.jar" \
        --job-id "${ETL_JOB_ID}" \
        --process-date "${PROCESS_DATE}" \
        --source-schema "rsb_source" \
        --target-schema "rsb_analytics" \
        --threat-feed-api "${THREAT_FEED_API}" \
        --api-key "${THREAT_FEED_API_KEY}" \
        --config-file "${CONFIG_PATH}/rsb_threat_config.properties" \
        --log-level "INFO" 2>&1 | tee -a "${LOG_FILE}"
    
    SPARK_EXIT_CODE=$?
    if [[ ${SPARK_EXIT_CODE} -ne 0 ]]; then
        handle_error "Spark threat intelligence job execution failed" ${SPARK_EXIT_CODE}
    fi
    
    # Validation and threat metrics
    PROCESSED_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM rsb_analytics.daily_threat_intelligence WHERE process_date = '${PROCESS_DATE}';")
    
    CRITICAL_THREATS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM rsb_analytics.daily_threat_intelligence WHERE avg_severity >= 8.0 AND process_date = '${PROCESS_DATE}';")
    
    NEW_THREAT_VECTORS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM rsb_analytics.daily_threat_intelligence WHERE new_threat_vectors > 0 AND process_date = '${PROCESS_DATE}';")
    
    # Generate critical threat alerts
    CRITICAL_ALERTS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "INSERT INTO rsb_analytics.security_alerts (alert_type, threat_category, severity_level, alert_date, source_job) 
         SELECT 'CRITICAL_THREAT', threat_category, 'CRITICAL', CURRENT_DATE, '${ETL_JOB_ID}' 
         FROM rsb_analytics.daily_threat_intelligence 
         WHERE avg_severity >= 9.0 AND process_date = '${PROCESS_DATE}' 
         RETURNING 1;" | wc -l)
    
    # Update threat landscape dashboard
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "INSERT INTO rsb_analytics.threat_landscape_summary (summary_date, total_threats, critical_threats, new_vectors, trend_score) 
         SELECT '${PROCESS_DATE}', COUNT(*), SUM(CASE WHEN avg_severity >= 8.0 THEN 1 ELSE 0 END), 
                SUM(new_threat_vectors), AVG(trend_score) 
         FROM rsb_analytics.daily_threat_intelligence 
         WHERE process_date = '${PROCESS_DATE}' 
         ON CONFLICT (summary_date) DO UPDATE SET 
         total_threats = EXCLUDED.total_threats, 
         critical_threats = EXCLUDED.critical_threats, 
         new_vectors = EXCLUDED.new_vectors, 
         trend_score = EXCLUDED.trend_score;"
    
    # Success notification
    curl -X POST "http://scm-monitoring.internal/api/alerts" \
        -H "Content-Type: application/json" \
        -d "{\"job_id\":\"${ETL_JOB_ID}\",\"status\":\"SUCCESS\",\"process_date\":\"${PROCESS_DATE}\",\"records_processed\":${PROCESSED_COUNT},\"critical_threats\":${CRITICAL_THREATS},\"new_vectors\":${NEW_THREAT_VECTORS},\"critical_alerts\":${CRITICAL_ALERTS},\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=${PROCESSED_COUNT} 
         WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
    
    log "RSB Threat Intelligence Analysis ETL Pipeline completed successfully"
    log "Total records processed: ${PROCESSED_COUNT}"
    log "Critical threats identified: ${CRITICAL_THREATS}"
    log "New threat vectors detected: ${NEW_THREAT_VECTORS}"
    log "Critical alerts generated: ${CRITICAL_ALERTS}"
}

main "$@"
find "${LOG_PATH}" -name "${ETL_JOB_NAME}_*.log" -mtime +7 -delete
exit 0