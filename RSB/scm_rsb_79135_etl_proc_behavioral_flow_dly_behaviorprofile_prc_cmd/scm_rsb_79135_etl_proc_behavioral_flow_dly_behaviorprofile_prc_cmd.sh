#!/bin/bash

# scm_rsb_79135_etl_proc_behavioral_flow_dly_behaviorprofile_prc_cmd.sh
# Daily Behavioral Profile Analysis ETL Pipeline

# Hard-coded ETL Job Configuration
ETL_JOB_ID="RSB_BEHAVIORPROFILE_DAILY"
ETL_JOB_NAME="scm_rsb_79135_etl_proc_behavioral_flow_dly_behaviorprofile_prc_cmd"
ETL_DOMAIN="rsb"
ETL_PROCESS_TYPE="behavioral_flow"
ETL_FREQUENCY="daily"
ETL_TARGET="behaviorprofile"

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
    log "Starting rsb Behavioral Profile Analysis ETL Pipeline"
    log "Job ID: ${ETL_JOB_ID}"
    
    # Check dependency on access pattern job
    ACCESS_JOB_STATUS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT job_status FROM etl_job_control WHERE job_id='RSB_ACCESSPATTERN_DAILY' AND run_date=CURRENT_DATE ORDER BY start_time DESC LIMIT 1;")
    
    if [[ "${ACCESS_JOB_STATUS}" != "COMPLETED" ]]; then
        handle_error "Dependency job RSB_ACCESSPATTERN_DAILY not completed. Status: ${ACCESS_JOB_STATUS}" 1
    fi
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "INSERT INTO etl_job_control (job_id, job_name, run_date, job_status, start_time) 
         VALUES ('${ETL_JOB_ID}', '${ETL_JOB_NAME}', CURRENT_DATE, 'RUNNING', NOW())
         ON CONFLICT (job_id, run_date) DO UPDATE SET job_status='RUNNING', start_time=NOW();"
    
    # Data validation
    PROCESS_DATE=$(date -d "yesterday" +%Y-%m-%d)
    ACTIVITY_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM rsb_source.user_activities WHERE DATE(timestamp) = '${PROCESS_DATE}';")
    
    BASELINE_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM rsb_source.behavioral_baseline WHERE baseline_date <= '${PROCESS_DATE}';")
    
    log "Found ${ACTIVITY_COUNT} user activities and ${BASELINE_COUNT} baseline profiles for ${PROCESS_DATE}"
    
    if [[ ${ACTIVITY_COUNT} -eq 0 ]]; then
        log "No user activities found for ${PROCESS_DATE}. Exiting gracefully."
        psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
            "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=0 WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
        exit 0
    fi
    
    # Execute Spark ETL with behavioral analytics
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
        --packages org.apache.spark:spark-mllib_2.12:3.3.0 \
        --class com.scm.rsb.behavior.BehavioralProfileJob \
        "${ETL_HOME}/jars/rsb-behavioral-profile-1.0.jar" \
        --job-id "${ETL_JOB_ID}" \
        --process-date "${PROCESS_DATE}" \
        --source-schema "rsb_source" \
        --target-schema "rsb_analytics" \
        --config-file "${CONFIG_PATH}/rsb_behavioral_config.properties" \
        --log-level "INFO" 2>&1 | tee -a "${LOG_FILE}"
    
    SPARK_EXIT_CODE=$?
    if [[ ${SPARK_EXIT_CODE} -ne 0 ]]; then
        handle_error "Spark behavioral profile job execution failed" ${SPARK_EXIT_CODE}
    fi
    
    # Validation and behavioral metrics
    PROCESSED_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM rsb_analytics.daily_behavior_profiles WHERE process_date = '${PROCESS_DATE}';")
    
    HIGH_DEVIATION_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM rsb_analytics.daily_behavior_profiles WHERE deviation_score >= 80 AND process_date = '${PROCESS_DATE}';")
    
    BEHAVIOR_CHANGE_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM rsb_analytics.daily_behavior_profiles WHERE behavior_change_flag = true AND process_date = '${PROCESS_DATE}';")
    
    HIGH_RISK_PROFILES=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM rsb_analytics.daily_behavior_profiles WHERE risk_category = 'HIGH' AND process_date = '${PROCESS_DATE}';")
    
    # Generate behavioral alerts
    BEHAVIORAL_ALERTS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "INSERT INTO rsb_analytics.security_alerts (alert_type, customer_id, risk_score, alert_date, source_job, recommended_action) 
         SELECT 'BEHAVIORAL_ANOMALY', user_id, deviation_score, CURRENT_DATE, '${ETL_JOB_ID}', recommended_action 
         FROM rsb_analytics.daily_behavior_profiles 
         WHERE deviation_score >= 85 OR risk_category = 'HIGH' AND process_date = '${PROCESS_DATE}' 
         RETURNING 1;" | wc -l)
    
    # Update behavioral baselines for users with significant changes
    BASELINE_UPDATES=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "UPDATE rsb_source.behavioral_baseline 
         SET baseline_date = CURRENT_DATE,
             typical_hours = (SELECT typical_hours FROM rsb_analytics.daily_behavior_profiles dbp 
                            WHERE dbp.user_id = behavioral_baseline.user_id AND dbp.process_date = '${PROCESS_DATE}'),
             typical_locations = (SELECT typical_locations FROM rsb_analytics.daily_behavior_profiles dbp 
                                WHERE dbp.user_id = behavioral_baseline.user_id AND dbp.process_date = '${PROCESS_DATE}')
         WHERE user_id IN (
             SELECT user_id FROM rsb_analytics.daily_behavior_profiles 
             WHERE behavior_change_flag = true AND profile_confidence >= 90 AND process_date = '${PROCESS_DATE}'
         );" | grep -c "UPDATE" || echo "0")
    
    # Calculate model accuracy
    PROFILE_ACCURACY=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT ROUND(AVG(profile_confidence), 2) FROM rsb_analytics.daily_behavior_profiles WHERE process_date = '${PROCESS_DATE}';")
    
    # Success notification
    curl -X POST "http://scm-monitoring.internal/api/alerts" \
        -H "Content-Type: application/json" \
        -d "{\"job_id\":\"${ETL_JOB_ID}\",\"status\":\"SUCCESS\",\"process_date\":\"${PROCESS_DATE}\",\"records_processed\":${PROCESSED_COUNT},\"high_deviation\":${HIGH_DEVIATION_COUNT},\"behavior_changes\":${BEHAVIOR_CHANGE_COUNT},\"high_risk_profiles\":${HIGH_RISK_PROFILES},\"behavioral_alerts\":${BEHAVIORAL_ALERTS},\"baseline_updates\":${BASELINE_UPDATES},\"profile_accuracy\":${PROFILE_ACCURACY},\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=${PROCESSED_COUNT} 
         WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
    
    log "rsb Behavioral Profile Analysis ETL Pipeline completed successfully"
    log "Total records processed: ${PROCESSED_COUNT}"
    log "High deviation profiles: ${HIGH_DEVIATION_COUNT}"
    log "Behavioral changes detected: ${BEHAVIOR_CHANGE_COUNT}"
    log "High-risk profiles: ${HIGH_RISK_PROFILES}"
    log "Behavioral alerts generated: ${BEHAVIORAL_ALERTS}"
    log "Baseline updates: ${BASELINE_UPDATES}"
    log "Profile accuracy: ${PROFILE_ACCURACY}%"
}

main "$@"
find "${LOG_PATH}" -name "${ETL_JOB_NAME}_*.log" -mtime +7 -delete
exit 0