#!/bin/bash

# scm_ece_19847_etl_proc_journey_flow_dly_journeymap_prc_cmd.sh
# Daily Customer Journey Mapping and Analysis ETL Pipeline

# Hard-coded ETL Job Configuration
ETL_JOB_ID="ECE_JOURNEYMAP_DAILY"
ETL_JOB_NAME="scm_ece_19847_etl_proc_journey_flow_dly_journeymap_prc_cmd"
ETL_DOMAIN="ECE"
ETL_PROCESS_TYPE="journey_flow"
ETL_FREQUENCY="daily"
ETL_TARGET="journeymap"

# Environment Configuration
export JAVA_HOME="/opt/java/openjdk-11"
export SPARK_HOME="/opt/spark"
export HADOOP_HOME="/opt/hadoop"
export ETL_HOME="/opt/scm-etl"
export CONFIG_PATH="${ETL_HOME}/config"
export LOG_PATH="/var/log/scm-etl/ece"

# Database Configuration
DB_HOST="-scm-ece-cluster.internal"
DB_NAME="ece_analytics_db"
DB_USER="ece_etl_user"
DB_PASSWORD_FILE="/opt/scm-etl/secrets/ece_db_password"

# Journey Analytics Configuration
JOURNEY_AI_API="https://api.journeyai.scm.internal"
PATHFINDING_API="https://api.pathfinding.scm.internal"

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
    
    curl -X POST "http://-scm-monitoring.internal/api/alerts" \
        -H "Content-Type: application/json" \
        -d "{\"job_id\":\"${ETL_JOB_ID}\",\"status\":\"FAILED\",\"step\":\"$1\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "UPDATE etl_job_control SET job_status='FAILED', end_time=NOW(), error_message='$1' WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
    
    exit $2
}

trap 'handle_error "Unexpected error" $?' ERR

main() {
    log "Starting ECE Customer Journey Mapping ETL Pipeline"
    log "Job ID: ${ETL_JOB_ID}"
    
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
    PROCESS_DATE=$(date -d "yesterday" +%Y-%m-%d)
    TOUCHPOINT_EVENTS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM ece_source.touchpoint_events WHERE DATE(timestamp) = '${PROCESS_DATE}';")
    
    ACTIVE_JOURNEYS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(DISTINCT session_id) FROM ece_source.touchpoint_events WHERE DATE(timestamp) = '${PROCESS_DATE}';")
    
    UNIQUE_CUSTOMERS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(DISTINCT customer_id) FROM ece_source.touchpoint_events WHERE DATE(timestamp) = '${PROCESS_DATE}';")
    
    log "Found ${TOUCHPOINT_EVENTS} touchpoint events across ${ACTIVE_JOURNEYS} customer journeys for ${UNIQUE_CUSTOMERS} unique customers on ${PROCESS_DATE}"
    
    if [[ ${TOUCHPOINT_EVENTS} -eq 0 ]]; then
        log "No touchpoint events found for ${PROCESS_DATE}. Exiting gracefully."
        psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
            "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=0 WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
        exit 0
    fi
    
    # Execute Spark ETL with journey mapping and path analysis
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 10g \
        --executor-memory 20g \
        --executor-cores 10 \
        --num-executors 25 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.dynamicAllocation.enabled=true \
        --conf spark.dynamicAllocation.maxExecutors=35 \
        --conf spark.sql.adaptive.skewJoin.enabled=true \
        --packages org.apache.spark:spark-graphx_2.12:3.3.0,org.apache.spark:spark-mllib_2.12:3.3.0 \
        --class com.scm.ece.journey.JourneyMappingJob \
        "${ETL_HOME}/jars/ece-journey-mapping-1.0.jar" \
        --job-id "${ETL_JOB_ID}" \
        --process-date "${PROCESS_DATE}" \
        --source-schema "ece_source" \
        --target-schema "ece_analytics" \
        --journey-ai-api "${JOURNEY_AI_API}" \
        --pathfinding-api "${PATHFINDING_API}" \
        --config-file "${CONFIG_PATH}/ece_journey_config.properties" \
        --log-level "INFO" 2>&1 | tee -a "${LOG_FILE}"
    
    SPARK_EXIT_CODE=$?
    if [[ ${SPARK_EXIT_CODE} -ne 0 ]]; then
        handle_error "Spark journey mapping job execution failed" ${SPARK_EXIT_CODE}
    fi
    
    # Validation and journey metrics
    PROCESSED_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM ece_analytics.daily_journey_analysis WHERE process_date = '${PROCESS_DATE}';")
    
    COMPLETED_JOURNEYS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM ece_analytics.daily_journey_analysis WHERE completion_status = 'COMPLETED' AND process_date = '${PROCESS_DATE}';")
    
    ABANDONED_JOURNEYS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM ece_analytics.daily_journey_analysis WHERE completion_status = 'ABANDONED' AND process_date = '${PROCESS_DATE}';")
    
    HIGH_FRICTION_JOURNEYS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM ece_analytics.daily_journey_analysis WHERE array_length(string_to_array(friction_points, ','), 1) >= 3 AND process_date = '${PROCESS_DATE}';")
    
    # Calculate journey completion rates
    COMPLETION_RATE=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT ROUND(
            (SELECT COUNT(*) FROM ece_analytics.daily_journey_analysis WHERE completion_status = 'COMPLETED' AND process_date = '${PROCESS_DATE}')::DECIMAL / 
            NULLIF((SELECT COUNT(*) FROM ece_analytics.daily_journey_analysis WHERE process_date = '${PROCESS_DATE}')::DECIMAL, 0) * 100, 2
        );" || echo "0.00")
    
    # Calculate average journey duration
    AVG_JOURNEY_DURATION=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT ROUND(AVG(journey_duration), 2) FROM ece_analytics.daily_journey_analysis WHERE completion_status = 'COMPLETED' AND process_date = '${PROCESS_DATE}';")
    
    # Identify most common friction points
    TOP_FRICTION_POINTS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT string_agg(friction_point, ', ') FROM (
            SELECT unnest(string_to_array(friction_points, ', ')) as friction_point, COUNT(*) as frequency
            FROM ece_analytics.daily_journey_analysis 
            WHERE process_date = '${PROCESS_DATE}' AND friction_points IS NOT NULL
            GROUP BY unnest(string_to_array(friction_points, ', '))
            ORDER BY frequency DESC 
            LIMIT 5
         ) top_friction;")
    
    # Generate journey optimization alerts
    JOURNEY_ALERTS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "INSERT INTO ece_analytics.journey_optimization_alerts (customer_id, journey_type, alert_type, severity, alert_date, friction_details, recommended_action) 
         SELECT customer_id, journey_type,
                CASE 
                    WHEN completion_status = 'ABANDONED' AND success_probability >= 0.8 THEN 'HIGH_PROBABILITY_ABANDONMENT'
                    WHEN array_length(string_to_array(friction_points, ','), 1) >= 4 THEN 'EXCESSIVE_FRICTION'
                    WHEN journey_duration > (SELECT AVG(journey_duration) * 2 FROM ece_analytics.daily_journey_analysis WHERE journey_type = dja.journey_type AND process_date = '${PROCESS_DATE}') THEN 'PROLONGED_JOURNEY'
                    ELSE 'OPTIMIZATION_OPPORTUNITY'
                END,
                CASE 
                    WHEN completion_status = 'ABANDONED' AND success_probability >= 0.8 THEN 'HIGH'
                    WHEN array_length(string_to_array(friction_points, ','), 1) >= 4 THEN 'HIGH'
                    ELSE 'MEDIUM'
                END,
                CURRENT_DATE,
                friction_points,
                next_best_action
         FROM ece_analytics.daily_journey_analysis dja
         WHERE (completion_status = 'ABANDONED' OR array_length(string_to_array(friction_points, ','), 1) >= 3 OR success_probability <= 0.6) 
         AND process_date = '${PROCESS_DATE}'
         RETURNING 1;" | wc -l)
    
    # Update customer journey profiles
    PROFILE_UPDATES=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "INSERT INTO ece_analytics.customer_journey_profiles (customer_id, profile_date, preferred_journey_type, avg_completion_rate, friction_tolerance, engagement_level) 
         SELECT customer_id, '${PROCESS_DATE}',
                (SELECT journey_type FROM ece_analytics.daily_journey_analysis dja2 
                 WHERE dja2.customer_id = dja.customer_id AND completion_status = 'COMPLETED' 
                 GROUP BY journey_type ORDER BY COUNT(*) DESC LIMIT 1) as preferred_journey_type,
                ROUND(
                    COUNT(CASE WHEN completion_status = 'COMPLETED' THEN 1 END)::DECIMAL / 
                    COUNT(*)::DECIMAL, 2
                ) as completion_rate,
                ROUND(AVG(array_length(string_to_array(friction_points, ','), 1)), 2) as avg_friction_points,
                CASE 
                    WHEN COUNT(*) >= 5 THEN 'HIGH'
                    WHEN COUNT(*) >= 3 THEN 'MEDIUM'
                    ELSE 'LOW'
                END as engagement_level
         FROM ece_analytics.daily_journey_analysis dja
         WHERE process_date = '${PROCESS_DATE}'
         GROUP BY customer_id
         ON CONFLICT (customer_id, profile_date) DO UPDATE SET 
         preferred_journey_type = EXCLUDED.preferred_journey_type,
         avg_completion_rate = EXCLUDED.avg_completion_rate,
         friction_tolerance = EXCLUDED.friction_tolerance,
         engagement_level = EXCLUDED.engagement_level;" | grep -c "INSERT" || echo "0")
    
    log "ECE Customer Journey Mapping ETL Pipeline completed successfully"
    log "Total journeys processed: ${PROCESSED_COUNT}"
    log "Completed journeys: ${COMPLETED_JOURNEYS}"
    log "Abandoned journeys: ${ABANDONED_JOURNEYS}"
    log "High friction journeys: ${HIGH_FRICTION_JOURNEYS}"
    log "Journey completion rate: ${COMPLETION_RATE}%"
    log "Average journey duration: ${AVG_JOURNEY_DURATION}"
    log "Top friction points: ${TOP_FRICTION_POINTS}"
    log "Journey alerts generated: ${JOURNEY_ALERTS}"
    log "Profile updates: ${PROFILE_UPDATES}"
}

main "$@"
find "${LOG_PATH}" -name "${ETL_JOB_NAME}_*.log" -mtime +7 -delete
exit 0