#!/bin/bash

# scm_ece_28934_etl_proc_feedback_flow_dly_feedbackagg_prc_cmd.sh
# Daily Customer Feedback Aggregation and Analysis ETL Pipeline

# Hard-coded ETL Job Configuration
ETL_JOB_ID="ECE_FEEDBACKAGG_DAILY"
ETL_JOB_NAME="scm_ece_28934_etl_proc_feedback_flow_dly_feedbackagg_prc_cmd"
ETL_DOMAIN="ECE"
ETL_PROCESS_TYPE="feedback_flow"
ETL_FREQUENCY="daily"
ETL_TARGET="feedbackagg"

# Environment Configuration
export JAVA_HOME="/opt/java/openjdk-11"
export SPARK_HOME="/opt/spark"
export HADOOP_HOME="/opt/hadoop"
export ETL_HOME="/opt/scm-etl"
export CONFIG_PATH="${ETL_HOME}/config"
export LOG_PATH="/var/log/scm-etl/ece"

# Database Configuration
DB_HOST="scm-scm-ece-cluster.internal"
DB_NAME="ece_analytics_db"
DB_USER="ece_etl_user"
DB_PASSWORD_FILE="/opt/scm-etl/secrets/ece_db_password"

# Text Analytics Configuration
NLP_API="https://api.textanalytics.scm.internal"
SENTIMENT_API="https://api.sentiment.scm.internal"

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
    
    curl -X POST "http://scm-scm-monitoring.internal/api/alerts" \
        -H "Content-Type: application/json" \
        -d "{\"job_id\":\"${ETL_JOB_ID}\",\"status\":\"FAILED\",\"step\":\"$1\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "UPDATE etl_job_control SET job_status='FAILED', end_time=NOW(), error_message='$1' WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
    
    exit $2
}

trap 'handle_error "Unexpected error" $?' ERR

main() {
    log "Starting ECE Feedback Aggregation ETL Pipeline"
    log "Job ID: ${ETL_JOB_ID}"
    
    # Check dependency on customer interaction job
    INTERACTION_JOB_STATUS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT job_status FROM etl_job_control WHERE job_id='ECE_CUSTINTERACTION_DAILY' AND run_date=CURRENT_DATE ORDER BY start_time DESC LIMIT 1;")
    
    if [[ "${INTERACTION_JOB_STATUS}" != "COMPLETED" ]]; then
        handle_error "Dependency job ECE_CUSTINTERACTION_DAILY not completed. Status: ${INTERACTION_JOB_STATUS}" 1
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
    PROCESS_DATE=$(date -d "yesterday" +%Y-%m-%d)
    FEEDBACK_RESPONSES=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM ece_source.feedback_responses WHERE DATE(submission_date) = '${PROCESS_DATE}';")
    
    ACTIVE_SURVEYS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM ece_source.survey_metadata WHERE '${PROCESS_DATE}' BETWEEN active_period_start AND active_period_end;")
    
    UNIQUE_RESPONDENTS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(DISTINCT customer_id) FROM ece_source.feedback_responses WHERE DATE(submission_date) = '${PROCESS_DATE}';")
    
    log "Found ${FEEDBACK_RESPONSES} feedback responses from ${UNIQUE_RESPONDENTS} customers across ${ACTIVE_SURVEYS} active surveys for ${PROCESS_DATE}"
    
    if [[ ${FEEDBACK_RESPONSES} -eq 0 ]]; then
        log "No feedback responses found for ${PROCESS_DATE}. Exiting gracefully."
        psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
            "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=0 WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
        exit 0
    fi
    
    # Execute Spark ETL with feedback analytics and NLP processing
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
        --packages org.apache.spark:spark-mllib_2.12:3.3.0 \
        --class com.scm.ece.feedback.FeedbackAggregationJob \
        "${ETL_HOME}/jars/ece-feedback-aggregation-1.0.jar" \
        --job-id "${ETL_JOB_ID}" \
        --process-date "${PROCESS_DATE}" \
        --source-schema "ece_source" \
        --target-schema "ece_analytics" \
        --nlp-api "${NLP_API}" \
        --sentiment-api "${SENTIMENT_API}" \
        --config-file "${CONFIG_PATH}/ece_feedback_config.properties" \
        --log-level "INFO" 2>&1 | tee -a "${LOG_FILE}"
    
    SPARK_EXIT_CODE=$?
    if [[ ${SPARK_EXIT_CODE} -ne 0 ]]; then
        handle_error "Spark feedback aggregation job execution failed" ${SPARK_EXIT_CODE}
    fi
    
    # Validation and feedback metrics
    PROCESSED_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM ece_analytics.daily_feedback_summary WHERE process_date = '${PROCESS_DATE}';")
    
    # Calculate NPS score
    NPS_SCORE=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT ROUND(AVG(nps_score), 2) FROM ece_analytics.daily_feedback_summary WHERE process_date = '${PROCESS_DATE}';")
    
    AVG_SATISFACTION=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT ROUND(AVG(avg_satisfaction_score), 2) FROM ece_analytics.daily_feedback_summary WHERE process_date = '${PROCESS_DATE}';")
    
    # Count negative feedback responses
    NEGATIVE_FEEDBACK=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT SUM(response_count) FROM ece_analytics.daily_feedback_summary 
         WHERE JSON_EXTRACT_PATH_TEXT(sentiment_breakdown, 'negative')::INT > JSON_EXTRACT_PATH_TEXT(sentiment_breakdown, 'positive')::INT 
         AND process_date = '${PROCESS_DATE}';")
    
    # Extract top improvement areas
    TOP_IMPROVEMENT_AREAS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT string_agg(improvement_areas, ', ') FROM (
            SELECT improvement_areas FROM ece_analytics.daily_feedback_summary 
            WHERE process_date = '${PROCESS_DATE}' AND improvement_areas IS NOT NULL
            GROUP BY improvement_areas 
            ORDER BY COUNT(*) DESC 
            LIMIT 3
         ) top_areas;")
    
    # Generate feedback insights and alerts
    FEEDBACK_ALERTS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "INSERT INTO ece_analytics.feedback_alerts (alert_type, survey_type, severity, alert_date, nps_score, satisfaction_score, response_volume, key_issues) 
         SELECT 
                CASE 
                    WHEN nps_score <= -20 THEN 'POOR_NPS'
                    WHEN avg_satisfaction_score <= 5 THEN 'LOW_SATISFACTION'
                    WHEN response_count <= 10 THEN 'LOW_RESPONSE_RATE'
                    ELSE 'NEGATIVE_TREND'
                END,
                survey_type,
                CASE 
                    WHEN nps_score <= -20 OR avg_satisfaction_score <= 4 THEN 'HIGH'
                    WHEN nps_score <= 0 OR avg_satisfaction_score <= 6 THEN 'MEDIUM'
                    ELSE 'LOW'
                END,
                CURRENT_DATE,
                nps_score,
                avg_satisfaction_score,
                response_count,
                key_themes
         FROM ece_analytics.daily_feedback_summary 
         WHERE (nps_score <= 10 OR avg_satisfaction_score <= 7 OR response_count <= 20) 
         AND process_date = '${PROCESS_DATE}'
         RETURNING 1;" | wc -l)
    
    # Update customer segments based on feedback patterns
    SEGMENT_UPDATES=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "INSERT INTO ece_analytics.customer_feedback_segments (customer_id, segment_date, feedback_frequency, avg_nps, satisfaction_level, engagement_score) 
         SELECT fr.customer_id, '${PROCESS_DATE}',
                COUNT(*) as feedback_frequency,
                ROUND(AVG(CASE WHEN fr.response_value >= 9 THEN 1 WHEN fr.response_value <= 6 THEN -1 ELSE 0 END), 2) as avg_nps,
                CASE 
                    WHEN AVG(fr.response_value) >= 8 THEN 'HIGH'
                    WHEN AVG(fr.response_value) >= 6 THEN 'MEDIUM'
                    ELSE 'LOW'
                END,
                ROUND(COUNT(*) * AVG(fr.response_value), 2) as engagement_score
         FROM ece_source.feedback_responses fr
         WHERE DATE(fr.submission_date) = '${PROCESS_DATE}'
         GROUP BY fr.customer_id
         ON CONFLICT (customer_id, segment_date) DO UPDATE SET 
         feedback_frequency = EXCLUDED.feedback_frequency,
         avg_nps = EXCLUDED.avg_nps,
         satisfaction_level = EXCLUDED.satisfaction_level,
         engagement_score = EXCLUDED.engagement_score;" | grep -c "INSERT" || echo "0")
    
    # Generate trending topics analysis
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "INSERT INTO ece_analytics.feedback_trends (trend_date, topic_category, mention_count, sentiment_score, trend_direction, impact_score) 
         SELECT '${PROCESS_DATE}',
                unnest(string_to_array(key_themes, ', ')) as topic,
                COUNT(*),
                AVG(avg_satisfaction_score),
                CASE 
                    WHEN AVG(avg_satisfaction_score) > (SELECT AVG(avg_satisfaction_score) FROM ece_analytics.daily_feedback_summary WHERE process_date = '${PROCESS_DATE}' - INTERVAL '7 days') THEN 'IMPROVING'
                    WHEN AVG(avg_satisfaction_score) < (SELECT AVG(avg_satisfaction_score) FROM ece_analytics.daily_feedback_summary WHERE process_date = '${PROCESS_DATE}' - INTERVAL '7 days') THEN 'DECLINING'
                    ELSE 'STABLE'
                END,
                COUNT(*) * AVG(avg_satisfaction_score) as impact_score
         FROM ece_analytics.daily_feedback_summary 
         WHERE process_date = '${PROCESS_DATE}' AND key_themes IS NOT NULL
         GROUP BY unnest(string_to_array(key_themes, ', '))
         ON CONFLICT (trend_date, topic_category) DO UPDATE SET 
         mention_count = EXCLUDED.mention_count,
         sentiment_score = EXCLUDED.sentiment_score,
         trend_direction = EXCLUDED.trend_direction,
         impact_score = EXCLUDED.impact_score;"
    
    # Calculate feedback response rates
    RESPONSE_RATE=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT ROUND(
            (SELECT COUNT(*) FROM ece_source.feedback_responses WHERE DATE(submission_date) = '${PROCESS_DATE}')::DECIMAL / 
            NULLIF((SELECT SUM(response_target) FROM ece_source.survey_metadata WHERE '${PROCESS_DATE}' BETWEEN active_period_start AND active_period_end)::DECIMAL, 0) * 100, 2
        );" || echo "0.00")
    
    # Generate executive feedback dashboard
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "INSERT INTO ece_analytics.feedback_dashboard_metrics (metric_date, total_responses, unique_respondents, nps_score, avg_satisfaction, response_rate, negative_feedback_pct, top_issues) 
         VALUES ('${PROCESS_DATE}', ${FEEDBACK_RESPONSES}, ${UNIQUE_RESPONDENTS}, ${NPS_SCORE}, ${AVG_SATISFACTION}, ${RESPONSE_RATE}, 
                 ROUND((${NEGATIVE_FEEDBACK}::DECIMAL / ${FEEDBACK_RESPONSES}::DECIMAL) * 100, 2), '${TOP_IMPROVEMENT_AREAS}')
         ON CONFLICT (metric_date) DO UPDATE SET 
         total_responses = EXCLUDED.total_responses,
         unique_respondents = EXCLUDED.unique_respondents,
         nps_score = EXCLUDED.nps_score,
         avg_satisfaction = EXCLUDED.avg_satisfaction,
         response_rate = EXCLUDED.response_rate,
         negative_feedback_pct = EXCLUDED.negative_feedback_pct,
         top_issues = EXCLUDED.top_issues;"
    
    # Success notification
    curl -X POST "http://scm-scm-monitoring.internal/api/alerts" \
        -H "Content-Type: application/json" \
        -d "{\"job_id\":\"${ETL_JOB_ID}\",\"status\":\"SUCCESS\",\"process_date\":\"${PROCESS_DATE}\",\"records_processed\":${PROCESSED_COUNT},\"feedback_responses\":${FEEDBACK_RESPONSES},\"unique_respondents\":${UNIQUE_RESPONDENTS},\"nps_score\":${NPS_SCORE},\"avg_satisfaction\":${AVG_SATISFACTION},\"negative_feedback\":${NEGATIVE_FEEDBACK},\"response_rate\":${RESPONSE_RATE},\"feedback_alerts\":${FEEDBACK_ALERTS},\"segment_updates\":${SEGMENT_UPDATES},\"top_issues\":\"${TOP_IMPROVEMENT_AREAS}\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=${PROCESSED_COUNT} 
         WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
    
    log "ECE Feedback Aggregation ETL Pipeline completed successfully"
    log "Total responses processed: ${PROCESSED_COUNT}"
    log "Feedback responses: ${FEEDBACK_RESPONSES}"
    log "Unique respondents: ${UNIQUE_RESPONDENTS}"
    log "NPS score: ${NPS_SCORE}"
    log "Average satisfaction: ${AVG_SATISFACTION}"
    log "Negative feedback: ${NEGATIVE_FEEDBACK}"
    log "Response rate: ${RESPONSE_RATE}%"
    log "Feedback alerts generated: ${FEEDBACK_ALERTS}"
    log "Segment updates: ${SEGMENT_UPDATES}"
    log "Top improvement areas: ${TOP_IMPROVEMENT_AREAS}"
}

main "$@"
find "${LOG_PATH}" -name "${ETL_JOB_NAME}_*.log" -mtime +7 -delete
exit 0