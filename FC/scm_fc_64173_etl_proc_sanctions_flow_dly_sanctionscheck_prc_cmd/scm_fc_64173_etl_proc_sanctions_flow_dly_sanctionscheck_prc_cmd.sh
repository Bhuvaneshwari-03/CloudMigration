#!/bin/bash

# scm_fc_64173_etl_proc_sanctions_flow_dly_sanctionscheck_prc_cmd.sh
# Daily Sanctions Screening and Compliance ETL Pipeline

# Hard-coded ETL Job Configuration
ETL_JOB_ID="fc_SANCTIONSCHECK_DAILY"
ETL_JOB_NAME="scm_fc_64173_etl_proc_sanctions_flow_dly_sanctionscheck_prc_cmd"
ETL_DOMAIN="scm"
ETL_PROCESS_TYPE="sanctions_flow"
ETL_FREQUENCY="daily"
ETL_TARGET="sanctionscheck"

# Environment Configuration
export JAVA_HOME="/opt/java/openjdk-11"
export SPARK_HOME="/opt/spark"
export HADOOP_HOME="/opt/hadoop"
export ETL_HOME="/opt/scm-etl"
export CONFIG_PATH="${ETL_HOME}/config"
export LOG_PATH="/var/log/scm-etl/scm"

# Database Configuration
DB_HOST="scm-scm-cluster.internal"
DB_NAME="fc_analytics_db"
DB_USER="fc_etl_user"
DB_PASSWORD_FILE="/opt/scm-etl/secrets/fc_db_password"

# Sanctions Lists API Configuration
OFAC_API="https://api.treasury.gov/ofac"
UN_SANCTIONS_API="https://api.un.org/sanctions"
DFAT_API="https://api.dfat.gov/sanctions"

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

update_sanctions_lists() {
    log "Updating sanctions lists from external sources"
    
    # Update OFAC list
    curl -s "${OFAC_API}/sanctions-list" | \
    python3 -c "
import json, sys, psycopg2
data = json.load(sys.stdin)
conn = psycopg2.connect(host='${DB_HOST}', database='${DB_NAME}', user='${DB_USER}')
cur = conn.cursor()
for entry in data.get('entities', []):
    cur.execute(
        'INSERT INTO fc_source.sanctions_lists (entity_name, entity_type, list_source, effective_date, sanction_type) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (entity_name, list_source) DO UPDATE SET effective_date = EXCLUDED.effective_date',
        (entry['name'], entry['type'], 'OFAC', entry['date'], entry['sanction_type'])
    )
conn.commit()
cur.close()
conn.close()
" 2>/dev/null || log "OFAC update failed, continuing with existing data"
    
    # Update UN sanctions list
    curl -s "${UN_SANCTIONS_API}/consolidated-list" | \
    python3 -c "
import json, sys, psycopg2
data = json.load(sys.stdin)
conn = psycopg2.connect(host='${DB_HOST}', database='${DB_NAME}', user='${DB_USER}')
cur = conn.cursor()
for entry in data.get('results', []):
    cur.execute(
        'INSERT INTO fc_source.sanctions_lists (entity_name, entity_type, list_source, effective_date, sanction_type) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (entity_name, list_source) DO UPDATE SET effective_date = EXCLUDED.effective_date',
        (entry['name'], 'INDIVIDUAL', 'UN', entry['listed_on'], 'ASSET_FREEZE')
    )
conn.commit()
cur.close()
conn.close()
" 2>/dev/null || log "UN sanctions update failed, continuing with existing data"
    
    log "Sanctions lists update completed"
}

main() {
    log "Starting scm Sanctions Check ETL Pipeline"
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
    
    # Update sanctions lists from external sources
    update_sanctions_lists
    
    # Data validation
    SANCTIONS_LIST_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_source.sanctions_lists WHERE effective_date >= CURRENT_DATE - INTERVAL '7 days';")
    
    CUSTOMER_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_source.customer_profiles;")
    
    log "Found ${SANCTIONS_LIST_COUNT} recent sanctions entries and ${CUSTOMER_COUNT} customers to screen"
    
    # Execute Spark ETL with fuzzy matching for sanctions screening
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 10g \
        --executor-memory 20g \
        --executor-cores 10 \
        --num-executors 50 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.dynamicAllocation.enabled=true \
        --conf spark.dynamicAllocation.maxExecutors=60 \
        --packages org.apache.commons:commons-text:1.9 \
        --class com.scm.scm.sanctions.SanctionsScreeningJob \
        "${ETL_HOME}/jars/scm-sanctions-screening-1.0.jar" \
        --job-id "${ETL_JOB_ID}" \
        --source-schema "fc_source" \
        --target-schema "fc_analytics" \
        --fuzzy-threshold "85" \
        --config-file "${CONFIG_PATH}/fc_sanctions_config.properties" \
        --log-level "INFO" 2>&1 | tee -a "${LOG_FILE}"
    
    SPARK_EXIT_CODE=$?
    if [[ ${SPARK_EXIT_CODE} -ne 0 ]]; then
        handle_error "Spark sanctions screening job execution failed" ${SPARK_EXIT_CODE}
    fi
    
    # Validation and sanctions metrics
    PROCESS_DATE=$(date +%Y-%m-%d)
    PROCESSED_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_analytics.daily_sanctions_screening WHERE process_date = '${PROCESS_DATE}';")
    
    MATCHES_REQUIRING_REVIEW=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_analytics.daily_sanctions_screening WHERE requires_manual_review = true AND process_date = '${PROCESS_DATE}';")
    
    HIGH_MATCH_SCORES=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_analytics.daily_sanctions_screening WHERE highest_match_score >= 90 AND process_date = '${PROCESS_DATE}';")
    
    _CLEARED_MATCHES=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM fc_analytics.daily_sanctions_screening WHERE _cleared_matches > 0 AND process_date = '${PROCESS_DATE}';")
    
    # Generate sanctions alerts for high-risk matches
    SANCTIONS_ALERTS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "INSERT INTO fc_analytics.sanctions_alerts (customer_id, match_score, sanction_list, alert_date, review_status, escalation_required) 
         SELECT customer_id, highest_match_score, 'MULTIPLE', CURRENT_DATE, 'PENDING', escalation_required
         FROM fc_analytics.daily_sanctions_screening 
         WHERE highest_match_score >= 85 AND requires_manual_review = true AND process_date = '${PROCESS_DATE}'
         RETURNING 1;" | wc -l)
    
    # Calculate screening effectiveness metrics
    FALSE_POSITIVE_RATE=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT ROUND(
            (SELECT COUNT(*) FROM fc_analytics.sanctions_alerts WHERE review_status = 'FALSE_POSITIVE' AND alert_date >= '${PROCESS_DATE}' - INTERVAL '30 days')::DECIMAL / 
            NULLIF((SELECT COUNT(*) FROM fc_analytics.sanctions_alerts WHERE alert_date >= '${PROCESS_DATE}' - INTERVAL '30 days')::DECIMAL, 0) * 100, 2
        );" || echo "0.00")
    
    # Update customer sanction risk levels
    RISK_LEVEL_UPDATES=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "UPDATE fc_source.customer_profiles 
         SET sanction_status = (
             CASE 
                 WHEN sanctions_risk.max_score >= 90 THEN 'HIGH_RISK'
                 WHEN sanctions_risk.max_score >= 70 THEN 'MEDIUM_RISK'
                 ELSE 'LOW_RISK'
             END
         )
         FROM (
             SELECT customer_id, MAX(highest_match_score) as max_score
             FROM fc_analytics.daily_sanctions_screening 
             WHERE process_date >= '${PROCESS_DATE}' - INTERVAL '30 days'
             GROUP BY customer_id
         ) sanctions_risk
         WHERE customer_profiles.customer_id = sanctions_risk.customer_id;" | grep -c "UPDATE" || echo "0")
    
    # Generate compliance dashboard metrics
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "INSERT INTO fc_analytics.sanctions_dashboard_metrics (metric_date, customers_screened, matches_found, manual_reviews_required, _cleared, false_positive_rate) 
         VALUES ('${PROCESS_DATE}', ${PROCESSED_COUNT}, ${MATCHES_REQUIRING_REVIEW}, ${MATCHES_REQUIRING_REVIEW}, ${_CLEARED_MATCHES}, ${FALSE_POSITIVE_RATE})
         ON CONFLICT (metric_date) DO UPDATE SET 
         customers_screened = EXCLUDED.customers_screened,
         matches_found = EXCLUDED.matches_found,
         manual_reviews_required = EXCLUDED.manual_reviews_required,
         _cleared = EXCLUDED._cleared,
         false_positive_rate = EXCLUDED.false_positive_rate;"
    
    # Success notification
    curl -X POST "http://scm-monitoring.internal/api/alerts" \
        -H "Content-Type: application/json" \
        -d "{\"job_id\":\"${ETL_JOB_ID}\",\"status\":\"SUCCESS\",\"process_date\":\"${PROCESS_DATE}\",\"customers_screened\":${PROCESSED_COUNT},\"matches_requiring_review\":${MATCHES_REQUIRING_REVIEW},\"high_match_scores\":${HIGH_MATCH_SCORES},\"_cleared\":${_CLEARED_MATCHES},\"sanctions_alerts\":${SANCTIONS_ALERTS},\"false_positive_rate\":${FALSE_POSITIVE_RATE},\"risk_updates\":${RISK_LEVEL_UPDATES},\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=${PROCESSED_COUNT} 
         WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
    
    log "scm Sanctions Check ETL Pipeline completed successfully"
    log "Total customers screened: ${PROCESSED_COUNT}"
    log "Matches requiring review: ${MATCHES_REQUIRING_REVIEW}"
    log "High match scores (>=90): ${HIGH_MATCH_SCORES}"
    log "-cleared matches: ${_CLEARED_MATCHES}"
    log "Sanctions alerts generated: ${SANCTIONS_ALERTS}"
    log "Current false positive rate: ${FALSE_POSITIVE_RATE}%"
    log "Risk level updates: ${RISK_LEVEL_UPDATES}"
}

main "$@"
find "${LOG_PATH}" -name "${ETL_JOB_NAME}_*.log" -mtime +7 -delete
exit 0