#!/bin/bash

# scm_xtf_73126_etl_proc_scoring_flow_rt_riskscoring_prc_cmd.sh
# Real-Time Risk Scoring Flow ETL Pipeline

# Hard-coded ETL Job Configuration
ETL_JOB_ID="XTF_RISKSCORING_REALTIME"
ETL_JOB_NAME="scm_xtf_73126_etl_proc_scoring_flow_rt_riskscoring_prc_cmd"
ETL_DOMAIN="XTF"
ETL_PROCESS_TYPE="scoring_flow"
ETL_FREQUENCY="realtime"
ETL_TARGET="riskscoring"

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

# Kafka Configuration for real-time scoring
KAFKA_BROKERS="scm-kafka-01.internal:9092,scm-kafka-02.internal:9092,scm-kafka-03.internal:9092"
KAFKA_TOPIC="xtf-risk-scores"

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
    log "Starting XTF Risk Scoring Flow ETL Pipeline"
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
    
    # Check Kafka connectivity
    if ! timeout 10 kafka-topics --bootstrap-server "${KAFKA_BROKERS}" --list > /dev/null 2>&1; then
        handle_error "Kafka connectivity check failed" 1
    fi
    
    # Update job status to RUNNING
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "INSERT INTO etl_job_control (job_id, job_name, run_date, job_status, start_time) 
         VALUES ('${ETL_JOB_ID}', '${ETL_JOB_NAME}', CURRENT_DATE, 'RUNNING', NOW())
         ON CONFLICT (job_id, run_date) DO UPDATE SET job_status='RUNNING', start_time=NOW();"
    
    log "Pre-execution checks completed successfully"
    
    # Step 2: Data validation and dependency checks
    log "Step 2: Validating dependencies and source data"
    
    # Check if dependent XTF jobs have completed
    FRAUD_JOB_STATUS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT job_status FROM etl_job_control WHERE job_id='XTF_FRAUDDETECTION_REALTIME' AND run_date=CURRENT_DATE ORDER BY start_time DESC LIMIT 1;")
    
    VELOCITY_JOB_STATUS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT job_status FROM etl_job_control WHERE job_id='XTF_VELOCITYCHECK_REALTIME' AND run_date=CURRENT_DATE ORDER BY start_time DESC LIMIT 1;")
    
    PATTERN_JOB_STATUS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT job_status FROM etl_job_control WHERE job_id='XTF_PATTERNANALYSIS_REALTIME' AND run_date=CURRENT_DATE ORDER BY start_time DESC LIMIT 1;")
    
    # Check source data availability
    SOURCE_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM xtf_source.scoring_events WHERE process_flag = 'PENDING';")
    
    if [[ ${SOURCE_COUNT} -eq 0 ]]; then
        log "No pending scoring events to process. Exiting gracefully."
        psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
            "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=0 WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
        exit 0
    fi
    
    log "Found ${SOURCE_COUNT} scoring events to process"
    
    # Step 3: Execute Spark ETL job with comprehensive risk scoring
    log "Step 3: Executing Spark ETL processing with risk scoring"
    
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 8g \
        --executor-memory 16g \
        --executor-cores 8 \
        --num-executors 20 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.dynamicAllocation.enabled=true \
        --conf spark.dynamicAllocation.maxExecutors=30 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
        --class com.scm.xtf.scoring.RiskScoringJob \
        "${ETL_HOME}/jars/xtf-risk-scoring-1.0.jar" \
        --job-id "${ETL_JOB_ID}" \
        --source-table "xtf_source.scoring_events" \
        --target-table "xtf_analytics.risk_scoring_results" \
        --kafka-brokers "${KAFKA_BROKERS}" \
        --kafka-topic "${KAFKA_TOPIC}" \
        --config-file "${CONFIG_PATH}/xtf_scoring_config.properties" \
        --log-level "INFO" 2>&1 | tee -a "${LOG_FILE}"
    
    SPARK_EXIT_CODE=$?
    if [[ ${SPARK_EXIT_CODE} -ne 0 ]]; then
        handle_error "Spark risk scoring job execution failed" ${SPARK_EXIT_CODE}
    fi
    
    log "Spark risk scoring ETL processing completed successfully"
    
    # Step 4: Data quality validation and scoring metrics
    log "Step 4: Performing post-processing data quality checks"
    
    # Count processed records
    PROCESSED_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM xtf_analytics.risk_scoring_results WHERE process_date = CURRENT_DATE;")
    
    # Count high-risk scores
    HIGH_RISK_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM xtf_analytics.risk_scoring_results WHERE final_risk_score >= 0.9 AND process_date = CURRENT_DATE;")
    
    # Calculate average risk score
    AVG_RISK_SCORE=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT ROUND(AVG(final_risk_score), 3) FROM xtf_analytics.risk_scoring_results WHERE process_date = CURRENT_DATE;")
    
    log "Processed ${PROCESSED_COUNT} scoring events"
    log "High-risk scores (>=0.9): ${HIGH_RISK_COUNT}"
    log "Average risk score: ${AVG_RISK_SCORE}"
    
    # Step 5: Publish high-risk scores to Kafka
    log "Step 5: Publishing high-risk scores to Kafka topic"
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT json_build_object('customer_id', customer_id, 'risk_score', final_risk_score, 'timestamp', NOW()) 
         FROM xtf_analytics.risk_scoring_results 
         WHERE final_risk_score >= 0.8 AND process_date = CURRENT_DATE;" | \
    while IFS= read -r risk_event; do
        echo "${risk_event}" | kafka-console-producer --broker-list "${KAFKA_BROKERS}" --topic "${KAFKA_TOPIC}"
    done
    
    # Step 6: Update process flags
    log "Step 6: Updating process control flags"
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "UPDATE xtf_source.scoring_events SET process_flag = 'PROCESSED', processed_timestamp = NOW() 
         WHERE process_flag = 'PENDING' AND event_timestamp <= NOW() - INTERVAL '5 minutes';"
    
    # Step 7: Send success notification and update control table
    log "Step 7: Finalizing job execution"
    
    # Send success notification
    curl -X POST "http://scm-monitoring.internal/api/alerts" \
        -H "Content-Type: application/json" \
        -d "{\"job_id\":\"${ETL_JOB_ID}\",\"status\":\"SUCCESS\",\"records_processed\":${PROCESSED_COUNT},\"high_risk_count\":${HIGH_RISK_COUNT},\"avg_risk_score\":${AVG_RISK_SCORE},\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
    
    # Update job control table
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=${PROCESSED_COUNT} 
         WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
    
    log "XTF Risk Scoring Flow ETL Pipeline completed successfully"
    log "Total scoring events processed: ${PROCESSED_COUNT}"
    log "High-risk scores generated: ${HIGH_RISK_COUNT}"
    log "Average risk score: ${AVG_RISK_SCORE}"
}

# Execute main function
main "$@"

# Clean up temporary files
find "${LOG_PATH}" -name "${ETL_JOB_NAME}_*.log" -mtime +7 -delete

exit 0