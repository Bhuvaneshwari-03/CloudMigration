#!/bin/bash

# scm_ece_33251_etl_proc_sentiment_flow_dly_sentimentanalysis_prc_cmd.sh
# Daily Customer Sentiment Analysis ETL Pipeline

# Hard-coded ETL Job Configuration
ETL_JOB_ID="ECE_SENTIMENTANALYSIS_DAILY"
ETL_JOB_NAME="scm_ece_33251_etl_proc_sentiment_flow_dly_sentimentanalysis_prc_cmd"
ETL_DOMAIN="ECE"
ETL_PROCESS_TYPE="sentiment_flow"
ETL_FREQUENCY="daily"
ETL_TARGET="sentimentanalysis"

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

# Advanced Analytics Configuration
SENTIMENT_AI_API="https://api.sentimentai.scm.internal"
NLP_PROCESSING_API="https://api.nlp.scm.internal"
EMOTION_DETECTION_API="https://api.emotion.scm.internal"

# Social Media APIs
TWITTER_API="https://api.twitter.com/2"
FACEBOOK_API="https://graph.facebook.com/v18.0"
LINKEDIN_API="https://api.linkedin.com/v2"
SOCIAL_MEDIA_CREDENTIALS="/opt/scm-etl/secrets/social_media_keys"

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

ingest_social_media_data() {
    log "Ingesting social media mentions and sentiment data"
    
    PROCESS_DATE=$(date -d "yesterday" +%Y-%m-%d)
    
    # Ingest Twitter mentions
    if [[ -f "${SOCIAL_MEDIA_CREDENTIALS}" ]]; then
        source "${SOCIAL_MEDIA_CREDENTIALS}"
        
        curl -X GET "${TWITTER_API}/tweets/search/recent?query=@scm OR scm OR \"scm Bank\"&max_results=100&tweet.fields=created_at,public_metrics,context_annotations" \
            -H "thorization: Bearer ${TWITTER_BEARER_TOKEN}" | \
        python3 -c "
import json, sys, psycopg2
from datetime import datetime
data = json.load(sys.stdin)
conn = psycopg2.connect(host='${DB_HOST}', database='${DB_NAME}', user='${DB_USER}')
cur = conn.cursor()
for tweet in data.get('data', []):
    cur.execute(
        'INSERT INTO ece_source.social_media_mentions (platform, content, thor_handle, post_timestamp, reach_metrics, engagement_metrics, mention_type) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING',
        ('TWITTER', tweet['text'], tweet.get('username', 'unknown'), tweet['created_at'], 
         json.dumps(tweet.get('public_metrics', {})), json.dumps(tweet.get('public_metrics', {})), 'MENTION')
    )
conn.commit()
cur.close()
conn.close()
" 2>/dev/null || log "Twitter data ingestion failed, continuing with existing data"
        
        # Ingest Facebook mentions
        curl -X GET "${FACEBOOK_API}/search?q=scm Bank&type=post&access_token=${FACEBOOK_ACCESS_TOKEN}" | \
        python3 -c "
import json, sys, psycopg2
data = json.load(sys.stdin)
conn = psycopg2.connect(host='${DB_HOST}', database='${DB_NAME}', user='${DB_USER}')
cur = conn.cursor()
for post in data.get('data', []):
    cur.execute(
        'INSERT INTO ece_source.social_media_mentions (platform, content, post_timestamp, reach_metrics, mention_type) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING',
        ('FACEBOOK', post.get('message', ''), post['created_time'], json.dumps({'likes': post.get('likes', {}).get('summary', {}).get('total_count', 0)}), 'MENTION')
    )
conn.commit()
cur.close()
conn.close()
" 2>/dev/null || log "Facebook data ingestion failed, continuing with existing data"
    fi
}

main() {
    log "Starting ECE Customer Sentiment Analysis ETL Pipeline"
    log "Job ID: ${ETL_JOB_ID}"
    
    # Check dependency on feedback aggregation job
    FEEDBACK_JOB_STATUS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT job_status FROM etl_job_control WHERE job_id='ECE_FEEDBACKAGG_DAILY' AND run_date=CURRENT_DATE ORDER BY start_time DESC LIMIT 1;")
    
    if [[ "${FEEDBACK_JOB_STATUS}" != "COMPLETED" ]]; then
        handle_error "Dependency job ECE_FEEDBACKAGG_DAILY not completed. Status: ${FEEDBACK_JOB_STATUS}" 1
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
    
    # Ingest social media data
    ingest_social_media_data
    
    # Data validation
    PROCESS_DATE=$(date -d "yesterday" +%Y-%m-%d)
    COMMUNICATIONS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM ece_source.customer_communications WHERE DATE(timestamp) = '${PROCESS_DATE}';")
    
    SOCIAL_MENTIONS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM ece_source.social_media_mentions WHERE DATE(post_timestamp) = '${PROCESS_DATE}';")
    
    TOTAL_TEXT_DATA=$(($COMMUNICATIONS + $SOCIAL_MENTIONS))
    
    log "Found ${COMMUNICATIONS} customer communications and ${SOCIAL_MENTIONS} social media mentions (total: ${TOTAL_TEXT_DATA}) for sentiment analysis on ${PROCESS_DATE}"
    
    if [[ ${TOTAL_TEXT_DATA} -eq 0 ]]; then
        log "No text data found for sentiment analysis on ${PROCESS_DATE}. Exiting gracefully."
        psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
            "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=0 WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
        exit 0
    fi
    
    # Execute Spark ETL with advanced sentiment analysis
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 8g \
        --executor-memory 14g \
        --executor-cores 7 \
        --num-executors 25 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.dynamicAllocation.enabled=true \
        --conf spark.dynamicAllocation.maxExecutors=35 \
        --packages org.apache.spark:spark-mllib_2.12:3.3.0 \
        --class com.scm.ece.sentiment.SentimentAnalysisJob \
        "${ETL_HOME}/jars/ece-sentiment-analysis-1.0.jar" \
        --job-id "${ETL_JOB_ID}" \
        --process-date "${PROCESS_DATE}" \
        --source-schema "ece_source" \
        --target-schema "ece_analytics" \
        --sentiment-ai-api "${SENTIMENT_AI_API}" \
        --nlp-api "${NLP_PROCESSING_API}" \
        --emotion-api "${EMOTION_DETECTION_API}" \
        --config-file "${CONFIG_PATH}/ece_sentiment_config.properties" \
        --log-level "INFO" 2>&1 | tee -a "${LOG_FILE}"
    
    SPARK_EXIT_CODE=$?
    if [[ ${SPARK_EXIT_CODE} -ne 0 ]]; then
        handle_error "Spark sentiment analysis job execution failed" ${SPARK_EXIT_CODE}
    fi
    
    # Validation and sentiment metrics
    PROCESSED_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM ece_analytics.daily_sentiment_analysis WHERE process_date = '${PROCESS_DATE}';")
    
    NEGATIVE_SENTIMENT_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM ece_analytics.daily_sentiment_analysis WHERE sentiment_category = 'NEGATIVE' AND process_date = '${PROCESS_DATE}';")
    
    POSITIVE_SENTIMENT_COUNT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM ece_analytics.daily_sentiment_analysis WHERE sentiment_category = 'POSITIVE' AND process_date = '${PROCESS_DATE}';")
    
    CRITICAL_ALERTS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT COUNT(*) FROM ece_analytics.daily_sentiment_analysis WHERE alert_threshold_met = true AND process_date = '${PROCESS_DATE}';")
    
    # Calculate average sentiment score
    AVG_SENTIMENT_SCORE=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT ROUND(AVG(sentiment_score), 3) FROM ece_analytics.daily_sentiment_analysis WHERE process_date = '${PROCESS_DATE}';")
    
    # Generate sentiment insights and alerts
    SENTIMENT_ALERTS=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "INSERT INTO ece_analytics.sentiment_alerts (customer_id, alert_type, severity, alert_date, sentiment_score, emotion_indicators, affected_channels, recommended_action) 
         SELECT customer_id,
                CASE 
                    WHEN sentiment_score <= -0.7 THEN 'EXTREMELY_NEGATIVE'
                    WHEN sentiment_score <= -0.4 THEN 'HIGHLY_NEGATIVE'
                    WHEN trend_direction = 'DECLINING' AND sentiment_score <= -0.2 THEN 'NEGATIVE_TREND'
                    ELSE 'ATTENTION_REQUIRED'
                END,
                CASE 
                    WHEN sentiment_score <= -0.7 THEN 'CRITICAL'
                    WHEN sentiment_score <= -0.4 OR alert_threshold_met THEN 'HIGH'
                    ELSE 'MEDIUM'
                END,
                CURRENT_DATE,
                sentiment_score,
                emotion_indicators,
                channel,
                CASE 
                    WHEN sentiment_score <= -0.7 THEN 'IMMEDIATE_ESCALATION'
                    WHEN sentiment_score <= -0.4 THEN 'PROACTIVE_OUTREACH'
                    ELSE 'MONITOR_CLOSELY'
                END
         FROM ece_analytics.daily_sentiment_analysis 
         WHERE (sentiment_score <= -0.3 OR alert_threshold_met = true) 
         AND process_date = '${PROCESS_DATE}'
         RETURNING 1;" | wc -l)
    
    # Update customer sentiment profiles
    PROFILE_UPDATES=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "INSERT INTO ece_analytics.customer_sentiment_profiles (customer_id, profile_date, avg_sentiment, dominant_emotion, sentiment_volatility, risk_level) 
         SELECT customer_id, '${PROCESS_DATE}',
                ROUND(AVG(sentiment_score), 3),
                (SELECT unnest(string_to_array(emotion_indicators, ', ')) 
                 FROM ece_analytics.daily_sentiment_analysis dsa2 
                 WHERE dsa2.customer_id = dsa.customer_id AND process_date = '${PROCESS_DATE}'
                 GROUP BY unnest(string_to_array(emotion_indicators, ', '))
                 ORDER BY COUNT(*) DESC LIMIT 1) as dominant_emotion,
                ROUND(STDDEV(sentiment_score), 3) as volatility,
                CASE 
                    WHEN AVG(sentiment_score) <= -0.5 THEN 'HIGH'
                    WHEN AVG(sentiment_score) <= -0.2 THEN 'MEDIUM'
                    ELSE 'LOW'
                END
         FROM ece_analytics.daily_sentiment_analysis dsa
         WHERE process_date = '${PROCESS_DATE}'
         GROUP BY customer_id
         ON CONFLICT (customer_id, profile_date) DO UPDATE SET 
         avg_sentiment = EXCLUDED.avg_sentiment,
         dominant_emotion = EXCLUDED.dominant_emotion,
         sentiment_volatility = EXCLUDED.sentiment_volatility,
         risk_level = EXCLUDED.risk_level;" | grep -c "INSERT" || echo "0")
    
    # Generate sentiment trend analysis
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "INSERT INTO ece_analytics.sentiment_trends (trend_date, channel, topic_category, sentiment_movement, volume_change, alert_generated) 
         SELECT '${PROCESS_DATE}',
                channel,
                unnest(string_to_array(topic_categories, ', ')) as topic,
                CASE 
                    WHEN AVG(sentiment_score) > (SELECT AVG(sentiment_score) FROM ece_analytics.daily_sentiment_analysis WHERE process_date = '${PROCESS_DATE}' - INTERVAL '7 days' AND channel = dsa.channel) THEN 'IMPROVING'
                    WHEN AVG(sentiment_score) < (SELECT AVG(sentiment_score) FROM ece_analytics.daily_sentiment_analysis WHERE process_date = '${PROCESS_DATE}' - INTERVAL '7 days' AND channel = dsa.channel) THEN 'DECLINING'
                    ELSE 'STABLE'
                END,
                COUNT(*) - COALESCE((SELECT COUNT(*) FROM ece_analytics.daily_sentiment_analysis WHERE process_date = '${PROCESS_DATE}' - INTERVAL '1 days' AND channel = dsa.channel), 0),
                CASE WHEN COUNT(CASE WHEN alert_threshold_met THEN 1 END) > 0 THEN true ELSE false END
         FROM ece_analytics.daily_sentiment_analysis dsa
         WHERE process_date = '${PROCESS_DATE}' AND topic_categories IS NOT NULL
         GROUP BY channel, unnest(string_to_array(topic_categories, ', '))
         ON CONFLICT (trend_date, channel, topic_category) DO UPDATE SET 
         sentiment_movement = EXCLUDED.sentiment_movement,
         volume_change = EXCLUDED.volume_change,
         alert_generated = EXCLUDED.alert_generated;"
    
    # Calculate sentiment distribution
    SENTIMENT_NEGATIVE_PCT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT ROUND((${NEGATIVE_SENTIMENT_COUNT}::DECIMAL / ${PROCESSED_COUNT}::DECIMAL) * 100, 2);" || echo "0.00")
    
    SENTIMENT_POSITIVE_PCT=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -t -c \
        "SELECT ROUND((${POSITIVE_SENTIMENT_COUNT}::DECIMAL / ${PROCESSED_COUNT}::DECIMAL) * 100, 2);" || echo "0.00")
    
    # Generate executive sentiment dashboard
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "INSERT INTO ece_analytics.sentiment_dashboard_metrics (metric_date, total_analyzed, avg_sentiment_score, negative_pct, positive_pct, critical_alerts, social_mentions, customer_communications) 
         VALUES ('${PROCESS_DATE}', ${PROCESSED_COUNT}, ${AVG_SENTIMENT_SCORE}, ${SENTIMENT_NEGATIVE_PCT}, ${SENTIMENT_POSITIVE_PCT}, ${CRITICAL_ALERTS}, ${SOCIAL_MENTIONS}, ${COMMUNICATIONS})
         ON CONFLICT (metric_date) DO UPDATE SET 
         total_analyzed = EXCLUDED.total_analyzed,
         avg_sentiment_score = EXCLUDED.avg_sentiment_score,
         negative_pct = EXCLUDED.negative_pct,
         positive_pct = EXCLUDED.positive_pct,
         critical_alerts = EXCLUDED.critical_alerts,
         social_mentions = EXCLUDED.social_mentions,
         customer_communications = EXCLUDED.customer_communications;"
    
    # Success notification
    curl -X POST "http://-scm-monitoring.internal/api/alerts" \
        -H "Content-Type: application/json" \
        -d "{\"job_id\":\"${ETL_JOB_ID}\",\"status\":\"SUCCESS\",\"process_date\":\"${PROCESS_DATE}\",\"records_processed\":${PROCESSED_COUNT},\"avg_sentiment\":${AVG_SENTIMENT_SCORE},\"negative_sentiment\":${NEGATIVE_SENTIMENT_COUNT},\"positive_sentiment\":${POSITIVE_SENTIMENT_COUNT},\"critical_alerts\":${CRITICAL_ALERTS},\"social_mentions\":${SOCIAL_MENTIONS},\"communications\":${COMMUNICATIONS},\"sentiment_alerts\":${SENTIMENT_ALERTS},\"profile_updates\":${PROFILE_UPDATES},\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
    
    psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -c \
        "UPDATE etl_job_control SET job_status='COMPLETED', end_time=NOW(), records_processed=${PROCESSED_COUNT} 
         WHERE job_id='${ETL_JOB_ID}' AND run_date=CURRENT_DATE;"
    
    log "ECE Sentiment Analysis ETL Pipeline completed successfully"
    log "Total records analyzed: ${PROCESSED_COUNT}"
    log "Average sentiment score: ${AVG_SENTIMENT_SCORE}"
    log "Negative sentiment instances: ${NEGATIVE_SENTIMENT_COUNT} (${SENTIMENT_NEGATIVE_PCT}%)"
    log "Positive sentiment instances: ${POSITIVE_SENTIMENT_COUNT} (${SENTIMENT_POSITIVE_PCT}%)"
    log "Critical alerts: ${CRITICAL_ALERTS}"
    log "Social media mentions: ${SOCIAL_MENTIONS}"
    log "Customer communications: ${COMMUNICATIONS}"
    log "Sentiment alerts generated: ${SENTIMENT_ALERTS}"
    log "Profile updates: ${PROFILE_UPDATES}"
}

main "$@"
find "${LOG_PATH}" -name "${ETL_JOB_NAME}_*.log" -mtime +7 -delete
exit 0