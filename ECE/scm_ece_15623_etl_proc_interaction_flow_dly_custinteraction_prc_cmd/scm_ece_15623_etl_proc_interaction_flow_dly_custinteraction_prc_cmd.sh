job_config:
  job_name: scm_ece_15623_etl_proc_interaction_flow_dly_custinteraction_prc_cmd
  job_id: 15623
  domain: ECE
  process_type: interaction_flow
  frequency: daily
  description: "Daily ETL process for customer interaction analytics"
  
source_config:
  source_type: database
  connection:
    host: scm-ece-db01.internal
    database: ece_source_db
    schema: customer_interactions
  tables:
    - table_name: interaction_events
      columns:
        - event_id
        - customer_id
        - channel
        - interaction_type
        - timestamp
        - duration
        - resolution_status
        - satisfaction_score
        - agent_id
    - table_name: channel_metadata
      columns:
        - channel_id
        - channel_name
        - channel_type
        - availability_hours
        - cost_per_interaction
        - effectiveness_rating
  
target_config:
  target_type: database
  connection:
    host: scm-dwh-cluster.internal
    database: ece_analytics_db
    schema: interaction_analytics
  tables:
    - table_name: daily_customer_interactions
      columns:
        - process_date
        - customer_id
        - total_interactions
        - channel_mix
        - avg_interaction_duration
        - resolution_rate
        - satisfaction_trend
        - preferred_channel
        - escalation_count
        - last_updated
      partition_key: process_date
      
processing_config:
  batch_size: 30000
  parallel_threads: 8
  data_quality_checks:
    - null_check: customer_id, channel
    - range_check: satisfaction_score [1,10]
    - enum_check: resolution_status [RESOLVED, PENDING, ESCALATED]
  transformations:
    - aggregate_by_customer
    - calculate_channel_preferences
    - identify_interaction_patterns
    - compute_satisfaction_metrics
    
schedule_config:
  start_time: "05:00"
  timezone: "stralia/Sydney"
  dependencies:
    - daily_interaction_data_sync
  
monitoring_config:
  sla_hours: 2
  alert_recipients:
    - ece-team@scm.com.
    - customer-experience@scm.com.
  metrics:
    - interaction_volume
    - channel_performance
    - customer_satisfaction
    - processing_time