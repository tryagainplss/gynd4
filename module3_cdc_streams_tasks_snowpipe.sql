-- =====================================================
-- MODULE 3: CDC WITH STREAMS, TASKS & SNOWPIPE
-- =====================================================
-- Duration: 60 minutes
-- Prerequisites: Basic SQL knowledge, Snowflake trial account
-- 
-- This worksheet covers:
-- 1. Introduction to CDC, Streams, Tasks & Snowpipe
-- 2. Warm-up exercises (3 simple examples)
-- 3. Intermediate use-cases (3 telco scenarios)
-- 4. Advanced challenge (end-to-end mini-project)
-- 5. Summary & Next Steps
-- =====================================================

-- =====================================================
-- INTRODUCTION
-- =====================================================
-- Change Data Capture (CDC) in Snowflake enables:
-- • Real-time data processing and analytics
-- • Automated data pipelines and workflows
-- • Continuous data ingestion from external sources
-- • Event-driven architecture for telco operations
--
-- In this module, we'll build:
-- • Real-time CDR processing pipeline
-- • Automated data quality monitoring
-- • Subscriber data synchronization system
-- • Network performance alerting
-- =====================================================

-- =====================================================
-- SETUP: Create sample telco data tables
-- =====================================================
-- Estimated time: 5 minutes

-- Use the same database from previous modules
USE DATABASE TELCO_WORKSHOP;
CREATE SCHEMA IF NOT EXISTS CDC_DATA;
USE SCHEMA CDC_DATA;

-- Create source tables for CDC processing
CREATE OR REPLACE TABLE cdr_source (
    call_id STRING,
    subscriber_id STRING,
    phone_number STRING,
    call_start_time TIMESTAMP_NTZ,
    call_end_time TIMESTAMP_NTZ,
    call_duration_seconds INTEGER,
    call_type STRING,
    network_type STRING,
    location_lat DECIMAL(10,6),
    location_lon DECIMAL(10,6),
    cost_usd DECIMAL(10,4),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create target tables for processed data
CREATE OR REPLACE TABLE cdr_processed (
    call_id STRING,
    subscriber_id STRING,
    phone_number STRING,
    call_start_time TIMESTAMP_NTZ,
    call_end_time TIMESTAMP_NTZ,
    call_duration_seconds INTEGER,
    call_duration_minutes DECIMAL(10,2),
    call_type STRING,
    network_type STRING,
    location_lat DECIMAL(10,6),
    location_lon DECIMAL(10,6),
    cost_usd DECIMAL(10,4),
    cost_per_minute DECIMAL(10,4),
    processing_status STRING DEFAULT 'processed',
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create data quality monitoring table
CREATE OR REPLACE TABLE data_quality_alerts (
    alert_id STRING,
    table_name STRING,
    alert_type STRING, -- 'missing_data', 'invalid_format', 'duplicate_record'
    alert_message STRING,
    record_count INTEGER,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create subscriber synchronization table
CREATE OR REPLACE TABLE subscriber_sync_log (
    sync_id STRING,
    subscriber_id STRING,
    sync_type STRING, -- 'insert', 'update', 'delete'
    old_values STRING,
    new_values STRING,
    sync_status STRING, -- 'success', 'failed', 'pending'
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create network performance alerts table
CREATE OR REPLACE TABLE network_alerts (
    alert_id STRING,
    cell_tower_id STRING,
    alert_type STRING, -- 'low_signal', 'high_latency', 'bandwidth_issue'
    severity STRING, -- 'low', 'medium', 'high', 'critical'
    alert_message STRING,
    threshold_value DECIMAL(10,2),
    actual_value DECIMAL(10,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert initial sample data
INSERT INTO cdr_source VALUES
('CDR001', 'SUB001', '+1234567890', '2024-01-15 10:30:00', '2024-01-15 10:32:15', 135, 'voice', '5G', 40.7128, -74.0060, 0.25, CURRENT_TIMESTAMP()),
('CDR002', 'SUB002', '+1234567891', '2024-01-15 11:15:00', '2024-01-15 11:16:30', 90, 'sms', '4G', 40.7589, -73.9851, 0.10, CURRENT_TIMESTAMP()),
('CDR003', 'SUB001', '+1234567890', '2024-01-15 12:00:00', '2024-01-15 12:05:30', 330, 'data', '5G', 40.7505, -73.9934, 1.50, CURRENT_TIMESTAMP()),
('CDR004', 'SUB003', '+1234567892', '2024-01-15 13:20:00', '2024-01-15 13:22:45', 165, 'voice', '4G', 40.7829, -73.9654, 0.30, CURRENT_TIMESTAMP()),
('CDR005', 'SUB002', '+1234567891', '2024-01-15 14:10:00', '2024-01-15 14:12:20', 140, 'voice', '5G', 40.7128, -74.0060, 0.28, CURRENT_TIMESTAMP());

-- =====================================================
-- WARM-UP EXERCISES (3 simple examples)
-- =====================================================
-- Estimated time: 15 minutes

-- Exercise 1: Create a Basic Stream
-- Create a stream to track changes in the CDR source table
-- This demonstrates the fundamental CDC capability
CREATE OR REPLACE STREAM cdr_source_stream ON TABLE cdr_source;

-- View the stream to see current state (should be empty initially)
SELECT * FROM cdr_source_stream;

-- Exercise 2: Basic Task Creation
-- Create a simple task to process stream data
-- This shows how to automate data processing
CREATE OR REPLACE TASK process_cdr_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 minute'
AS
    INSERT INTO cdr_processed (
        call_id, subscriber_id, phone_number, call_start_time, call_end_time,
        call_duration_seconds, call_duration_minutes, call_type, network_type,
        location_lat, location_lon, cost_usd, cost_per_minute
    )
    SELECT 
        call_id, subscriber_id, phone_number, call_start_time, call_end_time,
        call_duration_seconds, 
        call_duration_seconds / 60.0 AS call_duration_minutes,
        call_type, network_type, location_lat, location_lon, cost_usd,
        CASE 
            WHEN call_duration_seconds > 0 THEN cost_usd / (call_duration_seconds / 60.0)
            ELSE 0 
        END AS cost_per_minute
    FROM cdr_source_stream
    WHERE metadata$action = 'INSERT';

-- Exercise 3: Simple Snowpipe Simulation
-- Create a view to simulate continuous data ingestion
-- This demonstrates the concept of continuous data loading
CREATE OR REPLACE VIEW continuous_ingestion_view AS
SELECT 
    'Simulated Ingestion' AS ingestion_type,
    COUNT(*) AS records_processed,
    MAX(created_at) AS last_ingestion_time,
    'Active' AS ingestion_status
FROM cdr_source
WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL '1 hour';

-- Test the continuous ingestion view
SELECT * FROM continuous_ingestion_view;

-- =====================================================
-- INTERMEDIATE USE-CASES (3 telco scenarios)
-- =====================================================
-- Estimated time: 25 minutes

-- Scenario 1: Real-time CDR Processing Pipeline
-- Build a complete CDC pipeline for call detail records
-- This addresses real telco needs for real-time processing

-- Step 1: Create a comprehensive stream for CDR processing
CREATE OR REPLACE STREAM cdr_processing_stream ON TABLE cdr_source;

-- Step 2: Create an advanced processing task
CREATE OR REPLACE TASK advanced_cdr_processing_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '2 minutes'
AS
    -- Process new CDR records
    INSERT INTO cdr_processed (
        call_id, subscriber_id, phone_number, call_start_time, call_end_time,
        call_duration_seconds, call_duration_minutes, call_type, network_type,
        location_lat, location_lon, cost_usd, cost_per_minute
    )
    SELECT 
        call_id, subscriber_id, phone_number, call_start_time, call_end_time,
        call_duration_seconds, 
        call_duration_seconds / 60.0 AS call_duration_minutes,
        call_type, network_type, location_lat, location_lon, cost_usd,
        CASE 
            WHEN call_duration_seconds > 0 THEN cost_usd / (call_duration_seconds / 60.0)
            ELSE 0 
        END AS cost_per_minute
    FROM cdr_processing_stream
    WHERE metadata$action = 'INSERT'
    AND call_duration_seconds > 0; -- Data quality check

-- Step 3: Create a data quality monitoring task
CREATE OR REPLACE TASK data_quality_monitoring_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 minutes'
AS
    -- Monitor for data quality issues
    INSERT INTO data_quality_alerts (alert_id, table_name, alert_type, alert_message, record_count)
    SELECT 
        'DQ_' || CURRENT_TIMESTAMP()::STRING AS alert_id,
        'cdr_source' AS table_name,
        'invalid_duration' AS alert_type,
        'CDR records with zero or negative duration detected' AS alert_message,
        COUNT(*) AS record_count
    FROM cdr_source
    WHERE call_duration_seconds <= 0
    AND created_at >= CURRENT_TIMESTAMP() - INTERVAL '10 minutes';

-- Scenario 2: Subscriber Data Synchronization
-- Implement CDC for subscriber data changes
-- This supports telco customer management systems

-- Create a stream for subscriber changes (using data from Module 1)
CREATE OR REPLACE STREAM subscriber_sync_stream ON TABLE TELCO_WORKSHOP.CDR_DATA.subscribers;

-- Create a synchronization task
CREATE OR REPLACE TASK subscriber_sync_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '3 minutes'
AS
    -- Log subscriber changes for audit trail
    INSERT INTO subscriber_sync_log (sync_id, subscriber_id, sync_type, old_values, new_values, sync_status)
    SELECT 
        'SYNC_' || CURRENT_TIMESTAMP()::STRING AS sync_id,
        subscriber_id,
        metadata$action AS sync_type,
        'Previous values not available in stream' AS old_values,
        OBJECT_CONSTRUCT(*)::STRING AS new_values,
        'success' AS sync_status
    FROM subscriber_sync_stream
    WHERE metadata$action IN ('INSERT', 'UPDATE', 'DELETE');

-- Scenario 3: Network Performance Alerting
-- Create automated alerts for network performance issues
-- This supports telco network operations

-- Create a stream for network performance monitoring
CREATE OR REPLACE STREAM network_performance_stream ON TABLE TELCO_WORKSHOP.CDR_DATA.network_performance;

-- Create a network alerting task
CREATE OR REPLACE TASK network_alerting_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 minute'
AS
    -- Generate alerts for poor network performance
    INSERT INTO network_alerts (alert_id, cell_tower_id, alert_type, severity, alert_message, threshold_value, actual_value)
    SELECT 
        'NET_' || CURRENT_TIMESTAMP()::STRING AS alert_id,
        cell_tower_id,
        CASE 
            WHEN signal_strength_dbm < -80 THEN 'low_signal'
            WHEN latency_ms > 100 THEN 'high_latency'
            WHEN bandwidth_mbps < 50 THEN 'bandwidth_issue'
            ELSE 'normal'
        END AS alert_type,
        CASE 
            WHEN signal_strength_dbm < -90 OR latency_ms > 200 OR bandwidth_mbps < 25 THEN 'critical'
            WHEN signal_strength_dbm < -85 OR latency_ms > 150 OR bandwidth_mbps < 40 THEN 'high'
            WHEN signal_strength_dbm < -80 OR latency_ms > 100 OR bandwidth_mbps < 50 THEN 'medium'
            ELSE 'low'
        END AS severity,
        CASE 
            WHEN signal_strength_dbm < -80 THEN 'Low signal strength detected'
            WHEN latency_ms > 100 THEN 'High latency detected'
            WHEN bandwidth_mbps < 50 THEN 'Low bandwidth detected'
            ELSE 'Normal performance'
        END AS alert_message,
        CASE 
            WHEN signal_strength_dbm < -80 THEN -80
            WHEN latency_ms > 100 THEN 100
            WHEN bandwidth_mbps < 50 THEN 50
            ELSE 0
        END AS threshold_value,
        CASE 
            WHEN signal_strength_dbm < -80 THEN signal_strength_dbm
            WHEN latency_ms > 100 THEN latency_ms
            WHEN bandwidth_mbps < 50 THEN bandwidth_mbps
            ELSE 0
        END AS actual_value
    FROM network_performance_stream
    WHERE metadata$action = 'INSERT'
    AND (signal_strength_dbm < -80 OR latency_ms > 100 OR bandwidth_mbps < 50);

-- =====================================================
-- ADVANCED CHALLENGE: End-to-End Mini-Project
-- =====================================================
-- Estimated time: 15 minutes

-- Challenge: Build a complete telco CDC pipeline system
-- This integrates all components for a comprehensive solution

-- Step 1: Create a master orchestration task
CREATE OR REPLACE TASK cdc_orchestration_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 minutes'
AS
    -- This task coordinates all CDC activities
    CALL SYSTEM$LOG_INFO('CDC Pipeline Orchestration Started');
    
    -- Check pipeline health
    INSERT INTO data_quality_alerts (alert_id, table_name, alert_type, alert_message, record_count)
    SELECT 
        'HEALTH_' || CURRENT_TIMESTAMP()::STRING AS alert_id,
        'pipeline_health' AS table_name,
        'pipeline_status' AS alert_type,
        'CDC Pipeline Health Check' AS alert_message,
        (SELECT COUNT(*) FROM cdr_processed) AS record_count;

-- Step 2: Create a comprehensive monitoring view
CREATE OR REPLACE VIEW cdc_pipeline_monitoring_view AS
WITH stream_status AS (
    SELECT 
        'CDR Processing Stream' AS stream_name,
        COUNT(*) AS pending_records,
        'Active' AS status
    FROM cdr_processing_stream
    
    UNION ALL
    
    SELECT 
        'Subscriber Sync Stream' AS stream_name,
        COUNT(*) AS pending_records,
        'Active' AS status
    FROM subscriber_sync_stream
    
    UNION ALL
    
    SELECT 
        'Network Performance Stream' AS stream_name,
        COUNT(*) AS pending_records,
        'Active' AS status
    FROM network_performance_stream
),
task_status AS (
    SELECT 
        'CDR Processing' AS task_name,
        COUNT(*) AS processed_records,
        'Running' AS status
    FROM cdr_processed
    WHERE processed_at >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
    
    UNION ALL
    
    SELECT 
        'Data Quality Monitoring' AS task_name,
        COUNT(*) AS processed_records,
        'Running' AS status
    FROM data_quality_alerts
    WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
    
    UNION ALL
    
    SELECT 
        'Network Alerting' AS task_name,
        COUNT(*) AS processed_records,
        'Running' AS status
    FROM network_alerts
    WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
)
SELECT 
    'Stream Status' AS component_type,
    stream_name AS component_name,
    pending_records AS metric_value,
    status AS component_status
FROM stream_status

UNION ALL

SELECT 
    'Task Status' AS component_type,
    task_name AS component_name,
    processed_records AS metric_value,
    status AS component_status
FROM task_status;

-- Test the monitoring view
SELECT * FROM cdc_pipeline_monitoring_view;

-- Step 3: Create a performance analytics view
CREATE OR REPLACE VIEW cdc_performance_analytics_view AS
SELECT 
    'CDR Processing Performance' AS metric_category,
    COUNT(*) AS total_records_processed,
    AVG(call_duration_minutes) AS avg_call_duration,
    SUM(cost_usd) AS total_revenue,
    COUNT(CASE WHEN processing_status = 'processed' THEN 1 END) AS successful_processing,
    COUNT(CASE WHEN processing_status != 'processed' THEN 1 END) AS failed_processing
FROM cdr_processed
WHERE processed_at >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'

UNION ALL

SELECT 
    'Data Quality Metrics' AS metric_category,
    COUNT(*) AS total_alerts,
    COUNT(CASE WHEN alert_type = 'invalid_duration' THEN 1 END) AS duration_alerts,
    COUNT(CASE WHEN alert_type = 'missing_data' THEN 1 END) AS missing_data_alerts,
    COUNT(CASE WHEN alert_type = 'duplicate_record' THEN 1 END) AS duplicate_alerts,
    0 AS failed_processing
FROM data_quality_alerts
WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'

UNION ALL

SELECT 
    'Network Performance Alerts' AS metric_category,
    COUNT(*) AS total_alerts,
    COUNT(CASE WHEN severity = 'critical' THEN 1 END) AS critical_alerts,
    COUNT(CASE WHEN severity = 'high' THEN 1 END) AS high_alerts,
    COUNT(CASE WHEN severity = 'medium' THEN 1 END) AS medium_alerts,
    COUNT(CASE WHEN severity = 'low' THEN 1 END) AS low_alerts
FROM network_alerts
WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL '24 hours';

-- Test the performance analytics
SELECT * FROM cdc_performance_analytics_view;

-- Step 4: Create a business intelligence dashboard view
CREATE OR REPLACE VIEW telco_cdc_dashboard_view AS
WITH real_time_metrics AS (
    SELECT 
        'Real-time CDR Processing' AS metric_name,
        COUNT(*) AS current_value,
        'records/hour' AS unit,
        'High' AS priority
    FROM cdr_processed
    WHERE processed_at >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
    
    UNION ALL
    
    SELECT 
        'Active Data Quality Alerts' AS metric_name,
        COUNT(*) AS current_value,
        'alerts' AS unit,
        'Medium' AS priority
    FROM data_quality_alerts
    WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
    
    UNION ALL
    
    SELECT 
        'Network Performance Issues' AS metric_name,
        COUNT(*) AS current_value,
        'issues' AS unit,
        'High' AS priority
    FROM network_alerts
    WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
    AND severity IN ('high', 'critical')
)
SELECT 
    metric_name,
    current_value,
    unit,
    priority,
    CASE 
        WHEN priority = 'High' AND current_value > 10 THEN 'Warning'
        WHEN priority = 'Medium' AND current_value > 5 THEN 'Warning'
        ELSE 'Normal'
    END AS status
FROM real_time_metrics
ORDER BY priority DESC, current_value DESC;

-- Test the dashboard
SELECT * FROM telco_cdc_dashboard_view;

-- =====================================================
-- SUMMARY & NEXT STEPS
-- =====================================================
-- Estimated time: 5 minutes

-- Key Takeaways:
-- 1. Streams enable real-time change detection and processing
-- 2. Tasks automate data processing workflows
-- 3. Snowpipe provides continuous data ingestion capabilities
-- 4. CDC pipelines support real-time telco operations

-- Best Practices:
-- • Design streams for specific use cases
-- • Monitor task execution and performance
-- • Implement data quality checks in pipelines
-- • Use appropriate warehouse sizing for tasks

-- Business Benefits:
-- • Real-time data processing and analytics
-- • Automated operational workflows
-- • Improved data quality and monitoring
-- • Enhanced customer experience through real-time insights

-- Next Steps:
-- • Configure warehouse auto-scaling for tasks
-- • Implement error handling and retry logic
-- • Set up monitoring and alerting for pipeline health
-- • Scale CDC pipelines for production workloads

-- Clean up (optional - for workshop environment)
-- DROP STREAM IF EXISTS cdr_source_stream;
-- DROP STREAM IF EXISTS cdr_processing_stream;
-- DROP STREAM IF EXISTS subscriber_sync_stream;
-- DROP STREAM IF EXISTS network_performance_stream;
-- DROP TASK IF EXISTS process_cdr_task;
-- DROP TASK IF EXISTS advanced_cdr_processing_task;
-- DROP TASK IF EXISTS data_quality_monitoring_task;
-- DROP TASK IF EXISTS subscriber_sync_task;
-- DROP TASK IF EXISTS network_alerting_task;
-- DROP TASK IF EXISTS cdc_orchestration_task;
-- DROP VIEW IF EXISTS continuous_ingestion_view;
-- DROP VIEW IF EXISTS cdc_pipeline_monitoring_view;
-- DROP VIEW IF EXISTS cdc_performance_analytics_view;
-- DROP VIEW IF EXISTS telco_cdc_dashboard_view;