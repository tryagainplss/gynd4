-- =====================================================
-- MODULE 1: SNOWFLAKE VIEWS & PERFORMANCE OPTIMIZATION
-- =====================================================
-- Duration: 60 minutes
-- Prerequisites: Basic SQL knowledge, Snowflake trial account
-- 
-- This worksheet covers:
-- 1. Introduction to Snowflake Views
-- 2. Warm-up exercises (3 simple examples)
-- 3. Intermediate use-cases (3 telco scenarios)
-- 4. Advanced challenge (end-to-end mini-project)
-- 5. Summary & Next Steps
-- =====================================================

-- =====================================================
-- INTRODUCTION
-- =====================================================
-- Views in Snowflake are virtual tables that provide:
-- • Query simplification and data abstraction
-- • Security and access control
-- • Performance optimization through caching
-- • Consistent data access patterns
--
-- In this module, we'll work with telco data including:
-- • Call Detail Records (CDR)
-- • Network performance metrics
-- • Subscriber information
-- • Billing data
-- =====================================================

-- =====================================================
-- SETUP: Create sample telco data tables
-- =====================================================
-- Estimated time: 5 minutes

-- Create database and schema for our workshop
CREATE DATABASE IF NOT EXISTS TELCO_WORKSHOP;
USE DATABASE TELCO_WORKSHOP;
CREATE SCHEMA IF NOT EXISTS CDR_DATA;
USE SCHEMA CDR_DATA;

-- Create sample Call Detail Records table
CREATE OR REPLACE TABLE call_detail_records (
    call_id STRING,
    subscriber_id STRING,
    phone_number STRING,
    call_start_time TIMESTAMP_NTZ,
    call_end_time TIMESTAMP_NTZ,
    call_duration_seconds INTEGER,
    call_type STRING, -- 'voice', 'sms', 'data'
    network_type STRING, -- '4G', '5G', '3G'
    location_lat DECIMAL(10,6),
    location_lon DECIMAL(10,6),
    cost_usd DECIMAL(10,4),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create sample subscriber table
CREATE OR REPLACE TABLE subscribers (
    subscriber_id STRING,
    phone_number STRING,
    plan_type STRING, -- 'basic', 'premium', 'unlimited'
    activation_date DATE,
    status STRING, -- 'active', 'suspended', 'cancelled'
    monthly_fee DECIMAL(10,2),
    data_limit_gb INTEGER,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create sample network performance table
CREATE OR REPLACE TABLE network_performance (
    record_id STRING,
    cell_tower_id STRING,
    timestamp TIMESTAMP_NTZ,
    signal_strength_dbm INTEGER,
    bandwidth_mbps DECIMAL(10,2),
    latency_ms INTEGER,
    packet_loss_percent DECIMAL(5,2),
    region STRING,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample data
INSERT INTO call_detail_records VALUES
('CDR001', 'SUB001', '+1234567890', '2024-01-15 10:30:00', '2024-01-15 10:32:15', 135, 'voice', '5G', 40.7128, -74.0060, 0.25),
('CDR002', 'SUB002', '+1234567891', '2024-01-15 11:15:00', '2024-01-15 11:16:30', 90, 'sms', '4G', 40.7589, -73.9851, 0.10),
('CDR003', 'SUB001', '+1234567890', '2024-01-15 12:00:00', '2024-01-15 12:05:30', 330, 'data', '5G', 40.7505, -73.9934, 1.50),
('CDR004', 'SUB003', '+1234567892', '2024-01-15 13:20:00', '2024-01-15 13:22:45', 165, 'voice', '4G', 40.7829, -73.9654, 0.30),
('CDR005', 'SUB002', '+1234567891', '2024-01-15 14:10:00', '2024-01-15 14:12:20', 140, 'voice', '5G', 40.7128, -74.0060, 0.28);

INSERT INTO subscribers VALUES
('SUB001', '+1234567890', 'premium', '2023-01-15', 'active', 89.99, 50),
('SUB002', '+1234567891', 'basic', '2023-03-20', 'active', 49.99, 10),
('SUB003', '+1234567892', 'unlimited', '2023-06-10', 'active', 129.99, 100),
('SUB004', '+1234567893', 'basic', '2023-02-28', 'suspended', 49.99, 10),
('SUB005', '+1234567894', 'premium', '2023-04-15', 'active', 89.99, 50);

INSERT INTO network_performance VALUES
('NP001', 'TOWER001', '2024-01-15 10:30:00', -65, 150.5, 25, 0.1, 'Manhattan'),
('NP002', 'TOWER002', '2024-01-15 11:15:00', -72, 85.2, 35, 0.3, 'Brooklyn'),
('NP003', 'TOWER001', '2024-01-15 12:00:00', -58, 200.1, 18, 0.05, 'Manhattan'),
('NP004', 'TOWER003', '2024-01-15 13:20:00', -78, 45.8, 45, 0.8, 'Queens'),
('NP005', 'TOWER001', '2024-01-15 14:10:00', -62, 180.3, 22, 0.2, 'Manhattan');

-- =====================================================
-- WARM-UP EXERCISES (3 simple examples)
-- =====================================================
-- Estimated time: 15 minutes

-- Exercise 1: Basic View Creation
-- Create a simple view to show active subscribers with their basic info
-- This demonstrates the fundamental concept of views as virtual tables
CREATE OR REPLACE VIEW active_subscribers_view AS
SELECT 
    subscriber_id,
    phone_number,
    plan_type,
    monthly_fee,
    data_limit_gb
FROM subscribers 
WHERE status = 'active';

-- Test the view
SELECT * FROM active_subscribers_view;

-- Exercise 2: View with Calculated Fields
-- Create a view that calculates call duration in minutes and cost per minute
-- This shows how views can simplify complex calculations
CREATE OR REPLACE VIEW call_analytics_view AS
SELECT 
    call_id,
    subscriber_id,
    phone_number,
    call_start_time,
    call_duration_seconds,
    call_duration_seconds / 60.0 AS duration_minutes,
    cost_usd,
    CASE 
        WHEN call_duration_seconds > 0 THEN cost_usd / (call_duration_seconds / 60.0)
        ELSE 0 
    END AS cost_per_minute,
    call_type,
    network_type
FROM call_detail_records;

-- Test the view
SELECT * FROM call_analytics_view;

-- Exercise 3: View with Joins
-- Create a view that combines subscriber and call data
-- This demonstrates how views can simplify complex joins
CREATE OR REPLACE VIEW subscriber_call_summary_view AS
SELECT 
    s.subscriber_id,
    s.phone_number,
    s.plan_type,
    s.monthly_fee,
    COUNT(c.call_id) AS total_calls,
    SUM(c.call_duration_seconds) AS total_duration_seconds,
    SUM(c.cost_usd) AS total_cost
FROM subscribers s
LEFT JOIN call_detail_records c ON s.subscriber_id = c.subscriber_id
WHERE s.status = 'active'
GROUP BY s.subscriber_id, s.phone_number, s.plan_type, s.monthly_fee;

-- Test the view
SELECT * FROM subscriber_call_summary_view;

-- =====================================================
-- INTERMEDIATE USE-CASES (3 telco scenarios)
-- =====================================================
-- Estimated time: 25 minutes

-- Scenario 1: Network Performance Monitoring View
-- Create a view for network engineers to monitor performance by region
-- This addresses real telco needs for network monitoring
CREATE OR REPLACE VIEW network_performance_monitoring_view AS
SELECT 
    region,
    DATE_TRUNC('hour', timestamp) AS hour_bucket,
    AVG(signal_strength_dbm) AS avg_signal_strength,
    AVG(bandwidth_mbps) AS avg_bandwidth,
    AVG(latency_ms) AS avg_latency,
    AVG(packet_loss_percent) AS avg_packet_loss,
    COUNT(*) AS record_count
FROM network_performance
GROUP BY region, DATE_TRUNC('hour', timestamp)
ORDER BY region, hour_bucket;

-- Test the view
SELECT * FROM network_performance_monitoring_view;

-- Scenario 2: Revenue Analysis View
-- Create a view for business analysts to track revenue by plan type
-- This supports telco business intelligence needs
CREATE OR REPLACE VIEW revenue_analysis_view AS
SELECT 
    s.plan_type,
    COUNT(DISTINCT s.subscriber_id) AS subscriber_count,
    SUM(s.monthly_fee) AS monthly_recurring_revenue,
    SUM(c.cost_usd) AS usage_revenue,
    SUM(s.monthly_fee) + SUM(COALESCE(c.cost_usd, 0)) AS total_revenue
FROM subscribers s
LEFT JOIN (
    SELECT 
        subscriber_id,
        SUM(cost_usd) AS cost_usd
    FROM call_detail_records
    GROUP BY subscriber_id
) c ON s.subscriber_id = c.subscriber_id
WHERE s.status = 'active'
GROUP BY s.plan_type
ORDER BY total_revenue DESC;

-- Test the view
SELECT * FROM revenue_analysis_view;

-- Scenario 3: Data Usage Monitoring View
-- Create a view to monitor data usage patterns for capacity planning
-- This helps telco operations teams with network planning
CREATE OR REPLACE VIEW data_usage_monitoring_view AS
SELECT 
    DATE_TRUNC('day', call_start_time) AS usage_date,
    network_type,
    COUNT(*) AS data_sessions,
    SUM(call_duration_seconds) AS total_duration_seconds,
    AVG(call_duration_seconds) AS avg_session_duration,
    SUM(cost_usd) AS total_data_revenue
FROM call_detail_records
WHERE call_type = 'data'
GROUP BY DATE_TRUNC('day', call_start_time), network_type
ORDER BY usage_date DESC, network_type;

-- Test the view
SELECT * FROM data_usage_monitoring_view;

-- =====================================================
-- ADVANCED CHALLENGE: End-to-End Mini-Project
-- =====================================================
-- Estimated time: 15 minutes

-- Challenge: Create a comprehensive telco dashboard view system
-- This integrates multiple views to create a complete operational dashboard

-- Step 1: Create a materialized view for expensive aggregations
-- Materialized views store pre-computed results for better performance
CREATE OR REPLACE MATERIALIZED VIEW daily_network_metrics_mv AS
SELECT 
    DATE_TRUNC('day', timestamp) AS metric_date,
    region,
    network_type,
    AVG(signal_strength_dbm) AS avg_signal_strength,
    AVG(bandwidth_mbps) AS avg_bandwidth,
    AVG(latency_ms) AS avg_latency,
    COUNT(*) AS measurement_count
FROM network_performance np
JOIN (
    SELECT DISTINCT network_type, call_start_time
    FROM call_detail_records
) cdr ON DATE_TRUNC('hour', np.timestamp) = DATE_TRUNC('hour', cdr.call_start_time)
GROUP BY DATE_TRUNC('day', timestamp), region, network_type;

-- Step 2: Create a comprehensive dashboard view
CREATE OR REPLACE VIEW telco_operations_dashboard_view AS
WITH daily_metrics AS (
    SELECT * FROM daily_network_metrics_mv
),
revenue_summary AS (
    SELECT * FROM revenue_analysis_view
),
usage_summary AS (
    SELECT * FROM data_usage_monitoring_view
    WHERE usage_date = CURRENT_DATE() - INTERVAL '1 day'
)
SELECT 
    'Network Performance' AS metric_category,
    region AS metric_name,
    avg_signal_strength AS metric_value,
    'dBm' AS unit
FROM daily_metrics
WHERE metric_date = CURRENT_DATE() - INTERVAL '1 day'

UNION ALL

SELECT 
    'Revenue' AS metric_category,
    plan_type AS metric_name,
    total_revenue AS metric_value,
    'USD' AS unit
FROM revenue_summary

UNION ALL

SELECT 
    'Data Usage' AS metric_category,
    network_type AS metric_name,
    total_duration_seconds AS metric_value,
    'seconds' AS unit
FROM usage_summary;

-- Test the comprehensive dashboard
SELECT * FROM telco_operations_dashboard_view;

-- Step 3: Create a view for performance monitoring
-- This view helps identify performance bottlenecks
CREATE OR REPLACE VIEW performance_monitoring_view AS
SELECT 
    'Query Performance' AS monitoring_area,
    'View Cache Hit Rate' AS metric,
    'High' AS status,
    'Views are cached for 24 hours by default' AS description

UNION ALL

SELECT 
    'Data Access' AS monitoring_area,
    'Active Subscribers' AS metric,
    (SELECT COUNT(*) FROM active_subscribers_view)::STRING AS status,
    'Real-time subscriber count' AS description

UNION ALL

SELECT 
    'Network Health' AS monitoring_area,
    'Average Signal Strength' AS metric,
    (SELECT AVG(avg_signal_strength) FROM daily_network_metrics_mv)::STRING AS status,
    'Daily network performance average' AS description;

-- Test the performance monitoring view
SELECT * FROM performance_monitoring_view;

-- =====================================================
-- SUMMARY & NEXT STEPS
-- =====================================================
-- Estimated time: 5 minutes

-- Key Takeaways:
-- 1. Views provide data abstraction and security
-- 2. Materialized views improve performance for expensive queries
-- 3. Views can simplify complex business logic
-- 4. Proper view design supports telco operational needs

-- Performance Best Practices:
-- • Use materialized views for expensive aggregations
-- • Leverage Snowflake's automatic caching (24 hours)
-- • Monitor query performance with EXPLAIN PLAN
-- • Design views for specific use cases and user roles

-- Next Steps:
-- • Explore Snowflake's query optimization features
-- • Learn about clustering and partitioning
-- • Practice with larger datasets
-- • Implement role-based access control with views

-- Clean up (optional - for workshop environment)
-- DROP VIEW IF EXISTS active_subscribers_view;
-- DROP VIEW IF EXISTS call_analytics_view;
-- DROP VIEW IF EXISTS subscriber_call_summary_view;
-- DROP VIEW IF EXISTS network_performance_monitoring_view;
-- DROP VIEW IF EXISTS revenue_analysis_view;
-- DROP VIEW IF EXISTS data_usage_monitoring_view;
-- DROP MATERIALIZED VIEW IF EXISTS daily_network_metrics_mv;
-- DROP VIEW IF EXISTS telco_operations_dashboard_view;
-- DROP VIEW IF EXISTS performance_monitoring_view;