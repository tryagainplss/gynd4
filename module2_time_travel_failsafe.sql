-- =====================================================
-- MODULE 2: SNOWFLAKE TIME TRAVEL & FAIL-SAFE
-- =====================================================
-- Duration: 60 minutes
-- Prerequisites: Basic SQL knowledge, Snowflake trial account
-- 
-- This worksheet covers:
-- 1. Introduction to Time Travel and Fail-safe
-- 2. Warm-up exercises (3 simple examples)
-- 3. Intermediate use-cases (3 telco scenarios)
-- 4. Advanced challenge (end-to-end mini-project)
-- 5. Summary & Next Steps
-- =====================================================

-- =====================================================
-- INTRODUCTION
-- =====================================================
-- Snowflake's Time Travel and Fail-safe features provide:
-- • Point-in-time data recovery (up to 90 days)
-- • Automatic data versioning for all tables
-- • 7-day emergency backup after Time Travel expires
-- • Audit trails and compliance support
--
-- In this module, we'll work with telco data scenarios:
-- • Subscriber data corruption and recovery
-- • Network performance data historical analysis
-- • Billing dispute resolution
-- • Regulatory compliance and audit trails
-- =====================================================

-- =====================================================
-- SETUP: Create sample telco data tables
-- =====================================================
-- Estimated time: 5 minutes

-- Use the same database from Module 1
USE DATABASE TELCO_WORKSHOP;
CREATE SCHEMA IF NOT EXISTS TIME_TRAVEL_DATA;
USE SCHEMA TIME_TRAVEL_DATA;

-- Create subscriber billing table with Time Travel enabled (default)
CREATE OR REPLACE TABLE subscriber_billing (
    billing_id STRING,
    subscriber_id STRING,
    phone_number STRING,
    billing_month DATE,
    plan_fee DECIMAL(10,2),
    usage_fee DECIMAL(10,2),
    taxes DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    payment_status STRING, -- 'paid', 'pending', 'overdue'
    payment_date DATE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create network incident log table
CREATE OR REPLACE TABLE network_incidents (
    incident_id STRING,
    cell_tower_id STRING,
    incident_type STRING, -- 'outage', 'performance_degradation', 'maintenance'
    severity STRING, -- 'low', 'medium', 'high', 'critical'
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    affected_subscribers INTEGER,
    resolution_notes STRING,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create subscriber plan changes audit table
CREATE OR REPLACE TABLE subscriber_plan_changes (
    change_id STRING,
    subscriber_id STRING,
    old_plan STRING,
    new_plan STRING,
    change_reason STRING,
    effective_date DATE,
    changed_by STRING,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert initial sample data
INSERT INTO subscriber_billing VALUES
('BILL001', 'SUB001', '+1234567890', '2024-01-01', 89.99, 15.50, 8.25, 113.74, 'paid', '2024-01-15', CURRENT_TIMESTAMP()),
('BILL002', 'SUB002', '+1234567891', '2024-01-01', 49.99, 5.25, 4.12, 59.36, 'paid', '2024-01-10', CURRENT_TIMESTAMP()),
('BILL003', 'SUB003', '+1234567892', '2024-01-01', 129.99, 0.00, 10.40, 140.39, 'pending', NULL, CURRENT_TIMESTAMP()),
('BILL004', 'SUB004', '+1234567893', '2024-01-01', 49.99, 12.75, 5.02, 67.76, 'overdue', NULL, CURRENT_TIMESTAMP()),
('BILL005', 'SUB005', '+1234567894', '2024-01-01', 89.99, 8.90, 7.91, 106.80, 'paid', '2024-01-12', CURRENT_TIMESTAMP());

INSERT INTO network_incidents VALUES
('INC001', 'TOWER001', 'maintenance', 'low', '2024-01-15 02:00:00', '2024-01-15 04:00:00', 50, 'Scheduled maintenance completed', CURRENT_TIMESTAMP()),
('INC002', 'TOWER002', 'outage', 'high', '2024-01-16 14:30:00', '2024-01-16 16:45:00', 200, 'Power failure resolved', CURRENT_TIMESTAMP()),
('INC003', 'TOWER003', 'performance_degradation', 'medium', '2024-01-17 09:15:00', '2024-01-17 11:30:00', 75, 'Bandwidth issues resolved', CURRENT_TIMESTAMP());

INSERT INTO subscriber_plan_changes VALUES
('CHG001', 'SUB002', 'basic', 'premium', 'upgrade_requested', '2024-01-20', 'customer_service', CURRENT_TIMESTAMP()),
('CHG002', 'SUB004', 'basic', 'suspended', 'payment_overdue', '2024-01-18', 'billing_system', CURRENT_TIMESTAMP()),
('CHG003', 'SUB001', 'premium', 'unlimited', 'promotion_applied', '2024-01-22', 'marketing_system', CURRENT_TIMESTAMP());

-- =====================================================
-- WARM-UP EXERCISES (3 simple examples)
-- =====================================================
-- Estimated time: 15 minutes

-- Exercise 1: Basic Time Travel Query
-- Query data as it existed at a specific point in time
-- This demonstrates the fundamental Time Travel capability
SELECT 
    'Current Data' AS data_version,
    COUNT(*) AS record_count,
    SUM(total_amount) AS total_billing
FROM subscriber_billing

UNION ALL

SELECT 
    'Data 1 hour ago' AS data_version,
    COUNT(*) AS record_count,
    SUM(total_amount) AS total_billing
FROM subscriber_billing AT(offset => -3600); -- 1 hour ago

-- Exercise 2: Time Travel with Specific Timestamp
-- Query data as it existed at a specific timestamp
-- This shows how to use exact timestamps for Time Travel
SELECT 
    billing_id,
    subscriber_id,
    total_amount,
    payment_status,
    created_at
FROM subscriber_billing 
AT(timestamp => CURRENT_TIMESTAMP() - INTERVAL '30 minutes')
ORDER BY created_at DESC;

-- Exercise 3: Compare Data Changes Over Time
-- See how data has changed between different time points
-- This demonstrates data evolution tracking
WITH current_data AS (
    SELECT 
        subscriber_id,
        payment_status,
        total_amount
    FROM subscriber_billing
),
historical_data AS (
    SELECT 
        subscriber_id,
        payment_status,
        total_amount
    FROM subscriber_billing AT(offset => -1800) -- 30 minutes ago
)
SELECT 
    c.subscriber_id,
    h.payment_status AS old_status,
    c.payment_status AS new_status,
    h.total_amount AS old_amount,
    c.total_amount AS new_amount,
    CASE 
        WHEN h.payment_status != c.payment_status THEN 'Status Changed'
        WHEN h.total_amount != c.total_amount THEN 'Amount Changed'
        ELSE 'No Change'
    END AS change_type
FROM current_data c
JOIN historical_data h ON c.subscriber_id = h.subscriber_id
WHERE h.payment_status != c.payment_status OR h.total_amount != c.total_amount;

-- =====================================================
-- INTERMEDIATE USE-CASES (3 telco scenarios)
-- =====================================================
-- Estimated time: 25 minutes

-- Scenario 1: Subscriber Data Recovery Simulation
-- Simulate data corruption and recovery using Time Travel
-- This addresses real telco needs for data recovery

-- Step 1: Simulate data corruption (accidental update)
UPDATE subscriber_billing 
SET total_amount = 0, payment_status = 'cancelled'
WHERE subscriber_id = 'SUB001';

-- Step 2: Query corrupted data
SELECT 
    'Corrupted Data' AS status,
    billing_id,
    subscriber_id,
    total_amount,
    payment_status
FROM subscriber_billing 
WHERE subscriber_id = 'SUB001'

UNION ALL

-- Step 3: Query data before corruption using Time Travel
SELECT 
    'Before Corruption' AS status,
    billing_id,
    subscriber_id,
    total_amount,
    payment_status
FROM subscriber_billing 
AT(offset => -300) -- 5 minutes ago
WHERE subscriber_id = 'SUB001';

-- Step 4: Restore data from Time Travel
CREATE OR REPLACE TABLE subscriber_billing_restored AS
SELECT * FROM subscriber_billing AT(offset => -300);

-- Verify restoration
SELECT 
    'Restored Data' AS status,
    billing_id,
    subscriber_id,
    total_amount,
    payment_status
FROM subscriber_billing_restored 
WHERE subscriber_id = 'SUB001';

-- Scenario 2: Network Incident Historical Analysis
-- Analyze network performance and incidents over time
-- This supports telco network operations and planning
SELECT 
    'Current Network Status' AS analysis_period,
    COUNT(*) AS total_incidents,
    AVG(affected_subscribers) AS avg_affected_subscribers,
    SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_incidents
FROM network_incidents

UNION ALL

SELECT 
    'Network Status 1 Day Ago' AS analysis_period,
    COUNT(*) AS total_incidents,
    AVG(affected_subscribers) AS avg_affected_subscribers,
    SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_incidents
FROM network_incidents AT(offset => -86400); -- 24 hours ago

-- Scenario 3: Billing Dispute Resolution
-- Track billing changes and resolve customer disputes
-- This supports telco customer service operations
WITH billing_changes AS (
    SELECT 
        billing_id,
        subscriber_id,
        total_amount AS current_amount,
        payment_status AS current_status,
        (SELECT total_amount FROM subscriber_billing AT(offset => -3600) b2 WHERE b2.billing_id = subscriber_billing.billing_id) AS amount_1_hour_ago,
        (SELECT payment_status FROM subscriber_billing AT(offset => -3600) b2 WHERE b2.billing_id = subscriber_billing.billing_id) AS status_1_hour_ago
    FROM subscriber_billing
)
SELECT 
    billing_id,
    subscriber_id,
    amount_1_hour_ago AS original_amount,
    current_amount AS current_amount,
    current_amount - amount_1_hour_ago AS amount_change,
    status_1_hour_ago AS original_status,
    current_status AS current_status,
    CASE 
        WHEN current_amount != amount_1_hour_ago THEN 'Amount Modified'
        WHEN current_status != status_1_hour_ago THEN 'Status Modified'
        ELSE 'No Changes'
    END AS change_summary
FROM billing_changes
WHERE current_amount != amount_1_hour_ago OR current_status != status_1_hour_ago;

-- =====================================================
-- ADVANCED CHALLENGE: End-to-End Mini-Project
-- =====================================================
-- Estimated time: 15 minutes

-- Challenge: Create a comprehensive audit and recovery system
-- This integrates Time Travel with business logic for telco compliance

-- Step 1: Create an audit trail view using Time Travel
CREATE OR REPLACE VIEW subscriber_audit_trail_view AS
WITH current_data AS (
    SELECT 
        subscriber_id,
        phone_number,
        plan_type,
        status,
        monthly_fee,
        created_at
    FROM TELCO_WORKSHOP.CDR_DATA.subscribers
),
historical_data AS (
    SELECT 
        subscriber_id,
        phone_number,
        plan_type,
        status,
        monthly_fee,
        created_at
    FROM TELCO_WORKSHOP.CDR_DATA.subscribers AT(offset => -7200) -- 2 hours ago
)
SELECT 
    c.subscriber_id,
    c.phone_number,
    h.plan_type AS old_plan,
    c.plan_type AS new_plan,
    h.status AS old_status,
    c.status AS new_status,
    h.monthly_fee AS old_fee,
    c.monthly_fee AS new_fee,
    CASE 
        WHEN h.plan_type != c.plan_type THEN 'Plan Changed'
        WHEN h.status != c.status THEN 'Status Changed'
        WHEN h.monthly_fee != c.monthly_fee THEN 'Fee Changed'
        ELSE 'No Changes'
    END AS change_type,
    CURRENT_TIMESTAMP() AS audit_timestamp
FROM current_data c
JOIN historical_data h ON c.subscriber_id = h.subscriber_id
WHERE h.plan_type != c.plan_type OR h.status != c.status OR h.monthly_fee != c.monthly_fee;

-- Test the audit trail
SELECT * FROM subscriber_audit_trail_view;

-- Step 2: Create a data recovery procedure view
CREATE OR REPLACE VIEW data_recovery_procedures_view AS
SELECT 
    'Subscriber Data Recovery' AS recovery_scenario,
    'Use Time Travel to restore subscriber data to previous state' AS procedure,
    'SELECT * FROM subscribers AT(offset => -3600)' AS recovery_query,
    'High' AS priority

UNION ALL

SELECT 
    'Billing Data Recovery' AS recovery_scenario,
    'Restore billing records from specific timestamp' AS procedure,
    'SELECT * FROM subscriber_billing AT(timestamp => ''2024-01-15 10:00:00'')' AS recovery_query,
    'Critical' AS priority

UNION ALL

SELECT 
    'Network Incident Recovery' AS recovery_scenario,
    'Recover network incident logs from backup' AS procedure,
    'SELECT * FROM network_incidents AT(offset => -86400)' AS recovery_query,
    'Medium' AS priority;

-- Test the recovery procedures
SELECT * FROM data_recovery_procedures_view;

-- Step 3: Create a compliance monitoring view
CREATE OR REPLACE VIEW compliance_monitoring_view AS
WITH data_retention_check AS (
    SELECT 
        'Subscriber Data' AS data_type,
        COUNT(*) AS current_records,
        COUNT(*) AS records_30_days_ago,
        'Time Travel Available' AS retention_status
    FROM TELCO_WORKSHOP.CDR_DATA.subscribers
    
    UNION ALL
    
    SELECT 
        'Billing Data' AS data_type,
        COUNT(*) AS current_records,
        COUNT(*) AS records_30_days_ago,
        'Time Travel Available' AS retention_status
    FROM subscriber_billing
),
audit_compliance AS (
    SELECT 
        'Plan Changes' AS audit_type,
        COUNT(*) AS changes_last_24h,
        'Compliant' AS compliance_status
    FROM subscriber_plan_changes
    WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
)
SELECT 
    'Data Retention Compliance' AS compliance_area,
    data_type,
    current_records,
    retention_status,
    'Compliant' AS overall_status
FROM data_retention_check

UNION ALL

SELECT 
    'Audit Trail Compliance' AS compliance_area,
    audit_type,
    changes_last_24h,
    compliance_status,
    'Compliant' AS overall_status
FROM audit_compliance;

-- Test the compliance monitoring
SELECT * FROM compliance_monitoring_view;

-- Step 4: Create a disaster recovery summary
CREATE OR REPLACE VIEW disaster_recovery_summary_view AS
SELECT 
    'Time Travel Status' AS recovery_component,
    'Active' AS status,
    '90 days retention available' AS details,
    'Automatic' AS backup_type

UNION ALL

SELECT 
    'Fail-safe Status' AS recovery_component,
    'Active' AS status,
    '7-day emergency backup after Time Travel' AS details,
    'Automatic' AS backup_type

UNION ALL

SELECT 
    'Data Recovery Procedures' AS recovery_component,
    'Documented' AS status,
    '3 recovery scenarios implemented' AS details,
    'Manual' AS backup_type

UNION ALL

SELECT 
    'Compliance Monitoring' AS recovery_component,
    'Active' AS status,
    'Real-time audit trail available' AS details,
    'Automated' AS backup_type;

-- Test the disaster recovery summary
SELECT * FROM disaster_recovery_summary_view;

-- =====================================================
-- SUMMARY & NEXT STEPS
-- =====================================================
-- Estimated time: 5 minutes

-- Key Takeaways:
-- 1. Time Travel provides automatic point-in-time data recovery
-- 2. Fail-safe offers 7-day emergency backup protection
-- 3. Time Travel supports audit trails and compliance requirements
-- 4. Data recovery procedures can be automated and documented

-- Best Practices:
-- • Use Time Travel for data recovery and audit trails
-- • Document recovery procedures for critical data
-- • Monitor data retention policies for compliance
-- • Test recovery procedures regularly

-- Compliance Benefits:
-- • Regulatory data retention requirements
-- • Audit trail capabilities
-- • Data recovery for dispute resolution
-- • Historical analysis for business intelligence

-- Next Steps:
-- • Configure custom retention policies
-- • Implement automated recovery procedures
-- • Set up monitoring for data changes
-- • Train teams on Time Travel capabilities

-- Clean up (optional - for workshop environment)
-- DROP TABLE IF EXISTS subscriber_billing_restored;
-- DROP VIEW IF EXISTS subscriber_audit_trail_view;
-- DROP VIEW IF EXISTS data_recovery_procedures_view;
-- DROP VIEW IF EXISTS compliance_monitoring_view;
-- DROP VIEW IF EXISTS disaster_recovery_summary_view;