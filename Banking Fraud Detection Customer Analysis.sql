-- =====================================================================
-- Banking Fraud Detection & Customer Analysis Portfolio Project
-- SQL Script for PostgreSQL (v14+)
-- Dataset: Synthetic Financial Transactions (https://www.kaggle.com/datasets/ealaxi/paysim1)
-- =====================================================================

-- Section 1: Database Setup
-- DROP DATABASE IF EXISTS banking_analysis;
CREATE DATABASE banking_analysis;
\c banking_analysis;  -- Connect to database

-- Section 2: Table Creation with Constraints
CREATE TABLE transactions (
    step INT NOT NULL,
    type VARCHAR(20) NOT NULL CHECK (type IN ('PAYMENT','TRANSFER','CASH_OUT','DEBIT','CASH_IN')),
    amount DECIMAL(15,2) NOT NULL CHECK (amount > 0),
    nameOrig VARCHAR(50) NOT NULL,
    oldbalanceOrg DECIMAL(15,2) NOT NULL,
    newbalanceOrig DECIMAL(15,2) NOT NULL,
    nameDest VARCHAR(50) NOT NULL,
    oldbalanceDest DECIMAL(15,2) NOT NULL,
    newbalanceDest DECIMAL(15,2) NOT NULL,
    isFraud INT NOT NULL CHECK (isFraud IN (0,1)),
    isFlaggedFraud INT NOT NULL CHECK (isFlaggedFraud IN (0,1)),
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Section 3: Data Import (Run separately in psql)
-- \COPY transactions(step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud)
-- FROM '/usairarshad/to/PS_20174392719_1491204439457_log.csv' 
-- DELIMITER ',' CSV HEADER;

-- Section 4: Data Quality Checks
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN amount <= 0 THEN 1 ELSE 0 END) AS invalid_amounts,
    SUM(CASE WHEN oldbalanceOrg < 0 THEN 1 ELSE 0 END) AS negative_oldbalanceOrg
FROM transactions;

-- Section 5: Core Analysis Queries
-- 5.1: Transaction Overview
SELECT
    type,
    COUNT(*) AS transaction_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(AVG(amount), 2) AS avg_amount,
    SUM(amount) AS total_volume
FROM transactions
GROUP BY type
ORDER BY transaction_count DESC;

-- 5.2: Fraud Analysis
SELECT
    type,
    SUM(isFraud) AS fraud_count,
    ROUND(SUM(isFraud) * 100.0 / COUNT(*), 3) AS fraud_rate,
    ROUND(AVG(CASE WHEN isFraud = 1 THEN amount END)) AS avg_fraud_amount
FROM transactions
GROUP BY type
ORDER BY fraud_rate DESC;

-- 5.3: Customer Segmentation
WITH customer_stats AS (
    SELECT
        nameOrig AS customer_id,
        COUNT(*) AS transaction_count,
        SUM(amount) AS total_amount,
        MAX(amount) AS max_transaction
    FROM transactions
    GROUP BY nameOrig
)
SELECT
    CASE 
        WHEN total_amount > 1000000 THEN 'Platinum'
        WHEN total_amount > 500000 THEN 'Gold'
        WHEN total_amount > 100000 THEN 'Silver'
        ELSE 'Standard'
    END AS segment,
    COUNT(*) AS customers,
    ROUND(AVG(total_amount)) AS avg_total_amount
FROM customer_stats
GROUP BY segment
ORDER BY avg_total_amount DESC;

-- 5.4: Fraud Pattern Detection
SELECT
    type,
    ROUND(AVG(amount)) AS avg_fraud_amount,
    ROUND(AVG(newbalanceOrig - oldbalanceOrg)) AS balance_change,
    ROUND(AVG(oldbalanceOrg)) AS avg_oldbalanceOrg
FROM transactions
WHERE isFraud = 1
GROUP BY type;

-- Section 6: Advanced Analytics
-- 6.1: Hourly Fraud Trends
SELECT
    EXTRACT(HOUR FROM transaction_date) AS hour_of_day,
    COUNT(*) AS total_transactions,
    SUM(isFraud) AS fraud_count,
    ROUND(SUM(isFraud)*100.0/COUNT(*), 2) AS fraud_rate
FROM transactions
GROUP BY hour_of_day
ORDER BY fraud_rate DESC;

-- 6.2: Balance Discrepancy Flags
SELECT
    nameOrig,
    amount,
    oldbalanceOrg,
    newbalanceOrig,
    (oldbalanceOrg - amount) AS expected_balance,
    (newbalanceOrig - (oldbalanceOrg - amount)) AS discrepancy
FROM transactions
WHERE ABS(newbalanceOrig - (oldbalanceOrg - amount)) > 1
AND type IN ('CASH_OUT', 'TRANSFER');

-- Section 7: Stored Procedures
-- 7.1: Daily Fraud Summary Report
CREATE OR REPLACE PROCEDURE generate_daily_fraud_report(report_date DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Fraud Report for %:', report_date;
    RAISE NOTICE '--------------------------------';
    
    -- Summary Stats
    WITH daily_fraud AS (
        SELECT *
        FROM transactions
        WHERE isFraud = 1
        AND transaction_date::DATE = report_date
    )
    SELECT
        COUNT(*) AS total_fraud,
        ROUND(AVG(amount)) AS avg_amount,
        type
    FROM daily_fraud
    GROUP BY type;
    
    -- Detailed Transactions
    SELECT 
        nameOrig,
        nameDest,
        amount,
        type
    FROM transactions
    WHERE isFraud = 1
    AND transaction_date::DATE = report_date
    ORDER BY amount DESC
    LIMIT 10;
END;
$$;

-- 7.2: Customer Activity Monitor
CREATE OR REPLACE FUNCTION flag_high_risk_customers()
RETURNS TABLE (
    customer_id VARCHAR,
    fraud_count INT,
    total_amount DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        nameOrig,
        COUNT(*) FILTER (WHERE isFraud = 1) AS fraud_incidents,
        SUM(amount) FILTER (WHERE isFraud = 1) AS fraud_total
    FROM transactions
    GROUP BY nameOrig
    HAVING COUNT(*) FILTER (WHERE isFraud = 1) >= 3
    ORDER BY fraud_total DESC;
END;
$$ LANGUAGE plpgsql;

-- Section 8: Optimization
CREATE INDEX idx_transactions_type ON transactions(type);
CREATE INDEX idx_transactions_fraud ON transactions(isFraud);
CREATE INDEX idx_transactions_date ON transactions(transaction_date);
CREATE INDEX idx_transactions_origin ON transactions(nameOrig);

-- Section 9: Export Results for Reporting
-- Export customer segments to CSV
COPY (
    WITH customer_stats AS (
        SELECT
            nameOrig,
            COUNT(*) AS transaction_count,
            SUM(amount) AS total_amount
        FROM transactions
        GROUP BY nameOrig
    )
    SELECT
        nameOrig AS customer_id,
        CASE 
            WHEN total_amount > 1000000 THEN 'Platinum'
            WHEN total_amount > 500000 THEN 'Gold'
            WHEN total_amount > 100000 THEN 'Silver'
            ELSE 'Standard'
        END AS segment,
        total_amount
    FROM customer_stats
) TO '/output/customer_segments.csv' WITH CSV HEADER;

-- Export fraud patterns
COPY (
    SELECT
        type,
        EXTRACT(HOUR FROM transaction_date) AS hour,
        amount,
        oldbalanceOrg,
        newbalanceOrig
    FROM transactions
    WHERE isFraud = 1
) TO '/output/fraud_patterns.csv' WITH CSV HEADER;