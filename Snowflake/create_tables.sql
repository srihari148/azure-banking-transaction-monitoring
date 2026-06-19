-- ═══════════════════════════════════════════════════════════════
-- Snowflake DDL Scripts
-- Database : BANKING_DW
-- Schema   : GOLD
-- ═══════════════════════════════════════════════════════════════

-- Create Database and Schema
CREATE DATABASE IF NOT EXISTS BANKING_DW;
CREATE SCHEMA IF NOT EXISTS BANKING_DW.GOLD;

USE DATABASE BANKING_DW;
USE SCHEMA GOLD;

-- ── Customer KPI Table ───────────────────────────────────────────
CREATE OR REPLACE TABLE CUSTOMER_KPI (
    account_number         STRING,
    customer_id            STRING,
    customer_name          STRING,
    account_type           STRING,
    customer_city          STRING,
    customer_state         STRING,
    total_debit            FLOAT,
    total_credit           FLOAT,
    total_transactions     INT,
    avg_transaction_amount FLOAT,
    highest_transaction    FLOAT,
    gold_load_timestamp    STRING
);

-- ── Customer Monthly Table ───────────────────────────────────────
CREATE OR REPLACE TABLE CUSTOMER_MONTHLY (
    account_number      STRING,
    txn_year            INT,
    txn_month           INT,
    monthly_spend       FLOAT,
    avg_transaction     FLOAT,
    max_transaction     FLOAT,
    min_transaction     FLOAT,
    txn_count           INT,
    customer_id         STRING,
    customer_name       STRING,
    account_type        STRING,
    branch_id           STRING,
    city                STRING,
    state               STRING,
    status              STRING,
    gold_load_timestamp STRING
);

-- ── Branch Summary Table ─────────────────────────────────────────
CREATE OR REPLACE TABLE BRANCH_SUMMARY (
    branch_id              STRING,
    branch_name            STRING,
    city                   STRING,
    state                  STRING,
    region                 STRING,
    total_transactions     INT,
    total_amount           FLOAT,
    avg_transaction_amount FLOAT,
    highest_transaction    FLOAT,
    unique_customers       INT,
    total_debit            FLOAT,
    total_credit           FLOAT,
    gold_load_timestamp    STRING
);

-- ── Region Summary Table ─────────────────────────────────────────
CREATE OR REPLACE TABLE REGION_SUMMARY (
    region              STRING,
    total_transactions  INT,
    total_amount        FLOAT,
    unique_customers    INT,
    total_branches      INT,
    gold_load_timestamp STRING
);

-- ── Fraud High Value Table ───────────────────────────────────────
CREATE OR REPLACE TABLE FRAUD_HIGH_VALUE (
    transaction_id        STRING,
    account_number        STRING,
    transaction_timestamp STRING,
    transaction_type      STRING,
    amount                FLOAT,
    merchant_name         STRING,
    channel               STRING,
    city                  STRING,
    bronze_load_timestamp STRING,
    source_file           STRING,
    silver_load_timestamp STRING,
    transaction_date      STRING,
    fraud_type            STRING,
    fraud_detected_at     STRING
);

-- ── Fraud Burst Table ────────────────────────────────────────────
CREATE OR REPLACE TABLE FRAUD_BURST (
    account_number    STRING,
    txn_count         INT,
    total_amount      FLOAT,
    fraud_type        STRING,
    fraud_detected_at STRING
);

-- ── Fraud High Spend Table ───────────────────────────────────────
CREATE OR REPLACE TABLE FRAUD_HIGH_SPEND (
    account_number    STRING,
    transaction_date  STRING,
    daily_spend       FLOAT,
    txn_count         INT,
    fraud_type        STRING,
    fraud_detected_at STRING
);

-- ── Fraud Dormant Table ──────────────────────────────────────────
CREATE OR REPLACE TABLE FRAUD_DORMANT (
    account_number      STRING,
    first_txn_date      STRING,
    last_txn_date       STRING,
    last_txn_timestamp  STRING,
    total_txns          INT,
    gap_days            INT,
    fraud_type          STRING,
    fraud_detected_at   STRING
);

-- ── Verify Tables Created ────────────────────────────────────────
SHOW TABLES IN SCHEMA BANKING_DW.GOLD;

-- ── Row Count Verification ───────────────────────────────────────
SELECT 'CUSTOMER_KPI'     AS table_name, COUNT(*) AS row_count FROM CUSTOMER_KPI
UNION ALL
SELECT 'CUSTOMER_MONTHLY' AS table_name, COUNT(*) AS row_count FROM CUSTOMER_MONTHLY
UNION ALL
SELECT 'BRANCH_SUMMARY'   AS table_name, COUNT(*) AS row_count FROM BRANCH_SUMMARY
UNION ALL
SELECT 'REGION_SUMMARY'   AS table_name, COUNT(*) AS row_count FROM REGION_SUMMARY
UNION ALL
SELECT 'FRAUD_HIGH_VALUE' AS table_name, COUNT(*) AS row_count FROM FRAUD_HIGH_VALUE
UNION ALL
SELECT 'FRAUD_BURST'      AS table_name, COUNT(*) AS row_count FROM FRAUD_BURST
UNION ALL
SELECT 'FRAUD_HIGH_SPEND' AS table_name, COUNT(*) AS row_count FROM FRAUD_HIGH_SPEND
UNION ALL
SELECT 'FRAUD_DORMANT'    AS table_name, COUNT(*) AS row_count FROM FRAUD_DORMANT;
