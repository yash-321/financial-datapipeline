-- =============================================================================
-- Snowflake Setup for Trade Data Pipeline
-- =============================================================================
-- This script creates all necessary Snowflake objects for ingesting trade data
-- from S3 via Snowpipe. Run with ACCOUNTADMIN or equivalent privileges.
--
-- Prerequisites:
--   1. S3 bucket created with parquet files at: s3://bucket/trades/date=.../symbol=.../
--   2. AWS IAM role configured for Snowflake storage integration
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Step 1: Create Database and Schema
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS DATAPIPELINE;
USE DATABASE DATAPIPELINE;

CREATE SCHEMA IF NOT EXISTS TRADES;
USE SCHEMA TRADES;

-- -----------------------------------------------------------------------------
-- Step 2: Create Storage Integration (requires ACCOUNTADMIN)
-- This creates an IAM role trust relationship with your AWS account.
-- After running, get the AWS IAM user ARN and external ID from:
--   DESC STORAGE INTEGRATION trades_s3_integration;
-- Then update your S3 bucket policy to allow this role.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STORAGE INTEGRATION trades_s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::587129419094:role/snowflake-trades-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://datapipeline-testing-587129419094-eu-west-1-an/trades/');

-- Show integration details (run after creation to get STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID)
-- DESC STORAGE INTEGRATION trades_s3_integration;

-- -----------------------------------------------------------------------------
-- Step 3: Create File Format for Parquet
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT trades_parquet_format
    TYPE = PARQUET
    COMPRESSION = SNAPPY;

-- -----------------------------------------------------------------------------
-- Step 4: Create External Stage pointing to S3
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STAGE trades_stage
    STORAGE_INTEGRATION = trades_s3_integration
    URL = 's3://datapipeline-testing-587129419094-eu-west-1-an/trades/'
    FILE_FORMAT = trades_parquet_format;

-- Verify stage access (should list parquet files):
-- LIST @trades_stage;

-- -----------------------------------------------------------------------------
-- Step 5: Create Target Table
-- Schema matches the parquet files produced by TradeConsumer.
-- Partition columns (date, symbol) are extracted from the file path.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_TRADES (
    -- Core trade fields (from parquet)
    symbol              VARCHAR(10)     NOT NULL,
    trade_id            VARCHAR(50)     NOT NULL,
    price               FLOAT           NOT NULL,
    quantity            FLOAT           NOT NULL,
    event_timestamp     BIGINT          NOT NULL,
    ingestion_timestamp BIGINT          NOT NULL,

    -- Partition columns extracted from Hive path
    trade_date          DATE,

    -- Metadata
    _loaded_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500)
);

-- Add clustering for query performance
ALTER TABLE RAW_TRADES CLUSTER BY (trade_date, symbol);

-- -----------------------------------------------------------------------------
-- Step 6: Create Snowpipe for Auto-Ingestion
-- Uses MATCH_BY_COLUMN_NAME to map parquet columns to table columns.
-- Partition columns are extracted from the file path metadata.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PIPE trades_pipe
    AUTO_INGEST = TRUE
    AS
    COPY INTO RAW_TRADES (
        symbol,
        trade_id,
        price,
        quantity,
        event_timestamp,
        ingestion_timestamp,
        trade_date,
        _source_file
    )
    FROM (
        SELECT
            $1:symbol::VARCHAR,
            $1:trade_id::VARCHAR,
            $1:price::FLOAT,
            $1:quantity::FLOAT,
            $1:event_timestamp::BIGINT,
            $1:ingestion_timestamp::BIGINT,
            -- Extract date from Hive partition path: .../date=2026-04-30/...
            TRY_TO_DATE(
                REGEXP_SUBSTR(METADATA$FILENAME, 'date=([0-9-]+)', 1, 1, 'e', 1)
            ),
            METADATA$FILENAME
        FROM @trades_stage
    )
    FILE_FORMAT = trades_parquet_format;

-- -----------------------------------------------------------------------------
-- Step 7: Configure S3 Event Notifications (manual step)
-- After creating the pipe, get the notification channel:
--   SHOW PIPES LIKE 'trades_pipe';
-- The notification_channel column contains the SQS ARN.
-- Configure your S3 bucket to send ObjectCreated events to this SQS queue.
-- -----------------------------------------------------------------------------

-- Show pipe details including SQS ARN for S3 event configuration:
-- SHOW PIPES LIKE 'trades_pipe';

-- -----------------------------------------------------------------------------
-- Step 8: Utility Views and Queries
-- -----------------------------------------------------------------------------

-- View for monitoring ingestion lag
CREATE OR REPLACE VIEW V_INGESTION_METRICS AS
SELECT
    trade_date,
    symbol,
    COUNT(*) as trade_count,
    MIN(event_timestamp) as min_event_ts,
    MAX(event_timestamp) as max_event_ts,
    MIN(_loaded_at) as first_loaded,
    MAX(_loaded_at) as last_loaded,
    DATEDIFF('second',
        TO_TIMESTAMP(MAX(event_timestamp) / 1000),
        MAX(_loaded_at)
    ) as ingestion_lag_seconds
FROM RAW_TRADES
GROUP BY trade_date, symbol;

-- View for daily trade summary
CREATE OR REPLACE VIEW V_DAILY_SUMMARY AS
SELECT
    trade_date,
    symbol,
    COUNT(*) as trade_count,
    SUM(price * quantity) as total_volume,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price
FROM RAW_TRADES
GROUP BY trade_date, symbol;

-- -----------------------------------------------------------------------------
-- Verification Queries (run after Snowpipe starts receiving data)
-- -----------------------------------------------------------------------------

-- Check pipe status:
-- SELECT SYSTEM$PIPE_STATUS('trades_pipe');

-- Check recent pipe history:
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
--     TABLE_NAME => 'RAW_TRADES',
--     START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
-- ))
-- ORDER BY LAST_LOAD_TIME DESC
-- LIMIT 20;

-- Check for ingestion errors:
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
--     TABLE_NAME => 'RAW_TRADES',
--     START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
-- ))
-- WHERE STATUS != 'Loaded'
-- ORDER BY LAST_LOAD_TIME DESC;

-- Sample query on loaded data:
-- SELECT * FROM RAW_TRADES LIMIT 10;
