-- POET Cloud Cost ETL - Database Initialization
-- Run this once to set up the schema and any required permissions

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS cost_analytics;

-- Grant permissions (adjust as needed for your setup)
-- GRANT ALL ON SCHEMA cost_analytics TO postgres;
-- GRANT USAGE ON SCHEMA cost_analytics TO anon, authenticated;

-- Create a sync log table to track ETL runs
CREATE TABLE IF NOT EXISTS cost_analytics.sync_log (
    id SERIAL PRIMARY KEY,
    sync_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    source_name VARCHAR(255) NOT NULL,
    rows_loaded INTEGER,
    status VARCHAR(50) NOT NULL,
    error_message TEXT,
    duration_seconds DECIMAL(10, 2)
);

-- Index for querying recent syncs
CREATE INDEX IF NOT EXISTS idx_sync_log_timestamp
ON cost_analytics.sync_log(sync_timestamp DESC);

-- Comment on schema
COMMENT ON SCHEMA cost_analytics IS 'Cloud cost data from AWS, GCP, Azure';

-- The normalized costs view will be created by the ETL process
-- Here's the expected structure for reference:
/*
CREATE OR REPLACE VIEW cost_analytics.costs AS
SELECT
    date,
    account_id,
    service,
    region,
    cost,
    currency,
    cloud_provider,
    source_table,
    sync_timestamp
FROM cost_analytics.cup_normalized
UNION ALL
SELECT
    date,
    account_id,
    service,
    region,
    cost,
    currency,
    cloud_provider,
    source_table,
    sync_timestamp
FROM cost_analytics.account_180431284529_normalized;
*/
