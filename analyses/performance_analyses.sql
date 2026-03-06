-- ============================================================
-- Snowflake Performance Analysis Queries
-- Run these in your Snowflake worksheet as ACCOUNTADMIN
-- or a role with access to SNOWFLAKE.ACCOUNT_USAGE
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- STEP 0: Grant access to ACCOUNT_USAGE (run as ACCOUNTADMIN)
-- ─────────────────────────────────────────────────────────────
USE ROLE ACCOUNTADMIN;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE TRANSFORMER;

-- Switch to TRANSFORMER role for all queries below
USE ROLE TRANSFORMER;
USE WAREHOUSE DEV_WH;

-- ─────────────────────────────────────────────────────────────
-- QUERY 1: View all recent dbt queries
-- Shows every query run by your dbt pipeline in the last 7 days
-- ─────────────────────────────────────────────────────────────
SELECT
    query_id,
    query_text,
    database_name,
    schema_name,
    query_type,
    warehouse_name,
    execution_status,
    start_time,
    end_time,
    -- Execution time in seconds
    round(total_elapsed_time / 1000, 2)         as execution_time_secs,
    -- Data scanned in MB
    round(bytes_scanned / 1024 / 1024, 2)       as mb_scanned,
    -- Data spilled to disk (bad — means warehouse too small)
    round(bytes_spilled_to_local_storage / 1024 / 1024, 2)
                                                as mb_spilled_to_disk,
    partitions_scanned,
    partitions_total,
    -- Partition pruning efficiency %
    round(
        (1 - partitions_scanned / nullif(partitions_total, 0)) * 100
    , 2)                                        as partition_pruning_pct,
    credits_used_cloud_services
FROM snowflake.account_usage.query_history
WHERE
    start_time >= dateadd('day', -7, current_timestamp())
    AND warehouse_name = 'DEV_WH'
    AND execution_status = 'SUCCESS'
ORDER BY start_time DESC
LIMIT 100;

-- ─────────────────────────────────────────────────────────────
-- QUERY 2: Compare BEFORE vs AFTER optimization
-- Run this after your first incremental run to see improvement
-- ─────────────────────────────────────────────────────────────
WITH query_runs AS (
    SELECT
        query_id,
        query_text,
        start_time,
        round(total_elapsed_time / 1000, 2)         as execution_time_secs,
        round(bytes_scanned / 1024 / 1024, 2)       as mb_scanned,
        partitions_scanned,
        partitions_total,
        round(
            (1 - partitions_scanned / nullif(partitions_total, 0)) * 100
        , 2)                                        as partition_pruning_pct
    FROM snowflake.account_usage.query_history
    WHERE
        start_time >= dateadd('day', -7, current_timestamp())
        AND warehouse_name = 'DEV_WH'
        AND execution_status = 'SUCCESS'
        -- Filter to fct_stock_prices related queries only
        AND (
            lower(query_text) like '%fct_stock_prices%'
            OR lower(query_text) like '%int_stock_moving_averages%'
        )
),

-- Tag queries as full refresh (before) or incremental (after)
tagged AS (
    SELECT
        *,
        CASE
            WHEN lower(query_text) like '%is_incremental%'
              OR lower(query_text) like '%merge into%'
            THEN 'Incremental Run'
            ELSE 'Full Refresh Run'
        END as run_type
    FROM query_runs
)

SELECT
    run_type,
    count(*)                                    as total_queries,
    round(avg(execution_time_secs), 2)          as avg_execution_secs,
    round(avg(mb_scanned), 2)                   as avg_mb_scanned,
    round(avg(partition_pruning_pct), 2)        as avg_pruning_pct
FROM tagged
GROUP BY run_type
ORDER BY run_type;


-- ─────────────────────────────────────────────────────────────
-- QUERY 3: Warehouse credit consumption over time
-- Shows how many credits DEV_WH consumed per hour
-- ─────────────────────────────────────────────────────────────
SELECT
    date_trunc('hour', start_time)              as hour,
    sum(credits_used)                           as credits_used,
    sum(credits_used_compute)                   as compute_credits,
    sum(credits_used_cloud_services)            as cloud_service_credits
FROM snowflake.account_usage.warehouse_metering_history
WHERE
    warehouse_name = 'DEV_WH'
    AND start_time >= dateadd('day', -7, current_timestamp())
GROUP BY 1
ORDER BY 1 DESC;


-- ─────────────────────────────────────────────────────────────
-- QUERY 4: Top 10 slowest queries
-- Identify your most expensive queries to target for optimization
-- ─────────────────────────────────────────────────────────────
SELECT
    query_id,
    LEFT(query_text, 100)                       as query_preview,
    round(total_elapsed_time / 1000, 2)         as execution_time_secs,
    round(bytes_scanned / 1024 / 1024, 2)       as mb_scanned,
    partitions_scanned,
    partitions_total,
    round(
        (1 - partitions_scanned / nullif(partitions_total, 0)) * 100
    , 2)                                        as partition_pruning_pct
FROM snowflake.account_usage.query_history
WHERE
    start_time >= dateadd('day', -7, current_timestamp())
    AND warehouse_name = 'DEV_WH'
    AND execution_status = 'SUCCESS'
ORDER BY total_elapsed_time DESC
LIMIT 10;


-- ─────────────────────────────────────────────────────────────
-- QUERY 5: Clustering effectiveness on fct_stock_prices
-- Run AFTER dbt build to verify clustering keys are working
-- ─────────────────────────────────────────────────────────────
SELECT
    SYSTEM$CLUSTERING_INFORMATION(
        'STOCK_ANALYTICS.MARTS.FCT_STOCK_PRICES',
        '(PRICE_DATE, TICKER)'
    ) as clustering_info;


-- ─────────────────────────────────────────────────────────────
-- QUERY 6: Table storage and row counts across all layers
-- Good baseline to include in your README
-- ─────────────────────────────────────────────────────────────
SELECT
    table_schema,
    table_name,
    row_count,
    round(bytes / 1024 / 1024, 2)              as size_mb,
    last_altered
FROM stock_analytics.information_schema.tables
WHERE table_schema IN ('RAW', 'STAGING', 'INTERMEDIATE', 'MARTS')
ORDER BY table_schema, table_name;