-- Test 1: initial load should create rows in dim_exoplanets
SELECT COUNT(*) AS dim_row_count
FROM dbo.dim_exoplanets;

-- Test 2: rerun without new source rows should not increase row count
-- (capture before/after counts around a second run)
SELECT COUNT(*) AS dim_row_count_after_second_run
FROM dbo.dim_exoplanets;

-- Test 3: staging dedup check per run (no duplicate pl_name/disc_pubdate per source_run_id)
SELECT source_run_id, pl_name, disc_pubdate, COUNT(*) AS cnt
FROM dbo.stg_exoplanets_raw
GROUP BY source_run_id, pl_name, disc_pubdate
HAVING COUNT(*) > 1;

-- Test 4: core business key uniqueness
SELECT pl_name, COUNT(*) AS cnt
FROM dbo.dim_exoplanets
GROUP BY pl_name
HAVING COUNT(*) > 1;

-- Test 5: ETL status and watermark progression
SELECT TOP 20
    id,
    entity_name,
    status,
    rows_loaded,
    watermark_before,
    watermark_after,
    start_time,
    end_time,
    error_message
FROM dbo.etl_log
ORDER BY id DESC;
