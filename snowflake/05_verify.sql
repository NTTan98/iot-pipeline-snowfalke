-- =============================================
-- 05_verify.sql: Pipeline Health Check
-- Dùng để kiểm tra sau khi chạy xong 01→04
-- và sau mỗi lần upload data
-- =============================================

USE DATABASE iot;
USE WAREHOUSE iot_xs;

-- ============ ROW COUNTS (3 LAYERS) ============
SELECT 'bronze' AS layer, COUNT(*) AS rows FROM bronze.device_telemetry
UNION ALL
SELECT 'silver',          COUNT(*)         FROM silver.device_telemetry_hourly
UNION ALL
SELECT 'gold',            COUNT(*)         FROM gold.fleet_metrics
ORDER BY layer;

-- ============ TASK STATUS ============
SELECT name, state, schedule, last_committed_on
FROM TABLE(iot.information_schema.task_history())
ORDER BY scheduled_time DESC
LIMIT 10;

-- ============ PIPE STATUS ============
SELECT SYSTEM$PIPE_STATUS('iot.bronze.iot_pipe');

-- ============ STREAM STATUS ============
SELECT 'bronze_iot_stream' AS stream_name,
       SYSTEM$STREAM_HAS_DATA('iot.silver.bronze_iot_stream') AS has_data
UNION ALL
SELECT 'silver_iot_stream',
       SYSTEM$STREAM_HAS_DATA('iot.gold.silver_iot_stream');

-- ============ LATEST DATA CHECK ============
-- Bronze: 5 records mới nhất
SELECT json_data:device_id::STRING AS device_id,
       json_data:site_id::STRING   AS site_id,
       json_data:temperature::FLOAT AS temperature,
       loaded_at
FROM bronze.device_telemetry
ORDER BY loaded_at DESC
LIMIT 5;

-- Silver: sites hiện có
SELECT site_id, COUNT(*) AS records, MAX(hour_bucket) AS latest_hour
FROM silver.device_telemetry_hourly
GROUP BY site_id
ORDER BY site_id;

-- Gold: KPIs mới nhất
SELECT analysis_hour, site_id, active_devices,
       avg_temperature, total_alerts, loaded_at
FROM gold.fleet_metrics
ORDER BY analysis_hour DESC, site_id
LIMIT 10;
