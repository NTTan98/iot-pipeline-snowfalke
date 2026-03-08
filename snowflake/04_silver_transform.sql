-- =============================================
-- D1T3: BRONZE → SILVER TRANSFORM
-- =============================================

USE DATABASE iot;
USE SCHEMA silver;
USE WAREHOUSE iot_xs;

-- ⭐ 1 LỆNH FLATTEN JSON → Typed table
INSERT INTO silver.device_metrics
SELECT 
  $1:device_id::STRING           as device_id,        -- "DigiXBee-001"
  $1:site_id::STRING             as site_id,          -- "Cold_Storage"
  $1:timestamp::TIMESTAMP_NTZ    as timestamp,         -- 2026-03-08 17:00:00
  $1:temperature::FLOAT          as temperature,      -- 5.23
  $1:humidity::FLOAT             as humidity,         -- 45.6
  $1:battery_pct::FLOAT          as battery_pct,      -- 87.3
  $1:signal_rssi::INT            as signal_rssi,      -- -65
  $1:uptime_hours::FLOAT         as uptime_hours,     -- 23456.7
  $1:data_usage_mb::FLOAT        as data_usage_mb,    -- 2.34
  $1:alert_type::STRING          as alert_type,       -- "TEMP_HIGH"
  $1:alert_severity::STRING      as alert_severity,   -- "HIGH"
  CURRENT_TIMESTAMP()            as loaded_at
FROM bronze.device_telemetry;

-- ✅ Verify transformation
SELECT COUNT(*) as silver_records FROM silver.device_metrics;

-- Stats
SELECT 
  site_id,
  COUNT(*) as records,
  ROUND(AVG(temperature), 2) as avg_temp,
  COUNT_IF(alert_type IS NOT NULL) as alerts,
  ROUND(AVG(battery_pct), 1) as avg_battery
FROM silver.device_metrics 
GROUP BY 1 ORDER BY 2 DESC;

-- Preview typed data
SELECT * FROM silver.device_metrics LIMIT 10;
