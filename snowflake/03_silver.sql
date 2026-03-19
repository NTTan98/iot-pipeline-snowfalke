-- =============================================
-- 03_silver.sql: Silver Layer
-- Includes: Table, Stream (CDC), Task (Bronze → Silver)
-- Schedule : every 5 minutes
-- Depends  : 02_bronze.sql
-- =============================================

USE DATABASE iot;
USE SCHEMA silver;
USE WAREHOUSE iot_xs;

-- ============ TABLE ============
CREATE OR REPLACE TABLE device_telemetry_hourly (
  device_id             VARCHAR(50),
  site_id               VARCHAR(50),
  hour_bucket           TIMESTAMP_NTZ,    -- DATE_TRUNC('HOUR', timestamp)
  avg_temperature       FLOAT,
  min_temperature       FLOAT,
  max_temperature       FLOAT,
  avg_humidity          FLOAT,
  min_humidity          FLOAT,
  max_humidity          FLOAT,
  avg_battery_pct       FLOAT,
  signal_rssi_avg       INT,
  uptime_hours          FLOAT,
  data_usage_mb         FLOAT,
  record_count          INT,
  has_temperature_alert BOOLEAN,          -- Alert flag (business logic)
  has_humidity_alert    BOOLEAN,
  load_ts               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============ STREAM (CDC từ Bronze) ============
CREATE OR REPLACE STREAM bronze_iot_stream
  ON TABLE bronze.device_telemetry
  APPEND_ONLY = TRUE;

-- ============ TASK (Bronze → Silver, mỗi 5 phút) ============
CREATE OR REPLACE TASK iot.silver.silver_iot_task
  WAREHOUSE = iot_xs
  SCHEDULE  = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('bronze_iot_stream')
AS
INSERT INTO silver.device_telemetry_hourly (
  device_id, site_id, hour_bucket,
  avg_temperature, min_temperature, max_temperature,
  avg_humidity, min_humidity, max_humidity,
  avg_battery_pct, signal_rssi_avg,
  uptime_hours, data_usage_mb, record_count,
  has_temperature_alert, has_humidity_alert,
  load_ts
)
WITH parsed AS (
  SELECT json_data AS data
  FROM bronze_iot_stream
)
SELECT
  data:device_id::VARCHAR                                                   AS device_id,
  data:site_id::VARCHAR                                                     AS site_id,
  DATE_TRUNC('HOUR', TRY_TO_TIMESTAMP_NTZ(data:timestamp::STRING))         AS hour_bucket,

  AVG(data:temperature::FLOAT)                                              AS avg_temperature,
  MIN(data:temperature::FLOAT)                                              AS min_temperature,
  MAX(data:temperature::FLOAT)                                              AS max_temperature,

  AVG(data:humidity::FLOAT)                                                 AS avg_humidity,
  MIN(data:humidity::FLOAT)                                                 AS min_humidity,
  MAX(data:humidity::FLOAT)                                                 AS max_humidity,

  AVG(data:battery_pct::FLOAT)                                              AS avg_battery_pct,
  AVG(data:signal_rssi::INT)                                                AS signal_rssi_avg,
  MAX(data:uptime_hours::FLOAT)                                             AS uptime_hours,
  SUM(data:data_usage_mb::FLOAT)                                            AS data_usage_mb,
  COUNT(*)                                                                  AS record_count,

  MAX(CASE
        WHEN data:temperature::FLOAT < 2 OR data:temperature::FLOAT > 8
        THEN TRUE ELSE FALSE
      END)                                                                  AS has_temperature_alert,
  MAX(CASE
        WHEN data:humidity::FLOAT < 30 OR data:humidity::FLOAT > 70
        THEN TRUE ELSE FALSE
      END)                                                                  AS has_humidity_alert,

  CURRENT_TIMESTAMP()                                                       AS load_ts
FROM parsed
GROUP BY
  data:device_id::VARCHAR,
  data:site_id::VARCHAR,
  DATE_TRUNC('HOUR', TRY_TO_TIMESTAMP_NTZ(data:timestamp::STRING));

-- ============ START TASK ============
ALTER TASK silver_iot_task RESUME;

-- ============ VERIFY ============
SHOW TABLES  IN SCHEMA silver;
SHOW STREAMS IN SCHEMA silver;
SHOW TASKS   IN SCHEMA silver;
