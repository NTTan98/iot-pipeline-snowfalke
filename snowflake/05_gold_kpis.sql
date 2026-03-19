USE DATABASE IOT;
USE SCHEMA GOLD;
USE WAREHOUSE COMPUTE_WH;

-- =============================================
-- 1. DDL: GOLD TABLE
-- =============================================
CREATE OR REPLACE TABLE fleet_metrics (
  analysis_hour       TIMESTAMP_NTZ,  -- Gộp theo giờ
  site_id             VARCHAR(50),
  active_devices      INT,            -- COUNT DISTINCT device trong giờ
  avg_temperature     FLOAT,
  max_temperature     FLOAT,
  min_temperature     FLOAT,
  avg_humidity        FLOAT,
  avg_battery_pct     FLOAT,
  total_data_usage_mb FLOAT,
  avg_uptime_hours    FLOAT,
  total_alerts        INT,            -- Tổng record có alert (temp OR humidity)
  alert_temp_count    INT,            -- Riêng temperature alert
  alert_humidity_count INT,           -- Riêng humidity alert
  total_records       INT,
  loaded_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================
-- 2. STREAM từ Silver
-- =============================================
CREATE OR REPLACE STREAM SILVER_IOT_STREAM
  ON TABLE SILVER.DEVICE_TELEMETRY_HOURLY
  APPEND_ONLY = TRUE;

-- =============================================
-- 3. TASK: Silver → Gold (MERGE = idempotent)
-- Schedule 10 phút (Silver task chạy 5 phút trước)
-- =============================================
CREATE OR REPLACE TASK IOT.GOLD.GOLD_IOT_TASK
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = '10 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('SILVER_IOT_STREAM')
AS
MERGE INTO GOLD.fleet_metrics AS target
USING (
  SELECT
    HOUR_BUCKET                                                             AS analysis_hour,
    SITE_ID                                                                 AS site_id,
    COUNT(DISTINCT DEVICE_ID)                                               AS active_devices,
    ROUND(AVG(AVG_TEMPERATURE), 2)                                          AS avg_temperature,
    ROUND(MAX(MAX_TEMPERATURE), 2)                                          AS max_temperature,
    ROUND(MIN(MIN_TEMPERATURE), 2)                                          AS min_temperature,
    ROUND(AVG(AVG_HUMIDITY), 2)                                             AS avg_humidity,
    ROUND(AVG(AVG_BATTERY_PCT), 2)                                          AS avg_battery_pct,
    ROUND(SUM(DATA_USAGE_MB), 2)                                            AS total_data_usage_mb,
    ROUND(AVG(UPTIME_HOURS), 1)                                             AS avg_uptime_hours,
    SUM(CASE WHEN HAS_TEMPERATURE_ALERT OR HAS_HUMIDITY_ALERT THEN 1 ELSE 0 END) AS total_alerts,
    SUM(CASE WHEN HAS_TEMPERATURE_ALERT THEN 1 ELSE 0 END)                  AS alert_temp_count,
    SUM(CASE WHEN HAS_HUMIDITY_ALERT    THEN 1 ELSE 0 END)                  AS alert_humidity_count,
    SUM(RECORD_COUNT)                                                       AS total_records
  FROM SILVER_IOT_STREAM
  GROUP BY HOUR_BUCKET, SITE_ID
) AS source
ON  target.analysis_hour = source.analysis_hour
AND target.site_id       = source.site_id

WHEN MATCHED THEN UPDATE SET
  target.active_devices       = source.active_devices,
  target.avg_temperature      = source.avg_temperature,
  target.max_temperature      = source.max_temperature,
  target.min_temperature      = source.min_temperature,
  target.avg_humidity         = source.avg_humidity,
  target.avg_battery_pct      = source.avg_battery_pct,
  target.total_data_usage_mb  = source.total_data_usage_mb,
  target.avg_uptime_hours     = source.avg_uptime_hours,
  target.total_alerts         = source.total_alerts,
  target.alert_temp_count     = source.alert_temp_count,
  target.alert_humidity_count = source.alert_humidity_count,
  target.total_records        = source.total_records,
  target.loaded_at            = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
  analysis_hour, site_id,
  active_devices,
  avg_temperature, max_temperature, min_temperature,
  avg_humidity, avg_battery_pct,
  total_data_usage_mb, avg_uptime_hours,
  total_alerts, alert_temp_count, alert_humidity_count,
  total_records, loaded_at
) VALUES (
  source.analysis_hour, source.site_id,
  source.active_devices,
  source.avg_temperature, source.max_temperature, source.min_temperature,
  source.avg_humidity, source.avg_battery_pct,
  source.total_data_usage_mb, source.avg_uptime_hours,
  source.total_alerts, source.alert_temp_count, source.alert_humidity_count,
  source.total_records, CURRENT_TIMESTAMP()
);

-- =============================================
-- 4. START TASK
-- =============================================
ALTER TASK GOLD_IOT_TASK RESUME;

-- =============================================
-- 5. VERIFY
-- =============================================
SHOW TASKS;
SELECT COUNT(*) FROM fleet_metrics;
