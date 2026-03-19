-- =============================================
-- 06_backfill.sql: Manual Backfill
-- Dùng khi: cần load lại toàn bộ data cũ từ Silver vào Gold
-- (Stream chỉ capture data mới sau khi được tạo)
-- Chạy 1 lần duy nhất sau khi reset hoặc khi Gold bị trống
-- =============================================

USE DATABASE iot;
USE SCHEMA gold;
USE WAREHOUSE iot_xs;

-- Suspend task trước khi backfill (tránh conflict)
ALTER TASK gold_iot_task SUSPEND;

-- MERGE toàn bộ Silver → Gold (query trực tiếp từ table, không qua stream)
MERGE INTO gold.fleet_metrics AS target
USING (
  SELECT
    hour_bucket                                                                   AS analysis_hour,
    site_id,
    COUNT(DISTINCT device_id)                                                     AS active_devices,
    ROUND(AVG(avg_temperature), 2)                                                AS avg_temperature,
    ROUND(MAX(max_temperature), 2)                                                AS max_temperature,
    ROUND(MIN(min_temperature), 2)                                                AS min_temperature,
    ROUND(AVG(avg_humidity), 2)                                                   AS avg_humidity,
    ROUND(AVG(avg_battery_pct), 2)                                                AS avg_battery_pct,
    ROUND(SUM(data_usage_mb), 2)                                                  AS total_data_usage_mb,
    ROUND(AVG(uptime_hours), 1)                                                   AS avg_uptime_hours,
    SUM(CASE WHEN has_temperature_alert OR has_humidity_alert THEN 1 ELSE 0 END)  AS total_alerts,
    SUM(CASE WHEN has_temperature_alert THEN 1 ELSE 0 END)                        AS alert_temp_count,
    SUM(CASE WHEN has_humidity_alert    THEN 1 ELSE 0 END)                        AS alert_humidity_count,
    SUM(record_count)                                                             AS total_records
  FROM silver.device_telemetry_hourly   -- ← trực tiếp từ table, không qua stream
  GROUP BY hour_bucket, site_id
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

-- Resume task sau khi backfill xong
ALTER TASK gold_iot_task RESUME;

-- Verify
SELECT COUNT(*) AS gold_records FROM gold.fleet_metrics;
SELECT analysis_hour, site_id, active_devices, total_alerts
FROM gold.fleet_metrics
ORDER BY analysis_hour DESC, site_id
LIMIT 10;
