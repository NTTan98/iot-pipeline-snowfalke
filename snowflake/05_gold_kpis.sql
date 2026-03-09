-- =============================================
-- D1T4: SILVER → GOLD BUSINESS KPIs
-- =============================================

USE DATABASE iot;
USE SCHEMA gold;
USE WAREHOUSE iot_xs;

-- ⭐ 1 LỆNH: Hourly KPIs từ Silver
INSERT INTO gold.fleet_metrics
WITH hourly_metrics AS (
  SELECT 
    DATE_TRUNC('HOUR', timestamp)                  as analysis_hour,
    -- 2026-03-08 17:00:00 (gộp theo giờ)
    
    site_id,
    
    COUNT(DISTINCT device_id)                      as active_devices,
    -- Số device hoạt động trong giờ đó
    
    COUNT_IF(alert_type IS NOT NULL)               as total_alerts,
    -- Tổng số alert
    
    ROUND(AVG(uptime_hours) / 24 * 100, 2)         as avg_uptime_pct,
    -- Uptime % = (uptime_hours / 24h) * 100
    
    ROUND(SUM(data_usage_mb), 2)                   as total_data_usage_mb,
    -- ARR billing data usage
    
    ROUND(AVG(temperature), 2)                     as avg_temperature,
    -- Dashboard KPI
    
    ROUND(MAX(temperature), 2)                     as max_temperature,
    -- Hotspot detection
    
    COUNT_IF(alert_severity = 'HIGH')              as alert_high_count,
    -- Critical alerts
    CURRENT_TIMESTAMP()
    
  FROM silver.device_metrics
  WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL '24 HOURS'
  GROUP BY 1, 2
)
SELECT * FROM hourly_metrics;

-- ✅ Verify Gold KPIs
SELECT COUNT(*) as gold_records FROM gold.fleet_metrics;

-- Dashboard-ready KPIs
SELECT 
  site_id,
  active_devices,
  total_alerts,
  ROUND(avg_uptime_pct, 1) as uptime_pct,
  total_data_usage_mb,
  ROUND(avg_temperature, 1) as avg_temp,
  alert_high_count
FROM gold.fleet_metrics 
ORDER BY analysis_hour DESC, site_id
LIMIT 20;

-- Business insights
SELECT 
  AVG(avg_uptime_pct) as fleet_uptime_pct,
  SUM(total_alerts) as total_24h_alerts,
  SUM(total_data_usage_mb) as arr_data_usage_mb,
  MAX(max_temperature) as hottest_temp
FROM gold.fleet_metrics;
