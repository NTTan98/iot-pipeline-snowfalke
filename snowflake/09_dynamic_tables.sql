-- =============================================
-- 09_dynamic_tables.sql: Dynamic Tables
-- Giải pháp thay thế Stream + Task bằng declarative pipeline
-- Snowflake tự động refresh, track dependency, retry khi fail
--
-- So sánh với 04_gold.sql (Stream + Task):
--   04_gold.sql : Stream + Task + MERGE = ~40 dòng, 3 objects
--   File này   : 1 Dynamic Table = ~15 dòng, 1 object
--
-- Chạy sau 03_silver.sql
-- Chạy as: SYSADMIN hoặc ACCOUNTADMIN
-- =============================================

USE ROLE SYSADMIN;
USE DATABASE iot;
USE SCHEMA gold;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================
-- 1. DYNAMIC TABLE — Gold layer (thay thế 04_gold.sql)
-- TARGET_LAG: data lag tối đa cho phép
--   '1 minute'   → gần real-time, tốn nhiều credit hơn
--   '10 minutes' → cân bằng giữa freshness và cost (dùng cho project này)
--   '1 hour'     → tiết kiệm credit, phù hợp báo cáo không cần real-time
-- ============================================================
CREATE OR REPLACE DYNAMIC TABLE gold.fleet_metrics_dynamic
  TARGET_LAG = '10 minutes'
  WAREHOUSE  = COMPUTE_WH
AS
SELECT
    DATE_TRUNC('hour', hour_bucket)                          AS analysis_hour,
    site_id,
    COUNT(DISTINCT device_id)                                AS active_devices,
    ROUND(AVG(avg_temperature),  2)                          AS avg_temperature,
    ROUND(MAX(max_temperature),  2)                          AS max_temperature,
    ROUND(MIN(min_temperature),  2)                          AS min_temperature,
    ROUND(AVG(avg_humidity),     2)                          AS avg_humidity,
    ROUND(AVG(avg_battery_pct),  2)                          AS avg_battery_pct,
    ROUND(AVG(avg_uptime_hours), 2)                          AS avg_uptime_hours,
    ROUND(SUM(total_data_usage_mb), 2)                       AS total_data_usage_mb,
    SUM(CASE WHEN has_temperature_alert OR has_humidity_alert THEN 1 ELSE 0 END) AS total_alerts,
    SUM(CASE WHEN has_temperature_alert THEN 1 ELSE 0 END)   AS alert_temp_count,
    SUM(CASE WHEN has_humidity_alert    THEN 1 ELSE 0 END)   AS alert_humidity_count,
    COUNT(*)                                                 AS total_records
FROM silver.device_telemetry_hourly
GROUP BY 1, 2;

-- ============================================================
-- 2. VERIFY
-- ============================================================

-- Kiểm tra trạng thái Dynamic Table
SHOW DYNAMIC TABLES IN SCHEMA gold;
-- Cột STATE: 'running' = đang hoạt động
-- Cột TARGET_LAG: lag mục tiêu
-- Cột SCHEDULING_STATE: chuội chạy tiếp theo

-- Kiểm tra dữ liệu đã được populate chưa
SELECT COUNT(*) AS row_count,
       MAX(analysis_hour) AS latest_hour
FROM gold.fleet_metrics_dynamic;

-- So sánh kết quả với bảng Gold gốc (04_gold.sql)
SELECT
    d.analysis_hour,
    d.site_id,
    d.total_alerts  AS dynamic_alerts,
    g.total_alerts  AS original_alerts,
    d.total_alerts - g.total_alerts AS diff
FROM gold.fleet_metrics_dynamic d
FULL OUTER JOIN gold.fleet_metrics g
  ON d.analysis_hour = g.analysis_hour
 AND d.site_id       = g.site_id
ORDER BY 1, 2;

-- ============================================================
-- 3. MONITOR LAG
-- Kiểm tra data có đang được refresh đúng TARGET_LAG không
-- ============================================================
SELECT
    name,
    target_lag,
    scheduling_state,
    data_timestamp,
    DATEDIFF('minute', data_timestamp, CURRENT_TIMESTAMP()) AS current_lag_minutes
FROM information_schema.dynamic_tables
WHERE name = 'FLEET_METRICS_DYNAMIC';

-- ============================================================
-- 4. SUSPEND / RESUME (tiết kiệm credit khi không dùng)
-- ============================================================
-- Tạm dừng refresh (Dynamic Table vẫn truy cập được, data không cập nhật)
ALTER DYNAMIC TABLE gold.fleet_metrics_dynamic SUSPEND;

-- Khởi động lại
ALTER DYNAMIC TABLE gold.fleet_metrics_dynamic RESUME;

-- Force refresh ngay lập tức (không chờ TARGET_LAG)
ALTER DYNAMIC TABLE gold.fleet_metrics_dynamic REFRESH;

-- ============================================================
-- 5. GHI CHÚ — DYNAMIC TABLE VS STREAM + TASK
-- ============================================================
-- Dùng Dynamic Table khi:
--   ✔ Logic là SELECT/GROUP BY đơn giản
--   ✔ Không cần kiểm soát chính xác thời điểm chạy
--   ✔ Muốn giảm số object cần maintain
--
-- Vẫn dùng Stream + Task khi:
--   ✘ Cần gửi notification/call API khi có data mới
--   ✘ MERGE logic phức tạp với nhiều điều kiện
--   ✘ Cần kiểm soát chính xác thời điểm chạy
