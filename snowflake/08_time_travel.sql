-- =============================================
-- 08_time_travel.sql: Time Travel + Zero-Copy Cloning
-- Ứng dụng thực tế:
--   1. Audit & debug pipeline
--   2. Báo cáo tại thời điểm cụ thể
--   3. So sánh data trước/sau transform
--   4. Restore data bị xóa/sửa nhầm
-- Chạy từng phần theo nhu cầu — không chạy toàn bộ
-- =============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE iot;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================
-- 1. AUDIT & DEBUG PIPELINE
-- Hỏi: "Tại sao số liệu báo cáo sáng nay khác chiều qua?"
-- ============================================================

-- Đếm rows tại các thời điểm khác nhau
SELECT 'now'        AS point_in_time, COUNT(*) AS rows FROM gold.fleet_metrics
UNION ALL
SELECT '1h ago',    COUNT(*) FROM gold.fleet_metrics AT (OFFSET => -3600)
UNION ALL
SELECT '6h ago',   COUNT(*) FROM gold.fleet_metrics AT (OFFSET => -21600)
UNION ALL
SELECT '24h ago',  COUNT(*) FROM gold.fleet_metrics AT (OFFSET => -86400);

-- Query data tại timestamp cụ thể
SELECT *
FROM gold.fleet_metrics
AT (TIMESTAMP => '2026-03-17 14:00:00'::TIMESTAMP_NTZ)
ORDER BY analysis_hour DESC
LIMIT 10;

-- ============================================================
-- 2. BÁO CÁO TẠI THỜI ĐIỂM CỤ THỂ
-- Hỏi: "Cho tôi số liệu của ngày 17/03 — không phải hôm nay"
-- ============================================================
SELECT
    site_id,
    SUM(active_devices) AS total_devices,
    SUM(total_alerts)   AS total_alerts,
    ROUND(AVG(avg_temperature), 2) AS avg_temp
FROM gold.fleet_metrics
AT (TIMESTAMP => '2026-03-17 23:59:59'::TIMESTAMP_NTZ)
GROUP BY site_id
ORDER BY total_alerts DESC;

-- ============================================================
-- 3. SO SÁNH DATA TRƯỚC/SAU TRANSFORM
-- Hỏi: "Transform mới có ảnh hưởng gì đến số liệu không?"
-- ============================================================
SELECT
    current_data.site_id,
    current_data.total_alerts  AS alerts_now,
    before_data.total_alerts   AS alerts_30min_ago,
    (current_data.total_alerts - before_data.total_alerts) AS delta
FROM gold.fleet_metrics AS current_data
JOIN gold.fleet_metrics
     BEFORE (OFFSET => -1800) AS before_data   -- 30 phút trước
  ON current_data.analysis_hour = before_data.analysis_hour
 AND current_data.site_id       = before_data.site_id
ORDER BY delta DESC;

-- ============================================================
-- 4. RESTORE DATA BỊ XÓA NHẠM
-- ============================================================

-- Restore toàn bộ table bị drop
-- DROP TABLE gold.fleet_metrics;   -- giả sử xóa nhầm
UNDROP TABLE gold.fleet_metrics;    -- khôi phục ngay lập tức

-- Restore 1 phần data bị sửa nhầm (cho Cold_Storage)
-- INSERT INTO gold.fleet_metrics
--   SELECT * FROM gold.fleet_metrics AT (OFFSET => -3600)
--   WHERE site_id = 'Cold_Storage';

-- ============================================================
-- 5. ZERO-COPY CLONING
-- Tạo môi trường dev/test tức thì — không tốn thêm storage
-- ============================================================

-- Clone toàn bộ DB để test (tạo ngay, không copy data vật lý)
CREATE OR REPLACE DATABASE iot_dev CLONE iot;

-- Clone 1 table để backup trước khi chạy script nguy hiểm
CREATE OR REPLACE TABLE gold.fleet_metrics_backup
  CLONE gold.fleet_metrics;

-- Verify: clone xong nhưng không tốn thêm storage cho đến khi có thay đổi
SELECT TABLE_NAME,
       ACTIVE_BYTES / 1024 / 1024 AS size_mb
FROM information_schema.table_storage_metrics
WHERE TABLE_SCHEMA = 'GOLD'
ORDER BY ACTIVE_BYTES DESC;

-- Xóa DB dev khi không cần nữa (tiết kiệm credit)
-- DROP DATABASE iot_dev;

-- ============================================================
-- 6. VERIFY TIME TRAVEL SETTINGS
-- ============================================================
SHOW TABLES IN SCHEMA gold;
-- Cột DATA_RETENTION_TIME_IN_DAYS cho biết còn giữ được bao lâu

-- Tăng retention cho Gold table (mặc định 1 ngày, max 90 ngày — Enterprise)
ALTER TABLE gold.fleet_metrics
  SET DATA_RETENTION_TIME_IN_DAYS = 1;  -- trial max = 1
