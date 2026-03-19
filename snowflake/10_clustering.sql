-- =============================================
-- 10_clustering.sql: Clustering Keys
-- Tối ưu query performance khi data lớn dần
-- Chạy sau 03_silver.sql và 04_gold.sql
-- Chạy as: ACCOUNTADMIN hoặc SYSADMIN
-- =============================================

USE ROLE SYSADMIN;
USE DATABASE iot;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================
-- TẠI SAO CẦN CLUSTERING KEY?
-- ============================================================
-- Snowflake lưu data theo micro-partitions (~16MB/partition)
-- Mặc định, data được sắp xếp theo thứ tự INSERT
-- => Query WHERE hour_bucket = '2026-03-19' phải scan TOÀN BỘ partitions
--
-- Sau khi đặt Clustering Key:
-- => Snowflake sắp xếp lại partitions theo key đó
-- => Query chỉ cần scan một phần nhỏ (partition pruning)
-- => Tiết kiệm compute, giảm query time đáng kể
-- ============================================================

-- ============================================================
-- 1. SILVER TABLE — device_telemetry_hourly
-- ============================================================
-- Query pattern phổ biến:
--   WHERE hour_bucket BETWEEN ... AND ...   → lọc theo giờ
--   WHERE site_id = 'Factory_A'             → lọc theo site
--   WHERE device_id = 'DigiXBee-001'        → lọc theo device
--
-- Chọn: (hour_bucket, site_id) — phù hợp nhất vì:
--   - Hầu hết query đều filter theo time range
--   - site_id giúp prune thêm khi filter theo location
--   - Cardinality vừa phải (không quá cao, không quá thấp)
-- ============================================================
USE SCHEMA silver;

ALTER TABLE silver.device_telemetry_hourly
  CLUSTER BY (DATE_TRUNC('day', hour_bucket), site_id);

-- Lưu ý: dùng DATE_TRUNC('day') thay vì hour_bucket gốc
-- vì nếu dùng TIMESTAMP đầy đủ thì cardinality quá cao,
-- Snowflake sẽ tạo quá nhiều micro-partition nhỏ

-- ============================================================
-- 2. GOLD TABLE — fleet_metrics (Stream + Task)
-- ============================================================
-- Query pattern:
--   WHERE analysis_hour BETWEEN ... AND ...
--   WHERE site_id = ...
--   GROUP BY analysis_hour, site_id
-- ============================================================
USE SCHEMA gold;

ALTER TABLE gold.fleet_metrics
  CLUSTER BY (analysis_hour, site_id);

-- ============================================================
-- LƯU Ý VỀ DYNAMIC TABLE
-- ============================================================
-- Dynamic Table (fleet_metrics_dynamic) KHÔNG hỗ trợ
-- ALTER TABLE ... CLUSTER BY trực tiếp.
-- Thay vào đó, định nghĩa clustering ngay trong CREATE:
--
-- CREATE OR REPLACE DYNAMIC TABLE gold.fleet_metrics_dynamic
--   TARGET_LAG = '10 minutes'
--   WAREHOUSE  = COMPUTE_WH
--   CLUSTER BY (analysis_hour, site_id)   ← thêm dòng này
-- AS
-- SELECT ...
-- ============================================================

-- ============================================================
-- 3. VERIFY — Kiểm tra clustering info
-- ============================================================

-- Xem clustering key đã được set chưa
SHOW TABLES IN SCHEMA silver;
SHOW TABLES IN SCHEMA gold;

-- Xem chi tiết clustering health của Silver
SELECT SYSTEM$CLUSTERING_INFORMATION(
  'silver.device_telemetry_hourly',
  '(DATE_TRUNC(''day'', hour_bucket), site_id)'
);

-- Xem chi tiết clustering health của Gold
SELECT SYSTEM$CLUSTERING_INFORMATION(
  'gold.fleet_metrics',
  '(analysis_hour, site_id)'
);

-- ============================================================
-- 4. ĐỌC KẾT QUẢ CLUSTERING_INFORMATION
-- ============================================================
-- average_depth:  càng gần 1 càng tốt (partition ít bị overlap)
-- average_overlap: càng thấp càng tốt
--
-- Nếu average_depth > 3 với data lớn → cân nhắc RECLUSTER thủ công:
--   ALTER TABLE silver.device_telemetry_hourly RECLUSTER;
--
-- Snowflake tự động recluster theo background service
-- nhưng với data nhỏ như hiện tại thì không cần lo

-- ============================================================
-- 5. KHI NÀO NÊN DÙNG CLUSTERING KEY?
-- ============================================================
-- ✅ Bảng > 1TB hoặc > 100 triệu rows
-- ✅ Query thường xuyên filter theo cùng 1-2 column
-- ✅ Query time hiện tại chậm do full table scan
--
-- ❌ Bảng nhỏ (như hiện tại) → overhead của clustering > lợi ích
-- ❌ Query pattern đa dạng, không có filter cố định
-- ❌ Bảng INSERT-heavy liên tục → clustering bị phân mảnh nhanh
