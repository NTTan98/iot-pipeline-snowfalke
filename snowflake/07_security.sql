-- =============================================
-- 07_security.sql: Security & Governance
--   1. Resource Monitor  — giới hạn chi phí
--   2. RBAC              — phân quyền theo role
--   3. Dynamic Masking   — ẩn sensitive data
-- Run as : ACCOUNTADMIN
-- Chạy sau khi đã chạy 01 → 04
-- =============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE iot;
USE WAREHOUSE iot_xs;

-- ============================================================
-- 1. RESOURCE MONITOR
-- Giới hạn credit — tảnh báo 80%, tắt warehouse khi đạt 100%
-- ============================================================
CREATE OR REPLACE RESOURCE MONITOR iot_monitor
  WITH CREDIT_QUOTA = 10
  TRIGGERS
    ON 80  PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE iot_xs
  SET RESOURCE_MONITOR = iot_monitor;

-- ============================================================
-- 2. RBAC — 3 roles cho 3 tầng truy cập
-- ============================================================

-- Role 1: Engineer — full access, chạy pipeline
CREATE ROLE IF NOT EXISTS iot_engineer;
GRANT USAGE ON WAREHOUSE iot_xs                    TO ROLE iot_engineer;
GRANT USAGE ON DATABASE  iot                       TO ROLE iot_engineer;
GRANT USAGE ON ALL SCHEMAS IN DATABASE iot         TO ROLE iot_engineer;
GRANT ALL   ON ALL TABLES  IN DATABASE iot         TO ROLE iot_engineer;

-- Role 2: Analyst — chỉ đọc Gold layer
CREATE ROLE IF NOT EXISTS iot_analyst;
GRANT USAGE  ON WAREHOUSE iot_xs                   TO ROLE iot_analyst;
GRANT USAGE  ON DATABASE  iot                      TO ROLE iot_analyst;
GRANT USAGE  ON SCHEMA    iot.gold                 TO ROLE iot_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA iot.gold      TO ROLE iot_analyst;

-- Role 3: Dashboard — service account cho Streamlit
CREATE ROLE IF NOT EXISTS iot_dashboard;
GRANT USAGE  ON WAREHOUSE iot_xs                   TO ROLE iot_dashboard;
GRANT USAGE  ON DATABASE  iot                      TO ROLE iot_dashboard;
GRANT USAGE  ON SCHEMA    iot.gold                 TO ROLE iot_dashboard;
GRANT SELECT ON TABLE iot.gold.fleet_metrics       TO ROLE iot_dashboard;

-- Gọn vào SYSADMIN (role hierarchy best practice)
GRANT ROLE iot_engineer  TO ROLE SYSADMIN;
GRANT ROLE iot_analyst   TO ROLE SYSADMIN;
GRANT ROLE iot_dashboard TO ROLE SYSADMIN;

-- ============================================================
-- 3. DYNAMIC DATA MASKING
-- file_name: analyst thấy dạng ***, engineer thấy full
-- ============================================================
USE SCHEMA bronze;

CREATE OR REPLACE MASKING POLICY mask_filename AS (val STRING)
  RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('IOT_ENGINEER', 'SYSADMIN', 'ACCOUNTADMIN') THEN val
    ELSE REGEXP_REPLACE(val, '[a-zA-Z0-9]', '*')   -- VD: ***_*****_******.****
  END;

-- Áp dụng vào column file_name của Bronze table
ALTER TABLE iot.bronze.device_telemetry
  MODIFY COLUMN file_name
  SET MASKING POLICY mask_filename;

-- ============================================================
-- 4. VERIFY
-- ============================================================
SHOW RESOURCE MONITORS;
SHOW ROLES LIKE 'iot%';
SHOW MASKING POLICIES IN SCHEMA iot.bronze;

-- Test masking: đổi sang role analyst để kiểm tra
-- USE ROLE iot_analyst;
-- SELECT file_name FROM iot.bronze.device_telemetry LIMIT 5;
-- USE ROLE ACCOUNTADMIN;
