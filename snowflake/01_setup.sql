-- =============================================
-- 01_setup.sql: Warehouse + Database + Schemas
-- Run as : ACCOUNTADMIN
-- Run once before anything else
-- =============================================

USE ROLE ACCOUNTADMIN;

-- ============ WAREHOUSE ============
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  COMMENT        = 'IoT pipeline warehouse — X-Small, auto-suspend 60s';

USE WAREHOUSE COMPUTE_WH;

-- ============ DATABASE & SCHEMAS ============
CREATE OR REPLACE DATABASE iot;

USE DATABASE iot;

CREATE SCHEMA IF NOT EXISTS bronze;  -- Raw JSON (Snowpipe ingest)
CREATE SCHEMA IF NOT EXISTS silver;  -- Cleaned + typed + hourly aggregated
CREATE SCHEMA IF NOT EXISTS gold;    -- Business KPIs per site/hour

-- ============ VERIFY ============
SHOW WAREHOUSES LIKE 'COMPUTE%';
SHOW SCHEMAS IN DATABASE iot;
