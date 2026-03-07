USE ROLE ACCOUNTADMIN;

-- 1. WAREHOUSE (XS + Auto-suspend tiết kiệm)
CREATE OR REPLACE WAREHOUSE iot_xs
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 300          -- 5 phút idle
  AUTO_RESUME = TRUE;


-- 2. DATABASE + MEDALLION ARCHITECTURE
CREATE OR REPLACE DATABASE iot;
CREATE SCHEMA IF NOT EXISTS bronze;    -- Raw JSON
CREATE SCHEMA IF NOT EXISTS silver;    -- Cleaned metrics
CREATE SCHEMA IF NOT EXISTS gold;      -- Business KPIs


-- 3. CONTEXT
USE DATABASE iot;
USE SCHEMA bronze;
USE WAREHOUSE iot_xs;

-- 4. BRONZE LAYER (Raw IoT JSON)
CREATE OR REPLACE TABLE device_telemetry (
  json_data VARIANT,                   -- Raw MQTT payload
  file_name STRING,
  loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 5. FILE FORMAT (JSON IoT)
CREATE OR REPLACE FILE FORMAT iot_json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE
  NULL_IF = ('NULL', '');

-- 6. SILVER LAYER (Normalized)
USE SCHEMA silver;
CREATE OR REPLACE TABLE device_metrics (
  device_id STRING,
  site_id STRING,
  timestamp TIMESTAMP_NTZ,
  temperature FLOAT,
  humidity FLOAT,
  battery_pct FLOAT,
  signal_rssi INT,
  uptime_hours FLOAT,
  data_usage_mb FLOAT,
  alert_type STRING,
  alert_severity STRING,
  loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 7. GOLD LAYER (Hourly KPIs)
USE SCHEMA gold;
CREATE OR REPLACE TABLE fleet_metrics (
  analysis_hour TIMESTAMP_NTZ,     -- Hour bucket
  site_id STRING,
  active_devices INT,
  total_alerts INT,
  avg_uptime_pct FLOAT,
  total_data_usage_mb FLOAT,
  avg_temperature FLOAT,
  max_temperature FLOAT,
  alert_high_count INT,
  loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 8. RBAC SECURITY
CREATE ROLE IF NOT EXISTS user_analyst;
GRANT USAGE ON DATABASE iot TO ROLE user_analyst;
GRANT USAGE ON ALL SCHEMAS IN DATABASE iot TO ROLE user_analyst;
GRANT SELECT ON gold.fleet_metrics TO ROLE user_analyst;
GRANT USAGE ON WAREHOUSE iot_xs TO ROLE user_analyst;

-- 9. RESOURCE MONITOR 
DROP RESOURCE MONITOR IF EXISTS user_monitor;
CREATE RESOURCE MONITOR user_monitor
  CREDIT_QUOTA = 10
  TRIGGERS ON 80 PERCENT DO NOTIFY
  ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE iot_xs SET RESOURCE_MONITOR = user_monitor;

-- 10. VERIFY EVERYTHING ✅
SELECT 'SUCCESS: DDL Complete!' AS status;
SHOW DATABASES LIKE 'iot';
SHOW SCHEMAS IN DATABASE iot;
SHOW TABLES IN SCHEMA bronze;
SHOW TABLES IN SCHEMA silver;
SHOW TABLES IN SCHEMA gold;
SHOW WAREHOUSES LIKE 'iot%';
SHOW FILE FORMATS LIKE 'iot%';