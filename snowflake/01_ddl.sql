USE ROLE ACCOUNTADMIN;

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

-- 6. CREATE STAGE CONNECT AZURE BLOB
CREATE STAGE iotdata 
	URL = 'azure://iotsnowflake.blob.core.windows.net/iot-data' 
	CREDENTIALS = ( AZURE_SAS_TOKEN = '*****' ) 
	DIRECTORY = ( ENABLE = true );  

-- 7. CREATE Notification Integration
CREATE OR REPLACE NOTIFICATION INTEGRATION AZURE_SNOWPIPE_NI
  TYPE = QUEUE
  ENABLED = TRUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_TENANT_ID = <tenantid>
  AZURE_STORAGE_QUEUE_PRIMARY_URI = <queue URL>;

-- 8. consent get URL and get information
DESC NOTIFICATION INTEGRATION AZURE_SNOWPIPE_NI;

-- 9. GRANT QUYỀN
GRANT USAGE ON INTEGRATION AZURE_SNOWPIPE_NI TO ROLE SYSADMIN;

-- 10. Create Pipe
CREATE OR REPLACE PIPE IOT_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = 'AZURE_SNOWPIPE_NI'
  AS COPY INTO IOT.BRONZE.DEVICE_TELEMETRY (JSON_DATA, FILE_NAME)
  FROM (
      SELECT 
      $1, 
      METADATA$FILENAME 
  FROM @IOT.BRONZE.IOTDATA
  )
FILE_FORMAT = (FORMAT_NAME = 'IOT.BRONZE.IOT_JSON_FORMAT')
ON_ERROR = 'CONTINUE';

-- -- 6. SILVER LAYER (Normalized)
-- USE SCHEMA silver;
-- CREATE OR REPLACE TABLE device_metrics (
--   device_id STRING,
--   site_id STRING,
--   timestamp TIMESTAMP_NTZ,
--   temperature FLOAT,
--   humidity FLOAT,
--   battery_pct FLOAT,
--   signal_rssi INT,
--   uptime_hours FLOAT,
--   data_usage_mb FLOAT,
--   alert_type STRING,
--   alert_severity STRING,
--   loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
-- );

-- -- 7. GOLD LAYER (Hourly KPIs)
-- USE SCHEMA gold;
-- CREATE OR REPLACE TABLE fleet_metrics (
--   analysis_hour TIMESTAMP_NTZ,     -- Hour bucket
--   site_id STRING,
--   active_devices INT,
--   total_alerts INT,
--   avg_uptime_pct FLOAT,
--   total_data_usage_mb FLOAT,
--   avg_temperature FLOAT,
--   max_temperature FLOAT,
--   alert_high_count INT,
--   loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
-- );

-- -- 8. RBAC SECURITY
-- CREATE ROLE IF NOT EXISTS user_analyst;
-- GRANT USAGE ON DATABASE iot TO ROLE user_analyst;
-- GRANT USAGE ON ALL SCHEMAS IN DATABASE iot TO ROLE user_analyst;
-- GRANT SELECT ON gold.fleet_metrics TO ROLE user_analyst;
-- GRANT USAGE ON WAREHOUSE iot_xs TO ROLE user_analyst;

-- -- 9. RESOURCE MONITOR 
-- DROP RESOURCE MONITOR IF EXISTS user_monitor;
-- CREATE RESOURCE MONITOR user_monitor
--   CREDIT_QUOTA = 10
--   TRIGGERS ON 80 PERCENT DO NOTIFY
--   ON 100 PERCENT DO SUSPEND;

-- ALTER WAREHOUSE iot_xs SET RESOURCE_MONITOR = user_monitor;

-- -- 10. VERIFY EVERYTHING ✅
-- SELECT 'SUCCESS: DDL Complete!' AS status;
-- SHOW DATABASES LIKE 'iot';
-- SHOW SCHEMAS IN DATABASE iot;
-- SHOW TABLES IN SCHEMA bronze;
-- SHOW TABLES IN SCHEMA silver;
-- SHOW TABLES IN SCHEMA gold;
-- SHOW WAREHOUSES LIKE 'iot%';
-- SHOW FILE FORMATS LIKE 'iot%';