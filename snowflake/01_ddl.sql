-- =============================================
-- 01_ddl.sql: IOT Pipeline — Full DDL Setup
-- Run order: 01_ddl → 04_silver_transform → 05_gold_kpis
-- Run as: ACCOUNTADMIN
-- =============================================


-- ============ [0] ROLE & WAREHOUSE ============
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS iot_xs
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE;

USE WAREHOUSE iot_xs;


-- ============ [1] DATABASE & SCHEMAS ============
CREATE OR REPLACE DATABASE iot;

USE DATABASE iot;

CREATE SCHEMA IF NOT EXISTS bronze;  -- Raw JSON (Snowpipe ingest)
CREATE SCHEMA IF NOT EXISTS silver;  -- Cleaned + typed + hourly aggregated
CREATE SCHEMA IF NOT EXISTS gold;    -- Business KPIs per site/hour


-- ============ [2] BRONZE LAYER ============
USE SCHEMA bronze;

CREATE OR REPLACE TABLE device_telemetry (
  json_data  VARIANT,                                    -- Raw IoT JSON payload
  file_name  STRING,                                     -- Source file (tracking)
  loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE FILE FORMAT iot_json_format
  TYPE              = 'JSON'
  STRIP_OUTER_ARRAY = TRUE
  NULL_IF           = ('NULL', '');

-- ⚠️  Thay YOUR_SAS_TOKEN bằng SAS Token thực từ Azure Portal
-- Nếu token hết hạn, chạy lại lệnh sau (không cần tạo lại stage):
--   ALTER STAGE iotdata SET CREDENTIALS = (AZURE_SAS_TOKEN = 'new_token');
CREATE OR REPLACE STAGE iotdata
  URL         = 'azure://iotsnowflake.blob.core.windows.net/iot-data'
  CREDENTIALS = (AZURE_SAS_TOKEN = 'YOUR_SAS_TOKEN')
  DIRECTORY   = (ENABLE = TRUE);


-- ============ [3] AZURE NOTIFICATION INTEGRATION ============
-- ⚠️  Thay thế 2 giá trị sau trước khi chạy:
--   YOUR_TENANT_ID  : Azure Portal → Azure Active Directory → Overview → Tenant ID
--   YOUR_QUEUE_URL  : Storage Account → Queues → chọn queue → copy URL
CREATE OR REPLACE NOTIFICATION INTEGRATION azure_snowpipe_ni
  TYPE                            = QUEUE
  ENABLED                         = TRUE
  NOTIFICATION_PROVIDER           = AZURE_STORAGE_QUEUE
  AZURE_TENANT_ID                 = 'YOUR_TENANT_ID'
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'YOUR_QUEUE_URL';

-- Bước bắt buộc: lấy consent URL → mở browser → đăng nhập Azure → Accept
DESC NOTIFICATION INTEGRATION azure_snowpipe_ni;

GRANT USAGE ON INTEGRATION azure_snowpipe_ni TO ROLE SYSADMIN;


-- ============ [4] SNOWPIPE (Auto-Ingest) ============
CREATE OR REPLACE PIPE iot_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = 'AZURE_SNOWPIPE_NI'
AS
  COPY INTO iot.bronze.device_telemetry (json_data, file_name)
  FROM (
    SELECT $1, METADATA$FILENAME
    FROM @iot.bronze.iotdata
  )
  FILE_FORMAT = (FORMAT_NAME = 'iot.bronze.iot_json_format')
  ON_ERROR    = 'CONTINUE';


-- ============ [5] SILVER LAYER ============
USE SCHEMA silver;

CREATE OR REPLACE TABLE device_telemetry_hourly (
  device_id             VARCHAR(50),
  site_id               VARCHAR(50),
  hour_bucket           TIMESTAMP_NTZ,    -- Aggregated theo giờ
  avg_temperature       FLOAT,
  min_temperature       FLOAT,
  max_temperature       FLOAT,
  avg_humidity          FLOAT,
  min_humidity          FLOAT,
  max_humidity          FLOAT,
  avg_battery_pct       FLOAT,
  signal_rssi_avg       INT,
  uptime_hours          FLOAT,
  data_usage_mb         FLOAT,
  record_count          INT,
  has_temperature_alert BOOLEAN,          -- Alert flag (business logic)
  has_humidity_alert    BOOLEAN,
  load_ts               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- ============ [6] GOLD LAYER ============
USE SCHEMA gold;

CREATE OR REPLACE TABLE fleet_metrics (
  analysis_hour        TIMESTAMP_NTZ,    -- Hour bucket (natural key part 1)
  site_id              VARCHAR(50),      -- Site name (natural key part 2)
  active_devices       INT,              -- COUNT DISTINCT devices trong giờ
  avg_temperature      FLOAT,
  max_temperature      FLOAT,
  min_temperature      FLOAT,
  avg_humidity         FLOAT,
  avg_battery_pct      FLOAT,
  total_data_usage_mb  FLOAT,
  avg_uptime_hours     FLOAT,
  total_alerts         INT,              -- Tổng alert (temp OR humidity)
  alert_temp_count     INT,
  alert_humidity_count INT,
  total_records        INT,
  loaded_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- ============ [7] VERIFY ============
SHOW DATABASES  LIKE 'iot';
SHOW SCHEMAS    IN DATABASE iot;
SHOW TABLES     IN SCHEMA bronze;
SHOW TABLES     IN SCHEMA silver;
SHOW TABLES     IN SCHEMA gold;
SHOW WAREHOUSES LIKE 'iot%';
SHOW FILE FORMATS LIKE 'iot%';
