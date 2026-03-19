-- =============================================
-- 02_bronze.sql: Bronze Layer
-- Includes: Table, File Format, Stage, Snowpipe
-- Run as : ACCOUNTADMIN
-- Depends : 01_setup.sql
-- =============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE iot;
USE SCHEMA bronze;
USE WAREHOUSE iot_xs;

-- ============ TABLE ============
CREATE OR REPLACE TABLE device_telemetry (
  json_data  VARIANT,                                    -- Raw IoT JSON payload
  file_name  STRING,                                     -- Source file (tracking)
  loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============ FILE FORMAT ============
CREATE OR REPLACE FILE FORMAT iot_json_format
  TYPE              = 'JSON'
  STRIP_OUTER_ARRAY = TRUE
  NULL_IF           = ('NULL', '');

-- ============ STAGE (Azure Blob) ============
-- ⚠️  Thay YOUR_SAS_TOKEN bằng SAS Token thực từ Azure Portal
-- Nếu token hết hạn, KHÔNG cần tạo lại stage — chỉ cần chạy:
--   ALTER STAGE iotdata SET CREDENTIALS = (AZURE_SAS_TOKEN = 'new_token');
CREATE OR REPLACE STAGE iotdata
  URL         = 'azure://iotsnowflake.blob.core.windows.net/iot-data'
  CREDENTIALS = (AZURE_SAS_TOKEN = 'YOUR_SAS_TOKEN')
  DIRECTORY   = (ENABLE = TRUE);

-- ============ NOTIFICATION INTEGRATION ============
-- ⚠️  Thay thế trước khi chạy:
--   YOUR_TENANT_ID : Azure Portal → Azure Active Directory → Overview → Tenant ID
--   YOUR_QUEUE_URL : Storage Account → Queues → chọn queue → copy URL
CREATE OR REPLACE NOTIFICATION INTEGRATION azure_snowpipe_ni
  TYPE                            = QUEUE
  ENABLED                         = TRUE
  NOTIFICATION_PROVIDER           = AZURE_STORAGE_QUEUE
  AZURE_TENANT_ID                 = 'YOUR_TENANT_ID'
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'YOUR_QUEUE_URL';

-- Lấy consent URL → mở browser → đăng nhập Azure → Accept
DESC NOTIFICATION INTEGRATION azure_snowpipe_ni;

GRANT USAGE ON INTEGRATION azure_snowpipe_ni TO ROLE SYSADMIN;

-- ============ SNOWPIPE (Auto-Ingest) ============
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

-- ============ VERIFY ============
SHOW TABLES IN SCHEMA bronze;
SHOW STAGES IN SCHEMA bronze;
SHOW PIPES  IN SCHEMA bronze;
