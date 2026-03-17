-- ==============================================================================
-- SNOWFLAKE HELPER QUERIES (TROUBLESHOOTING & HISTORY)
-- Description: Tập hợp các câu lệnh SQL tiện ích thường dùng để quản lý Data Ingestion, 
--              Stage, Pipe, và phục hồi dữ liệu bằng Time Travel trong Snowflake.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. QUẢN LÝ VÀ THEO DÕI STAGE (@STAGE)
-- ------------------------------------------------------------------------------
-- Liệt kê danh sách các file hiện có trong external stage (ví dụ: Azure Blob)
LIST @IOT.BRONZE.IOTDATA;

-- Đọc thử trực tiếp 10 dòng đầu tiên của file CSV/JSON nằm trong Stage mà không cần load vào bảng
SELECT $1, $2, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
FROM @IOT.BRONZE.IOTDATA
LIMIT 10;


-- ------------------------------------------------------------------------------
-- 2. COPY INTO VÀ XỬ LÝ LỖI (DATA INGESTION)
-- ------------------------------------------------------------------------------
-- Copy dữ liệu từ Stage vào bảng, tự động lấy tên file (metadata) cho vào cột
COPY INTO IOT.BRONZE.DEVICE_TELEMETRY(JSON_DATA, file_name)
FROM (
    SELECT 
        $1, 
        METADATA$FILENAME 
    FROM @IOT.BRONZE.IOTDATA
)
FILE_FORMAT = (FORMAT_NAME = 'IOT.BRONZE.IOT_JSON_FORMAT')
ON_ERROR = 'CONTINUE'; -- Bỏ qua các dòng lỗi để tiếp tục nạp các dòng đúng

-- Xem lịch sử load (COPY INTO / Snowpipe) của một bảng cụ thể trong 14 ngày qua (rất hữu ích để tìm lỗi)
SELECT 
    FILE_NAME,
    ROW_COUNT,
    STATUS,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM TABLE(information_schema.copy_history(
    table_name=>'IOT.BRONZE.DEVICE_TELEMETRY', 
    start_time=> DATEADD(days, -14, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;


-- ------------------------------------------------------------------------------
-- 3. QUẢN LÝ SNOWPIPE (AUTO-INGEST)
-- ------------------------------------------------------------------------------
-- Kiểm tra trạng thái hiện tại của một Pipe (đang chạy, lỗi, hay có bao nhiêu file pending)
SELECT SYSTEM$PIPE_STATUS('IOT.BRONZE.IOT_PIPE');

-- Ép Pipe phải "quét" lại và nạp các file cũ đang nằm trong Stage (dùng khi Pipe bị ngẽn hoặc đổi File Format)
ALTER PIPE IOT.BRONZE.IOT_PIPE REFRESH;


-- ------------------------------------------------------------------------------
-- 4. TIME TRAVEL VÀ PHỤC HỒI DỮ LIỆU (QUAY NGƯỢC THỜI GIAN)
-- ------------------------------------------------------------------------------
-- Xem dữ liệu của bảng ở trạng thái cách đây 30 phút (1800 giây)
SELECT * 
FROM IOT.BRONZE.DEVICE_TELEMETRY 
AT (OFFSET => -1800);

-- Xem dữ liệu của bảng tại một ngày/giờ chính xác trong quá khứ
SELECT * 
FROM IOT.BRONZE.DEVICE_TELEMETRY 
AT (TIMESTAMP => '2026-03-17 09:00:00.000 -0700'::timestamp_tz);

-- Xem dữ liệu của bảng NGAY TRƯỚC khi một câu lệnh cụ thể (VD: UPDATE/DELETE) được chạy
SELECT * 
FROM IOT.BRONZE.DEVICE_TELEMETRY 
BEFORE (STATEMENT => '<điền_query_id_vào_đây>');

-- Phục hồi ngay lập tức một bảng lỡ tay bị DROP (Xóa)
UNDROP TABLE IOT.BRONZE.DEVICE_TELEMETRY;

-- Tạo một bảng Backup bằng cách Clone dữ liệu từ quá khứ (cách đây 1 giờ)
CREATE TABLE IOT.BRONZE.DEVICE_TELEMETRY_BACKUP 
CLONE IOT.BRONZE.DEVICE_TELEMETRY AT (OFFSET => -3600);


-- ------------------------------------------------------------------------------
-- 5. QUẢN LÝ INTEGRATIONS (KẾT NỐI CLOUD)
-- ------------------------------------------------------------------------------
-- Xem danh sách tất cả Notification Integrations (như Azure Queue)
SHOW NOTIFICATION INTEGRATIONS;

-- Xem chi tiết một Integration để lấy thông tin Azure Consent URL hoặc App Name
DESC NOTIFICATION INTEGRATION AZURE_SNOWPIPE_NI;
