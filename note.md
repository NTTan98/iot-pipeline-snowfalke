# ❄️ SNOWFLAKE BATTLE-TESTED GUIDE: LESSONS LEARNED & TROUBLESHOOTING

Tài liệu này tổng hợp các bài học thực tế, các lỗi thường gặp (đặc biệt trong quá trình setup Auto-Ingest với Azure), và các mẹo tối ưu chi phí đắt giá khi làm việc với Snowflake.

---

## 💰 1. CHI PHÍ & TỐI ƯU HÓA (COST OPTIMIZATION)

Snowflake tính phí dựa trên **Thời gian Compute (Virtual Warehouse)** hoạt động, không phải tính trên dung lượng data query.

### 🔴 Luật 60 Giây (The 60-Second Rule)
- **Vấn đề:** Khi một Warehouse khởi động lại (Resume), nó **luôn bị tính phí ít nhất 60 giây đầu tiên** dù câu query chỉ chạy mất 1 giây. Sau 1 phút đó mới tính tiền theo từng giây.
- **Hậu quả:** Nếu setup một `TASK` chạy mỗi 2 phút, và mỗi lần mất 5 giây để hoàn thành => Bạn vẫn bị tính tiền trọn 1 phút cho mỗi lần chạy.
- **Giải pháp:** Đối với các tác vụ (Task/Batch) không yêu cầu Real-time khắt khe, hãy dãn lịch schedule ra (ví dụ: 10 phút, 30 phút, hoặc 1 giờ/lần). 

### 🟢 Auto-Suspend là Cứu Tinh
- Luôn luôn set `AUTO_SUSPEND = 60` (giây) cho các Warehouse dùng để test hoặc query ad-hoc.
- Tránh để mặc định (thường là 10 phút) vì Warehouse sẽ chạy không tải và "cắn" tiền liên tục 10 phút sau khi query kết thúc.

### 🔵 Chọn đúng Size Warehouse
- Để nạp data nhẹ hoặc demo, chỉ cần dùng `X-Small` (1 credit/giờ). Đừng dùng size bự để tiết kiệm "vài giây" xử lý nhưng lại tốn gấp nhiều lần chi phí.

---

## 🛠 2. DATA INGESTION & SNOWPIPE (AZURE)

Quá trình dựng luồng **Azure Blob -> Event Grid -> Queue -> Snowpipe** rất dễ dính các lỗi sau:

### Lỗi: `Copy executed with 0 files processed`
- **Nguyên nhân:** Lệnh `COPY INTO` và `PIPE` của Snowflake có bộ nhớ cache lưu lại các file đã xử lý trong 64 ngày. Nếu bạn test bằng việc upload lại cùng 1 file (dù đã sửa dữ liệu bên trong), Snowflake sẽ bỏ qua vì nó nghĩ đã nạp rồi.
- **Giải pháp:** 
  - Thêm `FORCE = TRUE` vào lệnh `COPY INTO`.
  - Hoặc upload một file có tên hoàn toàn mới.

### Lỗi: `Stage / Integration does not exist or not authorized`
- **Nguyên nhân 1:** Do khác biệt về Role. Tạo Integration bằng `ACCOUNTADMIN` nhưng lại dùng `SYSADMIN` để tạo Pipe.
  - **Giải pháp:** `GRANT USAGE ON INTEGRATION <tên_int> TO ROLE SYSADMIN;`
- **Nguyên nhân 2:** Lỗi cú pháp String. Trong lệnh `CREATE PIPE`, tham số INTEGRATION bắt buộc phải bọc trong nháy đơn và viết HOA toàn bộ.
  - **Sai:** `INTEGRATION = 'azure_snowpipe_ni'` hoặc `INTEGRATION = "azure_snowpipe_ni"`
  - **Đúng:** `INTEGRATION = 'AZURE_SNOWPIPE_NI'`

### Lỗi: `Pipe Notifications bind failure "Internal error, could not locate queue"`
- **Nguyên nhân:** 
  - Chưa cấp quyền (IAM Role) cho App Snowflake trên Azure.
  - Link URI của Queue bị sai (dư dấu `/` ở cuối hoặc sai định dạng).
- **Giải pháp:** 
  - Lấy App Name bằng lệnh `DESC NOTIFICATION INTEGRATION...`
  - Lên Azure Portal -> Storage Account -> Access Control (IAM) -> Gán Role **Storage Queue Data Contributor** cho App Name đó.
  - Chờ 1-2 phút rồi chạy lại lệnh `CREATE OR REPLACE PIPE`.

---

## 📊 3. THIẾT KẾ DATA MODELING & LỖI KIỂU DỮ LIỆU

Khi thiết kế theo kiến trúc Medallion (Bronze -> Silver -> Gold), tầng **Bronze (Raw)** có một triết lý quan trọng: **Đừng ép kiểu quá sớm!**

### Lỗi: `Timestamp '1/15/2015 19:05' is not recognized`
- **Tình huống:** Gắn kiểu `TIMESTAMP` cho cột trong bảng Raw, nhưng file CSV nguồn chứa format thời gian kỳ lạ. Snowflake lập tức vứt bỏ (fail) toàn bộ dòng đó.
- **Best Practice:** Bảng Bronze (Raw) chỉ nên được tạo bằng toàn bộ kiểu `STRING` (VARCHAR). Hãy dùng `TRY_CAST` hoặc `TRY_TO_TIMESTAMP` ở tầng Silver để làm sạch.

### Lỗi: `Number of columns in file (19) does not match...`
- **Tình huống:** Bảng có 6 cột, nhưng file CSV nạp vào có 19 cột.
- **Giải pháp xử lý nhanh:** Thêm `ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE` vào File Format để Snowflake bỏ qua các cột dư thừa và chỉ nạp đúng 6 cột đầu.
- **Giải pháp chuẩn:** Thiết kế lại bảng với đủ 19 cột tương ứng.

### Tuyệt chiêu xử lý IoT / JSON Data
- Đừng cố gắng tạo từng cột cho từng field của file JSON.
- Tạo duy nhất 1 cột kiểu `VARIANT` để chứa nguyên cục JSON. Snowflake có khả năng tự động tối ưu kiểu này.
- Khi cần lấy dữ liệu, dùng dấu `:` để query thẳng vào cục JSON (VD: `SELECT raw_data:device_id::STRING FROM table;`). Thiết bị IoT có thay đổi schema thì hệ thống vẫn không bị sập.

---

## 📌 4. SYNTAX TRONG SNOWFLAKE (CẦN NHỚ)

### Ký tự `@` (Stage Indicator)
- **Luôn dùng `@`** khi thao tác với file vật lý nằm trong Stage (Ví dụ: `LIST @my_stage;` hoặc `COPY INTO table FROM @my_stage`).
- **Không bao giờ dùng `@`** khi tương tác với các object logic (Table, View, Pipe, Task).

### Time Travel
- Cho phép xem lại dữ liệu quá khứ bằng cách thêm `AT (OFFSET => -1800)` hoặc `BEFORE (STATEMENT => 'query_id')` vào cuối lệnh SELECT.
- **Cảnh báo:** Bản Standard mặc định chỉ lưu time travel trong **1 ngày (24h)**. Qua ngày hôm sau là bạn không thể "undrop" hay khôi phục dữ liệu bị lỡ tay xóa.
- **Lỗi `Time travel data is not available`:** Xảy ra khi bảng mới được tạo chưa đủ lâu, hoặc `DATA_RETENTION_TIME_IN_DAYS = 0`. Kiểm tra bằng `SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN TABLE <table>;`
- **So sánh 2 thời điểm:** JOIN bảng với chính nó ở 2 offset khác nhau để xem column nào thay đổi:
```sql
SELECT now.site_id, now.avg_temperature AS temp_now, old.avg_temperature AS temp_old
FROM gold.fleet_metrics AS now
JOIN gold.fleet_metrics AT (OFFSET => -1800) AS old
  ON now.site_id = old.site_id AND now.analysis_hour = old.analysis_hour
WHERE now.avg_temperature != old.avg_temperature;
```

### Lấy Tên File Gốc (Metadata)
Khi dùng `COPY INTO`, có thể bóc tách tên file và lưu vào bảng để tiện tracking:
```sql
COPY INTO IOT.BRONZE.DEVICE_TELEMETRY(JSON_DATA, FILE_NAME)
FROM (
    SELECT $1, METADATA$FILENAME 
    FROM @IOT.BRONZE.IOTDATA
);
```

---

## 🏗️ 5. KIẾN TRÚC GOLD LAYER: DYNAMIC TABLE vs STREAM + TASK

> Ghi chú từ 2026-03-19: Project này có **2 bảng Gold song song** do quá trình migrate.

### Hai bảng Gold hiện tại

| | `gold.fleet_metrics` | `gold.fleet_metrics_dynamic` |
|---|---|---|
| **Tạo bởi** | `04_gold.sql` (Stream + Task + MERGE) | `09_dynamic_tables.sql` (Dynamic Table) |
| **Cơ chế refresh** | Task chạy theo schedule, MERGE vào | Snowflake tự động theo TARGET_LAG |
| **Số objects cần maintain** | Stream + Task + Table = 3 objects | 1 Dynamic Table |
| **Time Travel** | ✅ Có | ❌ Không hỗ trợ |
| **Khuyến nghị** | Dùng khi cần MERGE phức tạp / gửi notification | Dùng cho aggregate đơn giản ✅ |

### Cách Dynamic Table hoạt động
- **Không phải append** — tính lại toàn bộ query từ Silver mỗi lần refresh
- **GROUP BY logic:** 2 hour_bucket × 5 sites = **10 groups** trong Gold
- Nếu Silver có thêm row **cùng giờ + cùng site** → Gold cập nhật giá trị (Statistics: `+1 / -1`)
- Nếu Silver có thêm row **giờ mới** → Gold thêm group mới (Statistics: `+5 / -0`)
- Nếu Silver không có data mới → Gold **không refresh**, không tốn compute

### Đọc Refresh History
```
Source Data Timestamp = Thời điểm data Silver mà Gold đã đọc tới
Refresh Duration      = Thời gian chạy query để rebuild Gold
Refresh Lag           = Độ trễ thực tế so với Silver (phải < TARGET_LAG)
Statistics +X / -Y    = X rows được thêm / Y rows bị xóa sau refresh
```

### Monitor lag thực tế
```sql
SELECT name, target_lag, scheduling_state, data_timestamp,
       DATEDIFF('minute', data_timestamp, CURRENT_TIMESTAMP()) AS current_lag_minutes
FROM information_schema.dynamic_tables
WHERE name = 'FLEET_METRICS_DYNAMIC';
```

### So sánh Silver vs Gold (kiểm tra đồng bộ)
```sql
SELECT
    COUNT(DISTINCT s.hour_bucket || s.site_id) AS silver_groups,
    COUNT(DISTINCT g.analysis_hour || g.site_id) AS gold_groups
FROM silver.device_telemetry_hourly s
FULL OUTER JOIN gold.fleet_metrics_dynamic g
  ON DATE_TRUNC('hour', s.hour_bucket) = g.analysis_hour
 AND s.site_id = g.site_id;
```

---

## 🚨 6. TROUBLESHOOTING DYNAMIC TABLE

### Lỗi: `Insufficient privileges to operate on schema 'GOLD'`
- **Nguyên nhân:** Dynamic Table là object type riêng, **không nằm trong `ALL ON TABLES`**
- **Fix:**
```sql
USE ROLE ACCOUNTADMIN;
GRANT CREATE DYNAMIC TABLE ON SCHEMA iot.gold TO ROLE SYSADMIN;
GRANT CREATE DYNAMIC TABLE ON SCHEMA iot.silver TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA iot.gold TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA iot.silver TO ROLE SYSADMIN;
```
- **Hoặc đơn giản hơn:** Chạy thẳng `CREATE DYNAMIC TABLE` bằng `ROLE ACCOUNTADMIN`

### Lỗi: `invalid identifier 'AVG_UPTIME_HOURS'`
- **Nguyên nhân:** Tên column trong Silver không khớp với query Gold
- **Kiểm tra:** `DESC TABLE iot.silver.device_telemetry_hourly;`
- **Tên đúng trong Silver:**
  - `uptime_hours` (không có prefix `avg_`)
  - `data_usage_mb` (không có prefix `total_`)
  - `record_count` (dùng `SUM(record_count)`, không phải `COUNT(*)`)

### Lỗi: `warehouse 'COMPUTE_WH' is missing`
- **Nguyên nhân:** Warehouse bị dropped hoặc SYSADMIN thiếu quyền USAGE
- **Fix TH1 - Tạo lại warehouse:**
```sql
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
```
- **Fix TH2 - Grant quyền:**
```sql
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN;
```
- **Fix TH3 - Đổi warehouse trong Dynamic Table:**
```sql
ALTER DYNAMIC TABLE gold.fleet_metrics_dynamic SET WAREHOUSE = <tên_đúng>;
```
- **Sau khi fix:** `ALTER DYNAMIC TABLE gold.fleet_metrics_dynamic REFRESH;`

### Độ trễ tích lũy qua các tầng
```
Raw IoT Device
   ↓  ~vài giây    (Stage → Bronze stream)
Bronze
   ↓  ~vài giây    (Task chạy theo schedule)
Silver
   ↓  ≤ 10 phút   (Dynamic Table TARGET_LAG)
Gold (fleet_metrics_dynamic)
```
Tổng độ trễ từ device đến Gold: **~10–11 phút**
