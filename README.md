# ❄️ Digi IoT Analytics Pipeline — Snowflake

End-to-end real-time IoT data pipeline using **Snowflake + Azure Blob Storage**, built on the Medallion Architecture (Bronze → Silver → Gold).

---

## 🏗️ Architecture

```
[Python Generator]
        │ 1,000 IoT records → JSON files
        ▼
[Azure Blob Storage]
        │ upload → Event Grid → Storage Queue
        ▼
[Snowpipe — Auto-Ingest]
        │ event-driven, continuous loading
        ▼
 BRONZE.DEVICE_TELEMETRY          ← Raw VARIANT JSON
        │ Stream (APPEND_ONLY)
        │ Task: every 5 min
        ▼
 SILVER.DEVICE_TELEMETRY_HOURLY   ← Typed + aggregated per device/site/hour
        │ Stream (APPEND_ONLY)
        │ Task: every 10 min (MERGE)
        ▼
 GOLD.FLEET_METRICS               ← Business KPIs per site/hour
        │
        ▼
[Streamlit Dashboard]             ← Live KPI cards + charts
```

---

## 🔐 Security & Access Control

```
ACCOUNTADMIN
    │
    ├── SYSADMIN
    │       ├── iot_engineer   → Full access (Bronze + Silver + Gold)
    │       ├── iot_analyst    → Read-only  (Gold only)
    │       └── iot_dashboard  → Read-only  (fleet_metrics only)
    │
    └── SECURITYADMIN
            └── Quản lý user, role, masking policy
```

| Role | Bronze | Silver | Gold | Warehouse |
|---|---|---|---|---|
| `iot_engineer` | ✅ Full | ✅ Full | ✅ Full | ✅ COMPUTE_WH |
| `iot_analyst` | ❌ | ❌ | ✅ SELECT | ✅ COMPUTE_WH |
| `iot_dashboard` | ❌ | ❌ | ✅ fleet_metrics only | ✅ COMPUTE_WH |

**Resource Monitor:** `iot_monitor` — notify 80%, suspend warehouse tự động khi đạt 100% credit (giới hạn 10 credits/tháng)

**Data Masking:** Column `file_name` trong Bronze — `iot_engineer` thấy full, các role khác thấy `***`

---

## ⏳ Time Travel & Zero-Copy Cloning

### Time Travel

Snowflake tự động lưu lịch sử thay đổi data — không cần backup thủ công, có thể query hoặc restore tại bất kỳ thời điểm nào trong khoảng retention (trial = 1 ngày, Enterprise = 90 ngày).

**4 use case thực tế — tần suất dùng từ cao đến thấp:**

| Use Case | Mô tả | Tần suất |
|---|---|---|
| 🔍 **Audit & debug** | "Tại sao số liệu hôm nay khác hôm qua?" — so sánh row count theo giờ | Hàng ngày |
| ↔️ **So sánh trước/sau transform** | Kiểm tra transform mới có ảnh hưởng data không trước khi deploy | Mỗi deploy |
| 📅 **Báo cáo tại thời điểm cụ thể** | Lấy số liệu cuối tháng đúng ngày — không bị ảnh hưởng bởi data mới | Hàng tháng |
| 🛑 **Restore xóa nhầm** | `UNDROP TABLE` — khôi phục tức thì, không cần DBA can thiệp | Khẩn cấp |

### Zero-Copy Cloning

Giải quyết vấn đề cốt lõi: có bản copy data để test/backup **mà không tốn thêm storage và thời gian chờ**. Snowflake chỉ copy metadata (con trỏ), data vật lý vẫn share chung cho đến khi có thay đổi.

```
Lúc clone:                  Sau khi thay đổi iot_staging:

iot          ──┬            iot          ── shared blocks (giữ nguyên)
               ├── shared
iot_staging  ──┘            iot_staging  ── new blocks (chỉ phần thay đổi)
                                          + shared blocks
```

**3 mục đích chính:**

| Mục đích | Ví dụ |
|---|---|
| Môi trường dev/staging từ data thật | `CREATE DATABASE iot_staging CLONE iot` — dev team test ngay không ảnh hưởng production |
| Backup trước script nguy hiểm | `CREATE TABLE fleet_metrics_backup CLONE fleet_metrics` — restore ngay nếu sai |
| Test transform mới an toàn | Clone schema → chạy thử → nếu đúng mới apply lên production |

| | Traditional Copy | Zero-Copy Clone |
|---|---|---|
| Thời gian (TB data) | Hàng giờ | Vài giây |
| Storage | x2 | Chỉ tính phần thay đổi |
| Sync lại | Phải copy lại từ đầu | Clone lại bất cứ lúc nào |

Xem các query mẫu trong [`snowflake/08_time_travel.sql`](./snowflake/08_time_travel.sql)

---

## 📁 Project Structure

```
├── generator/
│   └── iot_generator.py              # Sinh 1,000 bản ghi IoT → output_json/
├── snowflake/
│   ├── 01_setup.sql                  # Warehouse COMPUTE_WH + Database + Schemas
│   ├── 02_bronze.sql                 # Bronze table + File format + Stage + Snowpipe
│   ├── 03_silver.sql                 # Silver table + Stream + Task (5 min)
│   ├── 04_gold.sql                   # Gold table + Stream + Task MERGE (10 min)
│   ├── 05_verify.sql                 # Pipeline health check
│   ├── 06_backfill.sql               # Manual backfill Silver → Gold (chạy 1 lần)
│   ├── 07_security.sql               # RBAC + Resource Monitor + Data Masking
│   └── 08_time_travel.sql            # Time Travel (audit, restore) + Zero-Copy Cloning
├── Azure/
│   └── snowflake-eventgrid-setup.ps1  # PowerShell: tạo Event Grid + Queue tự động
├── dashboard/
│   └── streamlit_app.py              # Streamlit dashboard kết nối Gold layer
├── help/
│   └── snowflake_helper_queries.md   # Queries tiện ích: debug, reset, monitor
├── note.md                           # Lessons learned & troubleshooting
├── .env.example                      # Template credentials
└── requirements.txt                  # Python dependencies (pinned)
```

---

## ⚙️ Setup Guide

### 1. Prerequisites
- Snowflake account (trial OK)
- Azure Storage Account + Blob Container
- Python 3.9+

### 2. Clone & Install
```bash
git clone https://github.com/NTTan98/iot-pipeline-snowfalke.git
cd iot-pipeline-snowfalke
pip install -r requirements.txt
cp .env.example .env   # điền credentials vào .env
```

### 3. Setup Azure Infrastructure
```powershell
# Tự động tạo Event Grid + Storage Queue:
.\Azure\snowflake-eventgrid-setup.ps1
```

### 4. Chạy Snowflake Scripts (theo đúng thứ tự)

| Bước | File | Nội dung |
|---|---|---|
| 1 | `snowflake/01_setup.sql` | Tạo warehouse `COMPUTE_WH`, database `iot`, 3 schemas |
| 2 | `snowflake/02_bronze.sql` | Tạo Bronze table, Stage (Azure), Snowpipe |
| 3 | `snowflake/03_silver.sql` | Tạo Silver table, Stream + Task (Bronze → Silver) |
| 4 | `snowflake/04_gold.sql` | Tạo Gold table, Stream + Task MERGE (Silver → Gold) |
| 5 | `snowflake/07_security.sql` | RBAC + Resource Monitor + Data Masking |

> ⚠️ `02_bronze.sql` có placeholder cần điền trước khi chạy: `YOUR_SAS_TOKEN`, `YOUR_TENANT_ID`, `YOUR_QUEUE_URL`.

### 5. Generate & Upload Data
```bash
python generator/iot_generator.py

# PUT thủ công qua Snowflake Worksheet:
PUT file://output_json/*.json @IOT.BRONZE.IOTDATA;
```

### 6. Verify Pipeline
```sql
snowflake/05_verify.sql
```

### 7. Chạy Dashboard
```bash
streamlit run dashboard/streamlit_app.py
```

---

## 🔄 Backfill (Nếu Cần)

Stream chỉ capture data **sau khi được tạo**. Nếu Gold bị trống sau khi reset, chạy:
```sql
snowflake/06_backfill.sql
```
Mọi thứ đều dùng MERGE nên an toàn — không duplicate dù chạy nhiều lần.

---

## 🔑 Credential Configuration

### Snowflake (`.env`)
```env
SNOWFLAKE_ACCOUNT=your_org-your_account   # VD: abc123-xy12345
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=iot
SNOWFLAKE_SCHEMA=gold
```
> 💡 `SNOWFLAKE_ACCOUNT`: Snowflake UI → góc dưới trái → copy account identifier (dạng `orgname-accountname`)

### Azure SAS Token (`02_bronze.sql`)
Khi SAS Token hết hạn, **không cần tạo lại Stage**:
```sql
ALTER STAGE IOT.BRONZE.IOTDATA
  SET CREDENTIALS = (AZURE_SAS_TOKEN = 'sv=...');

LIST @IOT.BRONZE.IOTDATA;  -- verify kết nối
```

**Lấy SAS Token mới:**
1. Azure Portal → Storage Account → **Shared access signature**
2. Permissions: ✅ Read ✅ Write ✅ List ✅ Add ✅ Create
3. Resource types: ✅ Container ✅ Object
4. Expiry: đặt xa (khuyến nghị 1 năm)
5. Click **Generate SAS and connection string** → copy **SAS token**

### Azure Notification Integration (`02_bronze.sql`)
```sql
-- YOUR_TENANT_ID : Azure Portal → Azure Active Directory → Overview → Tenant ID
-- YOUR_QUEUE_URL : Storage Account → Queues → chọn queue → copy URL
```
Sau khi tạo integration, lấy consent URL:
```sql
DESC NOTIFICATION INTEGRATION azure_snowpipe_ni;
-- Copy AZURE_CONSENT_URL → mở browser → đăng nhập Azure → Accept
```

---

## 📊 Data Model

### Bronze — `IOT.BRONZE.DEVICE_TELEMETRY`
| Column | Type | Description |
|---|---|---|
| json_data | VARIANT | Raw IoT JSON payload |
| file_name | STRING | Source file (tracking) — masked for non-engineers |
| loaded_at | TIMESTAMP_NTZ | Ingest timestamp |

### Silver — `IOT.SILVER.DEVICE_TELEMETRY_HOURLY`
Group by `device_id + site_id + hour_bucket`. Chứa avg/min/max temperature & humidity, battery, signal RSSI, uptime, data usage, alert flags.

### Gold — `IOT.GOLD.FLEET_METRICS`
Group by `site_id + hour_bucket`. MERGE idempotent theo natural key `analysis_hour + site_id`.

| Column | Description |
|---|---|
| analysis_hour | Hour bucket |
| site_id | Tên site |
| active_devices | Số device hoạt động trong giờ |
| avg / max / min_temperature | Temperature KPIs |
| avg_humidity | Humidity KPI |
| avg_battery_pct | Battery trung bình |
| total_alerts | Tổng alert (temp OR humidity) |
| alert_temp_count | Alert nhiệt độ |
| alert_humidity_count | Alert độ ẩm |
| total_records | Tổng bản ghi Silver được gộp |

---

## 🛠️ Troubleshooting

Xem chi tiết trong [`note.md`](./note.md) và [`help/snowflake_helper_queries.md`](./help/snowflake_helper_queries.md):
- `403 AuthenticationFailed` → SAS Token hết hạn, dùng `ALTER STAGE`
- `Copy executed with 0 files processed` → kiểm tra file format & stage path
- `Stage / Integration does not exist or not authorized` → kiểm tra GRANT
- Task không chạy → kiểm tra `SYSTEM$STREAM_HAS_DATA` và task state
- Cost optimization: auto-suspend 60s, Resource Monitor giới hạn 10 credits/tháng

---

## 🧑‍💻 Author

**NTTan98** — Digi IoT Pipeline Project
