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

## 📁 Project Structure

```
├── generator/
│   └── iot-genertor.py      # Sinh 1,000 bản ghi IoT → output_json/
├── snowflake/
│   ├── 01_setup.sql         # Warehouse + Database + Schemas
│   ├── 02_bronze.sql        # Bronze table + File format + Stage + Snowpipe
│   ├── 03_silver.sql        # Silver table + Stream + Task (5 min)
│   ├── 04_gold.sql          # Gold table + Stream + Task MERGE (10 min)
│   └── 05_verify.sql        # Pipeline health check (row counts, task/pipe status)
├── dashboard/
│   └── streamlit_app.py     # Streamlit dashboard kết nối Gold layer
├── note.md                  # Lessons learned & troubleshooting
├── .env.example             # Template credentials
└── requirements.txt
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

### 3. Chạy Snowflake Scripts (theo đúng thứ tự)

| Bước | File | Nội dung |
|---|---|---|
| 1 | `snowflake/01_setup.sql` | Tạo warehouse `iot_xs`, database `iot`, 3 schemas |
| 2 | `snowflake/02_bronze.sql` | Tạo Bronze table, Stage (Azure), Snowpipe |
| 3 | `snowflake/03_silver.sql` | Tạo Silver table, Stream + Task (Bronze → Silver) |
| 4 | `snowflake/04_gold.sql` | Tạo Gold table, Stream + Task MERGE (Silver → Gold) |

> ⚠️ `02_bronze.sql` có 2 placeholder cần điền trước khi chạy: `YOUR_SAS_TOKEN` và `YOUR_TENANT_ID` / `YOUR_QUEUE_URL`.

### 4. Generate & Upload Data
```bash
# Sinh JSON data
python generator/iot-genertor.py

# PUT thủ công qua Snowflake Worksheet:
PUT file://output_json/*.json @IOT.BRONZE.IOTDATA;

# Hoặc upload lên Azure Blob → Snowpipe tự trigger
```

### 5. Verify Pipeline
```sql
-- Chạy file này sau khi upload data
snowflake/05_verify.sql
```

### 6. Chạy Dashboard
```bash
streamlit run dashboard/streamlit_app.py
```

---

## 🔑 Credential Configuration

### Snowflake (`.env`)
```env
SNOWFLAKE_ACCOUNT=your_org-your_account   # VD: abc123-xy12345
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=iot_xs
SNOWFLAKE_DATABASE=iot
SNOWFLAKE_SCHEMA=gold
```
> 💡 `SNOWFLAKE_ACCOUNT`: Snowflake UI → góc dưới trái → copy account identifier (dạng `orgname-accountname`)

### Azure SAS Token (`02_bronze.sql`)

Khi SAS Token hết hạn, **không cần tạo lại Stage** — chỉ cần chạy:
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
| file_name | STRING | Source file (tracking) |
| loaded_at | TIMESTAMP_NTZ | Ingest timestamp |

### Silver — `IOT.SILVER.DEVICE_TELEMETRY_HOURLY`
Group by `device_id + site_id + hour_bucket`. Chứa avg/min/max temperature & humidity, battery, signal RSSI, uptime, data usage, alert flags.

### Gold — `IOT.GOLD.FLEET_METRICS`
Group by `site_id + hour_bucket` (fleet-level KPIs). MERGE idempotent theo natural key.

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

Xem chi tiết trong [`note.md`](./note.md):
- `403 AuthenticationFailed` → SAS Token hết hạn, dùng `ALTER STAGE`
- `Copy executed with 0 files processed` → kiểm tra file format & stage path
- `Stage / Integration does not exist or not authorized` → kiểm tra GRANT
- Task không chạy → kiểm tra `SYSTEM$STREAM_HAS_DATA` và task state
- Cost optimization: auto-suspend 60s, chỉ dùng warehouse khi task chạy

---

## 🧑‍💻 Author

**NTTan98** — Digi IoT Pipeline Project
