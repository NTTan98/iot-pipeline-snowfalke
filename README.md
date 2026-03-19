# ❄️ Digi IoT Analytics Pipeline — Snowflake

End-to-end real-time IoT data pipeline using **Snowflake + Azure Blob Storage**, built on Medallion Architecture (Bronze → Silver → Gold).

---

## 🏗️ Architecture

```
[Python Generator]
        │ generates 1,000 IoT records (JSON)
        ▼
[Azure Blob Storage]
        │ upload → Event Grid → Storage Queue
        ▼
[Snowpipe Auto-Ingest]
        │ event-driven, continuous loading
        ▼
 BRONZE.DEVICE_TELEMETRY       ← Raw VARIANT JSON
        │ Stream (APPEND_ONLY) + Task (every 5 min)
        ▼
 SILVER.DEVICE_TELEMETRY_HOURLY ← Typed, aggregated per device/site/hour
        │ Stream (APPEND_ONLY) + Task (every 10 min)
        ▼
 GOLD.FLEET_METRICS             ← Business KPIs per site/hour (MERGE)
        │
        ▼
[Streamlit Dashboard]           ← Live monitoring: KPI cards + charts
```

---

## 📁 Project Structure

```
├── generator/
│   └── iot-genertor.py        # Sinh 1,000 IoT records → output_json/
├── snowflake/
│   ├── 01_ddl.sql             # Setup DB, schemas, Bronze/Silver/Gold DDL, Pipe
│   ├── 04_silver_transform.sql # Stream + Task: Bronze → Silver
│   └── 05_gold_kpis.sql       # Stream + Task (MERGE): Silver → Gold
├── dashboard/
│   └── streamlit_app.py       # Streamlit dashboard kết nối Gold layer
├── note.md                    # Battle-tested lessons & troubleshooting
├── .env.example               # Template credentials
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
```

### 3. Configure Credentials
```bash
cp .env.example .env
# Chỉnh sửa .env với thông tin thực của bạn (xem hướng dẫn bên dưới)
```

### 4. Chạy Snowflake Scripts (theo thứ tự)
```sql
-- Bước 1: Setup toàn bộ infrastructure
-- Chạy file: snowflake/01_ddl.sql

-- Bước 2: Tạo Silver Stream + Task
-- Chạy file: snowflake/04_silver_transform.sql

-- Bước 3: Tạo Gold Stream + Task (MERGE)
-- Chạy file: snowflake/05_gold_kpis.sql
```

### 5. Generate & Upload Data
```bash
# Sinh JSON data
python generator/iot-genertor.py

# Upload lên Azure (Snowpipe tự trigger)
# Hoặc PUT thủ công qua Snowflake Worksheet:
# PUT file://output_json/*.json @IOT.BRONZE.IOTDATA;
```

### 6. Chạy Dashboard
```bash
streamlit run dashboard/streamlit_app.py
```

---

## 🔑 Credential Configuration

### Snowflake Credentials (`.env`)
```env
SNOWFLAKE_ACCOUNT=your_org-your_account   # VD: abc123-xy12345
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=iot
SNOWFLAKE_SCHEMA=gold
```
> 💡 Tìm `SNOWFLAKE_ACCOUNT`: Snowflake UI → góc dưới trái → copy account identifier (dạng `orgname-accountname`)

### Azure SAS Token (trong `01_ddl.sql`)
Khi SAS Token hết hạn, **không cần tạo lại Stage** — chỉ cần update:
```sql
ALTER STAGE IOT.BRONZE.IOTDATA
  SET CREDENTIALS = (
    AZURE_SAS_TOKEN = 'sv=2024-xx-xx&ss=b&srt=co&sp=rwdlacuptfx&...'
  );

-- Verify kết nối
LIST @IOT.BRONZE.IOTDATA;
```

**Cách lấy SAS Token mới từ Azure Portal:**
1. Azure Portal → Storage Account → **Shared access signature**
2. Permissions: ✅ Read, ✅ Write, ✅ List, ✅ Add, ✅ Create
3. Resource types: ✅ Container, ✅ Object
4. Set Expiry date xa (khuyến nghị: 1 năm)
5. Click **Generate SAS and connection string**
6. Copy phần **SAS token** (bắt đầu bằng `sv=...`)

### Azure Notification Integration (trong `01_ddl.sql`)
Thay `<tenantid>` và `<queue URL>` bằng thông tin thực:
```sql
CREATE OR REPLACE NOTIFICATION INTEGRATION AZURE_SNOWPIPE_NI
  TYPE = QUEUE
  ENABLED = TRUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_TENANT_ID = 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://<storage>.queue.core.windows.net/<queue-name>';
```
Sau đó lấy consent URL:
```sql
DESC NOTIFICATION INTEGRATION AZURE_SNOWPIPE_NI;
-- Copy AZURE_CONSENT_URL → mở trên browser → đăng nhập Azure → Accept
```

---

## 📊 Data Model

### Bronze — `IOT.BRONZE.DEVICE_TELEMETRY`
| Column | Type | Description |
|---|---|---|
| JSON_DATA | VARIANT | Raw IoT JSON payload |
| FILE_NAME | STRING | Source file (tracking) |
| LOADED_AT | TIMESTAMP_NTZ | Ingest timestamp |

### Silver — `IOT.SILVER.DEVICE_TELEMETRY_HOURLY`
Aggregated per `DEVICE_ID + SITE_ID + HOUR_BUCKET`. Columns: avg/min/max temperature & humidity, battery, signal RSSI, uptime, data usage, alert flags.

### Gold — `IOT.GOLD.FLEET_METRICS`
Aggregated per `SITE_ID + HOUR_BUCKET` (fleet-level KPIs). MERGE idempotent — safe to rerun.

| Column | Description |
|---|---|
| analysis_hour | Hour bucket |
| site_id | Site name |
| active_devices | COUNT DISTINCT devices active in hour |
| avg/max/min_temperature | Temperature KPIs |
| total_alerts | Total alert records (temp OR humidity) |
| alert_temp_count | Temperature alerts only |
| alert_humidity_count | Humidity alerts only |

---

## 🛠️ Troubleshooting

Xem chi tiết trong [`note.md`](./note.md) — bao gồm:
- Lỗi `403 AuthenticationFailed` (SAS Token hết hạn)
- Lỗi `Copy executed with 0 files processed`
- Lỗi `Stage / Integration does not exist or not authorized`
- Cost optimization (60-second rule, auto-suspend)
- Medallion Architecture best practices

---

## 🧑‍💻 Author

**NTTan98** — Digi IoT Pipeline Project
