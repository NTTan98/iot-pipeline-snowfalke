# ❄️ Snowflake Knowledge Mindmap

> Hệ thống kiến thức Snowflake đúc kết từ project IoT Pipeline

---

## 🗺️ Sơ đồ tổng quan

```
❄️ SNOWFLAKE
│
├── 🏗️ 1. CORE ARCHITECTURE
│   ├── Virtual Warehouse (Compute)
│   ├── Database / Schema / Table
│   ├── Micro-partition (Storage)
│   └── Cloud Layer (AWS / Azure / GCP)
│
├── 📥 2. DATA INGESTION
│   ├── Stage (Internal / External)
│   ├── File Format (CSV / JSON / Parquet)
│   ├── COPY INTO (batch manual)
│   ├── Snowpipe (auto-ingest)
│   └── Storage Integration + Notification Integration
│
├── 🔄 3. DATA PIPELINE
│   ├── Stream (CDC)
│   │   ├── Append-only
│   │   └── Standard (INSERT/UPDATE/DELETE)
│   ├── Task
│   │   ├── Schedule (CRON / interval)
│   │   ├── WHEN condition
│   │   └── Task dependency (DAG)
│   ├── Dynamic Table
│   │   ├── TARGET_LAG
│   │   ├── Refresh History
│   │   └── Statistics (+X / -Y)
│   └── MERGE INTO (upsert / idempotent)
│
├── 🏅 4. MEDALLION ARCHITECTURE
│   ├── Bronze — Raw, VARIANT, lưu nguyên JSON
│   ├── Silver — Typed, clean, aggregate theo giờ
│   └── Gold   — Business metrics, sẵn sàng cho dashboard
│
├── ⏱️ 5. TIME TRAVEL
│   ├── AT (OFFSET => -seconds)
│   ├── BEFORE (STATEMENT => 'query_id')
│   ├── UNDROP TABLE
│   └── DATA_RETENTION_TIME_IN_DAYS
│
├── 🔐 6. SECURITY & GOVERNANCE
│   ├── RBAC (Role-Based Access Control)
│   │   ├── ACCOUNTADMIN / SYSADMIN / PUBLIC
│   │   └── Custom roles + GRANT / REVOKE
│   ├── Dynamic Data Masking
│   ├── Row Access Policy
│   └── Resource Monitor (giới hạn credit)
│
├── ⚡ 7. PERFORMANCE
│   ├── Clustering Keys
│   │   ├── Partition Pruning
│   │   ├── SYSTEM$CLUSTERING_INFORMATION()
│   │   └── average_depth / average_overlap
│   ├── Warehouse Sizing (XS → XL)
│   ├── Auto-suspend / Auto-resume
│   └── Result Cache (query giống → free)
│
├── 🤖 8. AI & ML (Cortex)
│   ├── Cortex AI
│   │   ├── COMPLETE (LLM)
│   │   ├── SUMMARIZE / SENTIMENT
│   │   └── Analyst (NL2SQL)
│   └── Machine Learning
│       ├── ANOMALY_DETECTION
│       ├── FORECAST
│       └── CLASSIFICATION
│
└── 💰 9. COST OPTIMIZATION
    ├── 60-second rule (billing minimum)
    ├── AUTO_SUSPEND = 60
    ├── Dùng X-Small cho dev/test
    └── Dynamic Table vs Task (chọn đúng use case)
```

---

## 📌 Chi Tiết Từng Nhóm

### 1️⃣ Core Architecture

| Khái niệm | Mô tả |
|---|---|
| **Virtual Warehouse** | Compute engine — tính phí theo thời gian hoạt động |
| **Micro-partition** | Đơn vị lưu trữ ~16MB, Snowflake tự quản lý |
| **Separation of Storage & Compute** | Lưu trữ và xử lý độc lập — scale riêng nhau |
| **Cloud Agnostic** | Chạy trên AWS / Azure / GCP |

### 2️⃣ Data Ingestion

| Object | Mục đích |
|---|---|
| **Internal Stage** | Lưu file trong Snowflake |
| **External Stage** | Trỏ tới S3 / Azure Blob / GCS |
| **Snowpipe** | Auto-ingest khi có file mới (event-driven) |
| **Storage Integration** | Kết nối an toàn với cloud storage (không cần key) |
| **Notification Integration** | Nhận/gửi sự kiện tới cloud queue |

### 3️⃣ Data Pipeline

| Object | Khi nào dùng |
|---|---|
| **Stream** | Cần theo dõi thay đổi (CDC) |
| **Task** | Cần kiểm soát chính xác thời điểm chạy |
| **Dynamic Table** | Logic SELECT đơn giản, muốn ít object maintain |
| **MERGE INTO** | Upsert — INSERT nếu chưa có, UPDATE nếu đã tồn tại |

### 4️⃣ Medallion Architecture

```
Bronze  →  Silver  →  Gold
  ↓          ↓         ↓
Raw JSON   Cleaned   Aggregated
VARIANT    Typed     Business KPIs
Append     Hourly    Fleet Metrics
```

### 5️⃣ Time Travel

```sql
-- Query data 30 phút trước
SELECT * FROM table AT (OFFSET => -1800);

-- Khôi phục bảng bị xóa
UNDROP TABLE my_table;

-- Bật retention
ALTER TABLE t SET DATA_RETENTION_TIME_IN_DAYS = 1;
```

### 6️⃣ Security

```
ACCOUNTADMIN  →  SYSADMIN  →  iot_engineer
                           →  iot_analyst   (chỉ đọc Gold)
                           →  iot_dashboard  (service account)
```

### 7️⃣ Performance

| Kỹ thuật | Tác dụng |
|---|---|
| **Clustering Key** | Sắp xếp micro-partition → giảm scan khi filter |
| **Auto-suspend** | Tắt warehouse khi idle → tiết kiệm chi phí |
| **Result Cache** | Query giống hệt trong 24h → không tốn compute |
| **Warehouse Size** | Tăng size → nhanh hơn nhưng tốn credit hơn |

### 8️⃣ AI & ML (Cortex)

| Function | Ứng dụng |
|---|---|
| `ANOMALY_DETECTION` | Phát hiện nhiệt độ/humidity bất thường |
| `FORECAST` | Dự đoán battery sẽ cạn khi nào |
| `COMPLETE` | LLM tóm tắt tình trạng fleet |
| `Analyst` | Hỏi data bằng ngôn ngữ tự nhiên (NL2SQL) |
> ⚠️ Yêu cầu Enterprise edition — không available trên Trial

### 9️⃣ Cost Optimization

| Rule | Chi tiết |
|---|---|
| **60-second rule** | Mỗi lần resume warehouse → tính tối thiểu 60 giây |
| **Auto-suspend** | Set = 60 giây cho dev/test warehouse |
| **Warehouse size** | Dùng X-Small cho pipeline nhẹ, không cần size lớn |
| **Dynamic Table lag** | Lag dài hơn = ít refresh = ít credit |

---

## 🛣️ Hành Trình Học

```
Setup → Bronze → Silver → Gold (Stream+Task)
                              ↓
                         Nâng cấp
                              ↓
              Security → Gold (Dynamic Table)
                              ↓
          Time Travel → Clustering → AI/ML
```
