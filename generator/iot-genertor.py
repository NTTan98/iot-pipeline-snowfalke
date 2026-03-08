import snowflake.connector
import json
import random
from datetime import datetime, timezone
import time
from dotenv import load_dotenv  # NEW
import os
load_dotenv()  

# =============================================
# CONFIG - Thay thông tin Snowflake của bạn
# =============================================
SNOWFLAKE_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "iot_xs"),
    "database":  os.getenv("SNOWFLAKE_DATABASE", "iot"),
    "schema":    os.getenv("SNOWFLAKE_SCHEMA", "bronze"),
}

# =============================================
# IOT DEVICES CONFIG (Digi Remote Manager)
# =============================================
DEVICES = [f"DigiXBee-{i:03d}" for i in range(1, 21)]  # 20 devices
SITES = {
    "Factory_A":    {"temp_range": (18, 35), "alert_prob": 0.15},
    "Factory_B":    {"temp_range": (20, 38), "alert_prob": 0.10},
    "Railway_1":    {"temp_range": (15, 45), "alert_prob": 0.20},
    "Cold_Storage": {"temp_range": (2, 8),   "alert_prob": 0.25},  # Cold chain!
    "Data_Center":  {"temp_range": (18, 28), "alert_prob": 0.05},
}

ALERT_TYPES = ["TEMP_HIGH", "SIGNAL_WEAK", "BATTERY_LOW", "UPTIME_CRITICAL"]

# =============================================
# GENERATE 1 TELEMETRY RECORD
# =============================================
def generate_record(device_id: str, site_id: str) -> dict:
    site_cfg = SITES[site_id]
    temp_min, temp_max = site_cfg["temp_range"]
    has_alert = random.random() < site_cfg["alert_prob"]

    temperature = round(random.uniform(temp_min, temp_max), 2)
    alert_type = random.choice(ALERT_TYPES) if has_alert else None
    alert_severity = random.choice(["HIGH", "LOW"]) if has_alert else None

    # Auto-generate TEMP_HIGH alert if cold chain breach
    if site_id == "Cold_Storage" and temperature > 8:
        alert_type = "TEMP_HIGH"
        alert_severity = "HIGH"

    return {
        "device_id":       device_id,
        "site_id":         site_id,
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "temperature":     temperature,
        "humidity":        round(random.uniform(30, 70), 1),
        "battery_pct":     round(random.uniform(20, 100), 1),
        "signal_rssi":     random.randint(-90, -40),
        "uptime_hours":    round(random.uniform(1000, 50000), 1),
        "data_usage_mb":   round(random.uniform(0.1, 5.0), 2),
        "alert_type":      alert_type,
        "alert_severity":  alert_severity
    }

# =============================================
# LOAD VÀO BRONZE LAYER
# =============================================
def insert_bronze(conn, records: list):
    cursor = conn.cursor()
    insert_sql = """
        INSERT INTO iot.bronze.device_telemetry (json_data, file_name)
        SELECT PARSE_JSON(%s), %s
    """
    for record in records:
        cursor.execute(insert_sql, (
            json.dumps(record),
            f"generator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        ))
    cursor.close()

# =============================================
# MAIN: GENERATE 1,000 RECORDS
# =============================================
def main():
    print("🚀 Digi IoT Generator Starting...")
    print(f"📡 Generating data for {len(DEVICES)} devices across {len(SITES)} sites")

    # Connect Snowflake
    print("\n🔌 Connecting to Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    print("✅ Connected!")

    # Generate records
    records = []
    for i in range(1000):
        device = random.choice(DEVICES)
        site = random.choice(list(SITES.keys()))
        records.append(generate_record(device, site))

    print(f"\n📊 Generated {len(records)} records")
    print(f"   🌡️  Cold Storage alerts: {sum(1 for r in records if r['site_id'] == 'Cold_Storage' and r['alert_type'])}")
    print(f"   ⚠️  Total alerts: {sum(1 for r in records if r['alert_type'])}")

    # Insert into Bronze
    print("\n📥 Loading into Snowflake Bronze layer...")
    BATCH_SIZE = 100
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        insert_bronze(conn, batch)
        print(f"   ✅ Batch {i//BATCH_SIZE + 1}/10: {len(batch)} records loaded")
        time.sleep(0.1)

    # Verify
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM iot.bronze.device_telemetry")
    total = cursor.fetchone()[0]
    print(f"\n🎉 DONE! Total records in Bronze: {total}")

    # Preview
    cursor.execute("""
        SELECT 
          json_data:device_id::STRING AS device_id,
          json_data:site_id::STRING AS site_id,
          json_data:temperature::FLOAT AS temp,
          json_data:alert_type::STRING AS alert
        FROM iot.bronze.device_telemetry 
        LIMIT 5
    """)
    print("\n📋 Preview (5 records):")
    print(f"{'DEVICE':<15} {'SITE':<15} {'TEMP':>8} {'ALERT':<15}")
    print("-" * 55)
    for row in cursor.fetchall():
        print(f"{row[0]:<15} {row[1]:<15} {row[2]:>8.1f} {str(row[3]):<15}")

    conn.close()
    print("\n✅ Connection closed. Generator complete!")

if __name__ == "__main__":
    main()
