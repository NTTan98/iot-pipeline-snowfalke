import random
from datetime import datetime, timezone
import os
import json

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
        "device_id":      device_id,
        "site_id":        site_id,
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "temperature":    temperature,
        "humidity":       round(random.uniform(30, 70), 1),
        "battery_pct":    round(random.uniform(20, 100), 1),
        "signal_rssi":    random.randint(-90, -40),
        "uptime_hours":   round(random.uniform(1000, 50000), 1),
        "data_usage_mb":  round(random.uniform(0.1, 5.0), 2),
        "alert_type":     alert_type,
        "alert_severity": alert_severity,
    }

# =============================================
# MAIN: GENERATE 1,000 RECORDS
# =============================================
def main():
    print("🚀 Digi IoT Generator Starting...")
    print(f"📡 Devices: {len(DEVICES)} | Sites: {len(SITES)}")

    os.makedirs("output_json", exist_ok=True)

    records = [
        generate_record(random.choice(DEVICES), random.choice(list(SITES.keys())))
        for _ in range(1000)
    ]

    filename = f"output_json/iot_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, "w") as f:
        json.dump(records, f, indent=2)

    cold_alerts  = sum(1 for r in records if r["site_id"] == "Cold_Storage" and r["alert_type"])
    total_alerts = sum(1 for r in records if r["alert_type"])

    print(f"💾 Saved: {filename}")
    print(f"📊 Records: {len(records)} | Total alerts: {total_alerts} | Cold Storage alerts: {cold_alerts}")
    print("📤 Next: PUT file://output_json/*.json @IOT.BRONZE.IOTDATA;")
    print("✅ READY FOR SNOWFLAKE STAGE!")

if __name__ == "__main__":
    main()
