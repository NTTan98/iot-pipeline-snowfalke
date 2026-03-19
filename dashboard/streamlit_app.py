import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

@st.cache_resource(ttl=600)
def init_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse="COMPUTE_WH",
        database="iot",
        schema="gold"
    )

conn = init_connection()

# =============================================
# PAGE CONFIG
# =============================================
st.set_page_config(page_title="IoT Dashboard", layout="wide")
st.title("🛰️ Remote Manager Analytics")
st.markdown("**Live IoT monitoring: 20 devices × 5 sites**")

# =============================================
# KPI CARDS
# =============================================
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    active = pd.read_sql("""
        SELECT SUM(active_devices) AS total
        FROM fleet_metrics
        WHERE analysis_hour > CURRENT_TIMESTAMP() - INTERVAL '1 HOUR'
    """, conn)
    st.metric("Active Devices (1h)", active.iloc[0, 0] or 0)

with col2:
    alerts = pd.read_sql("""
        SELECT SUM(total_alerts) AS total
        FROM fleet_metrics
        WHERE analysis_hour > CURRENT_TIMESTAMP() - INTERVAL '1 HOUR'
    """, conn)
    st.metric("Total Alerts (1h)", alerts.iloc[0, 0] or 0)

with col3:
    uptime = pd.read_sql("""
        SELECT ROUND(AVG(avg_uptime_hours), 1) AS val
        FROM fleet_metrics
        WHERE analysis_hour > CURRENT_TIMESTAMP() - INTERVAL '24 HOURS'
    """, conn)
    st.metric("Avg Uptime (hrs)", f"{uptime.iloc[0, 0] or 0} hrs")  # ✅ fixed: None + wrong column

with col4:
    usage = pd.read_sql("""
        SELECT ROUND(SUM(total_data_usage_mb), 1) AS mb
        FROM fleet_metrics
        WHERE analysis_hour > CURRENT_TIMESTAMP() - INTERVAL '24 HOURS'
    """, conn)
    st.metric("Data Usage (24h)", f"{usage.iloc[0, 0] or 0} MB")  # ✅ fixed: None

with col5:
    temp = pd.read_sql("""
        SELECT ROUND(AVG(avg_temperature), 1) AS val
        FROM fleet_metrics
        WHERE analysis_hour > CURRENT_TIMESTAMP() - INTERVAL '1 HOUR'
    """, conn)
    st.metric("Avg Temperature", f"{temp.iloc[0, 0] or 0}°C")  # ✅ fixed: None

# =============================================
# CHARTS
# =============================================
st.markdown("---")
col1, col2 = st.columns(2)

with col1:
    st.subheader("🌡️ Temperature by Site")
    df_temp = pd.read_sql("""
        SELECT site_id::VARCHAR AS site_id,
               ROUND(AVG(avg_temperature), 1) AS avg_temp
        FROM fleet_metrics
        GROUP BY site_id
        ORDER BY avg_temp DESC
    """, conn)
    fig_temp = px.bar(
        df_temp, x="SITE_ID", y="AVG_TEMP",
        color="AVG_TEMP",
        color_continuous_scale="RdYlBu_r",
        title="Average Temperature per Site (all time)"
    )
    st.plotly_chart(fig_temp, use_container_width=True)

with col2:
    st.subheader("⚠️ Alert Timeline")
    df_alerts = pd.read_sql("""
        SELECT analysis_hour,
               SUM(total_alerts)         AS total_alerts,
               SUM(alert_temp_count)     AS temp_alerts,
               SUM(alert_humidity_count) AS humidity_alerts
        FROM fleet_metrics
        GROUP BY analysis_hour
        ORDER BY analysis_hour
    """, conn)  # ✅ fixed: removed '24 HOURS' filter, stacked by type
    fig_alerts = px.bar(
        df_alerts, x="ANALYSIS_HOUR",
        y=["TEMP_ALERTS", "HUMIDITY_ALERTS"],
        title="Alerts per Hour (all time)",
        barmode="stack"
    )
    st.plotly_chart(fig_alerts, use_container_width=True)

# =============================================
# LIVE TABLE
# =============================================
st.markdown("---")
st.subheader("📊 Latest Fleet Metrics")
df_live = pd.read_sql("""
    SELECT analysis_hour, site_id,
           active_devices,
           avg_temperature, max_temperature, min_temperature,
           avg_humidity, avg_battery_pct,
           total_alerts, alert_temp_count, alert_humidity_count,
           total_records
    FROM fleet_metrics
    ORDER BY analysis_hour DESC, site_id
    LIMIT 20
""", conn)
st.dataframe(df_live, use_container_width=True)

# Footer
st.markdown("---")
st.caption("🔄 Data updates every 10 min via Snowflake Task | NTTan98 — Digi IoT Pipeline")
