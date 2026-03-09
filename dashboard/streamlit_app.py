import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
from dotenv import load_dotenv

# Load .env (nếu dùng)
load_dotenv()

# Snowflake config
@st.cache_resource(ttl=600)  # Cache 10 phút
def init_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse="iot_xs",
        database="iot",
        schema="gold"
    )

conn = init_connection()

# =============================================
# DASHBOARD TITLE & METRICS
# =============================================
st.set_page_config(page_title="IoT Dashboard", layout="wide")
st.title("🛰️ Remote Manager Analytics")
st.markdown("**Live IoT monitoring: 20 devices × 5 sites**")

# 5 KPI Cards (Dashboard metrics)
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    active = pd.read_sql("SELECT SUM(active_devices) as total FROM fleet_metrics WHERE analysis_hour > CURRENT_TIMESTAMP() - INTERVAL '1 HOUR'", conn)
    st.metric("Active Devices (1h)", active.iloc[0,0], delta="20")

with col2:
    alerts = pd.read_sql("SELECT SUM(total_alerts) as total FROM fleet_metrics WHERE analysis_hour > CURRENT_TIMESTAMP() - INTERVAL '1 HOUR'", conn)
    st.metric("Total Alerts (1h)", alerts.iloc[0,0], delta="+12")

with col3:
    uptime = pd.read_sql("SELECT ROUND(AVG(avg_uptime_pct),1) as pct FROM fleet_metrics WHERE analysis_hour > CURRENT_TIMESTAMP() - INTERVAL '24 HOURS'", conn)
    st.metric("Fleet Uptime %", f"{uptime.iloc[0,0]}%", delta="+0.5")

with col4:
    usage = pd.read_sql("SELECT ROUND(SUM(total_data_usage_mb),1) as mb FROM fleet_metrics WHERE analysis_hour > CURRENT_TIMESTAMP() - INTERVAL '24 HOURS'", conn)
    st.metric("Data Usage (24h)", f"{usage.iloc[0,0]} MB", delta="+45.6")

with col5:
    temp = pd.read_sql("SELECT ROUND(AVG(avg_temperature),1) as temp FROM fleet_metrics WHERE analysis_hour > CURRENT_TIMESTAMP() - INTERVAL '1 HOUR'", conn)
    st.metric("Avg Temperature", f"{temp.iloc[0,0]}°C", delta="+0.3")

# =============================================
# CHARTS
# =============================================
st.markdown("---")
col1, col2 = st.columns(2)

with col1:
    st.subheader("🌡️ Temperature Heatmap (Site)")
    df_temp = pd.read_sql("""
        SELECT site_id as site_id, 
            ROUND(AVG(avg_temperature),1) as avg_temp
        FROM fleet_metrics 
        WHERE analysis_hour > CURRENT_TIMESTAMP() - INTERVAL '24 HOURS'
        GROUP BY 1 ORDER BY 2 DESC
    """, conn)

    fig_temp = px.bar(df_temp, x="SITE_ID", y="AVG_TEMP",
                    color="AVG_TEMP",
                    color_continuous_scale="RdYlBu_r",
                    title="🌡️ Temperature Heatmap (24h)")
    st.plotly_chart(fig_temp, use_container_width=True)

with col2:
    st.subheader("⚠️ Alert Timeline (24h)")
    df_alerts = pd.read_sql("""
        SELECT analysis_hour, SUM(total_alerts) as alerts
        FROM fleet_metrics 
        WHERE analysis_hour > CURRENT_TIMESTAMP() - INTERVAL '24 HOURS'
        GROUP BY 1 ORDER BY 1
    """, conn)
    
    fig_alerts = px.bar(df_alerts, x="ANALYSIS_HOUR", y="ALERTS",
                        title="Alerts per Hour")
    st.plotly_chart(fig_alerts, use_container_width=True)

# =============================================
# LIVE TABLE
# =============================================
st.markdown("---")
st.subheader("📊 Latest Fleet Metrics")
df_live = pd.read_sql("""
    SELECT * FROM fleet_metrics 
    ORDER BY analysis_hour DESC, site_id 
    LIMIT 20
""", conn)
st.dataframe(df_live, use_container_width=True)

# Footer
st.markdown("---")
st.caption("🔄 Auto-refreshing... | Cost: ~$0.01/query | NTTan98 - Digi IoT Pipeline")
