import streamlit as st
import pandas as pd
import psycopg2
import json
import plotly.express as px


st.set_page_config(
    page_title="Fraud Detection Dashboard",
    page_icon="🚨",
    layout="wide"
)

# DB CONNECTION
def get_data():
    conn = psycopg2.connect(
        host="127.0.0.1",
        database="fraudstream",
        user="fraud_user",
        password="fraud_pass",
        port=5433
    )

    query = "SELECT * FROM fraud_alerts ORDER BY created_at DESC"
    df = pd.read_sql(query, conn)
    conn.close()

    return df


# LOAD DATA
df = get_data()

# JSON string to dict 
def extract_user(event_str):
    try:
        event = json.loads(event_str) if isinstance(event_str, str) else event_str
        return event.get("user_id", "unknown")
    except:
        return "unknown"


df["user_id"] = df["event"].apply(extract_user)

# UI HEADER
st.title("Real-Time Fraud Detection System")

# KPI CARDS
col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Alerts", len(df))

col2.metric(
    "High Value Alerts",
    len(df[df["alert_type"] == "HIGH_VALUE_TRANSACTION"])
)

col3.metric(
    "Device Changes",
    len(df[df["alert_type"] == "DEVICE_CHANGE"])
)

col4.metric(
    "Location Changes",
    len(df[df["alert_type"] == "LOCATION_CHANGE"])
)

st.divider()

# FRAUD TYPE CHART
st.subheader("📊 Fraud Type Distribution")

chart_data = df["alert_type"].value_counts().reset_index()
chart_data.columns = ["Fraud Type", "Count"]

fig = px.bar(
    chart_data,
    x="Fraud Type",
    y="Count",
    text="Count",
    color="Fraud Type"
)

st.plotly_chart(fig, use_container_width=True)

# TOP RISKY USERS
st.subheader("👤 Most Affected Users")

user_counts = df["user_id"].value_counts().head(10).reset_index()
user_counts.columns = ["User", "Fraud Count"]

fig2 = px.bar(
    user_counts,
    x="User",
    y="Fraud Count",
    text="Fraud Count",
    color="Fraud Count"
)

st.plotly_chart(fig2, use_container_width=True)

# RECENT ALERTS TABLE
st.subheader("📋 Recent Fraud Alerts")

st.dataframe(
    df.head(20),
    use_container_width=True,
    height=400
)

# FOOTER
st.markdown("---")
st.markdown("⚡ Built using Kafka + Streamlit + PostgreSQL for real-time fraud detection")