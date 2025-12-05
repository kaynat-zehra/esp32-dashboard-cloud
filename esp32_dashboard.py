import streamlit as st
import boto3
import pandas as pd
import plotly.express as px
import time

# --- AWS credentials from Streamlit Secrets ---
AWS_ACCESS_KEY_ID = st.secrets["AWS"]["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = st.secrets["AWS"]["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = st.secrets["AWS"]["AWS_REGION"]

ATHENA_DATABASE = "esp32_data"           # Your Athena database
ATHENA_TABLE = "sensor_history"          # Your Athena table
S3_OUTPUT = "s3://esp32-athena-results-rek/"  # Athena query results bucket

# --- Initialize Athena client ---
athena_client = boto3.client(
    "athena",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# --- Streamlit page config ---
st.set_page_config(page_title="ESP32 Sensor Dashboard", layout="wide")
st.title("ESP32 Sensor Dashboard")

# --- Function to query Athena ---
def run_athena_query(query):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": S3_OUTPUT},
    )
    query_id = response["QueryExecutionId"]

    # Wait for query to finish
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        st.error(f"Athena query {state}")
        return pd.DataFrame()

    # Get results
    results = athena_client.get_query_results(QueryExecutionId=query_id)
    columns = [col["Label"] for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = [[c.get("VarCharValue", None) for c in r["Data"]] for r in results["ResultSet"]["Rows"][1:]]
    df = pd.DataFrame(rows, columns=columns)
    return df

# --- Athena query (extract struct fields properly) ---
query = f"""
SELECT 
    timestamp_ms,
    ky028_temp_c,
    bme_temp_c,
    humidity_percent,
    pressure_hpa,
    distance_cm,
    mpu6050.roll_deg AS roll_deg,
    mpu6050.pitch_deg AS pitch_deg,
    mpu6050.yaw_deg AS yaw_deg
FROM {ATHENA_TABLE}
ORDER BY timestamp_ms DESC
LIMIT 100
"""
df = run_athena_query(query)

# --- Debug: show raw data ---
st.subheader("Raw Data Preview")
st.write(df.head())
st.write("Columns:", df.columns)

# --- Convert numeric columns ---
numeric_cols = [
    "ky028_temp_c", "bme_temp_c", "humidity_percent", "pressure_hpa",
    "distance_cm", "roll_deg", "pitch_deg", "yaw_deg"
]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# --- Layout: Line charts ---
st.subheader("Temperature over time")
if "bme_temp_c" in df.columns:
    fig_temp = px.line(df, x=df.index, y="bme_temp_c", title="BME Temperature (°C)", markers=True)
    st.plotly_chart(fig_temp, use_container_width=True)

st.subheader("Humidity over time")
if "humidity_percent" in df.columns:
    fig_humidity = px.line(df, x=df.index, y="humidity_percent", title="Humidity (%)", markers=True)
    st.plotly_chart(fig_humidity, use_container_width=True)

st.subheader("Pressure over time")
if "pressure_hpa" in df.columns:
    fig_pressure = px.line(df, x=df.index, y="pressure_hpa", title="Pressure (hPa)", markers=True)
    st.plotly_chart(fig_pressure, use_container_width=True)

st.subheader("Distance over time")
if "distance_cm" in df.columns:
    fig_distance = px.line(df, x=df.index, y="distance_cm", title="Distance (cm)", markers=True)
    st.plotly_chart(fig_distance, use_container_width=True)

st.subheader("IMU Data (Roll / Pitch / Yaw)")
imu_cols = ["roll_deg", "pitch_deg", "yaw_deg"]
if all(c in df.columns for c in imu_cols):
    fig_imu = px.line(df, x=df.index, y=imu_cols, title="MPU6050 IMU Data", markers=True)
    st.plotly_chart(fig_imu, use_container_width=True)

# --- Latest metrics with distance alert ---
st.subheader("Latest Sensor Values")
if not df.empty:
    latest = df.iloc[0]
    cols = st.columns(4)
    
    # BME Temp
    if "bme_temp_c" in latest:
        cols[0].metric("BME Temp (°C)", latest["bme_temp_c"])
        
    # Humidity
    if "humidity_percent" in latest:
        cols[1].metric("Humidity (%)", latest["humidity_percent"])
        
    # Distance with alert
    if "distance_cm" in latest:
        distance_val = latest["distance_cm"]
        if distance_val < 3:
            # Red alert
            cols[2].metric("Distance (cm)", f"{distance_val} ⚠️", delta="Too Close!", delta_color="inverse")
            st.error(f"⚠️ ALERT: Object too close! Distance = {distance_val} cm")
        else:
            cols[2].metric("Distance (cm)", distance_val)
    
    # IMU Data
    if all(c in latest for c in imu_cols):
        imu_text = f"Roll: {latest['roll_deg']:.2f}, Pitch: {latest['pitch_deg']:.2f}, Yaw: {latest['yaw_deg']:.2f}"
        cols[3].metric("IMU Data", imu_text)

