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
S3_OUTPUT = "s3://iot-esp32-firehose-data-rek/iot_data"  # Athena query results bucket

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

# --- Fetch latest 1000 rows from Athena ---
query = f"SELECT * FROM {ATHENA_TABLE} ORDER BY timestamp_ms DESC LIMIT 1000"
df = run_athena_query(query)

# --- Convert numeric columns ---
numeric_cols = ["ky028_temp_C", "bme_temp_C", "humidity_percent", "pressure_hPa", 
                "distance_cm", "processing_time_ms", "payload_size_bytes"]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# --- IMU columns ---
if "mpu6050.roll_deg" in df.columns:
    df["roll_deg"] = pd.to_numeric(df["mpu6050.roll_deg"], errors="coerce")
if "mpu6050.pitch_deg" in df.columns:
    df["pitch_deg"] = pd.to_numeric(df["mpu6050.pitch_deg"], errors="coerce")
if "mpu6050.yaw_deg" in df.columns:
    df["yaw_deg"] = pd.to_numeric(df["mpu6050.yaw_deg"], errors="coerce")

# --- Layout: Line charts ---
st.subheader("Temperature over time")
if "bme_temp_C" in df.columns:
    fig_temp = px.line(df, x=df.index, y="bme_temp_C", title="BME Temperature (°C)")
    st.plotly_chart(fig_temp, use_container_width=True)

st.subheader("Humidity over time")
if "humidity_percent" in df.columns:
    fig_humidity = px.line(df, x=df.index, y="humidity_percent", title="Humidity (%)")
    st.plotly_chart(fig_humidity, use_container_width=True)

st.subheader("Distance over time")
if "distance_cm" in df.columns:
    fig_distance = px.line(df, x=df.index, y="distance_cm", title="Distance (cm)")
    st.plotly_chart(fig_distance, use_container_width=True)

st.subheader("IMU Data (Roll / Pitch / Yaw)")
imu_cols = ["roll_deg", "pitch_deg", "yaw_deg"]
if all(c in df.columns for c in imu_cols):
    fig_imu = px.line(df, x=df.index, y=imu_cols, title="MPU6050 IMU Data")
    st.plotly_chart(fig_imu, use_container_width=True)

# --- Latest metrics ---
st.subheader("Latest Sensor Values")
if not df.empty:
    latest = df.iloc[0]
    cols = st.columns(3)
    if "bme_temp_C" in latest:
        cols[0].metric("BME Temp (°C)", latest["bme_temp_C"])
    if "humidity_percent" in latest:
        cols[1].metric("Humidity (%)", latest["humidity_percent"])
    if "distance_cm" in latest:
        cols[2].metric("Distance (cm)", latest["distance_cm"])
