import streamlit as st
import boto3
import pandas as pd
import plotly.express as px
import time

# --- AWS credentials from Streamlit Secrets ---
AWS_ACCESS_KEY_ID = st.secrets["AWS"]["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = st.secrets["AWS"]["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = st.secrets["AWS"]["AWS_REGION"]

ATHENA_DATABASE = "esp32_data"           # Athena database
ATHENA_TABLE = "sensor_history"          # Athena table
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

# --- Fetch latest 1000 rows from Athena ---
query = f"SELECT *, \
json_extract_scalar(mpu6050, '$.roll_deg') AS roll_deg, \
json_extract_scalar(mpu6050, '$.pitch_deg') AS pitch_deg, \
json_extract_scalar(mpu6050, '$.yaw_deg') AS yaw_deg \
FROM {ATHENA_TABLE} ORDER BY timestamp_ms DESC LIMIT 100"

df = run_athena_query(query)

if df.empty:
    st.warning("No data retrieved from Athena.")
else:
    # --- Convert timestamp ---
    df['timestamp_ms'] = pd.to_datetime(df['timestamp_ms'], unit='ms')

    # --- Convert numeric columns ---
    numeric_cols = ["ky028_temp_C", "bme_temp_C", "humidity_percent", "pressure_hPa",
                    "distance_cm", "roll_deg", "pitch_deg", "yaw_deg",
                    "processing_time_ms", "payload_size_bytes"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- Layout: Charts ---
    st.subheader("Temperature over time")
    temp_cols = ["ky028_temp_C", "bme_temp_C"]
    available_temp_cols = [c for c in temp_cols if c in df.columns]
    if available_temp_cols:
        fig_temp = px.line(
            df,
            x="timestamp_ms",
            y=available_temp_cols,
            title="Temperature Sensors (°C)",
            labels={"value": "Temperature (°C)", "timestamp_ms": "Time", "variable": "Sensor"}
        )
        st.plotly_chart(fig_temp, use_container_width=True)

    st.subheader("Humidity over time")
    if "humidity_percent" in df.columns:
        fig_humidity = px.line(
            df,
            x="timestamp_ms",
            y="humidity_percent",
            title="Humidity (%)",
            labels={"humidity_percent": "Humidity (%)", "timestamp_ms": "Time"}
        )
        st.plotly_chart(fig_humidity, use_container_width=True)

    st.subheader("Pressure over time")
    if "pressure_hPa" in df.columns:
        fig_pressure = px.line(
            df,
            x="timestamp_ms",
            y="pressure_hPa",
            title="Pressure (hPa)",
            labels={"pressure_hPa": "Pressure (hPa)", "timestamp_ms": "Time"}
        )
        st.plotly_chart(fig_pressure, use_container_width=True)

    st.subheader("Distance over time")
    if "distance_cm" in df.columns:
        fig_distance = px.line(
            df,
            x="timestamp_ms",
            y="distance_cm",
            title="Distance (cm)",
            labels={"distance_cm": "Distance (cm)", "timestamp_ms": "Time"}
        )
        st.plotly_chart(fig_distance, use_container_width=True)

    st.subheader("IMU Data (Roll / Pitch / Yaw)")
    imu_cols = ["roll_deg", "pitch_deg", "yaw_deg"]
    available_imu_cols = [c for c in imu_cols if c in df.columns]
    if available_imu_cols:
        fig_imu = px.line(
            df,
            x="timestamp_ms",
            y=available_imu_cols,
            title="MPU6050 IMU Data",
            labels={"value": "Degrees", "timestamp_ms": "Time", "variable": "Axis"}
        )
        st.plotly_chart(fig_imu, use_container_width=True)

    # --- Latest metrics ---
    st.subheader("Latest Sensor Values")
    latest = df.iloc[0]
    cols = st.columns(4)
    metric_idx = 0
    sensor_metrics = {
        "BME Temp (°C)": "bme_temp_C",
        "KY028 Temp (°C)": "ky028_temp_C",
        "Humidity (%)": "humidity_percent",
        "Pressure (hPa)": "pressure_hPa",
        "Distance (cm)": "distance_cm",
        "Roll (°)": "roll_deg",
        "Pitch (°)": "pitch_deg",
        "Yaw (°)": "yaw_deg"
    }
    for name, col_name in sensor_metrics.items():
        if col_name in latest:
            cols[metric_idx % 4].metric(name, latest[col_name])
            metric_idx += 1
