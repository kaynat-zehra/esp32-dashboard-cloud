import streamlit as st
import boto3
import pandas as pd
import plotly.express as px
import time

# --- Streamlit page config ---
st.set_page_config(page_title="ESP32 Sensor Dashboard", layout="wide")
st.title("ESP32 Sensor Dashboard")

# --- AWS credentials from Streamlit Secrets ---
AWS_ACCESS_KEY_ID = st.secrets["AWS"]["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = st.secrets["AWS"]["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = st.secrets["AWS"]["AWS_REGION"]

ATHENA_DATABASE = "esp32_data"
ATHENA_TABLE = "sensor_history"
S3_OUTPUT = "s3://esp32-athena-results-rek/"  # Athena query results location

# --- Initialize Athena client ---
athena_client = boto3.client(
    "athena",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# --- Function to run Athena query ---
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

    results = athena_client.get_query_results(QueryExecutionId=query_id)
    columns = [col["Label"] for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = [[c.get("VarCharValue", None) for c in r["Data"]] for r in results["ResultSet"]["Rows"][1:]]
    df = pd.DataFrame(rows, columns=columns)
    return df

# --- Query latest sensor data ---
query = f"""
SELECT *
FROM {ATHENA_TABLE}
ORDER BY timestamp_ms DESC
LIMIT 100
"""
df = run_athena_query(query)

# --- Convert numeric columns ---
numeric_cols = ["ky028_temp_c", "bme_temp_c", "humidity_percent", "pressure_hpa",
                "distance_cm", "roll_deg", "pitch_deg", "yaw_deg"]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# --- Show raw data ---
st.subheader("Latest Data Preview")
st.dataframe(df.head())

# --- Latest sensor metrics ---
st.subheader("Latest Sensor Values")
if not df.empty:
    latest = df.iloc[0]  # pick the row with highest timestamp

    cols = st.columns(4)
    cols[0].metric("BME Temp (°C)", latest.get("bme_temp_c", "-"))
    cols[1].metric("Humidity (%)", latest.get("humidity_percent", "-"))

    distance_val = latest.get("distance_cm", "-")
    if distance_val != "-" and distance_val < 3:
        cols[2].metric("Distance (cm)", f"{distance_val} ⚠️", delta="Too Close!", delta_color="inverse")
        st.error(f"⚠️ ALERT: Object too close! Distance = {distance_val} cm")
    else:
        cols[2].metric("Distance (cm)", distance_val)

    imu_text = f"Roll: {latest.get('roll_deg','-')}, Pitch: {latest.get('pitch_deg','-')}, Yaw: {latest.get('yaw_deg','-')}"
    cols[3].metric("IMU Data", imu_text)

# --- Charts for historical trends ---
st.subheader("Historical Sensor Data")
if not df.empty:
    for col_name in ["bme_temp_c", "humidity_percent", "pressure_hpa", "distance_cm"]:
        if col_name in df.columns:
            fig = px.line(df, x="timestamp_ms", y=col_name, title=f"{col_name} over time", markers=True)
            st.plotly_chart(fig, use_container_width=True)

    imu_cols = ["roll_deg", "pitch_deg", "yaw_deg"]
    if all(c in df.columns for c in imu_cols):
        fig_imu = px.line(df, x="timestamp_ms", y=imu_cols, title="IMU Data", markers=True)
        st.plotly_chart(fig_imu, use_container_width=True)

# --- Auto-refresh every 5 seconds ---
st.experimental_rerun()
