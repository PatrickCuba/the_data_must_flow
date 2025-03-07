# Import python packages
import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# Set page config for full screen width
st.set_page_config(layout="wide")

# Get the current credentials
session = get_active_session()

# Fetch available sat_xt_% tables
table_list = session.sql("""
    SELECT table_name
    FROM datawarehouse.information_schema.tables
    WHERE table_schema = 'DATAVAULT'
    AND table_name LIKE 'SAT_XT_%'
""").to_pandas()

# Centered title
st.markdown("""
    <h1 style='text-align: center;'>ℹ️ Tracking Record State ℹ️</h1>
    """, unsafe_allow_html=True)

st.markdown(
    """
    **Records visible to Snowflake can be in one of several states:**  

    - **Active** - You can query the record.  
    - **Archived** - The record must be restored to be queried.  
    - **Purged** - The record is permanently deleted through an archival or deletion policy (certificate of destruction).  
    """
)

# Select satellite table
available_tables = table_list["TABLE_NAME"].tolist()
selected_table = st.selectbox("Select Satellite Table Type:", available_tables, index=0)

# Fetch data
df = session.sql(
    f"""
    SELECT dv_applieddate, dv_rectarget, dv_record_retention_state, 
           COUNT(dv_record_retention_state) AS dv_record_retention_state_count
    FROM datawarehouse.datavault.{selected_table}
    GROUP BY 1, 2, 3;
    """
).to_pandas()

# Convert date column to datetime
df["DV_APPLIEDDATE"] = pd.to_datetime(df["DV_APPLIEDDATE"])

# Define color scale
dv_color_scale = alt.Scale(domain=["Active", "Archived", "Purged"], range=["green", "orange", "red"])

# Date range slider
min_date = df["DV_APPLIEDDATE"].min().date()
max_date = df["DV_APPLIEDDATE"].max().date()

def reset_dates():
    st.session_state["date_range"] = (min_date, max_date)

if "date_range" not in st.session_state:
    st.session_state["date_range"] = (min_date, max_date)

start_date, end_date = st.slider(
    "Select Date Range:",
    min_value=min_date,
    max_value=max_date,
    value=st.session_state["date_range"],
    #key="date_range"
)

if st.button("Reset Date Range"):
    reset_dates()

# Filter dataframe by selected date range
df_filtered = df[(df["DV_APPLIEDDATE"] >= pd.to_datetime(start_date)) & (df["DV_APPLIEDDATE"] <= pd.to_datetime(end_date))]

# Create dropdowns side by side
col1, col2 = st.columns(2)

# Filter by DV_RECTARGET
with col1:
    available_targets = df_filtered["DV_RECTARGET"].unique().tolist()
    selected_target = st.selectbox("Filter by Record Target:", ["All"] + available_targets)

# Filter by DV_RECORD_RETENTION_STATE
with col2:
    available_states = df_filtered["DV_RECORD_RETENTION_STATE"].unique().tolist()
    selected_state = st.selectbox("Filter by Record State:", ["All"] + available_states)

# Apply filters if values are selected
if selected_target != "All":
    df_filtered = df_filtered[df_filtered["DV_RECTARGET"] == selected_target]

if selected_state != "All":
    df_filtered = df_filtered[df_filtered["DV_RECORD_RETENTION_STATE"] == selected_state]

# Aggregate data for pie chart
df_pie = df_filtered.groupby("DV_RECORD_RETENTION_STATE", as_index=False)["DV_RECORD_RETENTION_STATE_COUNT"].sum()

# Create pie chart (filtered by selectors)
pie_chart_filtered = alt.Chart(df_pie).mark_arc().encode(
    theta=alt.Theta("DV_RECORD_RETENTION_STATE_COUNT:Q"),
    color=alt.Color("DV_RECORD_RETENTION_STATE:N", scale=dv_color_scale, title="Record State"),
    tooltip=["DV_RECORD_RETENTION_STATE", "DV_RECORD_RETENTION_STATE_COUNT"]
).properties(
    title="Record State Distribution (Filtered)",
    width=400,
    height=400
)

# Display the pie chart
st.altair_chart(pie_chart_filtered, use_container_width=True)
