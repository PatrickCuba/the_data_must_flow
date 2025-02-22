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
    <h1 style='text-align: center;'>⚠️ Purge Requests & Re-emergence ⚠️</h1>
    """, unsafe_allow_html=True)

st.markdown(
    """
    Here we track when record deletion requests are made and if a source application still has records we shouldn't have.  

    If a record appears that we shouldn't have, we must investigate that source application.  
    """
)

# Table selector
available_tables = table_list["TABLE_NAME"].tolist()
selected_table = st.selectbox("Select Satellite Table Type:", available_tables, index=0)

# Fetch data dynamically based on selected table
df = session.sql(
    f"""
    SELECT dv_applieddate, 
           dv_rectarget, 
           SUM(CASE WHEN dv_disposal_record_requested = TRUE THEN 1 ELSE 0 END) AS dv_disposal_record_requested_count,
           SUM(CASE WHEN dv_disposed_record_reemerged = TRUE THEN 1 ELSE 0 END) AS dv_disposed_record_reemerged_count
    FROM datawarehouse.datavault.{selected_table}
    GROUP BY 1, 2;
    """
).to_pandas()

df_agg = session.sql(
    f"""
    SELECT dv_rectarget,
           SUM(CASE WHEN dv_disposal_record_requested = TRUE THEN 1 ELSE 0 END) AS dv_disposal_record_requested_count,
           SUM(CASE WHEN dv_disposed_record_reemerged = TRUE THEN 1 ELSE 0 END) AS dv_disposed_record_reemerged_count
    FROM datawarehouse.datavault.{selected_table}
    GROUP BY 1;
    """
).to_pandas()

# Error trap dataset
df_trap = session.sql(
    f"""
    SELECT dv_applieddate, dv_rectarget, 
           COUNT(dv_disposed_record_reemerged) AS dv_disposed_record_reemerged
    FROM datawarehouse.datavault.sat_xt_hub_customer
    WHERE dv_disposal_record_requested = dv_disposed_record_reemerged
    AND dv_disposed_record_reemerged = TRUE
    GROUP BY 1, 2;
    """
).to_pandas()

# Convert date columns to datetime
df["DV_APPLIEDDATE"] = pd.to_datetime(df["DV_APPLIEDDATE"])
df_trap["DV_APPLIEDDATE"] = pd.to_datetime(df_trap["DV_APPLIEDDATE"])

# Define color scale
color_scale = alt.Scale(domain=["Purge Requests", "Re-emerged Records"], range=["orange", "red"])

# Add a date range slider
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

# Filter dataframe based on selected date range
df_filtered = df[(df["DV_APPLIEDDATE"] >= pd.to_datetime(start_date)) & (df["DV_APPLIEDDATE"] <= pd.to_datetime(end_date))]
df_trap_filtered = df_trap[(df_trap["DV_APPLIEDDATE"] >= pd.to_datetime(start_date)) & (df_trap["DV_APPLIEDDATE"] <= pd.to_datetime(end_date))]

# Add a selector for DV_RECTARGET
available_targets = df_filtered["DV_RECTARGET"].unique().tolist()
selected_target = st.selectbox("Filter by Record Target:", ["All"] + available_targets)

# Apply target filter
if selected_target != "All":
    df_filtered = df_filtered[df_filtered["DV_RECTARGET"] == selected_target]
    df_agg = df_agg[df_agg["DV_RECTARGET"] == selected_target]
    df_trap_filtered = df_trap_filtered[df_trap_filtered["DV_RECTARGET"] == selected_target]

# Transform df_filtered for stacked bars
df_melted = df_filtered.melt(id_vars=["DV_APPLIEDDATE", "DV_RECTARGET"], 
                             value_vars=["DV_DISPOSAL_RECORD_REQUESTED_COUNT", "DV_DISPOSED_RECORD_REEMERGED_COUNT"],
                             var_name="Violation Type", value_name="Count")

# Rename values for clarity
df_melted["Violation Type"] = df_melted["Violation Type"].replace({
    "DV_DISPOSAL_RECORD_REQUESTED_COUNT": "Purge Requests",
    "DV_DISPOSED_RECORD_REEMERGED_COUNT": "Re-emerged Records"
})

# Create grouped bar chart for main dataset
count_chart = alt.Chart(df_melted).mark_bar().encode(
    x=alt.X("DV_APPLIEDDATE:T", title="Date"),
    y=alt.Y("Count:Q", title="Count"),
    color=alt.Color("Violation Type:N", scale=color_scale, title="Event Type"),
    tooltip=["DV_APPLIEDDATE", "DV_RECTARGET", "Violation Type", "Count"]
).properties(
    title="Purge Requests & Re-emergence Over Time",
    width=900,
    height=400
)

# Create a horizontal bar chart for df_trap
trap_chart = alt.Chart(df_trap_filtered).mark_bar().encode(
    x=alt.X("DV_APPLIEDDATE:T", title="Date"),
    y=alt.Y("DV_DISPOSED_RECORD_REEMERGED:Q", title="Re-emerged Errors"),
    color=alt.Color("DV_RECTARGET:N", title="Record Target"),
    tooltip=["DV_APPLIEDDATE", "DV_RECTARGET", "DV_DISPOSED_RECORD_REEMERGED"]
).properties(
    title="Re-emerged Errors Over Time",
    width=900,
    height=400
)

# Transform df_agg for pie chart
df_pie = df_agg.melt(id_vars=["DV_RECTARGET"], 
                     value_vars=["DV_DISPOSAL_RECORD_REQUESTED_COUNT", "DV_DISPOSED_RECORD_REEMERGED_COUNT"],
                     var_name="Violation Type", value_name="Count")

# Rename for clarity
df_pie["Violation Type"] = df_pie["Violation Type"].replace({
    "DV_DISPOSAL_RECORD_REQUESTED_COUNT": "Purge Requests",
    "DV_DISPOSED_RECORD_REEMERGED_COUNT": "Re-emerged Records"
})

# Create pie chart
pie_chart = alt.Chart(df_pie).mark_arc().encode(
    theta=alt.Theta("Count:Q"),
    color=alt.Color("Violation Type:N", scale=color_scale, title="Event Type"),
    tooltip=["Violation Type", "Count"]
).properties(
    title="Total Purge Requests & Re-emergence",
    width=400,
    height=400
)

# Display charts
st.altair_chart(count_chart, use_container_width=True)
st.altair_chart(trap_chart, use_container_width=True)
st.altair_chart(pie_chart, use_container_width=True)
