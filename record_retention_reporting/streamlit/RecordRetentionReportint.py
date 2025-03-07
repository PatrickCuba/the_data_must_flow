# Import python packages
import streamlit as st
import pandas as pd
import altair as alt
import numpy as np
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

# Write directly to the app
# Centered title
st.markdown("""
    <h1 style='text-align: center;'>⚠️ Tracking Late Arriving Records ⚠️</h1>
    """, unsafe_allow_html=True)

st.write(
    """Sequence violations count the number of times records have arrived late.
    In other words, why is there upstream late-arriving records which could affect downstream analytics."""
)

# Default to first available table
available_tables = table_list["TABLE_NAME"].tolist()
default_table = available_tables[0] if available_tables else None
selected_table = st.selectbox("Select Satellite Table Type:", available_tables, index=0)

# Aggregate data
df_agg = session.sql(
    f"""
    select dv_rectarget
    , count(dv_sequence_violation) as dv_sequence_violation_count
    , case when dv_sequence_violation = TRUE then 'Late' else 'Normal' end as dv_sequence_violation
    from datawarehouse.datavault.{selected_table}
    group by 1, 3;
    """
    ).to_pandas()

# Date slider data
df = session.sql(
    f"""
    select dv_applieddate 
    , dv_rectarget
    , count(dv_sequence_violation) as dv_sequence_violation_count
    , case when dv_sequence_violation = TRUE then 'Late' else 'Normal' end as dv_sequence_violation
    from datawarehouse.datavault.{selected_table}
    group by 1, 2, 4;
    """
    ).to_pandas()

# Convert date column to datetime
df["DV_APPLIEDDATE"] = pd.to_datetime(df["DV_APPLIEDDATE"])

# Define color scale
dv_color_scale = alt.Scale(domain=["Normal", "Late"], range=["green", "orange"])

# Add a date range slider
min_date = df["DV_APPLIEDDATE"].min().date()
max_date = df["DV_APPLIEDDATE"].max().date()

def reset_dates():
    st.session_state["date_range"] = (min_date, max_date)

if "date_range" not in st.session_state:
    st.session_state["date_range"] = (min_date, max_date)

if min_date == max_date:
    st.write(f"Limited date range: {min_date}.")
    start_date, end_date = min_date, max_date
else:
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

# Create bar chart -- aggregate
chart_agg = alt.Chart(df_agg).mark_bar().encode(
    x=alt.X("DV_SEQUENCE_VIOLATION_COUNT", title="Violation Count"),
    y=alt.Y("DV_RECTARGET", title="Satellite Table", sort="-x", axis=alt.Axis(labelLimit=200)),
    color=alt.Color("DV_SEQUENCE_VIOLATION", scale=dv_color_scale)
).properties(
    title="Out of Sequence Events (Aggregate Total)"
)

# Create bar chart -- slider
chart = alt.Chart(df_filtered).mark_bar().encode(
    x=alt.X("DV_SEQUENCE_VIOLATION_COUNT", title="Violation Count"),
    y=alt.Y("DV_RECTARGET", title="Satellite Table", sort="-x", axis=alt.Axis(labelLimit=200)),
    color=alt.Color("DV_SEQUENCE_VIOLATION", scale=dv_color_scale)
).properties(
    title="Out of Sequence Events (Slider)"
)

# Display charts
st.altair_chart(chart_agg, use_container_width=True)
st.altair_chart(chart, use_container_width=True)

# Satellite Table Selection
satellite_options = ["All Satellites"] + list(df["DV_RECTARGET"].unique())
selected_satellite = st.selectbox("Select Satellite Table:", satellite_options)

if selected_satellite == "All Satellites":
    df_satellite_filtered = df_filtered
else:
    df_satellite_filtered = df_filtered[df_filtered["DV_RECTARGET"] == selected_satellite]

# Create pie chart -- satellite selection
pie_chart = alt.Chart(df_satellite_filtered).mark_arc().encode(
    theta=alt.Theta("DV_SEQUENCE_VIOLATION_COUNT", type="quantitative"),
    color=alt.Color("DV_SEQUENCE_VIOLATION", scale=dv_color_scale, title="Violation Type")
).properties(
    title=f"Violation Distribution for {selected_satellite}"
)

# Layout for pie chart and explanation
col1, col2 = st.columns([2, 1])
with col1:
    st.altair_chart(pie_chart, use_container_width=True)
with col2:
    st.markdown(
        """
        **Note:**
        
        Late arriving records are not necessarily a problem, but they should prompt an investigation into why they are occurring. 
        Understanding the reasons behind late arrivals can help improve data quality and pipeline efficiency.
        """
    )
