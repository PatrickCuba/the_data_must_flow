from datetime import timedelta
import altair as alt
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

# =============================================================================
# PAGE CONFIG
# =============================================================================

st.set_page_config(
    page_title="Data Vault DMF Metrics",
    page_icon="❄️",
    layout="wide",
)

# =============================================================================
# THEME — Snowflake colour system
# No bright reds. Violations use dark maroon (#7B241C).
# =============================================================================

SF_BLUE   = "#29B5E8"
SF_NAVY   = "#11567F"
SF_DARK   = "#0D3A52"
C_PASS    = "#1A7840"   # dark forest green
C_GOOD    = "#2E8B57"
C_WARN    = "#B7770D"   # dark amber
C_VIOLATE = "#7B241C"   # dark maroon  — replaces bright red
C_CRIT    = "#6E2C00"   # deep sienna
C_NODATA  = "#6C757D"   # muted grey

HEALTH_COLORS = {
    "HEALTHY":  C_PASS,
    "GOOD":     C_GOOD,
    "DEGRADED": C_WARN,
    "CRITICAL": C_VIOLATE,
    "NO DATA":  C_NODATA,
}

st.markdown("""
<style>
/* ── Page background ── */
.main .block-container { padding-top: 1rem; }

/* ── Multiselect tags: Snowflake navy ── */
span[data-baseweb="tag"] {
    background-color: #11567F !important;
    color: #ffffff !important;
}
span[data-baseweb="tag"] span { color: #ffffff !important; }
span[data-baseweb="tag"] button svg { fill: #ffffff !important; }

/* ── Slider: Snowflake navy ── */
.rc-slider-track { background-color: #11567F !important; }
.rc-slider-handle {
    border-color: #11567F !important;
    background-color: #11567F !important;
    box-shadow: 0 0 0 3px rgba(17,86,127,0.2) !important;
}
.rc-slider-handle:hover,
.rc-slider-handle:focus,
.rc-slider-handle-dragging {
    border-color: #29B5E8 !important;
    background-color: #29B5E8 !important;
    box-shadow: 0 0 0 4px rgba(41,181,232,0.3) !important;
}

/* ── Buttons ── */
.stButton > button {
    background-color: #11567F !important;
    color: #ffffff !important;
    border: 1px solid #11567F !important;
    border-radius: 4px !important;
}
.stButton > button:hover {
    background-color: #29B5E8 !important;
    border-color: #29B5E8 !important;
}

/* ── Active tab underline ── */
button[data-baseweb="tab"][aria-selected="true"] {
    border-bottom-color: #29B5E8 !important;
    color: #11567F !important;
    font-weight: 600 !important;
}

/* ── Metric labels ── */
[data-testid="stMetricLabel"] p {
    color: #11567F !important;
    font-weight: 600 !important;
}

/* ── Section headers ── */
.dv-section-header {
    background: linear-gradient(90deg, #11567F, #0D3A52);
    color: #ffffff;
    padding: 6px 14px;
    border-radius: 4px;
    font-size: 0.85rem;
    font-weight: 600;
    letter-spacing: 0.04em;
    margin-bottom: 0.6rem;
    display: inline-block;
}

/* ── Status pills ── */
.pill-pass    { background:#1A7840; color:#fff; padding:2px 10px; border-radius:12px; font-size:0.78rem; font-weight:600; }
.pill-fail    { background:#7B241C; color:#fff; padding:2px 10px; border-radius:12px; font-size:0.78rem; font-weight:600; }
.pill-nodata  { background:#6C757D; color:#fff; padding:2px 10px; border-radius:12px; font-size:0.78rem; font-weight:600; }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# HELPERS
# =============================================================================

def classify_check(metric_name: str) -> str:
    mn = (metric_name or "").upper()
    if any(x in mn for x in ("_SGTG_ERR", "_HDIF_SGTG")):
        return "RECON"
    if "_SGTG_" in mn:
        return "COUNTS"
    if "_ORPH_ERR" in mn:
        return "REFERENTIAL"
    if "_DUPE_ERR" in mn:
        return "DUPLICATE"
    return "OTHER"


def infer_dv_layer(table_name: str) -> str:
    tn = (table_name or "").upper()
    if tn.startswith("HUB_"):
        return "HUB"
    if tn.startswith("LNK_"):
        return "LNK"
    if tn.startswith("SAT_"):
        return "SAT"
    if tn.startswith("STG_"):
        return "STAGING"
    return "OTHER"


def health_label(pct: float, total: int) -> tuple:
    if total == 0:
        return "NO DATA", C_NODATA
    if pct == 100:
        return "HEALTHY", C_PASS
    if pct >= 90:
        return "GOOD", C_GOOD
    if pct >= 75:
        return "DEGRADED", C_WARN
    return "CRITICAL", C_VIOLATE


def section_header(title: str):
    st.markdown(f'<div class="dv-section-header">{title}</div>', unsafe_allow_html=True)


def status_pill(violated: bool) -> str:
    if violated:
        return '<span class="pill-fail">FAIL</span>'
    return '<span class="pill-pass">PASS</span>'


# =============================================================================
# DATA LOADERS  (all cached, SiS-compatible)
# =============================================================================

@st.cache_data(ttl=timedelta(minutes=5), show_spinner=True)
def load_latest_status() -> pd.DataFrame:
    session = get_active_session()
    df = session.sql("""
        WITH latest AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY TABLE_DATABASE, TABLE_SCHEMA, TABLE_NAME,
                                 METRIC_NAME, EXPECTATION_NAME
                    ORDER BY SCHEDULED_TIME DESC
                ) AS rn
            FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_EXPECTATION_STATUS
        )
        SELECT
            TABLE_DATABASE         AS table_database,
            TABLE_SCHEMA           AS table_schema,
            TABLE_NAME             AS table_name,
            METRIC_NAME            AS metric_name,
            EXPECTATION_NAME       AS expectation_name,
            ARGUMENT_NAMES         AS argument_names,
            TRY_TO_NUMBER(VALUE::VARCHAR) AS error_value,
            EXPECTATION_VIOLATED   AS violated,
            SCHEDULED_TIME         AS last_checked
        FROM latest
        WHERE rn = 1
        ORDER BY TABLE_NAME, METRIC_NAME
    """).to_pandas().rename(columns=str.lower)
    df["dv_layer"]   = df["table_name"].apply(infer_dv_layer)
    df["check_type"] = df["metric_name"].apply(classify_check)
    df["status"]     = df["violated"].apply(lambda v: "FAIL" if v else "PASS")
    return df


@st.cache_data(ttl=timedelta(minutes=5), show_spinner=True)
def load_history(days: int) -> pd.DataFrame:
    session = get_active_session()
    df = session.sql(f"""
        SELECT
            DATE_TRUNC('day', SCHEDULED_TIME)::DATE           AS check_date,
            TABLE_DATABASE         AS table_database,
            TABLE_SCHEMA           AS table_schema,
            TABLE_NAME             AS table_name,
            METRIC_NAME            AS metric_name,
            EXPECTATION_NAME       AS expectation_name,
            TRY_TO_NUMBER(VALUE::VARCHAR)                      AS error_value,
            EXPECTATION_VIOLATED   AS violated,
            SCHEDULED_TIME         AS scheduled_time
        FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_EXPECTATION_STATUS
        WHERE SCHEDULED_TIME >= DATEADD(day, {-days}, CURRENT_TIMESTAMP())
        ORDER BY SCHEDULED_TIME DESC
    """).to_pandas().rename(columns=str.lower)
    df["dv_layer"]   = df["table_name"].apply(infer_dv_layer)
    df["check_type"] = df["metric_name"].apply(classify_check)
    return df


@st.cache_data(ttl=timedelta(minutes=30), show_spinner=True)
def load_coverage() -> pd.DataFrame:
    session = get_active_session()
    df = session.sql("""
        SELECT
            REF_DATABASE_NAME      AS table_database,
            REF_SCHEMA_NAME        AS table_schema,
            REF_ENTITY_NAME        AS table_name,
            METRIC_DATABASE_NAME   AS metric_database,
            METRIC_SCHEMA_NAME     AS metric_schema,
            METRIC_NAME            AS metric_name,
            SCHEDULE_STATUS        AS schedule_status
        FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_METRIC_FUNCTION_REFERENCES
        WHERE METRIC_DATABASE_NAME = CURRENT_DATABASE()
        ORDER BY REF_ENTITY_NAME, METRIC_NAME
    """).to_pandas().rename(columns=str.lower)
    df["dv_layer"]   = df["table_name"].apply(infer_dv_layer)
    df["check_type"] = df["metric_name"].apply(classify_check)
    return df


# =============================================================================
# SIDEBAR
# =============================================================================

with st.sidebar:
    st.markdown(f"""
    <div style='background:linear-gradient(135deg,{SF_DARK},{SF_NAVY});
                padding:16px 12px;border-radius:6px;margin-bottom:16px;'>
      <div style='color:{SF_BLUE};font-size:1.1rem;font-weight:700;'>❄️ DV DMF Metrics</div>
      <div style='color:#cde8f6;font-size:0.72rem;margin-top:4px;'>Data Vault Quality Framework</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("### Filters")

    layer_opts = ["ALL", "HUB", "LNK", "SAT", "STAGING"]
    dv_layer_filter = st.selectbox("DV Layer", options=layer_opts, index=0)

    schema_opts = ["ALL", "SAL", "SAL_EXT", "ODS_STG"]
    schema_filter = st.selectbox("Schema", options=schema_opts, index=0)

    history_days = st.select_slider(
        "History window",
        options=[7, 14, 30, 60, 90],
        value=30,
        format_func=lambda x: f"{x} days",
    )

    st.divider()

    if st.button("↻  Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown(
        f"<div style='color:#8BA5B5;font-size:0.7rem;margin-top:8px;'>"
        f"Source: <code>SNOWFLAKE.LOCAL</code><br>"
        f"Coverage: <code>ACCOUNT_USAGE</code></div>",
        unsafe_allow_html=True,
    )


# =============================================================================
# LOAD DATA
# =============================================================================

status_df  = load_latest_status()
history_df = load_history(history_days)
coverage_df = load_coverage()

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if dv_layer_filter != "ALL":
        out = out[out["dv_layer"] == dv_layer_filter]
    if schema_filter != "ALL" and "table_schema" in out.columns:
        out = out[out["table_schema"] == schema_filter]
    return out

filtered_status  = apply_filters(status_df)
filtered_history = apply_filters(history_df)


# =============================================================================
# SHARED KPI ROW
# =============================================================================

def render_kpi_row(df: pd.DataFrame, context: str = ""):
    total   = len(df)
    passing = int((df["violated"] == False).sum()) if total > 0 else 0
    failing = int((df["violated"] == True).sum())  if total > 0 else 0
    pct     = round(passing * 100.0 / total, 1)    if total > 0 else 0.0
    label, color = health_label(pct, total)
    last_ts = df["last_checked"].max()              if total > 0 else None

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Checks Monitored", total)
    c2.metric("Passing", passing, delta=None)
    c3.metric("Failing", failing)
    with c4:
        st.markdown("**Health**")
        st.markdown(
            f"<p style='font-size:1.5rem;font-weight:700;color:{color};margin:0'>"
            f"{pct}%&nbsp;{label}</p>",
            unsafe_allow_html=True,
        )
    with c5:
        st.markdown("**Last Run**")
        val = pd.Timestamp(last_ts).strftime("%Y-%m-%d %H:%M") if last_ts is not None else "—"
        st.markdown(f"<p style='font-size:0.95rem;margin:0'>{val}</p>", unsafe_allow_html=True)


# =============================================================================
# ALTAIR v4 HELPERS
# =============================================================================

CHART_COLORS = {
    "RECON":       SF_BLUE,
    "DUPLICATE":   C_VIOLATE,
    "REFERENTIAL": C_WARN,
    "COUNTS":      SF_NAVY,
    "PASS":        C_PASS,
    "FAIL":        C_VIOLATE,
}

def make_time_series(df: pd.DataFrame, y_field: str, color_field: str,
                     title: str, height: int = 280) -> alt.Chart:
    color_scale = alt.Scale(
        domain=list(CHART_COLORS.keys()),
        range=list(CHART_COLORS.values()),
    )
    base = alt.Chart(df).encode(
        x=alt.X("scheduled_time:T", title=None),
        y=alt.Y(f"{y_field}:Q", title="Error Count"),
        color=alt.Color(f"{color_field}:N", scale=color_scale,
                        legend=alt.Legend(orient="bottom")),
        tooltip=[
            alt.Tooltip("scheduled_time:T", title="Run Time", format="%Y-%m-%d %H:%M"),
            alt.Tooltip(f"{color_field}:N", title=color_field.replace("_", " ").title()),
            alt.Tooltip(f"{y_field}:Q", title="Value"),
            alt.Tooltip("table_name:N", title="Table"),
        ],
    )
    return (
        base.mark_line(strokeWidth=2)
        + base.mark_point(size=50, filled=True)
    ).properties(title=title, height=height)


def make_bar_chart(df: pd.DataFrame, x_field: str, y_field: str,
                   color_field: str, title: str, height: int = 280) -> alt.Chart:
    color_scale = alt.Scale(
        domain=["PASS", "FAIL"],
        range=[C_PASS, C_VIOLATE],
    )
    return alt.Chart(df).mark_bar(cornerRadiusTopLeft=2, cornerRadiusTopRight=2).encode(
        x=alt.X(f"{x_field}:N", title=None, sort="-y"),
        y=alt.Y(f"{y_field}:Q", title="Count"),
        color=alt.Color(f"{color_field}:N", scale=color_scale,
                        legend=alt.Legend(orient="bottom")),
        tooltip=[
            alt.Tooltip(f"{x_field}:N", title=x_field.replace("_", " ").title()),
            alt.Tooltip(f"{y_field}:Q", title="Count"),
        ],
    ).properties(title=title, height=height)


def make_heatgrid(df: pd.DataFrame, x_field: str, y_field: str,
                  color_field: str, title: str, height: int = 320) -> alt.Chart:
    return alt.Chart(df).mark_rect(stroke="white", strokeWidth=0.5).encode(
        x=alt.X(f"{x_field}:O", title="Date"),
        y=alt.Y(f"{y_field}:N", title=None),
        color=alt.Color(
            f"{color_field}:Q",
            scale=alt.Scale(
                domain=[0, 1, 5, 20],
                range=["#EAF4FB", SF_BLUE, C_WARN, C_VIOLATE],
            ),
            title="Errors",
            legend=alt.Legend(orient="bottom"),
        ),
        tooltip=[
            alt.Tooltip(f"{x_field}:O", title="Date"),
            alt.Tooltip(f"{y_field}:N", title="Table"),
            alt.Tooltip(f"{color_field}:Q", title="Total Errors"),
        ],
    ).properties(title=title, height=height)


def render_check_table(df: pd.DataFrame, extra_cols: list = None):
    cols = ["dv_layer", "table_schema", "table_name", "metric_name",
            "expectation_name", "error_value", "status", "last_checked"]
    if extra_cols:
        cols = extra_cols
    disp = df[cols].copy()
    disp.columns = [c.replace("_", " ").title() for c in cols]
    st.dataframe(disp, use_container_width=True)


# =============================================================================
# PAGE HEADER
# =============================================================================

st.markdown(
    f"<h1 style='margin-bottom:0;color:{SF_DARK};'>❄️ Data Vault DMF Metrics</h1>",
    unsafe_allow_html=True,
)
st.caption("Snowflake-native Data Metric Function monitoring for Data Vault 2.0")
st.divider()


# =============================================================================
# TABS
# =============================================================================

(
    tab_overview,
    tab_counts,
    tab_recon,
    tab_referential,
    tab_dupe,
    tab_artefact,
    tab_growth,
    tab_coverage,
    tab_schedule,
) = st.tabs([
    "🏠 Overview",
    "📊 Basic Counts",
    "🔁 Reconciliation",
    "🔗 Referential Integrity",
    "🔍 Duplicate Checks",
    "🔬 Per Artefact",
    "📈 Growth Trends",
    "🗺️ DMF Coverage",
    "⏱️ Schedule Health",
])


# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — OVERVIEW
# ─────────────────────────────────────────────────────────────────────────────

with tab_overview:
    section_header("Overall Health")
    render_kpi_row(filtered_status)
    st.markdown("")

    if not filtered_status.empty:
        col_l, col_r = st.columns(2)

        with col_l:
            section_header("Health by DV Layer")
            layer_agg = (
                filtered_status.groupby("dv_layer")
                .agg(
                    total=("violated", "count"),
                    failing=("violated", "sum"),
                )
                .reset_index()
            )
            layer_agg["passing"] = layer_agg["total"] - layer_agg["failing"]
            layer_agg["pass_pct"] = (layer_agg["passing"] / layer_agg["total"] * 100).round(1)

            bar_data = pd.concat([
                layer_agg[["dv_layer", "passing"]].rename(columns={"passing": "count"}).assign(status="PASS"),
                layer_agg[["dv_layer", "failing"]].rename(columns={"failing": "count"}).assign(status="FAIL"),
            ])

            layer_chart = alt.Chart(bar_data).mark_bar(cornerRadiusTopLeft=2, cornerRadiusTopRight=2).encode(
                x=alt.X("dv_layer:N", title="Layer"),
                y=alt.Y("count:Q", title="Checks"),
                color=alt.Color("status:N",
                    scale=alt.Scale(domain=["PASS", "FAIL"], range=[C_PASS, C_VIOLATE]),
                    legend=alt.Legend(orient="bottom", title=None),
                ),
                tooltip=["dv_layer:N", "status:N", "count:Q"],
            ).properties(height=220)
            st.altair_chart(layer_chart, use_container_width=True)

        with col_r:
            section_header("Health by Check Type")
            type_agg = (
                filtered_status.groupby("check_type")
                .agg(total=("violated", "count"), failing=("violated", "sum"))
                .reset_index()
            )
            type_agg["passing"] = type_agg["total"] - type_agg["failing"]
            type_data = pd.concat([
                type_agg[["check_type", "passing"]].rename(columns={"passing": "count"}).assign(status="PASS"),
                type_agg[["check_type", "failing"]].rename(columns={"failing": "count"}).assign(status="FAIL"),
            ])
            type_chart = alt.Chart(type_data).mark_bar(cornerRadiusTopLeft=2, cornerRadiusTopRight=2).encode(
                x=alt.X("check_type:N", title="Check Type"),
                y=alt.Y("count:Q", title="Checks"),
                color=alt.Color("status:N",
                    scale=alt.Scale(domain=["PASS", "FAIL"], range=[C_PASS, C_VIOLATE]),
                    legend=alt.Legend(orient="bottom", title=None),
                ),
                tooltip=["check_type:N", "status:N", "count:Q"],
            ).properties(height=220)
            st.altair_chart(type_chart, use_container_width=True)

        failing_now = filtered_status[filtered_status["violated"] == True]
        if not failing_now.empty:
            section_header(f"⚠ Active Violations — {len(failing_now)} check(s) failing")
            render_check_table(failing_now)
        else:
            st.success("✅ All checks are currently passing.")

    else:
        st.info("No monitoring data available for the selected filters.")


# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 — BASIC COUNTS
# ─────────────────────────────────────────────────────────────────────────────

with tab_counts:
    section_header("Latest Check Results — All DMFs")
    render_kpi_row(filtered_status)
    st.markdown("")

    if not filtered_status.empty:
        summary = (
            filtered_status.groupby(["dv_layer", "table_schema", "table_name", "check_type"])
            .agg(
                total_checks=("violated", "count"),
                failing_checks=("violated", "sum"),
                max_error=("error_value", "max"),
                last_checked=("last_checked", "max"),
            )
            .reset_index()
        )
        summary["passing_checks"] = summary["total_checks"] - summary["failing_checks"]
        summary["status"] = summary["failing_checks"].apply(lambda x: "FAIL" if x > 0 else "PASS")
        summary["pass_pct"] = (summary["passing_checks"] / summary["total_checks"] * 100).round(1)

        col_l, col_r = st.columns([2, 1])

        with col_l:
            section_header("All Tables — Latest Status")
            disp = summary[[
                "dv_layer", "table_schema", "table_name", "check_type",
                "total_checks", "passing_checks", "failing_checks",
                "max_error", "pass_pct", "status", "last_checked",
            ]].copy()
            disp.columns = [
                "Layer", "Schema", "Table", "Check Type",
                "Total", "Passing", "Failing",
                "Max Error", "Pass %", "Status", "Last Checked",
            ]
            st.dataframe(disp, use_container_width=True)

        with col_r:
            section_header("By Layer")
            layer_agg = (
                summary.groupby("dv_layer")
                .agg(tables=("table_name", "nunique"), checks=("total_checks", "sum"),
                     failing=("failing_checks", "sum"))
                .reset_index()
            )
            for _, row in layer_agg.iterrows():
                pct = round((row["checks"] - row["failing"]) / row["checks"] * 100, 0) if row["checks"] > 0 else 0
                lbl, col = health_label(pct, int(row["checks"]))
                st.markdown(
                    f"<div style='background:#F4F6F8;border-left:4px solid {col};"
                    f"padding:8px 12px;margin-bottom:6px;border-radius:0 4px 4px 0;'>"
                    f"<b style='color:{SF_DARK}'>{row['dv_layer']}</b>"
                    f"<span style='float:right;color:{col};font-weight:700'>{pct:.0f}%</span><br>"
                    f"<small style='color:#566573'>{int(row['tables'])} tables · {int(row['checks'])} checks · {int(row['failing'])} failing</small>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

    else:
        st.info("No data for selected filters.")


# ─────────────────────────────────────────────────────────────────────────────
# TAB 3 — RECONCILIATION
# ─────────────────────────────────────────────────────────────────────────────

with tab_recon:
    recon_status  = filtered_status[filtered_status["check_type"] == "RECON"]
    recon_history = filtered_history[filtered_history["check_type"] == "RECON"]

    section_header("Reconciliation Checks — Staged → Target")
    st.markdown(
        "<small style='color:#566573'>Verifies that every staged surrogate key, business key, "
        "or hash-diff combination lands in the target Data Vault table after each load.</small>",
        unsafe_allow_html=True,
    )
    st.markdown("")

    if recon_status.empty:
        st.info("No reconciliation checks have run within the selected filters.")
    else:
        render_kpi_row(recon_status)
        st.markdown("")

        col_l, col_r = st.columns([3, 2])

        with col_l:
            section_header("Error Trend")
            if not recon_history.empty:
                chart_data = recon_history.copy()
                chart_data["scheduled_time"] = pd.to_datetime(chart_data["scheduled_time"])
                chart_data["label"] = chart_data["table_name"] + " / " + chart_data["expectation_name"]
                chart = make_time_series(
                    chart_data, y_field="error_value", color_field="label",
                    title=f"Recon errors — last {history_days} days"
                )
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("No history in selected window.")

        with col_r:
            section_header("Latest Status per Expectation")
            for _, row in recon_status.iterrows():
                col_icon = C_VIOLATE if row["violated"] else C_PASS
                icon = "✗" if row["violated"] else "✓"
                st.markdown(
                    f"<div style='background:#F4F6F8;border-left:4px solid {col_icon};"
                    f"padding:6px 10px;margin-bottom:5px;border-radius:0 4px 4px 0;font-size:0.82rem;'>"
                    f"<span style='color:{col_icon};font-weight:700'>{icon}</span> "
                    f"<b>{row['table_name']}</b><br>"
                    f"<small style='color:#566573'>{row['expectation_name']}</small><br>"
                    f"<small>Error count: <b>{int(row['error_value']) if pd.notna(row['error_value']) else '—'}</b> "
                    f"· {pd.Timestamp(row['last_checked']).strftime('%Y-%m-%d %H:%M')}</small>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

        section_header("Full Detail")
        render_check_table(recon_status)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 4 — REFERENTIAL INTEGRITY
# ─────────────────────────────────────────────────────────────────────────────

with tab_referential:
    orph_status  = filtered_status[filtered_status["check_type"] == "REFERENTIAL"]
    orph_history = filtered_history[filtered_history["check_type"] == "REFERENTIAL"]

    section_header("Referential Integrity — Orphan Checks")
    st.markdown(
        "<small style='color:#566573'>Checks that every LNK foreign key exists in its parent HUB, "
        "and every SAT foreign key exists in its parent HUB or LNK (GHOST records excluded).</small>",
        unsafe_allow_html=True,
    )
    st.markdown("")

    if orph_status.empty:
        st.info("No orphan checks have run for the selected filters.")
    else:
        render_kpi_row(orph_status)
        st.markdown("")

        col_l, col_r = st.columns([3, 2])

        with col_l:
            section_header("Orphan Error Trend")
            if not orph_history.empty:
                chart_data = orph_history.copy()
                chart_data["scheduled_time"] = pd.to_datetime(chart_data["scheduled_time"])
                chart_data["label"] = chart_data["dv_layer"] + " / " + chart_data["table_name"]
                chart = make_time_series(
                    chart_data, y_field="error_value", color_field="label",
                    title=f"Orphan errors — last {history_days} days"
                )
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("No history in selected window.")

        with col_r:
            section_header("Orphan Check Summary by Table")
            table_agg = (
                orph_status.groupby(["dv_layer", "table_name"])
                .agg(checks=("violated", "count"), failing=("violated", "sum"),
                     max_error=("error_value", "max"))
                .reset_index()
            )
            for _, row in table_agg.iterrows():
                status_ok = row["failing"] == 0
                col_bdr = C_PASS if status_ok else C_VIOLATE
                st.markdown(
                    f"<div style='background:#F4F6F8;border-left:4px solid {col_bdr};"
                    f"padding:6px 10px;margin-bottom:5px;border-radius:0 4px 4px 0;font-size:0.82rem;'>"
                    f"<b>{row['table_name']}</b> "
                    f"<span style='color:{SF_BLUE}'>[{row['dv_layer']}]</span><br>"
                    f"<small style='color:#566573'>{int(row['checks'])} FK checks · "
                    f"{int(row['failing'])} failing · max error: {int(row['max_error']) if pd.notna(row['max_error']) else '—'}</small>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

        section_header("Full Detail")
        render_check_table(orph_status)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 5 — DUPLICATE CHECKS
# ─────────────────────────────────────────────────────────────────────────────

with tab_dupe:
    dupe_status  = filtered_status[filtered_status["check_type"] == "DUPLICATE"]
    dupe_history = filtered_history[filtered_history["check_type"] == "DUPLICATE"]

    section_header("Duplicate Checks")
    st.markdown(
        "<small style='color:#566573'>Checks for duplicate surrogate keys, business key combinations, "
        "and hub FK combinations across HUB, LNK, and SAT tables.</small>",
        unsafe_allow_html=True,
    )
    st.markdown("")

    if dupe_status.empty:
        st.info("No duplicate checks have run for the selected filters.")
    else:
        render_kpi_row(dupe_status)
        st.markdown("")

        col_l, col_r = st.columns([3, 2])

        with col_l:
            section_header("Duplicate Error Trend")
            if not dupe_history.empty:
                chart_data = dupe_history.copy()
                chart_data["scheduled_time"] = pd.to_datetime(chart_data["scheduled_time"])
                chart_data["label"] = chart_data["table_name"] + " / " + chart_data["metric_name"]
                chart = make_time_series(
                    chart_data, y_field="error_value", color_field="label",
                    title=f"Duplicate errors — last {history_days} days"
                )
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("No history in selected window.")

        with col_r:
            section_header("Errors by Table")
            tbl_agg = (
                dupe_status.groupby(["dv_layer", "table_name"])
                .agg(checks=("violated", "count"), failing=("violated", "sum"),
                     max_error=("error_value", "max"))
                .reset_index()
            )
            for _, row in tbl_agg.iterrows():
                status_ok = row["failing"] == 0
                col_bdr = C_PASS if status_ok else C_VIOLATE
                icon = "✓" if status_ok else "✗"
                st.markdown(
                    f"<div style='background:#F4F6F8;border-left:4px solid {col_bdr};"
                    f"padding:6px 10px;margin-bottom:5px;border-radius:0 4px 4px 0;font-size:0.82rem;'>"
                    f"<span style='color:{col_bdr};font-weight:700'>{icon}</span> "
                    f"<b>{row['table_name']}</b> "
                    f"<span style='color:{SF_BLUE}'>[{row['dv_layer']}]</span><br>"
                    f"<small style='color:#566573'>{int(row['checks'])} checks · "
                    f"{int(row['failing'])} failing</small>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

        section_header("Full Detail")
        render_check_table(dupe_status)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 6 — PER ARTEFACT
# ─────────────────────────────────────────────────────────────────────────────

with tab_artefact:
    section_header("Per Artefact Analysis")
    st.markdown(
        "<small style='color:#566573'>Select a monitored table to see its complete DMF history "
        "across all check types.</small>",
        unsafe_allow_html=True,
    )
    st.markdown("")

    if status_df.empty:
        st.info("No data available.")
    else:
        all_tables = sorted(status_df["table_name"].unique().tolist())
        selected_table = st.selectbox("Select table", options=all_tables)

        if selected_table:
            tbl_status  = status_df[status_df["table_name"] == selected_table]
            tbl_history = history_df[history_df["table_name"] == selected_table]

            schema_name = tbl_status["table_schema"].iloc[0] if not tbl_status.empty else "—"
            db_name     = tbl_status["table_database"].iloc[0] if not tbl_status.empty else "—"
            layer       = tbl_status["dv_layer"].iloc[0] if not tbl_status.empty else "—"

            total   = len(tbl_status)
            failing = int(tbl_status["violated"].sum()) if total > 0 else 0
            pct     = round((total - failing) / total * 100, 1) if total > 0 else 0.0
            lbl, col = health_label(pct, total)

            st.markdown(
                f"<div style='background:linear-gradient(90deg,{SF_DARK},{SF_NAVY});"
                f"color:white;padding:10px 16px;border-radius:6px;margin-bottom:12px;'>"
                f"<b style='font-size:1.1rem'>{selected_table}</b>&nbsp;"
                f"<span style='background:{SF_BLUE};border-radius:10px;padding:2px 10px;"
                f"font-size:0.75rem'>{layer}</span><br>"
                f"<small style='color:#cde8f6'>{db_name}.{schema_name} · "
                f"{total} checks · "
                f"<span style='color:{col};font-weight:700'>{pct}% {lbl}</span></small>"
                f"</div>",
                unsafe_allow_html=True,
            )

            if not tbl_history.empty:
                col_l, col_r = st.columns([3, 2])

                with col_l:
                    section_header("Error Value Over Time (all checks)")
                    chart_data = tbl_history.copy()
                    chart_data["scheduled_time"] = pd.to_datetime(chart_data["scheduled_time"])
                    chart_data["label"] = chart_data["metric_name"] + " / " + chart_data["expectation_name"]
                    chart = make_time_series(
                        chart_data, y_field="error_value", color_field="label",
                        title=selected_table, height=300
                    )
                    st.altair_chart(chart, use_container_width=True)

                with col_r:
                    section_header("Check Type Breakdown")
                    type_agg = tbl_history.groupby("check_type").agg(
                        runs=("violated", "count"),
                        violations=("violated", "sum"),
                    ).reset_index()
                    for _, row in type_agg.iterrows():
                        col_bdr = C_VIOLATE if row["violations"] > 0 else C_PASS
                        st.markdown(
                            f"<div style='background:#F4F6F8;border-left:4px solid {col_bdr};"
                            f"padding:6px 10px;margin-bottom:5px;border-radius:0 4px 4px 0;'>"
                            f"<b>{row['check_type']}</b><br>"
                            f"<small style='color:#566573'>{int(row['runs'])} runs · "
                            f"{int(row['violations'])} violations</small>"
                            f"</div>",
                            unsafe_allow_html=True,
                        )
            else:
                st.info(f"No run history found for **{selected_table}** in the last {history_days} days.")

            section_header("All Expectations — Latest Status")
            render_check_table(tbl_status)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 7 — GROWTH TRENDS
# ─────────────────────────────────────────────────────────────────────────────

with tab_growth:
    section_header("Growth Trends — Error Heatgrid")
    st.markdown(
        "<small style='color:#566573'>Shows total error counts per table per day. "
        "Navy = clean (0 errors), amber = elevated, maroon = violations present.</small>",
        unsafe_allow_html=True,
    )
    st.markdown("")

    if filtered_history.empty:
        st.info("No history data for selected filters.")
    else:
        heat_data = (
            filtered_history.groupby(["table_name", "check_date"])
            .agg(
                total_errors=("error_value", "sum"),
                violation_count=("violated", "sum"),
                run_count=("violated", "count"),
            )
            .reset_index()
        )
        heat_data["check_date"] = heat_data["check_date"].astype(str)

        section_header("Error Count Heatgrid (Table × Day)")
        chart = make_heatgrid(
            heat_data, x_field="check_date", y_field="table_name",
            color_field="total_errors",
            title=f"Error count per table per day — last {history_days} days",
            height=max(250, len(heat_data["table_name"].unique()) * 22),
        )
        st.altair_chart(chart, use_container_width=True)

        st.markdown("")
        section_header("Daily Violation Run Count")

        daily = (
            filtered_history.groupby("check_date")
            .agg(
                total_runs=("violated", "count"),
                violations=("violated", "sum"),
            )
            .reset_index()
        )
        daily["check_date"] = pd.to_datetime(daily["check_date"])
        melted = daily.melt(
            id_vars=["check_date"],
            value_vars=["total_runs", "violations"],
            var_name="series",
            value_name="count",
        )
        melted["series"] = melted["series"].map(
            {"total_runs": "Total Runs", "violations": "Violations"}
        )
        line_chart = (
            alt.Chart(melted).mark_line(strokeWidth=2).encode(
                x=alt.X("check_date:T", title=None),
                y=alt.Y("count:Q", title="Count"),
                color=alt.Color("series:N",
                    scale=alt.Scale(domain=["Total Runs", "Violations"],
                                    range=[SF_BLUE, C_VIOLATE]),
                    legend=alt.Legend(orient="bottom", title=None),
                ),
                tooltip=[
                    alt.Tooltip("check_date:T", title="Date", format="%Y-%m-%d"),
                    alt.Tooltip("series:N", title="Series"),
                    alt.Tooltip("count:Q", title="Count"),
                ],
            )
            + alt.Chart(melted).mark_point(size=50, filled=True).encode(
                x="check_date:T",
                y="count:Q",
                color=alt.Color("series:N",
                    scale=alt.Scale(domain=["Total Runs", "Violations"],
                                    range=[SF_BLUE, C_VIOLATE]),
                ),
            )
        ).properties(title="Daily Check Runs vs Violations", height=250)
        st.altair_chart(line_chart, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 8 — DMF COVERAGE
# ─────────────────────────────────────────────────────────────────────────────

with tab_coverage:
    section_header("DMF Coverage Map")
    st.markdown(
        "<small style='color:#566573'>Shows which tables have Data Metric Functions attached "
        "(from ACCOUNT_USAGE — up to 3h latency). Identifies monitoring gaps.</small>",
        unsafe_allow_html=True,
    )
    st.markdown("")

    cov = apply_filters(coverage_df) if not coverage_df.empty else coverage_df

    if cov.empty:
        st.info("Coverage data not yet available (ACCOUNT_USAGE latency may apply).")
    else:
        c1, c2, c3, c4 = st.columns(4)
        tables_monitored = cov["table_name"].nunique()
        dmf_attachments  = len(cov)
        started          = int((cov["schedule_status"] == "STARTED").sum())
        layer_cnt        = cov["dv_layer"].nunique()

        c1.metric("Tables Monitored", tables_monitored)
        c2.metric("DMF Attachments", dmf_attachments)
        c3.metric("Active (STARTED)", started)
        c4.metric("DV Layers Covered", layer_cnt)
        st.markdown("")

        col_l, col_r = st.columns([2, 1])

        with col_l:
            section_header("Coverage by Layer & Check Type")
            pivot = (
                cov.groupby(["dv_layer", "check_type"])
                .size()
                .reset_index(name="attachment_count")
            )
            hm = alt.Chart(pivot).mark_rect(stroke="white", strokeWidth=1).encode(
                x=alt.X("check_type:N", title="Check Type"),
                y=alt.Y("dv_layer:N", title="Layer"),
                color=alt.Color("attachment_count:Q",
                    scale=alt.Scale(domain=[0, 5, 20],
                                    range=["#EAF4FB", SF_BLUE, SF_DARK]),
                    title="Attachments",
                    legend=alt.Legend(orient="bottom"),
                ),
                tooltip=["dv_layer:N", "check_type:N", "attachment_count:Q"],
            ).properties(height=200, title="DMF attachments per layer × check type")
            st.altair_chart(hm, use_container_width=True)

        with col_r:
            section_header("Attachments by Layer")
            layer_agg = (
                cov.groupby("dv_layer")
                .agg(tables=("table_name", "nunique"), attachments=("metric_name", "count"))
                .reset_index()
            )
            for _, row in layer_agg.iterrows():
                st.markdown(
                    f"<div style='background:#F4F6F8;border-left:4px solid {SF_BLUE};"
                    f"padding:8px 12px;margin-bottom:6px;border-radius:0 4px 4px 0;'>"
                    f"<b style='color:{SF_DARK}'>{row['dv_layer']}</b><br>"
                    f"<small style='color:#566573'>{int(row['tables'])} tables · "
                    f"{int(row['attachments'])} DMF attachments</small>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

        section_header("Full Coverage Detail")
        disp = cov[["dv_layer", "table_schema", "table_name", "metric_name",
                     "check_type", "schedule_status"]].copy()
        disp.columns = ["Layer", "Schema", "Table", "DMF", "Check Type", "Status"]
        st.dataframe(disp, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 9 — SCHEDULE HEALTH
# ─────────────────────────────────────────────────────────────────────────────

with tab_schedule:
    section_header("Schedule Health")
    st.markdown(
        "<small style='color:#566573'>Analyses run frequency per expectation. "
        "Identifies DMFs that are overdue or running less frequently than expected.</small>",
        unsafe_allow_html=True,
    )
    st.markdown("")

    if history_df.empty:
        st.info("No run history available.")
    else:
        sched_agg = (
            history_df.groupby(["table_name", "table_schema", "dv_layer",
                                 "metric_name", "expectation_name"])
            .agg(
                run_count=("scheduled_time", "count"),
                first_run=("scheduled_time", "min"),
                last_run=("scheduled_time", "max"),
                violations=("violated", "sum"),
            )
            .reset_index()
        )
        sched_agg["days_since_last"] = (
            pd.Timestamp("now", tz="UTC").tz_localize(None)
            - pd.to_datetime(sched_agg["last_run"]).dt.tz_localize(None)
        ).dt.days.fillna(999).astype(int)

        sched_agg["runs_per_day"] = (
            sched_agg["run_count"] / history_days
        ).round(2)

        sched_agg["health"] = sched_agg.apply(
            lambda r: "STALE" if r["days_since_last"] > 2
            else ("WARN" if r["runs_per_day"] < 0.5 else "OK"),
            axis=1,
        )

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Expectations Tracked", len(sched_agg))
        c2.metric("Running OK", int((sched_agg["health"] == "OK").sum()))
        stale_cnt = int((sched_agg["health"] == "STALE").sum())
        c3.metric("Stale / Overdue", stale_cnt)
        st.markdown("")

        col_l, col_r = st.columns([3, 2])

        with col_l:
            section_header("Run Frequency per Expectation")
            freq_chart = alt.Chart(sched_agg).mark_bar(
                cornerRadiusTopLeft=2, cornerRadiusTopRight=2
            ).encode(
                x=alt.X("runs_per_day:Q", title="Avg Runs / Day"),
                y=alt.Y("expectation_name:N", title=None, sort="-x"),
                color=alt.Color("dv_layer:N",
                    scale=alt.Scale(
                        domain=["HUB", "LNK", "SAT", "STAGING", "OTHER"],
                        range=[SF_BLUE, SF_NAVY, C_WARN, C_PASS, C_NODATA],
                    ),
                    legend=alt.Legend(orient="bottom", title="Layer"),
                ),
                tooltip=[
                    "expectation_name:N", "table_name:N", "runs_per_day:Q",
                    "run_count:Q", "last_run:T",
                ],
            ).properties(height=max(200, len(sched_agg) * 18), title="Average runs per day")
            st.altair_chart(freq_chart, use_container_width=True)

        with col_r:
            section_header("Overdue / Stale Checks")
            stale = sched_agg[sched_agg["health"] == "STALE"].sort_values("days_since_last", ascending=False)
            if stale.empty:
                st.success("All checks ran within the last 2 days.")
            else:
                for _, row in stale.iterrows():
                    st.markdown(
                        f"<div style='background:#F4F6F8;border-left:4px solid {C_VIOLATE};"
                        f"padding:6px 10px;margin-bottom:5px;border-radius:0 4px 4px 0;font-size:0.82rem;'>"
                        f"<b>{row['table_name']}</b><br>"
                        f"<small style='color:#566573'>{row['expectation_name']}</small><br>"
                        f"<small>Last run: <b>{int(row['days_since_last'])} days ago</b> · "
                        f"{int(row['run_count'])} runs total</small>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

        section_header("Full Schedule Detail")
        disp = sched_agg[[
            "dv_layer", "table_schema", "table_name", "metric_name",
            "expectation_name", "run_count", "runs_per_day",
            "violations", "days_since_last", "last_run", "health",
        ]].copy()
        disp.columns = [
            "Layer", "Schema", "Table", "DMF",
            "Expectation", "Runs", "Runs/Day",
            "Violations", "Days Since Last", "Last Run", "Health",
        ]
        st.dataframe(disp, use_container_width=True)
