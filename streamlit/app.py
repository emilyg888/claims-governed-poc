"""Production-style controls dashboard for claims data pipeline."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from components import (
    render_control_results_table,
    render_failure_details_panel,
    render_run_summary,
    render_trend_section,
)
from db import load_control_metadata, load_control_results, load_runs


st.set_page_config(
    page_title="Claims Data Control Dashboard",
    page_icon="📊",
    layout="wide",
)
st.title("📊 Claims Data Control Dashboard")


@st.cache_data(ttl=60)
def _runs() -> pd.DataFrame:
    return load_runs()


@st.cache_data(ttl=60)
def _results_for_run(run_id: str) -> pd.DataFrame:
    return load_control_results(run_id=run_id)


@st.cache_data(ttl=300)
def _results_last_n(last_n: int) -> pd.DataFrame:
    return load_control_results(last_n=last_n)


@st.cache_data(ttl=300)
def _metadata() -> pd.DataFrame:
    return load_control_metadata()


runs = _runs()
if runs.empty:
    st.warning("No run data found in CTRL.RUN_AUDIT.")
    st.stop()

run_options = runs["RUN_ID"].tolist()
run_labels = {
    row["RUN_ID"]: (
        f"{row['RUN_ID']} | {row['BATCH_DATE']} | {row.get('STATUS', 'UNKNOWN')} | "
        f"{row.get('START_TS', '')}"
    )
    for _, row in runs.iterrows()
}
selected_run = st.selectbox(
    "Run Selector",
    run_options,
    format_func=lambda run_id: run_labels.get(run_id, str(run_id)),
)

selected_run_row = runs[runs["RUN_ID"] == selected_run].iloc[0]
run_controls = _results_for_run(selected_run)
metadata = _metadata()

if run_controls.empty:
    st.warning("No control evidence found for selected run.")
    st.stop()

frame = run_controls.merge(metadata, on="CONTROL_ID", how="left")
if "REGISTER_SEVERITY" in frame.columns:
    frame["SEVERITY"] = frame["SEVERITY"].fillna(frame["REGISTER_SEVERITY"])
if "REGISTER_BLOCKING" in frame.columns:
    frame["BLOCKING_FLAG"] = frame["BLOCKING_FLAG"].fillna(frame["REGISTER_BLOCKING"])
if "BLOCKING_FLAG" not in frame.columns:
    frame["BLOCKING_FLAG"] = frame["SEVERITY"].fillna("").eq("BLOCK")

for col in ("SEVERITY", "STATUS", "CONTROL_TYPE"):
    if col in frame.columns:
        frame[col] = frame[col].astype(str).str.upper()

for col in ("TOTAL_COUNT", "FAIL_COUNT", "VARIANCE"):
    if col in frame.columns:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")

frame["TOTAL_RECORD_COUNT"] = frame["TOTAL_COUNT"] if "TOTAL_COUNT" in frame.columns else pd.NA
frame["FAILED_RECORD_COUNT"] = frame["FAIL_COUNT"] if "FAIL_COUNT" in frame.columns else pd.NA
frame["PASSED_RECORD_COUNT"] = pd.NA
if "TOTAL_RECORD_COUNT" in frame.columns and "FAILED_RECORD_COUNT" in frame.columns:
    valid = frame["TOTAL_RECORD_COUNT"].notna() & frame["FAILED_RECORD_COUNT"].notna()
    frame.loc[valid, "PASSED_RECORD_COUNT"] = (
        frame.loc[valid, "TOTAL_RECORD_COUNT"] - frame.loc[valid, "FAILED_RECORD_COUNT"]
    )

severity_values = sorted([item for item in frame["SEVERITY"].dropna().unique().tolist() if item and item != "NAN"])
status_values = sorted([item for item in frame["STATUS"].dropna().unique().tolist() if item and item != "NAN"])

c1, c2, c3 = st.columns(3)
with c1:
    severity_filter = st.multiselect("Severity Filter", severity_values, default=severity_values)
with c2:
    status_filter = st.multiselect("Status Filter", status_values, default=status_values)
with c3:
    control_filter = st.multiselect(
        "Control Filter",
        sorted(frame["CONTROL_ID"].dropna().unique().tolist()),
        default=sorted(frame["CONTROL_ID"].dropna().unique().tolist()),
    )

filtered = frame[
    frame["SEVERITY"].isin(severity_filter)
    & frame["STATUS"].isin(status_filter)
    & frame["CONTROL_ID"].isin(control_filter)
]

st.divider()
render_run_summary(
    batch_date=str(selected_run_row["BATCH_DATE"]),
    run_id=str(selected_run),
    frame=frame,
)
st.divider()
render_control_results_table(filtered)

trend_data = _results_last_n(7).merge(metadata, on="CONTROL_ID", how="left")
if "REGISTER_BLOCKING" in trend_data.columns:
    trend_data["BLOCKING_FLAG"] = trend_data["BLOCKING_FLAG"].fillna(trend_data["REGISTER_BLOCKING"])
if "BLOCKING_FLAG" not in trend_data.columns:
    trend_data["BLOCKING_FLAG"] = trend_data["SEVERITY"].fillna("").eq("BLOCK")
trend_data["STATUS"] = trend_data["STATUS"].astype(str).str.upper()

st.divider()
render_trend_section(trend_data)
st.divider()
render_failure_details_panel(frame)
