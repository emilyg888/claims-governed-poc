"""UI components for the Streamlit controls dashboard."""

from __future__ import annotations

import pandas as pd
import streamlit as st


def render_run_summary(batch_date: str, run_id: str, frame: pd.DataFrame) -> None:
    """Render top KPI summary cards for the selected run."""
    total_controls = len(frame)
    failed = int(frame["STATUS"].isin(["FAIL", "ERROR"]).sum()) if not frame.empty else 0
    blocking_failed = int(
        (
            frame["STATUS"].isin(["FAIL", "ERROR"])
            & frame["BLOCKING_FLAG"].fillna(False).astype(bool)
        ).sum()
    ) if not frame.empty else 0
    gate_status = "FAIL" if blocking_failed > 0 else "PASS"

    st.subheader("Run Summary")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Batch Date", batch_date)
    c2.metric("Total Controls", total_controls)
    c3.metric("Failed", failed)
    c4.metric("Blocking Failed", blocking_failed)
    c5.metric("Gate Status", gate_status)
    st.caption(f"Run ID: {run_id}")


def render_control_results_table(frame: pd.DataFrame) -> None:
    """Render sortable control result table."""
    st.subheader("Control Results")
    if frame.empty:
        st.info("No control rows matched current filters.")
        return
    display_columns = [
        "RUN_ID",
        "CONTROL_ID",
        "DESCRIPTION",
        "CONTROL_TYPE",
        "SEVERITY",
        "STATUS",
        "TOTAL_RECORD_COUNT",
        "FAILED_RECORD_COUNT",
        "PASSED_RECORD_COUNT",
        "FAIL_COUNT",
        "VARIANCE",
        "DETAILS",
        "BLOCKING_FLAG",
        "EXECUTED_AT",
        "EXECUTED_SQL_HASH",
    ]
    available = [col for col in display_columns if col in frame.columns]
    ordered = frame.sort_values(
        by=["BLOCKING_FLAG", "STATUS", "CONTROL_ID"],
        ascending=[False, True, True],
        na_position="last",
    )
    st.dataframe(
        _highlight_failures(ordered[available]),
        use_container_width=True,
        hide_index=True,
    )


def render_trend_section(frame: pd.DataFrame) -> None:
    """Render control failure and variance trends."""
    st.subheader("Control Trend (Last 7 Batches)")
    if frame.empty:
        st.info("Trend data is empty.")
        return

    failures = (
        frame.assign(IS_FAIL=frame["STATUS"].isin(["FAIL", "ERROR"]).astype(int))
        .groupby("BATCH_DATE", as_index=False)["IS_FAIL"]
        .sum()
        .rename(columns={"IS_FAIL": "FAIL_COUNT"})
        .sort_values("BATCH_DATE")
    )
    st.caption("Failures over time")
    st.line_chart(failures.set_index("BATCH_DATE")["FAIL_COUNT"])

    blocking = (
        frame.assign(
            IS_BLOCK_FAIL=(
                frame["STATUS"].isin(["FAIL", "ERROR"])
                & frame["BLOCKING_FLAG"].fillna(False).astype(bool)
            ).astype(int)
        )
        .groupby("BATCH_DATE", as_index=False)["IS_BLOCK_FAIL"]
        .sum()
        .rename(columns={"IS_BLOCK_FAIL": "BLOCKING_FAIL_COUNT"})
        .sort_values("BATCH_DATE")
    )
    st.caption("Blocking failures over time")
    st.line_chart(blocking.set_index("BATCH_DATE")["BLOCKING_FAIL_COUNT"])

    if "VARIANCE" in frame.columns and frame["VARIANCE"].notna().any():
        variance = (
            frame.groupby("BATCH_DATE", as_index=False)["VARIANCE"]
            .sum(min_count=1)
            .sort_values("BATCH_DATE")
        )
        st.caption("Variance trend")
        st.line_chart(variance.set_index("BATCH_DATE")["VARIANCE"])


def render_failure_details_panel(frame: pd.DataFrame) -> None:
    """Render drill-down details for one selected control."""
    st.subheader("Failure Details")
    if frame.empty:
        st.info("No control rows available for drill-down.")
        return

    failed = frame[frame["STATUS"].astype(str).str.upper().isin(["FAIL", "ERROR"])].copy()
    if failed.empty:
        st.info("No failed controls in this run.")
        return

    failed["FAIL_COUNT_SORT"] = pd.to_numeric(failed.get("FAIL_COUNT"), errors="coerce").fillna(0)
    failed = failed.sort_values(["FAIL_COUNT_SORT", "CONTROL_ID"], ascending=[False, True])
    options = failed["CONTROL_ID"].dropna().unique().tolist()
    selected = st.selectbox("Inspect Failed Control", options, key="inspect_control")
    details = failed[failed["CONTROL_ID"] == selected].drop(columns=["FAIL_COUNT_SORT"], errors="ignore")

    cols = [
        col
        for col in [
            "RUN_ID",
            "BATCH_DATE",
            "CONTROL_ID",
            "STATUS",
            "TOTAL_RECORD_COUNT",
            "FAILED_RECORD_COUNT",
            "PASSED_RECORD_COUNT",
            "FAIL_COUNT",
            "VARIANCE",
            "DETAILS",
            "EXECUTED_SQL_HASH",
        ]
        if col in details.columns
    ]
    st.dataframe(details[cols], use_container_width=True, hide_index=True)


def _highlight_failures(frame: pd.DataFrame):
    def _row_style(row: pd.Series) -> list[str]:
        styles = [""] * len(row)
        status = str(row.get("STATUS", "")).upper()
        if status in {"FAIL", "ERROR"}:
            base_style = "color: #cc0000"
            styles = [base_style] * len(row)

            # Emphasize BLOCKING_FLAG cell to make the checkbox/indicator stand out in red.
            if "BLOCKING_FLAG" in row.index:
                idx = list(row.index).index("BLOCKING_FLAG")
                styles[idx] = "color: #cc0000; font-weight: 700"
        return styles

    return frame.style.apply(_row_style, axis=1)
