from __future__ import annotations
import sys
import os
import subprocess
import time
from pathlib import Path

# Ensure src is in sys.path for local imports
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..", "src"))
)

import streamlit as st

from garmin_data_hub.paths import ensure_app_dirs, default_db_path, schema_sql_path
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db import queries
from garmin_data_hub.analytics.post_sync_refresh import refresh_post_sync_tables

st.set_page_config(page_title="Garmin Sync", layout="wide")
st.header("Garmin Sync  (garmin-givemydata)")

# Ensure dirs and app-internal schema once
ensure_app_dirs()
db_path = default_db_path()
conn = connect_sqlite(db_path)
apply_schema(conn, schema_sql_path())
conn.close()

st.caption(f"DB: {db_path}")


def _load_metrics_diagnostics() -> dict:
    conn = connect_sqlite(db_path)
    try:
        apply_schema(conn, schema_sql_path())
        return queries.get_activity_metrics_diagnostics(conn)
    finally:
        conn.close()


diag = _load_metrics_diagnostics()
col_d1, col_d2, col_d3 = st.columns(3)
col_d1.metric("Activities", f"{diag.get('total_activities', 0):,}")
col_d2.metric("Metrics Rows", f"{diag.get('total_metrics_rows', 0):,}")
col_d3.metric("Missing Metrics", f"{diag.get('missing_metrics_count', 0):,}")

last_refresh_utc = diag.get("last_refresh_utc")
if last_refresh_utc:
    st.caption(f"Last derived-metrics refresh: {last_refresh_utc}")
else:
    st.caption("Last derived-metrics refresh: not recorded yet")

repair_disabled = st.session_state.get("proc") is not None
if st.button(
    "Repair Derived Metrics",
    help="Rebuild persisted activity_metrics from activity + trackpoint tables.",
    disabled=repair_disabled,
):
    with st.spinner("Refreshing derived metrics..."):
        conn = connect_sqlite(db_path)
        try:
            apply_schema(conn, schema_sql_path())
            refresh_summary = refresh_post_sync_tables(conn)
        finally:
            conn.close()

    if refresh_summary.get("errors", 0) > 0:
        st.error(
            "Derived metrics refresh encountered errors. Check logs and retry after sync."
        )
    else:
        st.success(
            "Derived metrics refreshed: "
            f"targets={refresh_summary.get('target_activities', 0)}, "
            f"upserted={refresh_summary.get('rows_upserted', 0)}, "
            f"zones={refresh_summary.get('zones_updated', 0)}"
        )
    st.rerun()

st.divider()

if "proc" not in st.session_state:
    st.session_state.proc = None
    st.session_state.log = ""
    st.session_state.start_ts = None


def _progress_from_log(log_text: str) -> float:
    """Heuristic progress based on garmin-givemydata CLI messages."""
    lt = log_text.lower()
    checkpoints = [
        ("garmin data sync", 0.05),
        ("fetching", 0.15),
        ("activities", 0.40),
        ("health", 0.65),
        ("app schema applied", 0.90),
        ("sync complete", 1.0),
    ]
    prog = 0.05 if log_text.strip() else 0.0
    for marker, val in checkpoints:
        if marker in lt:
            prog = max(prog, val)
    return min(max(prog, 0.0), 1.0)


col_inputs, col_actions = st.columns([2, 1])
with col_inputs:
    st.caption(
        "Login happens in the external browser window opened by garmin-givemydata."
    )
    days = st.number_input(
        "Days to sync (leave 0 for garmin-givemydata default)",
        min_value=0,
        max_value=3650,
        value=0,
        step=1,
    )

with col_actions:
    start_disabled = st.session_state.proc is not None
    if st.button("Run sync", type="primary", disabled=start_disabled):
        if getattr(sys, "frozen", False):
            exe_dir = Path(sys.executable).parent
            cli_exe = exe_dir / "cli_backup_ingest.exe"
            if not cli_exe.exists():
                st.error(f"Could not find 'cli_backup_ingest.exe' in: {exe_dir}")
                st.stop()
            cmd = [str(cli_exe)]
        else:
            cli_script = Path(__file__).resolve().parents[2] / "cli_backup_ingest.py"
            cmd = [sys.executable, "-u", str(cli_script)]

        cmd.extend(["--db", str(db_path)])
        if days and int(days) > 0:
            cmd.extend(["--days", str(int(days))])
        cmd.append("--visible")
        cmd.append("--chrome")

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        st.session_state.proc = subprocess.Popen(
            cmd,
            stdout=None,
            stderr=None,
            env=env,
        )

        st.session_state.log = (
            "Started: "
            + " ".join(cmd)
            + "\nLive sync logs are written to the terminal running Streamlit."
        )
        st.session_state.start_ts = time.time()
        st.rerun()

    if st.button("Stop", disabled=st.session_state.proc is None):
        try:
            st.session_state.proc.terminate()
            st.session_state.log += "\nTerminated process."
        except Exception as e:
            st.session_state.log += f"\nTerminate failed: {e}"
        st.session_state.proc = None
        st.session_state.start_ts = None
        st.rerun()

# Progress and log display
prog_placeholder = st.empty()

proc = st.session_state.proc
if proc is not None:
    return_code = proc.poll()

    pct = _progress_from_log(st.session_state.get("log", ""))
    elapsed = None
    if st.session_state.start_ts:
        elapsed = time.time() - st.session_state.start_ts
    label = f"Running… ({pct*100:.0f}%)"
    if elapsed is not None:
        label += f" • {elapsed:0.0f}s"
    prog_placeholder.progress(pct, text=label)
    st.info("Sync is running. Open the terminal window for live output.")
    st.code(st.session_state.get("log", ""))
    if st.button("Refresh status", key="refresh_sync_status"):
        st.rerun()

    if return_code is not None:
        if f"Process exited with code {return_code}" not in st.session_state.log:
            st.session_state.log += f"\nProcess exited with code {return_code}"
        st.session_state.proc = None
        st.session_state.start_ts = None
        prog_placeholder.empty()
else:
    st.info(
        "Ready to sync. Click **Run sync** and complete login in the external browser window."
    )
    st.code(st.session_state.get("log", ""))

st.divider()
st.subheader("How the sync works")
st.write(
    """
    **Garmin Sync Process:**

    1. **Browser Login** — A visible browser window opens for you to log into your Garmin account and authorize the sync.
    2. **Fetch Latest Data** — Once authorized, your latest activities, health metrics, and training data are downloaded from Garmin Connect.
    3. **Store in Local DB** — All data is stored in the local SQLite database to preserve history and enable offline analysis.
    4. **Extract Trackpoints** — For new and changed activities, detailed GPS trackpoint data is extracted from FIT files and stored separately for mapping and analysis.
    5. **Schema Applied** — Database schema is applied automatically to ensure all tables and columns are up-to-date.
    6. **Ready to Analyze** — Once complete, navigate to other pages (Activities, Charts, Build Plan, etc.) to view and analyze your data.

    **Notes:**
    - **First-time setup takes longer** — The initial sync downloads all your historical data from Garmin. Depending on how much data you have, this can take 5–30 minutes. Subsequent syncs are much faster.
    - **MFA Code via Email** — If your Garmin account has two-factor authentication enabled, you will be prompted to enter a code sent to your registered email address. Check your inbox during the login process and have the code ready.
    - The sync runs with **browser visible** and **Chrome engine** to ensure MFA works reliably.
    - Specify **Days to sync** to limit the date range; leave at 0 for the default (recent data only).
    - The log shows real-time progress; watch it to confirm data is being fetched correctly.
    """
)
