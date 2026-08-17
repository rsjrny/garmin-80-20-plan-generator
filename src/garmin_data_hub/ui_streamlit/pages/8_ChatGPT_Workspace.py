from __future__ import annotations

from datetime import date

import streamlit as st

from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import default_db_path, ensure_app_dirs, schema_sql_path
from garmin_data_hub.services.athlete_metrics_service import get_athlete_metrics
from garmin_data_hub.services.plan_persistence import load_plan_settings
from garmin_data_hub.ui_streamlit.plan_exchange_panel import render_plan_exchange_panel
from garmin_data_hub.ui_streamlit.sidebar import render_sidebar


def _setting_date(value: object, fallback: date) -> date:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return fallback


def _setting_int(value: object, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


st.set_page_config(page_title="ChatGPT Workspace", layout="wide")

ensure_app_dirs()
db_path = default_db_path()
conn = connect_sqlite(db_path)
try:
    apply_schema(conn, schema_sql_path())
    render_sidebar(conn)
finally:
    conn.close()

settings = load_plan_settings(db_path)
metrics = get_athlete_metrics(db_path)
today = date.today()
plan_start = _setting_date(settings.get("plan_start_date"), today)
event_date = _setting_date(settings.get("plan_event_date"), today)
age = _setting_int(settings.get("plan_age"), 50)
run_days = _setting_int(settings.get("plan_run_days"), 5)
sodium = _setting_int(settings.get("plan_sodium"), 0)
hrmax = metrics.get("hrmax_effective")
lthr = metrics.get("lthr_effective")

st.header("ChatGPT Workspace")
st.caption(
    "Create and import an AI-assisted training plan without an API key. "
    "Your Garmin database stays local; only the coaching packet you download "
    "and choose to upload leaves the app."
)

step_one, step_two, step_three = st.columns(3)
step_one.markdown("**1. Prepare**  \nReview the saved Build Plan settings.")
step_two.markdown("**2. Ask ChatGPT**  \nDownload the packet and open ChatGPT.")
step_three.markdown("**3. Approve**  \nUpload, review, and save the response.")

st.subheader("Current request context")
context_cols = st.columns(4)
context_cols[0].metric("Plan start", plan_start.isoformat())
context_cols[1].metric("Event date", event_date.isoformat())
context_cols[2].metric("Distance", str(settings.get("plan_distance", "50K")))
context_cols[3].metric("Run days/week", run_days)
st.caption(
    f"Long run: {settings.get('plan_long_run_day', 'Saturday')} · "
    f"HRmax: {hrmax if hrmax is not None else 'not set'} · "
    f"LTHR: {lthr if lthr is not None else 'not set'} · "
    f"Sodium: {sodium if sodium > 0 else 'not set'} mg/hour"
)

action_left, action_right = st.columns(2)
with action_left:
    st.page_link(
        "pages/5_Build_Plan.py",
        label="Review or change Build Plan settings",
        icon="🛠️",
    )
with action_right:
    st.link_button(
        "Open ChatGPT in a new tab",
        "https://chatgpt.com/",
        help="Keep this workspace open while you work in ChatGPT.",
        type="primary",
        width="stretch",
    )

st.info(
    "Keep this page open. Download the packet below, upload it to ChatGPT, "
    "paste the supplied prompt, then return here with ChatGPT's JSON response."
)

render_plan_exchange_panel(
    db_path,
    plan_start_date=plan_start,
    event_date=event_date,
    age=age,
    distance=str(settings.get("plan_distance", "50K")),
    run_days_per_week=run_days,
    long_run_day=str(settings.get("plan_long_run_day", "Saturday")),
    sodium_mg_per_hour=sodium if sodium > 0 else None,
    hrmax=int(hrmax) if hrmax is not None else None,
    lthr=int(lthr) if lthr is not None else None,
)
