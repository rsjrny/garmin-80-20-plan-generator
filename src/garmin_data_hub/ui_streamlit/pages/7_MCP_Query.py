from __future__ import annotations

import json
import os
import re
import sys

# Ensure src is in sys.path for local imports
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..", "src"))
)

import pandas as pd
import streamlit as st

from garmin_data_hub.mcp_sidecar_client import (
    call_tool_via_sidecar,
    check_sidecar_available,
)
from garmin_data_hub.paths import default_db_path

st.set_page_config(page_title="MCP Query", layout="wide")
st.header("MCP Tool Console")

db_path = default_db_path()
os.environ["GARMIN_DATA_DIR"] = str(db_path.parent)

st.caption(f"DB: {db_path}")
st.info(
    "Execute the six garmin MCP tools directly from the UI: schema, query, "
    "health summary, activities, trends, and sync."
)

try:
    import mcp  # noqa: F401

    mcp_available, mcp_error = check_sidecar_available(db_path)
except Exception as exc:
    mcp_available = False
    mcp_error = str(exc)


def _validate_select(sql: str) -> tuple[bool, str]:
    normalized = sql.strip().lstrip(";").strip().upper()
    if not normalized:
        return False, "Query is empty."
    if not normalized.startswith("SELECT"):
        return False, "Only SELECT statements are allowed."

    blocked = [
        "DROP",
        "DELETE",
        "INSERT",
        "UPDATE",
        "ALTER",
        "CREATE",
        "ATTACH",
        "DETACH",
        "PRAGMA",
    ]
    leading = normalized.split("'")[0]
    for keyword in blocked:
        if keyword in leading:
            return False, f"'{keyword}' is not permitted."

    return True, ""


def _run_query(sql: str) -> pd.DataFrame:
    result_text = call_tool_via_sidecar("garmin_query", {"sql": sql}, db_path)
    payload = json.loads(result_text)
    if isinstance(payload, dict) and payload.get("error"):
        raise RuntimeError(str(payload["error"]))
    if not isinstance(payload, list):
        return pd.DataFrame()
    return pd.DataFrame(payload)


def _to_float(value) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _build_ai_response(tool_name: str, payload, user_prompt: str | None = None) -> str:
    lines: list[str] = []

    if user_prompt:
        lines.append(f"You asked: {user_prompt.strip()}")

    if tool_name == "garmin_schema" and isinstance(payload, dict):
        table_count = len(payload)
        rows = [
            {
                "table": t,
                "row_count": int((d or {}).get("row_count") or 0),
            }
            for t, d in payload.items()
        ]
        rows.sort(key=lambda x: x["row_count"], reverse=True)
        top = rows[:3]
        total_rows = sum(r["row_count"] for r in rows)
        lines.append(
            f"Database schema contains {table_count} tables with approximately {total_rows:,} rows total."
        )
        if top:
            top_str = ", ".join(f"{r['table']} ({r['row_count']:,})" for r in top)
            lines.append(f"Largest tables: {top_str}.")

    elif tool_name == "garmin_health_summary" and isinstance(payload, dict):
        period = payload.get("period", {})
        daily = payload.get("daily", {}) or {}
        sleep = payload.get("sleep", {}) or {}
        readiness = payload.get("training_readiness", {}) or {}
        lines.append(
            f"Health summary for {period.get('start_date', '?')} to {period.get('end_date', '?')}."
        )
        avg_steps = daily.get("avg_steps")
        avg_sleep = sleep.get("avg_sleep_hours")
        avg_stress = daily.get("avg_stress")
        tr = readiness.get("avg_training_readiness")
        snippets = []
        if avg_steps is not None:
            snippets.append(f"average steps {avg_steps}")
        if avg_sleep is not None:
            snippets.append(f"average sleep {avg_sleep} hours")
        if avg_stress is not None:
            snippets.append(f"average stress {avg_stress}")
        if tr is not None:
            snippets.append(f"training readiness {tr}")
        if snippets:
            lines.append("Key metrics: " + ", ".join(snippets) + ".")

    elif tool_name == "garmin_trends" and isinstance(payload, dict):
        metric = payload.get("metric", "metric")
        period = payload.get("period", "period")
        data = payload.get("data", []) or []
        lines.append(f"Trend for {metric} grouped by {period}.")
        if data:
            first = data[0]
            last = data[-1]
            first_v = _to_float(first.get("value"))
            last_v = _to_float(last.get("value"))
            if first_v is not None and last_v is not None:
                delta = last_v - first_v
                if abs(delta) < 1e-9:
                    direction = "stable"
                elif delta > 0:
                    direction = "increasing"
                else:
                    direction = "decreasing"
                lines.append(
                    f"Across {len(data)} points, the metric is {direction} (from {first_v:.2f} to {last_v:.2f}, change {delta:+.2f})."
                )
            lines.append(f"Latest value: {last.get('value')} in {last.get('period')}")

    elif tool_name == "garmin_activities" and isinstance(payload, list):
        lines.append(f"Returned {len(payload)} activities.")
        if payload:
            distances = [
                _to_float(r.get("distance_km"))
                for r in payload
                if _to_float(r.get("distance_km")) is not None
            ]
            durations = [
                _to_float(r.get("duration_min"))
                for r in payload
                if _to_float(r.get("duration_min")) is not None
            ]
            latest = payload[0]
            parts = []
            if distances:
                parts.append(f"average distance {sum(distances)/len(distances):.2f} km")
            if durations:
                parts.append(
                    f"average duration {sum(durations)/len(durations):.1f} min"
                )
            if latest.get("date"):
                parts.append(f"latest activity on {latest.get('date')}")
            if parts:
                lines.append("Summary: " + ", ".join(parts) + ".")

    elif tool_name == "garmin_query" and isinstance(payload, list):
        row_count = len(payload)
        col_count = len(payload[0]) if payload else 0
        lines.append(f"Query returned {row_count} rows across {col_count} columns.")
        if payload and row_count > 0:
            preview_cols = list(payload[0].keys())[:6]
            lines.append("Primary fields: " + ", ".join(preview_cols) + ".")

    elif tool_name == "garmin_sync" and isinstance(payload, dict):
        status = payload.get("status", "unknown")
        lines.append(f"Sync finished with status: {status}.")
        if payload.get("error"):
            lines.append(f"Reported error: {payload.get('error')}")
        elif payload.get("result") is not None:
            lines.append("Latest Garmin data fetch was triggered successfully.")

    if not lines:
        return "The tool completed successfully, but there is no additional narrative summary for this response type."

    return "\n\n".join(lines)


def _infer_tool_from_prompt(prompt: str) -> tuple[str, dict, str]:
    text = prompt.strip().lower()

    if not text:
        return st.session_state.mcp_tool, {}, "No prompt provided."

    # Sync intent
    if any(k in text for k in ["sync", "pull latest", "refresh data", "update data"]):
        return "garmin_sync", {}, "Detected data refresh/sync intent."

    # Schema intent
    if any(
        k in text for k in ["schema", "tables", "columns", "row counts", "structure"]
    ):
        return "garmin_schema", {}, "Detected database schema intent."

    # Trend intent with metric detection
    if any(
        k in text
        for k in [
            "trend",
            "trending",
            "over time",
            "6 months",
            "month",
            "weekly",
            "monthly",
        ]
    ):
        aliases = {
            "resting heart rate": "resting_hr",
            "resting hr": "resting_hr",
            "hrv": "hrv",
            "stress": "stress",
            "steps": "steps",
            "sleep": "sleep_hours",
            "body battery": "body_battery",
            "spo2": "spo2",
            "training readiness": "training_readiness",
            "floors": "floors",
            "calories": "calories",
            "active minutes": "active_minutes",
            "respiration": "respiration",
            "weight": "weight",
            "endurance score": "endurance_score",
            "hill score": "hill_score",
            "race 5k": "race_5k",
            "race 10k": "race_10k",
        }
        metric = "resting_hr"
        for alias, mapped in aliases.items():
            if alias in text:
                metric = mapped
                break

        period = "month" if "month" in text or "months" in text else "week"
        return (
            "garmin_trends",
            {"metric": metric, "period": period},
            f"Detected trend intent using metric '{metric}' and period '{period}'.",
        )

    # Activities intent
    if any(
        k in text
        for k in [
            "activities",
            "activity",
            "run",
            "running",
            "ride",
            "cycling",
            "swim",
            "workout",
        ]
    ):
        activity_type = ""
        if "run" in text or "running" in text:
            activity_type = "running"
        elif "ride" in text or "cycling" in text or "bike" in text:
            activity_type = "cycling"
        elif "swim" in text:
            activity_type = "swimming"

        return (
            "garmin_activities",
            {
                "activity_type": activity_type,
                "start_date": "",
                "end_date": "",
                "limit": 50,
            },
            "Detected activity-list intent.",
        )

    # Health summary intent
    if any(
        k in text
        for k in [
            "sleep",
            "readiness",
            "overtraining",
            "health",
            "recovery",
            "hrv",
            "stress",
        ]
    ):
        days = 30
        m = re.search(r"last\s+(\d{1,3})\s+days", text)
        if m:
            days = max(1, min(365, int(m.group(1))))
        elif "6 month" in text or "6 months" in text:
            days = 180
        elif "month" in text or "monthly" in text:
            days = 30
        elif "week" in text or "weekly" in text:
            days = 7

        return (
            "garmin_health_summary",
            {"start_date": "", "end_date": "", "days": days},
            f"Detected health-summary intent over last {days} day(s).",
        )

    # Fallback: keep selected tool
    return (
        st.session_state.mcp_tool,
        {},
        "Could not confidently infer a tool; using selected dropdown tool.",
    )


PRESETS: dict[str, str] = {
    "Schema overview": """SELECT name AS table_name\nFROM sqlite_master\nWHERE type='table'\nORDER BY name;""",
    "Recent activities": """SELECT activity_id, activity_name, activity_type, start_time_local, distance_meters, elapsed_duration_seconds\nFROM activity\nORDER BY start_time_local DESC\nLIMIT 50;""",
    "7-day daily summary": """SELECT calendar_date, total_steps, resting_heart_rate, average_stress_level, body_battery_highest\nFROM daily_summary\nORDER BY calendar_date DESC\nLIMIT 7;""",
    "Latest sleep rows": """SELECT calendar_date, sleep_time_seconds, deep_sleep_seconds, rem_sleep_seconds, average_hr_sleep\nFROM sleep\nORDER BY calendar_date DESC\nLIMIT 14;""",
}

TREND_METRICS = [
    "resting_hr",
    "stress",
    "steps",
    "sleep_hours",
    "body_battery",
    "spo2",
    "training_readiness",
    "floors",
    "calories",
    "active_minutes",
    "respiration",
    "weight",
    "hrv",
    "endurance_score",
    "hill_score",
    "race_5k",
    "race_10k",
]

if "mcp_sql" not in st.session_state:
    st.session_state.mcp_sql = PRESETS["Recent activities"]

if "mcp_tool" not in st.session_state:
    st.session_state.mcp_tool = "garmin_schema"

if "mcp_query_tool_sql" not in st.session_state:
    st.session_state.mcp_query_tool_sql = PRESETS["Recent activities"]

if "mcp_ai_prompt" not in st.session_state:
    st.session_state.mcp_ai_prompt = ""

if "mcp_tool_pending" not in st.session_state:
    st.session_state.mcp_tool_pending = ""

if "mcp_autorun" not in st.session_state:
    st.session_state.mcp_autorun = None

if "mcp_query_preset" not in st.session_state:
    st.session_state.mcp_query_preset = "Recent activities"

if "raw_sql_preset" not in st.session_state:
    st.session_state.raw_sql_preset = "Recent activities"


def _apply_mcp_query_preset() -> None:
    st.session_state.mcp_query_tool_sql = PRESETS[st.session_state.mcp_query_preset]


def _apply_raw_sql_preset() -> None:
    st.session_state.mcp_sql = PRESETS[st.session_state.raw_sql_preset]


if not mcp_available:
    st.error("MCP sidecar is unavailable in this environment.")
    st.warning(
        "The MCP Query tools require the garmin-mcp package to be installed and running. "
        "Without it, advanced query features are not available, but you can still view "
        "your Garmin data through other pages."
    )
    with st.expander("Diagnostic Information"):
        st.caption("Error details:")
        st.code(mcp_error)
        st.caption("To fix this:")
        st.markdown("""
1. Verify garmin_mcp is installed in the active .venv: `pip list | grep garmin-mcp`
2. Try manually starting the sidecar: `python -m garmin_mcp`
3. Restart this application and return to the MCP Query page
        """)
    st.info("Visit the **Garmin Sync** or **Activities** pages to explore your data.")
    mcp_available = False  # Prevent tool execution below

if mcp_available:
    mode = st.radio(
        "Mode",
        options=["MCP tools", "Raw SQL"],
        horizontal=True,
    )
else:
    # Graceful degradation when sidecar unavailable
    st.warning("⚠️ MCP sidecar is unavailable. Tool mode is disabled.")
    mode = None


# Tool documentation reference
TOOL_DOCS = {
    "garmin_schema": {
        "description": "Retrieve database schema with table names, row counts, and column info.",
        "inputs": "No parameters required. Optionally pass empty dict.",
        "output": "Dict with table names as keys; each value contains row_count and columns list.",
        "examples": [
            "View all available tables and row counts in the Garmin database",
            "Check column structure of specific tables (activity, daily_summary, sleep)",
        ],
        "error_cases": [
            "Database file missing or inaccessible",
            "Sidecar process timeout (connection issue)",
        ],
    },
    "garmin_query": {
        "description": "Execute a read-only SELECT query directly against the Garmin database.",
        "inputs": "sql (required, string): A SELECT query. No INSERT, UPDATE, DELETE, DROP, or DDL allowed.",
        "output": "List of dicts representing query rows. Each dict is a record with column names as keys.",
        "examples": [
            "SELECT * FROM activity ORDER BY start_time_local DESC LIMIT 10",
            "SELECT calendar_date, total_steps FROM daily_summary WHERE calendar_date >= '2024-01-01'",
        ],
        "error_cases": [
            "SQL syntax error: Invalid column name or table name",
            "Blocked keywords: INSERT, DELETE, DROP, PRAGMA, CREATE, etc.",
            "Query timeout: Too many rows or slow aggregation",
        ],
    },
    "garmin_health_summary": {
        "description": "Retrieve health metrics (HR, stress, body battery) for a date range or last N days.",
        "inputs": "start_date (optional, YYYY-MM-DD), end_date (optional, YYYY-MM-DD), days (optional, int: 1-365, default 7)",
        "output": "List of daily health summaries with metrics like resting_hr, stress, body_battery.",
        "examples": [
            "Last 30 days of daily health metrics to identify stress/recovery trends",
            "Compare health metrics for a specific date range (e.g., training block)",
        ],
        "error_cases": [
            "Invalid date format: Must be YYYY-MM-DD",
            "No data for requested date range",
        ],
    },
    "garmin_activities": {
        "description": "List activities with filtering by type, date range, and result limit.",
        "inputs": "activity_type (optional, string: 'running', 'cycling', 'swimming', etc.), start_date (optional, YYYY-MM-DD), end_date (optional, YYYY-MM-DD), limit (optional, 1-1000, default 20)",
        "output": "List of activity records with activity_id, activity_name, activity_type, distance, duration, calories, etc.",
        "examples": [
            "Last 20 running activities with distance and duration",
            "All cycling activities in the past 3 months (set date range, activity_type='cycling')",
        ],
        "error_cases": [
            "Invalid date range: start_date after end_date",
            "Activity type not found in database (case-sensitive)",
            "Limit too high causing slow query",
        ],
    },
    "garmin_trends": {
        "description": "Retrieve trend data for a single health metric over time (weekly or monthly aggregation).",
        "inputs": "metric (required, string: see TREND_METRICS list), period (required, 'week' or 'month')",
        "output": "Dict with trend data grouped by period; each entry contains metric value and aggregation info.",
        "examples": [
            "Weekly resting heart rate trend to see if HR is improving with training",
            "Monthly body battery trend to evaluate recovery patterns",
        ],
        "error_cases": [
            "Invalid metric name: Must be one of the supported TREND_METRICS",
            "No data for metric: Metric may not be tracked on your device",
        ],
    },
    "garmin_sync": {
        "description": "Trigger a data sync/refresh from Garmin Connect and return sync status.",
        "inputs": "No parameters required.",
        "output": "String message indicating sync result (e.g., 'Sync completed successfully', 'Latest data already up to date').",
        "examples": [
            "Force a fresh pull of the latest Garmin data before analyzing",
        ],
        "error_cases": [
            "Network error: Cannot reach Garmin Connect",
            "Auth error: Saved credentials are invalid or expired",
            "Sync timeout: Large data pull is taking too long",
        ],
    },
}

with st.expander("📖 Tool Documentation & Input Schema", expanded=False):
    st.markdown("""
### MCP Tool Reference

Below is a summary of all 6 available MCP tools. Click on a tool name to expand details.
    """)
    
    for tool_name, doc in TOOL_DOCS.items():
        with st.expander(f"**{tool_name}**"):
            st.write(f"**Description:** {doc['description']}")
            st.write(f"**Inputs:** {doc['inputs']}")
            st.write(f"**Output:** {doc['output']}")
            
            st.write("**Examples:**")
            for ex in doc["examples"]:
                st.write(f"- {ex}")
            
            st.write("**Error Cases:**")
            for err in doc["error_cases"]:
                st.write(f"- {err}")


def _show_mcp_result(
    tool_name: str,
    result_text: str,
    limit_rows: int,
    user_prompt: str | None = None,
) -> None:
    try:
        payload = json.loads(result_text)
    except Exception:
        st.code(result_text)
        return

    if isinstance(payload, dict) and payload.get("error"):
        st.error(str(payload["error"]))
        st.code(result_text)
        return

    st.success(f"{tool_name} completed.")
    st.subheader("AI Response")
    st.write(_build_ai_response(tool_name, payload, user_prompt=user_prompt))

    with st.expander("Structured Response", expanded=False):
        if tool_name == "garmin_schema" and isinstance(payload, dict):
            rows = []
            for table_name, details in payload.items():
                rows.append(
                    {
                        "table": table_name,
                        "row_count": details.get("row_count"),
                        "column_count": len(details.get("columns", [])),
                        "columns": ", ".join(details.get("columns", [])),
                    }
                )
            df = pd.DataFrame(rows)
            if not df.empty:
                st.dataframe(df.head(limit_rows), width="stretch")
        elif tool_name == "garmin_trends" and isinstance(payload, dict):
            trend_rows = payload.get("data", [])
            df = pd.DataFrame(trend_rows)
            if not df.empty:
                st.dataframe(df.head(limit_rows), width="stretch")
        elif isinstance(payload, list):
            df = pd.DataFrame(payload)
            if df.empty:
                st.info("No rows returned.")
            else:
                st.dataframe(df.head(limit_rows), width="stretch")
                csv_bytes = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="Download CSV",
                    data=csv_bytes,
                    file_name="mcp_query_results.csv",
                    mime="text/csv",
                )
        elif isinstance(payload, dict):
            st.json(payload)
        else:
            st.code(result_text)

    with st.expander("Raw JSON"):
        st.code(result_text, language="json")


if mode == "MCP tools":
    # Apply pending tool switch before the widget is instantiated.
    if st.session_state.mcp_tool_pending:
        st.session_state.mcp_tool = st.session_state.mcp_tool_pending
        st.session_state.mcp_tool_pending = ""

    tool_name = st.selectbox(
        "MCP tool",
        options=[
            "garmin_schema",
            "garmin_query",
            "garmin_health_summary",
            "garmin_activities",
            "garmin_trends",
            "garmin_sync",
        ],
        key="mcp_tool",
        help="Select an MCP tool from the six available: schema, query, health summary, activities, trends, or sync. See documentation above for input/output details.",
    )

    ai_prompt = st.text_area(
        "AI prompt (optional)",
        key="mcp_ai_prompt",
        height=120,
        help=(
            "Write your natural-language question here. "
            "Then choose the MCP tool and parameters to execute."
        ),
        placeholder=(
            "Example: Compare my sleep quality this month vs last month and "
            "highlight any recovery concerns."
        ),
    )

    limit_view_rows = st.slider(
        "Rows to display", min_value=25, max_value=1000, value=200, step=25
    )

    tool_args = {}
    if tool_name == "garmin_query":
        st.caption("Tool input: custom SELECT query")
        st.markdown("""
*Execute a read-only SQL query. Only SELECT statements are allowed. No INSERT, UPDATE, DELETE, DROP, or DDL.*
        """)
        st.selectbox(
            "Preset query",
            options=list(PRESETS.keys()),
            key="mcp_query_preset",
            on_change=_apply_mcp_query_preset,
            help="Choose a predefined query or write your own below.",
        )

        query_sql = st.text_area(
            "SQL for garmin_query",
            key="mcp_query_tool_sql",
            height=220,
            help="Enter a SELECT query. Common tables: activity, daily_summary, sleep, record. Use presets as examples.",
        )
        tool_args["sql"] = query_sql

    elif tool_name == "garmin_health_summary":
        st.caption("Tool input: optional date range or last N days")
        st.markdown("""
*Retrieve daily health metrics (resting HR, stress, body battery, etc.). Leave date fields empty to use 'days' parameter.*
        """)
        hs_col_1, hs_col_2, hs_col_3 = st.columns([1, 1, 1])
        with hs_col_1:
            start_date = st.text_input(
                "Start date (YYYY-MM-DD)",
                value="",
                help="Inclusive start date. Leave empty to use 'Days' parameter only.",
            )
        with hs_col_2:
            end_date = st.text_input(
                "End date (YYYY-MM-DD)",
                value="",
                help="Inclusive end date. Leave empty to use 'Days' parameter only.",
            )
        with hs_col_3:
            days = st.number_input(
                "Days",
                min_value=1,
                max_value=365,
                value=7,
                help="Fetch the last N days of data. Ignored if start_date/end_date are provided.",
            )
        tool_args.update(
            {
                "start_date": start_date.strip(),
                "end_date": end_date.strip(),
                "days": int(days),
            }
        )

    elif tool_name == "garmin_activities":
        st.caption("Tool input: filters for activity listing")
        st.markdown("""
*List activities with optional filtering by type, date range, and limit. Leave fields empty for defaults.*
        """)
        a_col_1, a_col_2, a_col_3, a_col_4 = st.columns([1, 1, 1, 1])
        with a_col_1:
            activity_type = st.text_input(
                "Activity type",
                value="",
                help="Filter by activity type (e.g., 'running', 'cycling', 'swimming'). Leave empty for all types.",
            )
        with a_col_2:
            start_date = st.text_input(
                "Start date (YYYY-MM-DD)",
                value="",
                help="Earliest activity date to include. Leave empty for no lower bound.",
            )
        with a_col_3:
            end_date = st.text_input(
                "End date (YYYY-MM-DD)",
                value="",
                help="Latest activity date to include. Leave empty for no upper bound.",
            )
        with a_col_4:
            limit = st.number_input(
                "Limit",
                min_value=1,
                max_value=1000,
                value=20,
                help="Maximum number of activities to return (1-1000).",
            )
        tool_args.update(
            {
                "activity_type": activity_type.strip(),
                "start_date": start_date.strip(),
                "end_date": end_date.strip(),
                "limit": int(limit),
            }
        )

    elif tool_name == "garmin_trends":
        st.caption("Tool input: metric and aggregation period")
        st.markdown("""
*Retrieve a single health metric trend over time, aggregated by week or month.*
        """)
        t_col_1, t_col_2 = st.columns([1, 1])
        with t_col_1:
            metric = st.selectbox(
                "Metric",
                options=TREND_METRICS,
                index=0,
                help="Select a health metric to trend: resting_hr, stress, body_battery, sleep_hours, etc.",
            )
        with t_col_2:
            period = st.selectbox(
                "Period",
                options=["week", "month"],
                index=1,
                help="Aggregation period: 'week' for weekly averages, 'month' for monthly averages.",
            )
        tool_args.update({"metric": metric, "period": period})

    run_tool = st.button("Run MCP Tool", type="primary", width="stretch", disabled=not mcp_available)
    autorun_payload = st.session_state.mcp_autorun
    if run_tool or autorun_payload is not None:
        if not mcp_available:
            st.error("MCP sidecar is unavailable. Cannot execute tools.")
        else:
            effective_prompt = None
            if autorun_payload is not None:
                effective_tool = autorun_payload["tool"]
                effective_args = dict(autorun_payload.get("args", {}))
                if autorun_payload.get("prompt"):
                    st.caption(f"Prompt: {autorun_payload['prompt']}")
                    effective_prompt = str(autorun_payload["prompt"])
                if autorun_payload.get("rationale"):
                    st.info(f"Prompt routing: {autorun_payload['rationale']}")
                st.session_state.mcp_autorun = None
            else:
                effective_tool = tool_name
                effective_args = dict(tool_args)
                effective_prompt = ai_prompt.strip() or None

                if ai_prompt.strip():
                    inferred_tool, inferred_args, rationale = _infer_tool_from_prompt(
                        ai_prompt
                    )
                    effective_tool = inferred_tool
                    if inferred_args:
                        effective_args = inferred_args
                    st.caption(f"Prompt: {ai_prompt.strip()}")
                    st.info(f"Prompt routing: {rationale}")

                    # Switch dropdown safely on next rerun, then auto-run once.
                    if inferred_tool != tool_name:
                        st.session_state.mcp_tool_pending = inferred_tool
                        st.session_state.mcp_autorun = {
                            "tool": effective_tool,
                            "args": effective_args,
                            "prompt": ai_prompt.strip(),
                            "rationale": rationale,
                        }
                        st.rerun()

            if effective_tool == "garmin_query":
                ok, msg = _validate_select(effective_args.get("sql", ""))
                if not ok:
                    st.error(msg)
                    st.stop()

            with st.spinner(f"Running {effective_tool}..."):
                try:
                    output = call_tool_via_sidecar(effective_tool, effective_args, db_path)
                except TimeoutError as exc:
                    st.error(
                        "Tool execution timed out. The sidecar may be busy. "
                        "Please try again or check if garmin_mcp is responsive."
                    )
                    st.caption(str(exc))
                except RuntimeError as exc:
                    st.error(f"Tool error: {exc}")
                except Exception as exc:
                    st.error(
                        f"Tool execution failed unexpectedly: {type(exc).__name__}: {exc}"
                    )
                else:
                    _show_mcp_result(
                        effective_tool,
                        output,
                        limit_view_rows,
                        user_prompt=effective_prompt,
                    )

else:
    st.caption("Advanced mode: run direct read-only SQL through MCP DB utilities.")

    st.selectbox(
        "Preset query",
        options=list(PRESETS.keys()),
        key="raw_sql_preset",
        on_change=_apply_raw_sql_preset,
    )

    sql_text = st.text_area(
        "SQL (read-only)",
        key="mcp_sql",
        height=220,
        help="Only SELECT statements are allowed.",
    )

    limit_view_rows = st.slider(
        "Rows to display",
        min_value=25,
        max_value=1000,
        value=200,
        step=25,
        key="raw_sql_rows",
    )

    run_col, clear_col = st.columns([1, 1])
    with run_col:
        run_now = st.button(
            "Run SQL", type="primary", width="stretch", key="raw_sql_run", disabled=not mcp_available
        )
    with clear_col:
        clear_now = st.button("Clear", width="stretch", key="raw_sql_clear")

    if clear_now:
        st.session_state.mcp_sql = ""
        st.rerun()

    if run_now:
        if not mcp_available:
            st.error("MCP sidecar is unavailable. Cannot execute queries.")
        else:
            ok, msg = _validate_select(sql_text)
            if not ok:
                st.error(msg)
            else:
                with st.spinner("Running query..."):
                    try:
                        df = _run_query(sql_text)
                    except TimeoutError as exc:
                        st.error(
                            "Query execution timed out. The sidecar may be busy. "
                            "Please try again or check if garmin_mcp is responsive."
                        )
                        st.caption(str(exc))
                    except RuntimeError as exc:
                        st.error(f"Query error: {exc}")
                    except Exception as exc:
                        st.error(
                            f"Query execution failed unexpectedly: {type(exc).__name__}: {exc}"
                        )
                    else:
                        st.success(f"Query returned {len(df)} row(s).")
                        st.subheader("AI Response")
                        st.write(
                            _build_ai_response(
                                "garmin_query",
                                df.to_dict(orient="records"),
                                user_prompt="Run SQL (read-only)",
                            )
                        )
                        with st.expander("Structured Response", expanded=False):
                        if df.empty:
                            st.info("No rows returned.")
                        else:
                            if len(df) > limit_view_rows:
                                st.warning(
                                    f"Showing first {limit_view_rows} row(s) of {len(df)} total. "
                                    "Refine your query for smaller result sets."
                                )
                            st.dataframe(df.head(limit_view_rows), width="stretch")

                            csv_bytes = df.to_csv(index=False).encode("utf-8")
                            st.download_button(
                                label="Download CSV",
                                data=csv_bytes,
                                file_name="mcp_query_results.csv",
                                mime="text/csv",
                            )
