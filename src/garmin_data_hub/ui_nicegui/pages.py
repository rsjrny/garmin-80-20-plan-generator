"""Core pages for the primary NiceGUI navigation surface."""

from __future__ import annotations

import json
import math
import sqlite3
import subprocess
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from garmin_data_hub import __version__
from garmin_data_hub.analytics.sleep_recovery import analyze_sleep_recovery
from garmin_data_hub.mcp_sidecar_client import call_tool_via_sidecar
from garmin_data_hub.services.baseline_plan_builder import (
    BASELINE_DISTANCE_OPTIONS,
    BaselinePlanRequest,
    build_and_save_baseline,
    normalize_baseline_distance,
)
from garmin_data_hub.services.garmin_auth_session import (
    BrowserSessionResetError,
    BrowserSessionResetRequired,
    BrowserSessionResetResult,
    garmin_browser_session_exists,
)
from garmin_data_hub.services.athlete_metrics_service import (
    calculate_metrics_from_db_sources,
    clear_override_metrics,
    get_athlete_metrics,
    set_calculated_metrics,
    set_override_metrics,
)
from garmin_data_hub.services.garmin_credentials import (
    CredentialStoreError,
    CredentialStoreUnavailable,
    GarminCredentials,
    credential_status,
    delete_credentials,
    load_credentials,
    save_credentials,
)
from garmin_data_hub.services.plan_persistence import get_active_plan_sha256
from garmin_data_hub.services.training_policy import TRAINING_METHODS
from garmin_data_hub.ui_nicegui.data import (
    INTERFACE_SETTING_DEFAULTS,
    activity_detail,
    activity_export_json,
    activity_sports,
    chart_dataframe,
    compliance_data,
    dashboard_data,
    distance_from_km,
    distance_unit,
    get_sync_job,
    interface_settings,
    list_activities,
    nutrition_rows,
    pace_minutes_from_mps,
    pace_text_from_mps,
    pace_unit,
    plan_rows,
    planning_settings,
    repair_derived_metrics,
    run_read_only_query,
    save_interface_settings,
    save_planning_settings,
    speed_from_mps,
    speed_unit,
    validate_planning_settings,
)
from garmin_data_hub.ui_nicegui.layout import (
    data_grid,
    metric_card,
    page_heading,
    render_shell,
)


HELP_WORKFLOW_SECTIONS = (
    (
        "1",
        "Set preferences",
        "Choose miles or kilometres and save reusable defaults for activity "
        "history, charts, the dashboard, and Garmin sync.",
        "Open Settings",
        "/settings",
    ),
    (
        "2",
        "Sync Garmin",
        "Set up your Garmin login, choose an optional lookback, and run the "
        "sync. You can keep using the rest of the app while it runs.",
        "Open Garmin Sync",
        "/sync",
    ),
    (
        "3",
        "Review history",
        "Filter activities, inspect splits and routes, export activity JSON, "
        "and explore volume, intensity, heart-rate, speed, and load trends.",
        "Open Activities",
        "/activities",
    ),
    (
        "4",
        "Configure the goal",
        "Set the athlete, event, schedule, nutrition context, and effective "
        "heart-rate thresholds used for training plans.",
        "Open Plan",
        "/plan",
    ),
    (
        "5",
        "Generate safely",
        "Build an offline baseline or ask Codex Coach for a proposal. Codex "
        "receives minimized context without GPS routes, raw trackpoints, exact "
        "activity times, or device identifiers.",
        "Open Codex Coach",
        "/coach",
    ),
    (
        "6",
        "Review and apply",
        "Read the rationale, policy warnings, nutrition targets, and exact "
        "calendar changes before explicitly approving a proposal.",
        "Review in Codex Coach",
        "/coach",
    ),
    (
        "7",
        "Track compliance",
        "Compare the active schedule with completed Garmin activities through "
        "today.",
        "Open Compliance",
        "/compliance",
    ),
)

ABOUT_LINKS = (
    (
        "Project source",
        "https://github.com/rsjrny/garmin-80-20-plan-generator",
    ),
    (
        "Report an issue",
        "https://github.com/rsjrny/garmin-80-20-plan-generator/issues",
    ),
    (
        "Releases",
        "https://github.com/rsjrny/garmin-80-20-plan-generator/releases",
    ),
)

CHART_OPTIONS: tuple[tuple[str, str], ...] = (
    ("activity_distribution", "Activity distribution"),
    ("weekly_distance", "Weekly distance"),
    ("weekly_duration", "Weekly duration"),
    ("average_heart_rate", "Average heart rate"),
    ("average_velocity", "Average pace/speed"),
    ("weekly_training_stress", "Weekly training stress"),
    ("weekly_hr_zones", "Weekly HR zones"),
    ("weekly_elevation", "Weekly elevation gain"),
    ("longest_activity", "Longest activity by week"),
    ("load_vs_duration", "Load vs duration"),
    ("drift_decoupling", "Drift and decoupling"),
)
CHART_LABELS = {chart_id: label for chart_id, label in CHART_OPTIONS}
CHART_IDS_BY_LABEL = {label: chart_id for chart_id, label in CHART_OPTIONS}


def _notify_error(message: object) -> None:
    from nicegui import ui

    ui.notify(str(message), type="negative", multi_line=True, close_button=True)


def _sync_controls_for_state(state: str) -> tuple[bool, bool, bool]:
    """Return enabled states for Run, Stop, and Clear sync controls."""
    if state == "running":
        return False, True, False
    if state in {"cancelling", "resetting_login"}:
        return False, False, False
    return True, False, True


def _resolve_sync_credentials(
    email: object,
    password: object,
    saved: GarminCredentials | None,
) -> tuple[GarminCredentials, bool]:
    """Resolve entered or saved credentials without exposing the saved password."""
    entered_email = str(email or "").strip()
    entered_password = str(password or "")
    if entered_password:
        return GarminCredentials(entered_email, entered_password), True
    if saved is None:
        if entered_email:
            raise ValueError("Enter your Garmin Connect password")
        raise ValueError("Enter your Garmin Connect email and password")
    if entered_email and entered_email.casefold() != saved.email.casefold():
        raise ValueError("Enter the password for the Garmin Connect email shown")
    return saved, False


def _credentials_for_sync(
    editing_login: bool,
    email: object,
    password: object,
    saved: GarminCredentials | None,
) -> tuple[GarminCredentials, bool]:
    """Use the saved login unless setup or an explicit update is open."""

    if not editing_login:
        if saved is None:
            raise ValueError(
                "The saved Garmin login is unavailable. Choose Update login."
            )
        return saved, False
    return _resolve_sync_credentials(email, password, saved)


def _browser_login_state_exists(data_directory: Path) -> bool:
    """Return whether Garmin browser state might supersede entered credentials."""

    return garmin_browser_session_exists(data_directory)


def _mark_saved_login_unavailable(
    state: dict[str, Any],
    problem: object,
    *,
    credential_may_exist: bool,
) -> None:
    """Move the Sync page into a non-cancellable login recovery state."""

    state["saved_email"] = None
    state["credential_problem"] = str(problem)
    state["credential_may_exist"] = credential_may_exist
    state["editing_login"] = True
    state["login_reset_required"] = True


def _sync_waiting_for_mfa(log: str) -> bool:
    """Return whether the latest login event is an unresolved MFA prompt."""
    normalized = str(log or "").casefold()
    mfa_marker = max(
        normalized.rfind("mfa required"),
        normalized.rfind("still waiting for mfa"),
    )
    login_result = max(
        normalized.rfind("login successful"),
        normalized.rfind("login failed"),
    )
    return mfa_marker >= 0 and mfa_marker > login_result


def _distance_rows(
    rows: list[dict[str, Any]],
    unit_system: str,
    *fields: str,
) -> list[dict[str, Any]]:
    """Copy rows while converting and relabelling kilometre fields."""
    unit = distance_unit(unit_system)
    converted: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        for field in fields:
            value = row.pop(field, None)
            stem = field[:-3] if field.endswith("_km") else field
            row[f"{stem}_{unit}"] = distance_from_km(value, unit_system)
        converted.append(row)
    return converted


def _activity_velocity_fields(
    speed_mps: object,
    unit_system: str,
    velocity_display: str,
    *,
    prefix: str = "avg",
) -> dict[str, object]:
    """Return one visible pace/speed field for activity summary tables."""
    if velocity_display == "Speed":
        suffix = "mph" if unit_system == "Imperial" else "kmh"
        return {f"{prefix}_speed_{suffix}": speed_from_mps(speed_mps, unit_system)}
    return {f"{prefix}_pace": pace_text_from_mps(speed_mps, unit_system)}


def _chart_ids_from_labels(labels: object) -> set[str]:
    if not labels:
        return set()
    if isinstance(labels, str):
        labels = [labels]
    return {
        chart_id
        for label in labels
        for chart_id in (CHART_IDS_BY_LABEL.get(str(label)),)
        if chart_id is not None
    }


def _add_chart(
    figures: list[go.Figure],
    selected_chart_ids: set[str],
    chart_id: str,
    figure: go.Figure,
) -> None:
    if chart_id in selected_chart_ids:
        figures.append(figure)


def _has_positive_sum(frame: pd.DataFrame, columns: list[str]) -> bool:
    available = [column for column in columns if column in frame.columns]
    if not available:
        return False
    values = frame[available].apply(pd.to_numeric, errors="coerce").fillna(0)
    return float(values.to_numpy().sum()) > 0


def _training_chart_figures(
    frame: pd.DataFrame,
    selected_chart_ids: set[str],
    *,
    unit: str,
    unit_system: str,
    velocity_display: str,
) -> list[go.Figure]:
    if not selected_chart_ids:
        return []

    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["start_time_utc"], errors="coerce")
    frame = frame.dropna(subset=["date"])
    frame["distance"] = pd.to_numeric(frame["total_distance_m"], errors="coerce").fillna(0) / 1000
    if unit_system == "Imperial":
        frame["distance"] *= 0.621371192237334
    frame["duration_hours"] = pd.to_numeric(frame["total_elapsed_s"], errors="coerce").fillna(0) / 3600
    frame["ascent"] = pd.to_numeric(frame["total_ascent_m"], errors="coerce").fillna(0)
    ascent_unit = "ft" if unit_system == "Imperial" else "m"
    if unit_system == "Imperial":
        frame["ascent"] *= 3.2808398950131
    frame["tss"] = pd.to_numeric(frame["tss"], errors="coerce").fillna(0)
    frame["week"] = frame["date"].dt.to_period("W").dt.start_time

    if velocity_display == "Speed":
        velocity_field = "display_speed"
        velocity_title = f"Average speed ({speed_unit(unit_system)})"
        frame[velocity_field] = frame["avg_speed_mps"].apply(
            lambda value: speed_from_mps(value, unit_system)
        )
        velocity_yaxis = None
    else:
        velocity_field = "display_pace"
        velocity_title = f"Average pace ({pace_unit(unit_system)})"
        frame[velocity_field] = frame["avg_speed_mps"].apply(
            lambda value: pace_minutes_from_mps(value, unit_system)
        )
        velocity_yaxis = {"autorange": "reversed"}

    weekly = frame.groupby("week", as_index=False).agg(
        distance=("distance", "sum"),
        duration_hours=("duration_hours", "sum"),
        tss=("tss", "sum"),
        ascent=("ascent", "sum"),
        longest_distance=("distance", "max"),
    )
    figures: list[go.Figure] = []
    _add_chart(
        figures,
        selected_chart_ids,
        "activity_distribution",
        px.bar(
            frame.groupby("sport", as_index=False).size(),
            x="sport",
            y="size",
            title="Activity distribution",
        ),
    )
    _add_chart(
        figures,
        selected_chart_ids,
        "weekly_distance",
        px.bar(weekly, x="week", y="distance", title=f"Weekly distance ({unit})"),
    )
    _add_chart(
        figures,
        selected_chart_ids,
        "weekly_duration",
        px.line(
            weekly,
            x="week",
            y="duration_hours",
            markers=True,
            title="Weekly duration (hours)",
        ),
    )
    _add_chart(
        figures,
        selected_chart_ids,
        "average_heart_rate",
        px.scatter(
            frame,
            x="date",
            y="avg_hr_bpm",
            color="sport",
            hover_data=["distance", "duration_hours"],
            title="Average heart rate",
        ),
    )
    velocity_figure = px.scatter(
        frame,
        x="date",
        y=velocity_field,
        color="sport",
        size="distance",
        title=velocity_title,
    )
    if velocity_yaxis is not None:
        velocity_figure.update_yaxes(**velocity_yaxis)
    _add_chart(figures, selected_chart_ids, "average_velocity", velocity_figure)
    _add_chart(
        figures,
        selected_chart_ids,
        "weekly_training_stress",
        px.bar(weekly, x="week", y="tss", title="Weekly training stress"),
    )
    zone_columns = ["zone_1_s", "zone_2_s", "zone_3_s", "zone_4_s", "zone_5_s"]
    if "weekly_hr_zones" in selected_chart_ids and _has_positive_sum(frame, zone_columns):
        weekly_zones = (
            frame.groupby("week", as_index=False)[zone_columns].sum()
            .rename(
                columns={
                    "zone_1_s": "Zone 1",
                    "zone_2_s": "Zone 2",
                    "zone_3_s": "Zone 3",
                    "zone_4_s": "Zone 4",
                    "zone_5_s": "Zone 5",
                }
            )
        )
        for column in ("Zone 1", "Zone 2", "Zone 3", "Zone 4", "Zone 5"):
            weekly_zones[column] = weekly_zones[column] / 3600
        zones_long = weekly_zones.melt(
            id_vars="week",
            var_name="zone",
            value_name="hours",
        )
        figures.append(
            px.bar(
                zones_long,
                x="week",
                y="hours",
                color="zone",
                title="Weekly HR zones (hours)",
                barmode="stack",
            )
        )
    _add_chart(
        figures,
        selected_chart_ids,
        "weekly_elevation",
        px.bar(weekly, x="week", y="ascent", title=f"Weekly elevation gain ({ascent_unit})"),
    )
    _add_chart(
        figures,
        selected_chart_ids,
        "longest_activity",
        px.line(
            weekly,
            x="week",
            y="longest_distance",
            markers=True,
            title=f"Longest activity by week ({unit})",
        ),
    )
    _add_chart(
        figures,
        selected_chart_ids,
        "load_vs_duration",
        px.scatter(
            frame,
            x="duration_hours",
            y="tss",
            color="sport",
            size="distance",
            title="Load vs duration",
        ),
    )
    if "drift_decoupling" in selected_chart_ids:
        drift_columns = [
            column
            for column in ("aerobic_decoupling_pct", "hr_drift_pct")
            if column in frame.columns and frame[column].notna().any()
        ]
        if drift_columns:
            drift_frame = frame[["date", "sport", *drift_columns]].melt(
                id_vars=["date", "sport"],
                value_vars=drift_columns,
                var_name="metric",
                value_name="percent",
            ).dropna(subset=["percent"])
            if not drift_frame.empty:
                figures.append(
                    px.scatter(
                        drift_frame,
                        x="date",
                        y="percent",
                        color="metric",
                        symbol="sport",
                        title="Drift and decoupling (%)",
                    )
                )
    return figures


def _format_upcoming_plan_for_sharing(
    rows: list[dict[str, Any]],
    distance_unit_label: str,
) -> str:
    """Format the visible Dashboard plan rows as shareable plain text."""

    session_count = len(rows)
    session_label = "session" if session_count == 1 else "sessions"
    heading = (
        f"Upcoming training plan ({session_count} {session_label} shown)"
        if rows
        else "Upcoming training plan"
    )
    if not rows:
        return f"{heading}\nNo upcoming sessions scheduled."

    distance_key = f"distance_{distance_unit_label}"

    def compact_number(value: object) -> str | None:
        if value is None:
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(number):
            return None
        return f"{number:g}"

    lines = [heading]
    for row in rows:
        raw_date = " ".join(str(row.get("date") or "Unscheduled").split())
        try:
            scheduled_date = date.fromisoformat(raw_date)
        except ValueError:
            date_label = raw_date
        else:
            date_label = f"{scheduled_date:%a} {scheduled_date.isoformat()}"

        workout = " ".join(str(row.get("workout") or "Workout").split())
        details: list[str] = []
        for value, prefix, suffix in (
            (row.get("duration_min"), "", " min"),
            (row.get(distance_key), "", f" {distance_unit_label}"),
            (row.get("tss"), "TSS ", ""),
        ):
            rendered = compact_number(value)
            if rendered is None:
                continue
            details.append(f"{prefix}{rendered}{suffix}")

        line = f"- {date_label} — {workout}"
        if details:
            line += " · " + " · ".join(details)
        lines.append(line)
    return "\n".join(lines)


def _split_rows(
    rows: list[dict[str, Any]],
    unit_system: str,
    velocity_display: str,
) -> list[dict[str, Any]]:
    unit = distance_unit(unit_system)
    converted: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        meters = row.pop("distance_meters", None)
        kilometres = float(meters) / 1000 if meters is not None else None
        row[f"distance_{unit}"] = distance_from_km(kilometres, unit_system)
        speed_mps = row.pop("speed_mps", None)
        row.update(
            _activity_velocity_fields(
                speed_mps,
                unit_system,
                velocity_display,
                prefix="split",
            )
        )
        converted.append(row)
    return converted


def _fiets_index(gain_m: float, distance_m: float, summit_altitude_m: float) -> float:
    if gain_m <= 0 or distance_m <= 0:
        return 0.0
    altitude_bonus = max(0.0, (summit_altitude_m - 1000) / 1000)
    return gain_m * gain_m / (distance_m * 4) + altitude_bonus


def _fiets_category(fiets: float) -> str:
    if fiets >= 6.5:
        return "HC"
    if fiets >= 5.0:
        return "1"
    if fiets >= 3.5:
        return "2"
    if fiets >= 2.0:
        return "3"
    if fiets >= 0.5:
        return "4"
    if fiets >= 0.25:
        return "5"
    return "Unclassified"


def _duration_text(seconds: float) -> str:
    total_seconds = max(0, int(round(seconds)))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"


def _pace_tick_text(minutes: float, unit_system: str) -> str:
    whole_minutes = int(minutes)
    seconds = int(round((minutes - whole_minutes) * 60))
    if seconds == 60:
        whole_minutes += 1
        seconds = 0
    return f"{whole_minutes}:{seconds:02d}"


def _pace_axis_ticks(values: pd.Series) -> dict[str, list[float] | list[str]]:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return {}
    low = math.floor(float(numeric.min()) * 2) / 2
    high = math.ceil(float(numeric.max()) * 2) / 2
    if high <= low:
        high = low + 0.5
    tick_values = [low + index * 0.5 for index in range(int((high - low) / 0.5) + 1)]
    return {
        "tickvals": tick_values,
        "ticktext": [_pace_tick_text(value, "Metric") for value in tick_values],
    }


def _climb_summary_rows(
    points: pd.DataFrame,
    unit_system: str,
    *,
    min_distance_m: float = 100.0,
    min_gain_m: float = 10.0,
    min_grade_pct: float = 2.0,
) -> list[dict[str, Any]]:
    """Identify sustained climbs and classify them with a FIETS-style index."""
    if points.empty or not {"distance_m", "altitude_m"}.issubset(points.columns):
        return []

    columns = [
        column
        for column in (
            "distance_m",
            "altitude_m",
            "speed_mps",
            "power_w",
            "heart_rate_bpm",
        )
        if column in points.columns
    ]
    frame = points[columns].copy()
    for column in columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["distance_m", "altitude_m"])
    frame = frame.sort_values("distance_m")
    frame = frame[frame["distance_m"].diff().fillna(0) >= 0].reset_index(drop=True)
    if len(frame) < 2:
        return []

    frame["altitude_smooth_m"] = (
        frame["altitude_m"].rolling(window=3, min_periods=1, center=True).median()
    )
    climbs: list[dict[str, Any]] = []
    start_idx: int | None = None
    descent_m = 0.0
    previous_altitude = float(frame.loc[0, "altitude_smooth_m"])

    def close_climb(end_idx: int) -> None:
        nonlocal start_idx
        if start_idx is None or end_idx <= start_idx:
            start_idx = None
            return
        start_distance = float(frame.loc[start_idx, "distance_m"])
        end_distance = float(frame.loc[end_idx, "distance_m"])
        segment = frame.iloc[start_idx : end_idx + 1]
        distance_m = end_distance - start_distance
        gain_m = float(segment["altitude_smooth_m"].diff().clip(lower=0).sum())
        if distance_m <= 0:
            start_idx = None
            return
        grade_pct = gain_m / distance_m * 100
        if (
            distance_m >= min_distance_m
            and gain_m >= min_gain_m
            and grade_pct >= min_grade_pct
        ):
            segment_steps = segment.copy()
            segment_steps["step_distance_m"] = segment_steps["distance_m"].diff()
            duration_s = None
            if (
                "speed_mps" in segment_steps.columns
                and segment_steps["speed_mps"].notna().any()
            ):
                step_seconds = (
                    segment_steps["step_distance_m"]
                    / segment_steps["speed_mps"].replace(0, pd.NA)
                )
                duration_s = float(step_seconds.dropna().sum()) or None
            avg_speed_mps = distance_m / duration_s if duration_s else None
            summit_altitude_m = float(segment["altitude_smooth_m"].max())
            fiets = _fiets_index(gain_m, distance_m, summit_altitude_m)
            row: dict[str, Any] = {
                "climb": len(climbs) + 1,
                "start": distance_from_km(start_distance / 1000, unit_system),
                "end": distance_from_km(end_distance / 1000, unit_system),
                "distance": distance_from_km(distance_m / 1000, unit_system),
                "gain": round(
                    gain_m * (3.2808398950131 if unit_system == "Imperial" else 1),
                    0,
                ),
                "avg_grade_pct": round(grade_pct, 1),
                "category": _fiets_category(fiets),
                "fiets": round(fiets, 2),
            }
            if duration_s is not None:
                row["duration"] = _duration_text(duration_s)
                row["pace"] = pace_text_from_mps(avg_speed_mps, unit_system)
                row["vam"] = round(gain_m / (duration_s / 3600))
            if "power_w" in segment.columns and segment["power_w"].notna().any():
                row["avg_power_w"] = round(float(segment["power_w"].mean()))
            if (
                "heart_rate_bpm" in segment.columns
                and segment["heart_rate_bpm"].notna().any()
            ):
                row["avg_hr_bpm"] = round(float(segment["heart_rate_bpm"].mean()))
            climbs.append(row)
        start_idx = None

    for idx in range(1, len(frame)):
        altitude = float(frame.loc[idx, "altitude_smooth_m"])
        delta = altitude - previous_altitude
        if delta > 0.4:
            if start_idx is None:
                start_idx = idx - 1
            descent_m = 0.0
        elif start_idx is not None and delta < -0.8:
            descent_m += abs(delta)
            if descent_m >= 4.0:
                close_climb(idx - 1)
                descent_m = 0.0
        previous_altitude = altitude
    close_climb(len(frame) - 1)
    unit = distance_unit(unit_system)
    ascent_unit = "ft" if unit_system == "Imperial" else "m"
    return [
        {
            "climb": row["climb"],
            f"start_{unit}": row["start"],
            f"end_{unit}": row["end"],
            f"distance_{unit}": row["distance"],
            f"gain_{ascent_unit}": row["gain"],
            "avg_grade_pct": row["avg_grade_pct"],
            "category": row["category"],
            "fiets": row["fiets"],
            **({"duration": row["duration"]} if row.get("duration") else {}),
            **({"pace": row["pace"]} if row.get("pace") else {}),
            **(
                {f"vam_{ascent_unit}_per_h": round(row["vam"] * (3.2808398950131 if unit_system == "Imperial" else 1))}
                if row.get("vam") is not None
                else {}
            ),
            **(
                {"avg_power_w": row["avg_power_w"]}
                if row.get("avg_power_w") is not None
                else {}
            ),
            **(
                {"avg_hr_bpm": row["avg_hr_bpm"]}
                if row.get("avg_hr_bpm") is not None
                else {}
            ),
        }
        for row in climbs
    ]


def _downhill_summary_rows(
    points: pd.DataFrame,
    unit_system: str,
    *,
    min_distance_m: float = 100.0,
    min_drop_m: float = 5.0,
    min_grade_pct: float = 2.0,
) -> list[dict[str, Any]]:
    """Identify sustained downhill segments using the same thresholds as climbs."""
    if points.empty or not {"distance_m", "altitude_m"}.issubset(points.columns):
        return []
    inverted = points.copy()
    inverted["altitude_m"] = -pd.to_numeric(inverted["altitude_m"], errors="coerce")
    rows = _climb_summary_rows(
        inverted,
        unit_system,
        min_distance_m=min_distance_m,
        min_gain_m=min_drop_m,
        min_grade_pct=min_grade_pct,
    )
    ascent_unit = "ft" if unit_system == "Imperial" else "m"
    for row in rows:
        row[f"drop_{ascent_unit}"] = row.pop(f"gain_{ascent_unit}")
        row["avg_grade_pct"] = -row["avg_grade_pct"]
    return rows


def _terrain_segments(points: pd.DataFrame) -> pd.DataFrame:
    if points.empty or not {"distance_m", "altitude_m"}.issubset(points.columns):
        return pd.DataFrame()
    columns = [
        column
        for column in (
            "distance_m",
            "altitude_m",
            "speed_mps",
            "power_w",
            "heart_rate_bpm",
        )
        if column in points.columns
    ]
    frame = points[columns].copy()
    for column in columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["distance_m", "altitude_m"])
    frame = frame.sort_values("distance_m")
    frame = frame[frame["distance_m"].diff().fillna(0) >= 0].reset_index(drop=True)
    if len(frame) < 2:
        return pd.DataFrame()
    frame["altitude_smooth_m"] = (
        frame["altitude_m"].rolling(window=5, min_periods=1, center=True).mean()
    )
    segments = pd.DataFrame(
        {
            "distance_m": frame["distance_m"].diff(),
            "start_m": frame["distance_m"].shift(),
            "end_m": frame["distance_m"],
            "elevation_delta_m": frame["altitude_smooth_m"].diff(),
        }
    )
    segments = segments.dropna(subset=["distance_m", "elevation_delta_m"])
    segments = segments[segments["distance_m"] > 0].copy()
    if segments.empty:
        return segments
    segments["grade_pct"] = segments["elevation_delta_m"] / segments["distance_m"] * 100
    for column in ("speed_mps", "power_w", "heart_rate_bpm"):
        if column in frame.columns:
            segments[column] = frame[column]
    return segments


def _terrain_summary(points: pd.DataFrame, climb_rows: list[dict[str, Any]], unit_system: str) -> dict[str, Any]:
    segments = _terrain_segments(points)
    unit = distance_unit(unit_system)
    ascent_unit = "ft" if unit_system == "Imperial" else "m"
    distance_factor = 0.000621371192237334 if unit_system == "Imperial" else 0.001
    ascent_factor = 3.2808398950131 if unit_system == "Imperial" else 1.0
    if segments.empty:
        return {
            "gdh_terrain_score": 0.0,
            f"distance_{unit}": 0.0,
            f"gain_{ascent_unit}": 0.0,
            "hilly_pct": 0,
            "flat_pct": 0,
        }
    total_distance_m = float(segments["distance_m"].sum())
    gain_m = float(segments["elevation_delta_m"].clip(lower=0).sum())
    hilly_distance_m = float(
        segments.loc[segments["grade_pct"].abs() >= 2, "distance_m"].sum()
    )
    hilly_pct = round(hilly_distance_m / total_distance_m * 100) if total_distance_m else 0
    total_fiets = sum(float(row.get("fiets") or 0) for row in climb_rows)
    terrain_score = min(10.0, (hilly_pct / 100 * 4) + min(6.0, total_fiets))
    return {
        "gdh_terrain_score": round(terrain_score, 1),
        f"distance_{unit}": round(total_distance_m * distance_factor, 2),
        f"gain_{ascent_unit}": round(gain_m * ascent_factor, 0),
        "hilly_pct": hilly_pct,
        "flat_pct": max(0, 100 - hilly_pct),
    }


def _gradient_distribution_rows(
    points: pd.DataFrame,
    unit_system: str,
) -> list[dict[str, Any]]:
    segments = _terrain_segments(points)
    unit = distance_unit(unit_system)
    if segments.empty:
        return []
    total_distance_m = float(segments["distance_m"].sum())
    categories = (
        ("Flat", segments["grade_pct"].abs() < 2),
        ("Ascending > +2%", segments["grade_pct"] >= 2),
        ("Descending < -2%", segments["grade_pct"] <= -2),
    )
    rows: list[dict[str, Any]] = []
    for label, mask in categories:
        category = segments.loc[mask].copy()
        distance_m = float(category["distance_m"].sum()) if not category.empty else 0
        percent = round(distance_m / total_distance_m * 100) if total_distance_m else 0
        row: dict[str, Any] = {
            "gradient": label,
            f"distance_{unit}": distance_from_km(distance_m / 1000, unit_system),
            "share_pct": percent,
        }
        if "speed_mps" in category.columns and category["speed_mps"].notna().any():
            row["avg_pace"] = pace_text_from_mps(
                category["speed_mps"].mean(),
                unit_system,
            )
        if "power_w" in category.columns and category["power_w"].notna().any():
            row["avg_power_w"] = round(float(category["power_w"].mean()))
        rows.append(row)
    return rows


def _elevation_profile_figure(
    points: pd.DataFrame,
    climb_rows: list[dict[str, Any]],
    unit_system: str,
) -> go.Figure | None:
    if points.empty or not {"distance_m", "altitude_m"}.issubset(points.columns):
        return None
    frame = points[["distance_m", "altitude_m"]].copy()
    frame["distance_m"] = pd.to_numeric(frame["distance_m"], errors="coerce")
    frame["altitude_m"] = pd.to_numeric(frame["altitude_m"], errors="coerce")
    frame = frame.dropna(subset=["distance_m", "altitude_m"])
    if frame.empty:
        return None

    unit = distance_unit(unit_system)
    altitude_unit = "ft" if unit_system == "Imperial" else "m"
    distance_factor = 0.000621371192237334 if unit_system == "Imperial" else 0.001
    altitude_factor = 3.2808398950131 if unit_system == "Imperial" else 1.0
    plot_frame = frame.assign(
        display_distance=frame["distance_m"] * distance_factor,
        display_altitude=frame["altitude_m"] * altitude_factor,
    )
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=plot_frame["display_distance"],
            y=plot_frame["display_altitude"],
            mode="lines",
            line={"color": "#d8c89c", "width": 2},
            fill="tozeroy",
            fillcolor="rgba(216, 200, 156, 0.28)",
            name="Elevation",
        )
    )
    for row in climb_rows:
        start = row.get(f"start_{unit}")
        end = row.get(f"end_{unit}")
        if start is None or end is None:
            continue
        figure.add_vrect(
            x0=start,
            x1=end,
            fillcolor="rgba(245, 158, 11, 0.20)",
            line_width=0,
            layer="below",
        )
        figure.add_trace(
            go.Scatter(
                x=[(float(start) + float(end)) / 2],
                y=[plot_frame["display_altitude"].max()],
                mode="markers+text",
                marker={"color": "#cf5265", "size": 11},
                text=[str(row.get("climb"))],
                textposition="middle center",
                textfont={"color": "white", "size": 9},
                hovertemplate=(
                    f"FIETS {row.get('fiets')}<br>"
                    f"Category {row.get('category')}<extra></extra>"
                ),
                showlegend=False,
            )
        )
    figure.update_layout(
        title="GDH terrain score",
        xaxis_title=f"Distance ({unit})",
        yaxis_title=f"Elevation ({altitude_unit})",
        margin={"l": 48, "r": 24, "t": 56, "b": 48},
        showlegend=False,
    )
    return figure


def _gradient_distribution_figure(
    points: pd.DataFrame,
    unit_system: str,
) -> go.Figure | None:
    segments = _terrain_segments(points)
    if segments.empty:
        return None
    segments = segments[
        (segments["grade_pct"] >= -12) & (segments["grade_pct"] <= 12)
    ].copy()
    if segments.empty:
        return None
    bins = list(range(-12, 14))
    labels = [f"{left}%" for left in bins[:-1]]
    segments["grade_bin"] = pd.cut(
        segments["grade_pct"],
        bins=bins,
        labels=labels,
        include_lowest=True,
        right=False,
    )
    total_distance_m = float(segments["distance_m"].sum())
    distribution = (
        segments.groupby("grade_bin", observed=False)["distance_m"].sum().reset_index()
    )
    distribution["share_pct"] = distribution["distance_m"] / total_distance_m * 100
    max_share = float(distribution["share_pct"].max() or 0)
    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=distribution["grade_bin"].astype(str),
            y=distribution["share_pct"],
            marker_color="#6ab04c",
            name="Distance",
            width=0.82,
            hovertemplate="%{x}: %{y:.1f}%<extra></extra>",
        )
    )
    pace_axis: dict[str, Any] = {}
    if "speed_mps" in segments.columns and segments["speed_mps"].notna().any():
        pace_segments = segments.dropna(subset=["speed_mps"]).copy()
        pace_segments["speed_distance"] = (
            pace_segments["speed_mps"] * pace_segments["distance_m"]
        )
        pace_by_bin = pace_segments.groupby("grade_bin", observed=False).agg(
            distance_m=("distance_m", "sum"),
            speed_distance=("speed_distance", "sum"),
        )
        pace_by_bin["speed_mps"] = (
            pace_by_bin["speed_distance"] / pace_by_bin["distance_m"].replace(0, pd.NA)
        )
        pace_by_bin = pace_by_bin.reset_index().dropna(subset=["speed_mps"])
        pace_by_bin["pace"] = pace_by_bin["speed_mps"].apply(
            lambda value: pace_minutes_from_mps(value, unit_system)
        )
        pace_axis = _pace_axis_ticks(pace_by_bin["pace"])
        figure.add_trace(
            go.Scatter(
                x=pace_by_bin["grade_bin"].astype(str),
                y=pace_by_bin["pace"],
                yaxis="y2",
                mode="lines",
                line={"color": "#8fc6d6", "width": 2, "shape": "spline"},
                name="Pace",
                connectgaps=True,
            )
        )
    figure.update_layout(
        title="Gradient distribution",
        xaxis_title="Gradient",
        xaxis={
            "tickmode": "array",
            "tickvals": [f"{value}%" for value in range(-12, 13, 2)],
            "showgrid": False,
        },
        yaxis={
            "title": "Distance share",
            "ticksuffix": "%",
            "range": [0, max(20, math.ceil(max_share / 5) * 5)],
            "gridcolor": "#edf0f2",
            "zeroline": False,
        },
        yaxis2={
            "title": f"Pace ({pace_unit(unit_system)})",
            "overlaying": "y",
            "side": "right",
            "autorange": "reversed",
            "showgrid": False,
            **pace_axis,
        },
        bargap=0.08,
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin={"l": 48, "r": 56, "t": 56, "b": 48},
        legend={"orientation": "h", "x": 1, "xanchor": "right", "y": 1.12},
    )
    return figure


def _show_activity_detail(
    state: dict[str, Any],
    selected_id: int,
    detail_panel: Any,
    detail_card: Any,
    navigation: Any,
) -> bool:
    """Refresh and reveal a newly selected activity without element-specific scrolling."""
    if state.get("selected_id") == selected_id:
        return False
    state["selected_id"] = selected_id
    detail_panel.refresh()
    navigation.to(detail_card)
    return True


async def _fit_leaflet_route(route_map: Any, coordinates: list[list[float]]) -> None:
    """Fit a route without waiting for a browser-side return value.

    Leaflet methods are commands in this case; their results are unused. Awaiting
    them applies NiceGUI's short JavaScript response timeout, which can expire
    while a map inside a newly opened tab is still being laid out.
    """
    await route_map.initialized()
    route_map.run_map_method("invalidateSize")
    route_map.run_map_method(
        "fitBounds",
        coordinates,
        {"padding": [24, 24]},
    )


TRACK_MAP_TILE_URL = (
    "https://server.arcgisonline.com/ArcGIS/rest/services/"
    "World_Street_Map/MapServer/tile/{z}/{y}/{x}"
)
TRACK_MAP_FALLBACK_TILE_URL = (
    "https://server.arcgisonline.com/ArcGIS/rest/services/"
    "World_Topo_Map/MapServer/tile/{z}/{y}/{x}"
)
TRACK_MAP_TILE_ATTRIBUTION = (
    "Tiles &copy; Esri &mdash; Sources: Esri, HERE, Garmin, USGS, "
    "Intermap, INCREMENT P, NRCan, Esri Japan, METI, Esri China "
    "(Hong Kong), Esri Korea, Esri (Thailand), NGCC, "
    "&copy; OpenStreetMap contributors, and the GIS User Community"
)
TRANSPARENT_TILE_URL = (
    "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw=="
)


def _use_track_map_tiles(route_map: Any) -> None:
    """Replace NiceGUI's default OSM tile endpoint with the configured basemap."""
    route_map.clear_layers()
    route_map.tile_layer(
        url_template=TRACK_MAP_FALLBACK_TILE_URL,
        options={
            "attribution": TRACK_MAP_TILE_ATTRIBUTION,
            "maxZoom": 19,
            "zIndex": 1,
        },
    )
    route_map.tile_layer(
        url_template=TRACK_MAP_TILE_URL,
        options={
            "attribution": TRACK_MAP_TILE_ATTRIBUTION,
            "errorTileUrl": TRANSPARENT_TILE_URL,
            "maxZoom": 19,
            "zIndex": 2,
        },
    )


def register_core_pages(db_path: Path, *, sandboxed: bool) -> None:
    from nicegui import run, ui

    @ui.page("/")
    def dashboard_page() -> None:
        render_shell("Dashboard", db_path, sandboxed=sandboxed)
        with ui.column().classes("gdh-page"):
            page_heading(
                "Training Dashboard",
                "A local overview of Garmin history, athlete thresholds, and the active plan.",
            )
            preferences = interface_settings(db_path)
            unit_system = preferences["unit_system"]
            unit = distance_unit(unit_system)
            velocity_display = preferences["activity_velocity_display"]
            data = dashboard_data(
                db_path,
                item_limit=preferences["dashboard_item_limit"],
            )
            stats = data["stats"]
            plan = data["plan"]
            athlete = data["athlete"]
            with ui.row().classes("w-full gap-3 flex-wrap"):
                metric_card("Activities", f"{int(stats.get('total_activities') or 0):,}", icon="directions_run")
                lifetime = distance_from_km(
                    float(stats.get("total_distance_m") or 0) / 1000,
                    unit_system,
                    digits=0,
                )
                metric_card("Lifetime distance", f"{lifetime:,.0f} {unit}", icon="route")
                metric_card("Plan sessions", int(plan.get("sessions") or 0), icon="event_note")
                metric_card("HRmax", athlete.get("hrmax_effective") or "Not set", icon="favorite")
                metric_card("LTHR", athlete.get("lthr_effective") or "Not set", icon="monitor_heart")

            diagnostics = data["diagnostics"]
            if diagnostics.get("missing_metrics_count"):
                with ui.card().classes("gdh-card w-full bg-amber-1"):
                    ui.label(
                        f"{diagnostics['missing_metrics_count']} activities need a derived-metrics refresh."
                    ).classes("font-medium")
                    ui.link("Open Garmin Sync", "/sync")

            display_upcoming = _distance_rows(
                data["upcoming"], unit_system, "distance_km"
            )
            shareable_upcoming = _format_upcoming_plan_for_sharing(
                display_upcoming,
                unit,
            )

            def copy_upcoming_plan() -> None:
                if not display_upcoming:
                    ui.notify(
                        "There are no upcoming sessions to copy.",
                        type="warning",
                    )
                    return
                ui.clipboard.write(shareable_upcoming)
                session_count = len(display_upcoming)
                session_label = "session" if session_count == 1 else "sessions"
                ui.notify(
                    f"Copied {session_count} upcoming {session_label} "
                    "to the clipboard.",
                    type="positive",
                )

            with ui.row().classes("w-full gap-4 items-stretch"):
                with ui.card().classes("gdh-card flex-1 min-w-[420px]"):
                    with ui.row().classes(
                        "w-full items-center justify-between gap-3 flex-wrap"
                    ):
                        ui.label("Upcoming plan").classes("text-xl font-semibold")
                        copy_plan_button = ui.button(
                            "Copy plan",
                            icon="content_copy",
                            on_click=copy_upcoming_plan,
                        ).props("outline")
                        copy_plan_button.set_enabled(bool(display_upcoming))
                        if display_upcoming:
                            copy_plan_button.tooltip(
                                f"Copy the {len(display_upcoming)} upcoming "
                                "sessions shown"
                            )
                    data_grid(display_upcoming, height="21rem")
                with ui.card().classes("gdh-card flex-1 min-w-[420px]"):
                    ui.label("Recent activities").classes("text-xl font-semibold")
                    recent = [
                        {
                            "id": row.get("activity_id"),
                            "date": row.get("start_utc"),
                            "sport": row.get("sport"),
                            f"distance_{unit}": distance_from_km(
                                row.get("distance_km"), unit_system
                            ),
                            "duration_min": round(float(row["elapsed_min"]), 1)
                            if row.get("elapsed_min") is not None
                            else None,
                            **_activity_velocity_fields(
                                row.get("speed_mps"),
                                unit_system,
                                velocity_display,
                            ),
                            "avg_hr": row.get("avg_hr"),
                        }
                        for row in data["recent"]
                    ]
                    data_grid(recent, height="21rem")

    @ui.page("/activities")
    def activities_page() -> None:
        render_shell("Activities", db_path, sandboxed=sandboxed)
        with ui.column().classes("gdh-page"):
            page_heading(
                "Past Activities",
                "Filter Garmin history, inspect splits and trackpoints, and export one complete activity as JSON.",
            )
            preferences = interface_settings(db_path)
            unit_system = preferences["unit_system"]
            unit = distance_unit(unit_system)
            velocity_display = preferences["activity_velocity_display"]
            sports = activity_sports(db_path)
            state: dict[str, Any] = {"selected_id": None, "rows": []}
            with ui.card().classes("gdh-card w-full"):
                with ui.row().classes("w-full items-end gap-3 flex-wrap"):
                    default_sport = preferences["activity_default_sport"]
                    if default_sport not in sports:
                        default_sport = "All"
                    sport = ui.select(["All", *sports], value=default_sport, label="Sport").props("outlined").classes("w-52")
                    start = ui.input("Start date", value=(date.today() - timedelta(days=preferences["activity_lookback_days"])).isoformat()).props("outlined type=date").classes("w-48")
                    end = ui.input("End date", value=date.today().isoformat()).props("outlined type=date").classes("w-48")
                    limit = ui.number("Maximum rows", value=preferences["activity_row_limit"], min=25, max=5000).props("outlined").classes("w-40")
                    refresh_button = ui.button("Load activities", icon="refresh")

            @ui.refreshable
            def activity_table() -> None:
                rows = []
                for source in _distance_rows(state["rows"], unit_system, "distance_km"):
                    row = dict(source)
                    speed_mps = row.pop("speed_mps", None)
                    row.update(
                        _activity_velocity_fields(
                            speed_mps,
                            unit_system,
                            velocity_display,
                        )
                    )
                    rows.append(row)
                grid = data_grid(rows, height="34rem")
                if grid is not None:
                    def select_row(event) -> None:
                        payload = event.args if isinstance(event.args, dict) else {}
                        selected = payload.get("data") or {}
                        if selected.get("id") is not None:
                            selected_id = int(selected["id"])
                            _show_activity_detail(
                                state,
                                selected_id,
                                detail_panel,
                                detail_card,
                                ui.navigate,
                            )

                    # ``cellClicked`` is reliably forwarded by NiceGUI's AG Grid
                    # wrapper. Keep ``rowClicked`` as a compatibility fallback;
                    # the selected-id guard prevents duplicate refreshes.
                    grid.on("cellClicked", select_row)
                    grid.on("rowClicked", select_row)

            @ui.refreshable
            def detail_panel() -> None:
                activity_id = state.get("selected_id")
                if activity_id is None:
                    ui.label("Select an activity row to see its details.").classes("text-grey-7")
                    return
                detail = activity_detail(db_path, activity_id)
                if detail is None:
                    ui.label("That activity no longer exists.").classes("text-red-8")
                    return
                activity = detail["activity"]
                ui.label(f"Activity {activity_id}").classes("text-2xl font-semibold")
                with ui.row().classes("w-full gap-3 flex-wrap"):
                    for label, key, suffix, divisor in (
                        ("Sport", "activity_type", "", 1),
                        ("Distance", "distance_meters", f" {unit}", 1609.344 if unit_system == "Imperial" else 1000),
                        ("Duration", "elapsed_duration_seconds", " min", 60),
                        ("Average HR", "average_hr", " bpm", 1),
                        ("Max HR", "max_hr", " bpm", 1),
                    ):
                        value = activity.get(key)
                        if isinstance(value, (int, float)):
                            value = f"{value / divisor:.1f}{suffix}"
                        metric_card(label, value if value is not None else "-" )
                    velocity_value = (
                        speed_from_mps(activity.get("average_speed"), unit_system)
                        if velocity_display == "Speed"
                        else pace_text_from_mps(activity.get("average_speed"), unit_system)
                    )
                    velocity_label = (
                        f"Average speed ({speed_unit(unit_system)})"
                        if velocity_display == "Speed"
                        else "Average pace"
                    )
                    metric_card(velocity_label, velocity_value or "-", icon="speed")
                ui.button(
                    "Download complete JSON",
                    icon="download",
                    on_click=lambda: ui.download(
                        activity_export_json(db_path, activity_id),
                        filename=f"garmin-activity-{activity_id}.json",
                        media_type="application/json",
                    ),
                )
                tabs = ui.tabs().classes("w-full")
                with tabs:
                    overview_tab = ui.tab("Overview")
                    splits_tab = ui.tab("Splits")
                    elevation_tab = ui.tab("Elevation")
                    track_tab = ui.tab("Track")
                    raw_tab = ui.tab("All fields")
                with ui.tab_panels(tabs, value=overview_tab).classes("w-full"):
                    with ui.tab_panel(overview_tab):
                        data_grid([detail["metrics"]] if detail["metrics"] else [])
                    with ui.tab_panel(splits_tab):
                        data_grid(
                            _split_rows(
                                detail["splits"],
                                unit_system,
                                velocity_display,
                            )
                        )
                    with ui.tab_panel(elevation_tab):
                        points = pd.DataFrame(detail["trackpoints"])
                        climb_rows = _climb_summary_rows(points, unit_system)
                        downhill_rows = _downhill_summary_rows(points, unit_system)
                        summary = _terrain_summary(points, climb_rows, unit_system)
                        elevation_figure = _elevation_profile_figure(
                            points,
                            climb_rows,
                            unit_system,
                        )
                        gradient_figure = _gradient_distribution_figure(
                            points,
                            unit_system,
                        )
                        if elevation_figure is None:
                            ui.label(
                                "No elevation trackpoints are stored for this activity."
                            ).classes("text-grey-7")
                        else:
                            with ui.row().classes("w-full gap-4 items-stretch"):
                                with ui.column().classes("min-w-[420px] flex-[1.1] gap-3"):
                                    ui.plotly(elevation_figure).classes(
                                        "w-full h-[22rem]"
                                    )
                                    with ui.row().classes("w-full gap-3 flex-wrap"):
                                        metric_card(
                                            "GDH terrain score",
                                            summary["gdh_terrain_score"],
                                            icon="terrain",
                                        )
                                        metric_card(
                                            "Distance",
                                            f"{summary[f'distance_{unit}']:,g} {unit}",
                                            icon="route",
                                        )
                                        ascent_unit = (
                                            "ft" if unit_system == "Imperial" else "m"
                                        )
                                        metric_card(
                                            "Elevation gain",
                                            f"{summary[f'gain_{ascent_unit}']:,g} {ascent_unit}",
                                            icon="trending_up",
                                        )
                                        metric_card(
                                            "Terrain mix",
                                            (
                                                f"{summary['hilly_pct']}% hilly / "
                                                f"{summary['flat_pct']}% flat"
                                            ),
                                            icon="stacked_bar_chart",
                                        )
                                    ui.label(
                                        "FIETS-style categories use published category bands; "
                                        "GDH terrain score is a local 0-10 estimate "
                                        "from hilly percentage and climb difficulty totals."
                                    ).classes("text-xs text-grey-7")
                                with ui.column().classes("min-w-[420px] flex-1 gap-3"):
                                    if gradient_figure is not None:
                                        ui.plotly(gradient_figure).classes(
                                            "w-full h-[22rem]"
                                        )
                                    data_grid(
                                        _gradient_distribution_rows(
                                            points,
                                            unit_system,
                                        ),
                                        height="12rem",
                                    )
                            ui.label("Climbs").classes("text-xl font-semibold mt-5")
                            data_grid(climb_rows, height="16rem")
                            ui.label("Downhill").classes("text-xl font-semibold mt-5")
                            data_grid(downhill_rows, height="16rem")
                    with ui.tab_panel(track_tab):
                        points = pd.DataFrame(detail["trackpoints"])
                        if not points.empty and {"lon_deg", "lat_deg"}.issubset(points.columns):
                            points = points.dropna(subset=["lon_deg", "lat_deg"])
                            if points.empty:
                                ui.label("No valid GPS coordinates are stored for this activity.")
                            else:
                                # Bound the browser payload while retaining the
                                # complete track in the downloadable JSON.
                                step = max(1, len(points) // 5000)
                                mapped = points.iloc[::step]
                                if mapped.index[-1] != points.index[-1]:
                                    mapped = pd.concat([mapped, points.iloc[[-1]]])
                                coordinates = [
                                    [float(row.lat_deg), float(row.lon_deg)]
                                    for row in mapped.itertuples()
                                ]
                                latitudes = [point[0] for point in coordinates]
                                longitudes = [point[1] for point in coordinates]
                                center = (
                                    (min(latitudes) + max(latitudes)) / 2,
                                    (min(longitudes) + max(longitudes)) / 2,
                                )
                                route_map = ui.leaflet(center=center, zoom=13).classes(
                                    "w-full h-[34rem]"
                                )
                                _use_track_map_tiles(route_map)
                                route_map.generic_layer(
                                    name="polyline",
                                    args=[
                                        coordinates,
                                        {"color": "#2563eb", "weight": 4, "opacity": 0.9},
                                    ],
                                )
                                route_map.marker(
                                    latlng=tuple(coordinates[0]),
                                    options={"title": "Start"},
                                )
                                route_map.marker(
                                    latlng=tuple(coordinates[-1]),
                                    options={"title": "Finish"},
                                )

                                async def fit_route() -> None:
                                    await _fit_leaflet_route(route_map, coordinates)

                                ui.timer(0.05, fit_route, once=True)
                                ui.label(
                                    f"Route drawn from {len(points):,} stored GPS points. "
                                    "Map tiles require an internet connection."
                                ).classes("text-xs text-grey-7")
                        else:
                            ui.label("No GPS trackpoints are stored for this activity.")
                    with ui.tab_panel(raw_tab):
                        data_grid([activity], height="24rem")

            def load_rows() -> None:
                try:
                    state["rows"] = list_activities(
                        db_path,
                        sport=None if sport.value == "All" else str(sport.value),
                        start_date=str(start.value or "") or None,
                        end_date=str(end.value or "") or None,
                        limit=int(limit.value or 1000),
                    )
                except (OSError, ValueError) as exc:
                    _notify_error(exc)
                    return
                activity_table.refresh()

            refresh_button.on("click", load_rows)
            load_rows()
            activity_table()
            with ui.card().classes("gdh-card w-full") as detail_card:
                detail_panel()

    @ui.page("/charts")
    def charts_page() -> None:
        render_shell("Charts", db_path, sandboxed=sandboxed)
        with ui.column().classes("gdh-page"):
            page_heading("Training Charts", "Interactive volume, intensity, heart-rate, pace, and load views.")
            preferences = interface_settings(db_path)
            unit_system = preferences["unit_system"]
            unit = distance_unit(unit_system)
            velocity_display = preferences["activity_velocity_display"]
            sports = activity_sports(db_path)
            with ui.card().classes("gdh-card w-full"):
                with ui.row().classes("items-end gap-3 flex-wrap"):
                    start = ui.input("Start date", value=(date.today() - timedelta(days=preferences["chart_lookback_days"])).isoformat()).props("outlined type=date").classes("w-48")
                    selected_sports = ui.select(sports, value=[], multiple=True, label="Sports (blank means all)").props("outlined use-chips").classes("w-96")
                    selected_charts = ui.select(
                        [label for _, label in CHART_OPTIONS],
                        value=[label for _, label in CHART_OPTIONS],
                        multiple=True,
                        label="Charts to show",
                    ).props("outlined use-chips").classes("min-w-[22rem] flex-1")
                    draw_button = ui.button("Update charts", icon="monitoring")

            @ui.refreshable
            def render_charts() -> None:
                selected_chart_ids = _chart_ids_from_labels(selected_charts.value)
                if not selected_chart_ids:
                    ui.label("Select one or more charts to display.").classes("text-grey-7")
                    return
                frame = chart_dataframe(
                    db_path,
                    start_date=str(start.value),
                    sports=tuple(selected_sports.value or ()) or None,
                )
                if frame.empty:
                    ui.label("No activities match the selected filters.").classes("text-grey-7")
                    return
                figures = _training_chart_figures(
                    frame,
                    selected_chart_ids,
                    unit=unit,
                    unit_system=unit_system,
                    velocity_display=velocity_display,
                )
                if not figures:
                    ui.label(
                        "The selected charts need data that is not available for these filters."
                    ).classes("text-grey-7")
                    return
                with ui.grid(columns=2).classes("w-full gap-4"):
                    for figure in figures:
                        with ui.card().classes("gdh-card w-full"):
                            ui.plotly(figure).classes("w-full h-[24rem]")

            draw_button.on("click", render_charts.refresh)
            render_charts()

    @ui.page("/plan")
    def plan_page() -> None:
        render_shell("Plan", db_path, sandboxed=sandboxed)
        state: dict[str, Any] = {
            "baseline_busy": False,
            "confirmation_pending": False,
            "last_workbook": None,
        }
        with ui.column().classes("gdh-page"):
            page_heading(
                "Plan Configuration & Review",
                "Build a deterministic offline baseline or use Codex for a "
                "personalized proposal, then review the active schedule.",
            )
            preferences = interface_settings(db_path)
            unit_system = preferences["unit_system"]
            settings = planning_settings(db_path)
            try:
                selected_distance = normalize_baseline_distance(
                    settings["distance"]
                )
            except ValueError:
                selected_distance = str(settings["distance"])
            distance_options = dict(BASELINE_DISTANCE_OPTIONS)
            if selected_distance not in distance_options:
                distance_options[selected_distance] = (
                    f"{selected_distance} (Codex only)"
                )

            with ui.card().classes("gdh-card w-full"):
                ui.label("Event and schedule").classes("text-xl font-semibold")
                fields: dict[str, Any] = {}
                with ui.grid(columns=3).classes("w-full gap-3"):
                    fields["athlete_name"] = ui.input(
                        "Athlete name", value=settings["athlete_name"]
                    ).props("outlined")
                    fields["age"] = ui.number(
                        "Age", value=settings["age"], min=10, max=100
                    ).props("outlined")
                    fields["distance"] = ui.select(
                        distance_options,
                        value=selected_distance,
                        label="Race distance",
                    ).props("outlined")
                    fields["event_name"] = ui.input(
                        "Event name", value=settings["event_name"]
                    ).props("outlined")
                    fields["plan_start"] = ui.input(
                        "Plan start", value=str(settings["plan_start"])
                    ).props("outlined type=date")
                    fields["event_date"] = ui.input(
                        "Event date", value=str(settings["event_date"])
                    ).props("outlined type=date")
                    fields["run_days_per_week"] = ui.number(
                        "Run days/week",
                        value=settings["run_days_per_week"],
                        min=1,
                        max=7,
                    ).props("outlined")
                    fields["training_method"] = ui.select(
                        {
                            key: method.label
                            for key, method in TRAINING_METHODS.items()
                        },
                        value=settings["training_method"],
                        label="Training philosophy",
                    ).props("outlined")
                    fields["long_run_day"] = ui.select(
                        [
                            "Monday",
                            "Tuesday",
                            "Wednesday",
                            "Thursday",
                            "Friday",
                            "Saturday",
                            "Sunday",
                        ],
                        value=settings["long_run_day"],
                        label="Long-session day",
                    ).props("outlined")
                    fields["sodium_mg_per_hour"] = ui.number(
                        "Sodium mg/hour (0 unknown)",
                        value=settings["sodium_mg_per_hour"],
                        min=0,
                        max=3000,
                    ).props("outlined")
                with ui.grid(columns=2).classes("w-full gap-3"):
                    fields["output_directory"] = ui.input(
                        "Workbook folder",
                        value=settings["output_directory"],
                    ).props("outlined")
                    fields["output_filename"] = ui.input(
                        "Workbook filename",
                        value=settings["output_filename"],
                    ).props("outlined")

                ui.label(
                    "Offline generation never uses Codex. It applies the same "
                    "local training-policy checks before replacing the selected "
                    "plan date range."
                ).classes("text-sm text-grey-7")

                def collect_settings() -> dict[str, Any]:
                    return {
                        key: element.value for key, element in fields.items()
                    }

                def save_settings() -> None:
                    try:
                        save_planning_settings(db_path, collect_settings())
                    except (OSError, TypeError, ValueError) as exc:
                        _notify_error(exc)
                    else:
                        ui.notify("Planning settings saved locally.", type="positive")

                with ui.row().classes("gap-3 flex-wrap"):
                    save_settings_button = ui.button(
                        "Save settings", icon="save", on_click=save_settings
                    )
                    generate_baseline_button = ui.button(
                        "Generate offline baseline", icon="offline_bolt"
                    ).props("color=primary")
                    generate_workbook_button = ui.button(
                        "Generate baseline + workbook", icon="table_view"
                    ).props("outline")
                    open_codex_button = ui.button(
                        "Open Codex Coach",
                        icon="auto_awesome",
                        on_click=lambda: ui.navigate.to("/coach"),
                    ).props("outline")

                baseline_status = ui.label("Ready").classes("font-medium")
                baseline_progress = ui.linear_progress(value=0).props(
                    "indeterminate"
                )
                baseline_progress.set_visibility(False)
                download_workbook_button = ui.button(
                    "Download last workbook", icon="download"
                ).props("outline")
                download_workbook_button.set_visibility(False)

                with ui.dialog() as baseline_dialog, ui.card().classes(
                    "w-full max-w-2xl"
                ):
                    ui.label("Replace the active plan range?").classes(
                        "text-xl font-semibold"
                    )
                    baseline_confirmation = ui.label().classes(
                        "max-w-xl whitespace-pre-wrap"
                    )
                    workbook_confirmation = ui.label().classes(
                        "max-w-xl text-amber-9 break-all"
                    )
                    baseline_acknowledgement = ui.checkbox(
                        "I understand that workouts in this date range will be replaced."
                    )

                    def confirm_baseline_generation() -> None:
                        if not baseline_acknowledgement.value:
                            ui.notify(
                                "Check the replacement acknowledgement first.",
                                type="warning",
                            )
                            return
                        baseline_dialog.submit(True)

                    with ui.row().classes("w-full justify-end gap-2"):
                        ui.button(
                            "Cancel",
                            on_click=lambda: baseline_dialog.submit(False),
                        ).props("flat")
                        ui.button(
                            "Replace range and generate",
                            icon="event_repeat",
                            on_click=confirm_baseline_generation,
                        ).props("color=primary")

                def set_baseline_busy(busy: bool) -> None:
                    state["baseline_busy"] = busy
                    baseline_progress.set_visibility(busy)
                    for button in (
                        save_settings_button,
                        generate_baseline_button,
                        generate_workbook_button,
                        open_codex_button,
                    ):
                        button.set_enabled(
                            not busy and not bool(state["confirmation_pending"])
                        )

                def download_last_workbook() -> None:
                    workbook = state.get("last_workbook")
                    if not isinstance(workbook, Path) or not workbook.is_file():
                        ui.notify(
                            "The last workbook is no longer available.",
                            type="warning",
                        )
                        download_workbook_button.set_visibility(False)
                        return
                    ui.download(str(workbook))

                async def generate_reserved_baseline(
                    *, include_workbook: bool
                ) -> None:
                    values = collect_settings()
                    values_to_save = dict(values)
                    if not include_workbook:
                        values_to_save.pop("output_directory", None)
                        values_to_save.pop("output_filename", None)
                    try:
                        values["distance"] = normalize_baseline_distance(
                            values["distance"]
                        )
                        values_to_save["distance"] = values["distance"]
                        validate_planning_settings(values_to_save)
                        metrics = get_athlete_metrics(db_path)
                        request = BaselinePlanRequest(
                            athlete_name=str(values["athlete_name"] or ""),
                            age=int(values["age"]),
                            lthr=(
                                int(metrics["lthr_effective"])
                                if metrics.get("lthr_effective") is not None
                                else None
                            ),
                            hrmax=(
                                int(metrics["hrmax_effective"])
                                if metrics.get("hrmax_effective") is not None
                                else None
                            ),
                            sodium_mg_per_hour=(
                                int(values["sodium_mg_per_hour"])
                                if int(values["sodium_mg_per_hour"] or 0) > 0
                                else None
                            ),
                            event_name=str(values["event_name"] or ""),
                            distance=str(values["distance"]),
                            start_date=str(values["plan_start"]),
                            event_date=str(values["event_date"]),
                            run_days_per_week=int(values["run_days_per_week"]),
                            long_run_day=str(values["long_run_day"]),
                            training_method=str(values["training_method"]),
                        )
                        workbook_path = (
                            Path(str(values["output_directory"]))
                            / str(values["output_filename"])
                            if include_workbook
                            else None
                        )
                        expected_plan_sha256 = get_active_plan_sha256(db_path)
                    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
                        _notify_error(exc)
                        return

                    replaced_rows = sum(
                        request.start_date
                        <= str(row.get("date") or "")
                        <= request.event_date
                        for row in plan_rows(db_path)
                    )
                    destination = (
                        f"\nWorkbook: {workbook_path.expanduser().resolve()}"
                        if workbook_path is not None
                        else ""
                    )
                    baseline_confirmation.text = (
                        f"The offline builder will replace {replaced_rows} existing "
                        f"schedule row(s) from {request.start_date} through "
                        f"{request.event_date}. Rows outside that range are retained."
                    )
                    workbook_confirmation.text = (
                        (
                            "The workbook will be replaced if it already exists."
                            + destination
                        )
                        if workbook_path is not None
                        else "No workbook will be created."
                    )
                    baseline_acknowledgement.value = False
                    if not await baseline_dialog:
                        return
                    if bool(state["baseline_busy"]):
                        return

                    try:
                        save_planning_settings(db_path, values_to_save)
                    except Exception as exc:
                        baseline_status.text = (
                            "Planning settings could not be saved; "
                            "the active plan was not changed."
                        )
                        _notify_error(exc)
                        return

                    set_baseline_busy(True)
                    baseline_status.text = (
                        "Generating validated baseline and workbook..."
                        if include_workbook
                        else "Generating validated offline baseline..."
                    )
                    try:
                        result = await run.io_bound(
                            build_and_save_baseline,
                            db_path,
                            request,
                            workbook_path=workbook_path,
                            expected_active_plan_sha256=expected_plan_sha256,
                        )
                    except Exception as exc:
                        baseline_status.text = (
                            "Baseline generation failed; the active plan was not changed."
                        )
                        _notify_error(exc)
                    else:
                        render_active_schedule.refresh()
                        baseline_status.text = (
                            f"Saved {result.schedule_row_count} schedule rows "
                            f"for {result.start_date} through {result.event_date}."
                        )
                        state["last_workbook"] = None
                        download_workbook_button.set_visibility(False)
                        if result.warnings:
                            ui.notify(
                                "Baseline policy warnings:\n- "
                                + "\n- ".join(result.warnings),
                                type="warning",
                                multi_line=True,
                                close_button=True,
                            )
                        if result.workbook_error:
                            ui.notify(
                                result.workbook_error,
                                type="warning",
                                multi_line=True,
                                close_button=True,
                            )
                        elif result.workbook_path is not None:
                            state["last_workbook"] = result.workbook_path
                            download_workbook_button.set_visibility(True)
                            ui.notify(
                                f"Workbook saved: {result.workbook_path}",
                                type="positive",
                                multi_line=True,
                            )
                        else:
                            ui.notify(
                                "Offline baseline saved to the active schedule.",
                                type="positive",
                            )
                    finally:
                        set_baseline_busy(False)

                async def generate_baseline(*, include_workbook: bool) -> None:
                    if bool(state["baseline_busy"]) or bool(
                        state["confirmation_pending"]
                    ):
                        return
                    state["confirmation_pending"] = True
                    set_baseline_busy(False)
                    try:
                        await generate_reserved_baseline(
                            include_workbook=include_workbook
                        )
                    except Exception as exc:
                        baseline_status.text = (
                            "The baseline action encountered an unexpected error. "
                            "Refresh Plan to verify the active schedule."
                        )
                        _notify_error(exc)
                    finally:
                        state["confirmation_pending"] = False
                        set_baseline_busy(False)

                async def generate_offline_baseline() -> None:
                    await generate_baseline(include_workbook=False)

                async def generate_baseline_workbook() -> None:
                    await generate_baseline(include_workbook=True)

                generate_baseline_button.on(
                    "click", generate_offline_baseline
                )
                generate_workbook_button.on(
                    "click", generate_baseline_workbook
                )
                download_workbook_button.on(
                    "click", download_last_workbook
                )

            with ui.card().classes("gdh-card w-full"):
                ui.label("Athlete thresholds").classes("text-xl font-semibold")
                metrics = get_athlete_metrics(db_path)
                with ui.row().classes("items-end gap-3 flex-wrap"):
                    hrmax = ui.number(
                        "HRmax override (0 clears)",
                        value=metrics.get("hrmax_effective") or 0,
                        min=0,
                        max=250,
                    ).props("outlined")
                    lthr = ui.number(
                        "LTHR override (0 clears)",
                        value=metrics.get("lthr_effective") or 0,
                        min=0,
                        max=220,
                    ).props("outlined")
                    years = ui.number(
                        "History years", value=5, min=1, max=50
                    ).props("outlined")

                    def save_thresholds() -> None:
                        high = int(hrmax.value or 0) or None
                        threshold = int(lthr.value or 0) or None
                        if high and threshold and threshold >= high:
                            _notify_error("LTHR must be below HRmax")
                            return
                        set_override_metrics(db_path, high, threshold)
                        ui.notify("Threshold overrides saved.", type="positive")

                    def clear_thresholds() -> None:
                        clear_override_metrics(db_path)
                        ui.notify("Threshold overrides cleared.", type="positive")

                    async def recalculate() -> None:
                        result = await run.io_bound(
                            calculate_metrics_from_db_sources,
                            db_path,
                            int(years.value or 5),
                        )
                        high, threshold, error = result
                        if error:
                            _notify_error(error)
                            return
                        set_calculated_metrics(db_path, high, threshold)
                        hrmax.value = high or 0
                        lthr.value = threshold or 0
                        ui.notify("Calculated thresholds refreshed.", type="positive")

                    ui.button("Save override", on_click=save_thresholds)
                    ui.button("Clear override", on_click=clear_thresholds).props("outline")
                    ui.button("Recalculate", on_click=recalculate).props("outline")

            with ui.card().classes("gdh-card w-full"):
                ui.label("Active training schedule").classes("text-xl font-semibold")

                @ui.refreshable
                def render_active_schedule() -> None:
                    schedule = plan_rows(db_path)
                    display_schedule = _distance_rows(
                        schedule, unit_system, "distance_km"
                    )
                    with ui.row().classes("w-full gap-3 flex-wrap"):
                        metric_card("Sessions", len(schedule))
                        metric_card(
                            "Start", schedule[0]["date"] if schedule else "-"
                        )
                        metric_card(
                            "End", schedule[-1]["date"] if schedule else "-"
                        )
                        metric_card(
                            "Strength",
                            sum(
                                1
                                for row in schedule
                                if row.get("sport") == "strength"
                            ),
                        )
                    plan_tabs = ui.tabs().classes("w-full")
                    with plan_tabs:
                        workouts_tab = ui.tab("Workouts")
                        macros_tab = ui.tab("Daily macros")
                    with ui.tab_panels(
                        plan_tabs, value=workouts_tab
                    ).classes("w-full"):
                        with ui.tab_panel(workouts_tab):
                            data_grid(display_schedule, height="34rem")
                        with ui.tab_panel(macros_tab):
                            data_grid(nutrition_rows(db_path), height="34rem")

                render_active_schedule()

    @ui.page("/compliance")
    def compliance_page() -> None:
        render_shell("Compliance", db_path, sandboxed=sandboxed)
        with ui.column().classes("gdh-page"):
            page_heading("Plan Compliance", "Compare planned and completed distance and duration through today.")
            preferences = interface_settings(db_path)
            unit_system = preferences["unit_system"]
            unit = distance_unit(unit_system)
            data = compliance_data(db_path)
            totals = data["totals"]
            display_rows = _distance_rows(
                data["rows"], unit_system, "planned_km", "actual_km"
            )
            with ui.row().classes("w-full gap-3 flex-wrap"):
                metric_card("Duration compliance", f"{totals['duration_compliance_pct']:.1f}%")
                metric_card("Distance compliance", f"{totals['distance_compliance_pct']:.1f}%")
                metric_card("Planned hours", totals["planned_hours"])
                metric_card("Actual hours", totals["actual_hours"])
                metric_card(
                    f"Planned distance ({unit})",
                    distance_from_km(totals["planned_km"], unit_system),
                )
                metric_card(
                    f"Actual distance ({unit})",
                    distance_from_km(totals["actual_km"], unit_system),
                )
            frame = pd.DataFrame(data["rows"])
            if not frame.empty:
                frame["date"] = pd.to_datetime(frame["date"])
                chart = go.Figure()
                chart.add_bar(x=frame["date"], y=frame["planned_hours"], name="Planned")
                chart.add_bar(x=frame["date"], y=frame["actual_hours"], name="Actual")
                chart.update_layout(barmode="group", title="Daily duration (hours)")
                with ui.card().classes("gdh-card w-full"):
                    ui.plotly(chart).classes("w-full h-[30rem]")
            with ui.card().classes("gdh-card w-full"):
                data_grid(display_rows, height="31rem")

    @ui.page("/sync")
    def sync_page() -> None:
        render_shell("Garmin Sync", db_path, sandboxed=sandboxed)
        job = get_sync_job(db_path)
        preferences = interface_settings(db_path)
        credential_store_available = True
        credential_problem: str | None = None
        try:
            saved_status = credential_status()
        except CredentialStoreUnavailable as exc:
            credential_store_available = False
            credential_problem = str(exc)
            saved_email = None
            credential_backend = "Windows Credential Manager"
        except CredentialStoreError as exc:
            credential_problem = str(exc)
            saved_email = None
            credential_backend = "Windows Credential Manager"
        else:
            saved_email = saved_status.email
            credential_backend = saved_status.backend_name
        legacy_env_path = db_path.parent / ".env"
        state = {
            "handled": "idle",
            "handled_error": None,
            "saved_email": saved_email,
            "credential_problem": credential_problem,
            "credential_may_exist": bool(saved_email)
            or bool(credential_problem and credential_store_available),
            "resetting_browser": False,
            "editing_login": not bool(saved_email),
            "login_reset_required": bool(saved_email)
            or bool(credential_problem)
            or _browser_login_state_exists(db_path.parent),
        }
        with ui.column().classes("gdh-page"):
            page_heading(
                "Garmin Sync",
                "Reuse the saved Garmin session for normal syncs. Update the "
                "login only when Garmin requires a fresh sign-in.",
            )
            with ui.card().classes("gdh-card w-full"):
                ui.label("Garmin login session").classes("text-lg font-semibold")
                ui.label(
                    "Normal sync restores Garmin's browser session first. Your "
                    "saved Windows credential is available only if Garmin sends "
                    "that browser through a fresh sign-in."
                ).classes("text-grey-7")
                credential_label = ui.label().classes("font-medium")
                with ui.row().classes("w-full items-center gap-3 flex-wrap"):
                    update_login_button = ui.button(
                        "Update login",
                        icon="manage_accounts",
                    ).props("outline")
                    forget_login_button = ui.button(
                        "Forget saved login",
                        icon="delete_outline",
                    ).props("outline color=negative")
                    reset_browser_button = ui.button(
                        "Reset browser login",
                        icon="restart_alt",
                    ).props("outline")
                with ui.column().classes("w-full gap-3") as login_editor:
                    ui.label("Initial sign-in or login update").classes(
                        "font-semibold"
                    )
                    with ui.row().classes("w-full items-end gap-3 flex-wrap"):
                        email = (
                            ui.input(
                                "Garmin Connect email",
                                value=str(saved_email or ""),
                            )
                            .props("outlined")
                            .classes("w-80 max-w-full")
                        )
                        password = (
                            ui.input(
                                "Garmin Connect password",
                                password=True,
                                password_toggle_button=True,
                            )
                            .props("outlined")
                            .classes("w-80 max-w-full")
                        )
                    with ui.row().classes(
                        "w-full items-center gap-3 flex-wrap"
                    ):
                        remember = ui.checkbox(
                            "Remember on this Windows account",
                            value=credential_store_available,
                        )
                        save_login_button = ui.button(
                            "Save login",
                            icon="key",
                        ).props("outline")
                        cancel_login_button = ui.button(
                            "Cancel login update",
                            icon="close",
                        ).props("flat")
                    ui.label(
                        "The password is never filled back into this page or added "
                        "to the sync command or log. Leave Remember enabled so "
                        "ordinary syncs do not ask for it again."
                    ).classes("text-grey-7")
                ui.label(
                    "If Garmin requests MFA during a fresh sign-in, enter the "
                    "separate code in the Chrome window. The MFA code is never saved."
                ).classes("text-grey-7")
                ui.label(
                    "Updating a different Garmin account also requires Reset browser "
                    "login; otherwise the existing session can take precedence. Use "
                    "a separate database so accounts are not mixed."
                ).classes("text-amber-9")
                if legacy_env_path.is_file():
                    ui.label(
                        f"Legacy plaintext credentials were found in {legacy_env_path}. "
                        "Credential Manager values take precedence for GUI syncs; remove the .env file after confirming the saved login works."
                    ).classes("text-amber-9")
                with ui.dialog() as reset_browser_dialog, ui.card():
                    ui.label("Reset Garmin browser login?").classes(
                        "text-lg font-semibold"
                    )
                    ui.label(
                        "This removes Garmin's local browser profile and session "
                        "cookie backup. It keeps your Windows credential and Garmin "
                        "database. The next sync will perform a fresh sign-in; Garmin "
                        "may request MFA. Use a separate database for another account."
                    ).classes("max-w-lg")
                    with ui.row().classes("w-full justify-end gap-2"):
                        ui.button(
                            "Cancel",
                            on_click=lambda: reset_browser_dialog.submit(False),
                        ).props("flat")
                        ui.button(
                            "Reset browser login",
                            on_click=lambda: reset_browser_dialog.submit(True),
                        ).props("color=negative")
            data = dashboard_data(db_path)
            diagnostics = data["diagnostics"]
            with ui.row().classes("w-full gap-3 flex-wrap"):
                metric_card("Activities", diagnostics.get("total_activities", 0))
                metric_card("Metrics rows", diagnostics.get("total_metrics_rows", 0))
                metric_card("Needs refresh", diagnostics.get("missing_metrics_count", 0))
            with ui.card().classes("gdh-card w-full"):
                with ui.row().classes("items-end gap-3 flex-wrap"):
                    days = ui.number("Days to sync (0 uses CLI default)", value=preferences["sync_lookback_days"], min=0, max=3650).props("outlined")
                    start_button = ui.button("Run sync", icon="sync")
                    stop_button = ui.button("Stop", icon="stop").props("color=negative")
                    stop_button.disable()
                    clear_button = ui.button("Clear log", icon="delete_sweep").props("outline")
                    repair_button = ui.button("Repair derived metrics", icon="build").props("outline")
                status = ui.label("Ready").classes("font-medium")
                progress = ui.linear_progress(value=0).classes("w-full")
                log = ui.textarea("Sync log", value="").props("outlined readonly").classes("w-full").style("height: 28rem")

            def refresh_login_editor() -> None:
                editing = bool(state["editing_login"])
                login_editor.set_visibility(editing)
                update_login_button.set_visibility(not editing)
                cancel_login_button.set_visibility(
                    editing and bool(state["saved_email"])
                )
                save_login_button.text = (
                    "Save updated login"
                    if state["saved_email"]
                    else "Save login"
                )

            def refresh_credential_controls(*, enabled: bool = True) -> None:
                enabled = enabled and not bool(state["resetting_browser"])
                editing = bool(state["editing_login"])
                email.set_enabled(enabled and editing)
                password.set_enabled(enabled and editing)
                remember.set_enabled(
                    enabled and editing and credential_store_available
                )
                save_login_button.set_enabled(
                    enabled and editing and credential_store_available
                )
                cancel_login_button.set_enabled(enabled and editing)
                update_login_button.set_enabled(enabled and not editing)
                forget_login_button.set_enabled(
                    enabled
                    and credential_store_available
                    and bool(state["credential_may_exist"])
                )
                reset_browser_button.set_enabled(enabled)

            def refresh_credential_label() -> None:
                if state["saved_email"]:
                    credential_label.text = (
                        f"Garmin session ready for {state['saved_email']}. "
                        f"The backup login is protected by {credential_backend}."
                    )
                elif state["credential_problem"]:
                    if credential_store_available:
                        recovery_action = (
                            " Enter a complete login to replace it."
                        )
                        if state["credential_may_exist"]:
                            recovery_action = (
                                " Enter a complete login to replace it, or choose "
                                "Forget saved login."
                            )
                        credential_label.text = (
                            "Could not read the saved Garmin login: "
                            f"{state['credential_problem']}.{recovery_action}"
                        )
                    else:
                        credential_label.text = (
                            f"Secure storage unavailable: {state['credential_problem']}. "
                            "Credentials can still be used once without being saved."
                        )
                else:
                    credential_label.text = (
                        "Set up the Garmin login once. After the browser sign-in "
                        f"and MFA are complete, {credential_backend} supplies it "
                        "without showing these fields again."
                    )

            def edit_login() -> None:
                state["editing_login"] = True
                email.value = str(state["saved_email"] or "")
                password.value = ""
                refresh_login_editor()
                refresh_credential_controls()

            def cancel_login_update() -> None:
                if not state["saved_email"]:
                    return
                state["editing_login"] = False
                email.value = str(state["saved_email"])
                password.value = ""
                refresh_login_editor()
                refresh_credential_controls()

            async def _confirm_browser_login_reset() -> bool:
                if job.snapshot().state in {
                    "running",
                    "cancelling",
                    "resetting_login",
                }:
                    _notify_error(
                        "Stop Garmin sync before resetting its browser login"
                    )
                    return False
                if not await reset_browser_dialog:
                    return False
                if job.snapshot().state in {
                    "running",
                    "cancelling",
                    "resetting_login",
                }:
                    _notify_error(
                        "Garmin sync started; stop it before resetting login"
                    )
                    return False
                return True

            async def _apply_browser_login_update(
                operation: Callable[[], Any],
                *,
                reset_required: bool,
            ) -> tuple[BrowserSessionResetResult | None, Any] | None:
                reset_confirmed = False
                confirmation_needed = reset_required
                while True:
                    if confirmation_needed:
                        if not await _confirm_browser_login_reset():
                            return None
                        reset_confirmed = True
                    state["resetting_browser"] = True
                    start_button.disable()
                    refresh_credential_controls(enabled=False)
                    retry_with_confirmation = False
                    try:
                        applied = await run.io_bound(
                            job.apply_browser_login_update,
                            operation,
                            reset_confirmed=reset_confirmed,
                        )
                    except BrowserSessionResetRequired:
                        if reset_confirmed:
                            raise
                        retry_with_confirmation = True
                    else:
                        if applied is None:
                            return None
                        if applied[0] is not None:
                            state["login_reset_required"] = False
                        return applied
                    finally:
                        state["resetting_browser"] = False
                        poll_sync()
                    if retry_with_confirmation:
                        confirmation_needed = True

            async def reset_browser_login() -> None:
                try:
                    applied = await _apply_browser_login_update(
                        lambda: None,
                        reset_required=True,
                    )
                except (BrowserSessionResetError, RuntimeError) as exc:
                    _notify_error(exc)
                    return
                if applied is None:
                    return
                result = applied[0]
                if result is None:
                    return
                message = (
                    "Garmin browser login reset. The next sync will perform a fresh sign-in."
                    if result.removed_anything
                    else "No saved Garmin browser login was found."
                )
                ui.notify(message, type="positive")

            async def save_login() -> None:
                entered_email = str(email.value or "")
                entered_password = str(password.value or "")
                password.value = ""
                if not credential_store_available:
                    _notify_error(
                        state["credential_problem"]
                        or "Windows Credential Manager is unavailable"
                    )
                    return
                try:
                    credentials = GarminCredentials(
                        entered_email,
                        entered_password,
                    )
                except ValueError as exc:
                    _notify_error(exc)
                    return

                def commit_login() -> GarminCredentials:
                    return save_credentials(
                        credentials.email,
                        credentials.password,
                    )

                try:
                    applied = await _apply_browser_login_update(
                        commit_login,
                        reset_required=bool(state["login_reset_required"])
                        or _browser_login_state_exists(db_path.parent),
                    )
                except (
                    BrowserSessionResetError,
                    CredentialStoreError,
                    RuntimeError,
                    ValueError,
                ) as exc:
                    _notify_error(exc)
                    return
                if applied is None:
                    return
                reset_result, credentials = applied
                state["saved_email"] = credentials.email
                state["credential_problem"] = None
                state["credential_may_exist"] = True
                state["editing_login"] = False
                state["login_reset_required"] = False
                email.value = credentials.email
                refresh_credential_label()
                refresh_login_editor()
                refresh_credential_controls()
                ui.notify(
                    (
                        "Garmin login updated securely after resetting the old "
                        "browser session."
                        if reset_result is not None
                        and reset_result.removed_anything
                        else "Garmin login saved securely for this Windows account."
                    ),
                    type="positive",
                )

            def forget_login() -> None:
                try:
                    removed = delete_credentials()
                except CredentialStoreError as exc:
                    _notify_error(exc)
                    return
                state["saved_email"] = None
                state["credential_problem"] = None
                state["credential_may_exist"] = False
                state["editing_login"] = True
                state["login_reset_required"] = _browser_login_state_exists(
                    db_path.parent
                )
                email.value = ""
                password.value = ""
                refresh_credential_label()
                refresh_login_editor()
                refresh_credential_controls()
                message = (
                    "Saved Garmin login removed. Browser session data was left unchanged."
                    if removed
                    else "No saved Garmin login was found."
                )
                ui.notify(message, type="info")

            async def start_sync() -> None:
                editing_login = bool(state["editing_login"])
                entered_email = str(email.value or "")
                entered_password = str(password.value or "")
                password.value = ""
                saved_credentials = None
                if credential_store_available and (
                    not editing_login or not entered_password
                ):
                    try:
                        saved_credentials = load_credentials()
                    except CredentialStoreError as exc:
                        _mark_saved_login_unavailable(
                            state,
                            exc,
                            credential_may_exist=True,
                        )
                        email.value = ""
                        refresh_credential_label()
                        refresh_login_editor()
                        refresh_credential_controls()
                        _notify_error(exc)
                        return
                try:
                    credentials, entered = _credentials_for_sync(
                        editing_login,
                        entered_email,
                        entered_password,
                        saved_credentials,
                    )
                except ValueError as exc:
                    if not editing_login or (
                        saved_credentials is None and bool(state["saved_email"])
                    ):
                        _mark_saved_login_unavailable(
                            state,
                            exc,
                            credential_may_exist=saved_credentials is not None,
                        )
                        email.value = ""
                        refresh_credential_label()
                        refresh_login_editor()
                        refresh_credential_controls()
                    _notify_error(exc)
                    return

                try:
                    days_to_sync = int(days.value or 0)
                except (TypeError, ValueError) as exc:
                    _notify_error(exc)
                    return

                login_commit: dict[str, str | None] = {}
                remember_login = entered and bool(remember.value)
                remove_old_login = (
                    entered
                    and not remember_login
                    and credential_store_available
                    and bool(state["credential_may_exist"])
                )

                def publish_login_commit() -> None:
                    if "saved_email" not in login_commit:
                        return
                    committed_email = login_commit["saved_email"]
                    state["saved_email"] = committed_email
                    state["credential_problem"] = None
                    state["credential_may_exist"] = bool(committed_email)
                    state["editing_login"] = not bool(committed_email)
                    email.value = str(committed_email or entered_email)
                    refresh_credential_label()
                    refresh_login_editor()
                    refresh_credential_controls()

                def launch_with_entered_login() -> GarminCredentials:
                    launch_credentials = credentials
                    if remember_login:
                        if not credential_store_available:
                            raise CredentialStoreUnavailable(
                                "Windows Credential Manager is unavailable; "
                                "clear Remember to use these credentials once"
                            )
                        launch_credentials = save_credentials(
                            credentials.email,
                            credentials.password,
                        )
                        login_commit["saved_email"] = launch_credentials.email
                    elif remove_old_login:
                        delete_credentials()
                        login_commit["saved_email"] = None
                    job.start(
                        days=days_to_sync,
                        credentials=launch_credentials,
                    )
                    return launch_credentials

                try:
                    if entered:
                        applied = await _apply_browser_login_update(
                            launch_with_entered_login,
                            reset_required=bool(state["login_reset_required"])
                            or _browser_login_state_exists(db_path.parent),
                        )
                        if applied is None:
                            return
                        _, credentials = applied
                    else:
                        job.start(
                            days=days_to_sync,
                            credentials=credentials,
                        )
                except (
                    BrowserSessionResetError,
                    CredentialStoreError,
                    OSError,
                    RuntimeError,
                    ValueError,
                ) as exc:
                    publish_login_commit()
                    state["login_reset_required"] = (
                        _browser_login_state_exists(db_path.parent)
                    )
                    _notify_error(exc)
                    return
                publish_login_commit()
                state["login_reset_required"] = True
                state["handled"] = "running"
                state["handled_error"] = None
                start_button.disable()
                stop_button.enable()
                clear_button.disable()
                refresh_credential_controls(enabled=False)
                ui.notify(
                    (
                        "Fresh Garmin sign-in started. Complete MFA in Chrome "
                        "if Garmin requests it."
                        if entered
                        else "Sync started with the saved Garmin session. Complete "
                        "MFA in Chrome only if Garmin requests a fresh sign-in."
                    ),
                    type="info",
                )

            def stop_sync() -> None:
                try:
                    if job.cancel():
                        state["handled_error"] = None
                        status.text = "Cancelling sync..."
                        stop_button.disable()
                except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
                    _notify_error(exc)

            def clear_sync_log() -> None:
                try:
                    job.clear_log()
                except (OSError, RuntimeError) as exc:
                    _notify_error(exc)
                else:
                    log.value = ""

            async def repair() -> None:
                repair_button.disable()
                try:
                    summary = await run.io_bound(repair_derived_metrics, db_path)
                except Exception as exc:
                    _notify_error(exc)
                else:
                    ui.notify(f"Derived metrics refreshed: {summary}", type="positive", multi_line=True)
                finally:
                    repair_button.enable()

            start_button.on("click", start_sync)
            stop_button.on("click", stop_sync)
            clear_button.on("click", clear_sync_log)
            repair_button.on("click", repair)
            save_login_button.on("click", save_login)
            update_login_button.on("click", edit_login)
            cancel_login_button.on("click", cancel_login_update)
            forget_login_button.on("click", forget_login)
            reset_browser_button.on("click", reset_browser_login)

            def poll_sync() -> None:
                snapshot = job.snapshot()
                progress.value = snapshot.progress
                minutes, seconds = divmod(int(snapshot.elapsed_seconds), 60)
                if snapshot.state == "resetting_login":
                    status.text = "Resetting browser login..."
                elif snapshot.state == "running" and _sync_waiting_for_mfa(
                    snapshot.log
                ):
                    status.text = "Waiting for Garmin MFA in Chrome..."
                else:
                    status.text = (
                        f"{snapshot.state.title()} - {minutes}:{seconds:02d}"
                    )
                start_enabled, stop_enabled, clear_enabled = (
                    _sync_controls_for_state(snapshot.state)
                )
                start_button.set_enabled(
                    start_enabled and not bool(state["resetting_browser"])
                )
                stop_button.set_enabled(stop_enabled)
                clear_button.set_enabled(clear_enabled)
                refresh_credential_controls(
                    enabled=snapshot.state
                    not in {"running", "cancelling", "resetting_login"}
                )
                if log.value != snapshot.log:
                    log.value = snapshot.log
                if (
                    snapshot.state == "running"
                    and snapshot.error
                    and state["handled_error"] != snapshot.error
                ):
                    state["handled_error"] = snapshot.error
                    _notify_error(snapshot.error)
                if snapshot.state in {
                    "running",
                    "cancelling",
                    "resetting_login",
                }:
                    return
                if state["handled"] == snapshot.state:
                    return
                state["handled"] = snapshot.state
                if snapshot.state == "completed":
                    if state["saved_email"]:
                        state["editing_login"] = False
                        refresh_login_editor()
                        refresh_credential_controls()
                    ui.notify("Garmin sync completed successfully.", type="positive")
                elif snapshot.state == "failed":
                    _notify_error(snapshot.error or f"Sync exited with {snapshot.return_code}")
                elif snapshot.state == "cancelled":
                    ui.notify("Garmin sync stopped.", type="info")

            refresh_credential_label()
            refresh_login_editor()
            poll_sync()
            ui.timer(1.0, poll_sync)

    @ui.page("/query")
    def query_page() -> None:
        render_shell("Data Query", db_path, sandboxed=sandboxed)
        with ui.column().classes("gdh-page"):
            page_heading(
                "Data Query",
                "Analyze sleep and recovery, run guarded read-only SQLite "
                "queries, or use advanced garmin_mcp tools.",
            )
            tabs = ui.tabs().classes("w-full")
            with tabs:
                recovery_tab = ui.tab("Sleep & Recovery", icon="bedtime")
                sql_tab = ui.tab("Read-only SQL")
                mcp_tab = ui.tab("MCP tool")
            with ui.tab_panels(tabs, value=recovery_tab).classes(
                "gdh-card w-full bg-white"
            ):
                with ui.tab_panel(recovery_tab):
                    today = date.today()
                    recovery_state: dict[str, Any] = {"analysis": None}
                    with ui.row().classes("w-full items-end gap-3 flex-wrap"):
                        recovery_start = ui.input(
                            "Start date",
                            value=(today - timedelta(days=27)).isoformat(),
                        ).props("outlined type=date").classes("w-48")
                        recovery_end = ui.input(
                            "End date",
                            value=today.isoformat(),
                        ).props("outlined type=date").classes("w-48")
                        analyze_recovery_button = ui.button(
                            "Analyze recovery",
                            icon="insights",
                        )
                        for days in (14, 30, 90):
                            ui.button(
                                f"{days} days",
                                on_click=lambda _, count=days: (
                                    setattr(recovery_end, "value", today.isoformat()),
                                    setattr(
                                        recovery_start,
                                        "value",
                                        (today - timedelta(days=count - 1)).isoformat(),
                                    ),
                                ),
                            ).props("flat dense")

                    with ui.card().classes("gdh-card w-full bg-blue-1"):
                        ui.label(
                            "This analysis compares your Garmin trends and personal "
                            "baselines. Consumer wearable sleep stages and recovery "
                            "signals are estimates, not a diagnosis; prioritize "
                            "patterns over individual nights."
                        ).classes("text-blue-10")

                    @ui.refreshable
                    def recovery_results() -> None:
                        analysis = recovery_state["analysis"]
                        if analysis is None:
                            ui.label(
                                "Choose a period and select Analyze recovery. A "
                                "28-day window gives the 7-day comparisons useful "
                                "context."
                            ).classes("text-grey-7 py-6")
                            return
                        rows = analysis["rows"]
                        summary = analysis["summary"]
                        if not rows:
                            with ui.card().classes("gdh-card w-full bg-amber-1"):
                                ui.label(
                                    "No sleep or recovery records were found in "
                                    "this period. Run Garmin Sync with health data "
                                    "enabled, then try again."
                                ).classes("text-amber-10")
                            data_grid(analysis["sources"], height="14rem")
                            return

                        def latest_value(key: str) -> Any:
                            return next(
                                (
                                    row[key]
                                    for row in reversed(rows)
                                    if row.get(key) is not None
                                ),
                                None,
                            )

                        def display(value: Any, suffix: str = "") -> str:
                            return f"{value}{suffix}" if value is not None else "—"

                        with ui.row().classes("w-full gap-3 flex-wrap"):
                            metric_card(
                                "Average sleep",
                                display(summary["average_sleep_hours"], " h"),
                                icon="bedtime",
                            )
                            metric_card(
                                "Average sleep score",
                                display(summary["average_sleep_score"], "/100"),
                                icon="hotel_class",
                            )
                            metric_card(
                                "Duration variability",
                                display(
                                    summary["sleep_duration_sd_hours"], " h SD"
                                ),
                                icon="swap_vert",
                            )
                            metric_card(
                                "Latest readiness",
                                display(latest_value("readiness_score"), "/100"),
                                icon="battery_charging_full",
                            )
                            metric_card(
                                "Latest HRV average",
                                display(latest_value("hrv_weekly"), " ms"),
                                icon="monitor_heart",
                            )
                            metric_card(
                                "Body Battery at wake",
                                display(latest_value("body_battery_wake"), "/100"),
                                icon="battery_full",
                            )

                        with ui.card().classes("gdh-card w-full"):
                            ui.label("Deep-dive observations").classes(
                                "text-xl font-semibold"
                            )
                            for insight in analysis["insights"]:
                                with ui.row().classes(
                                    "w-full items-start gap-3 flex-nowrap"
                                ):
                                    ui.icon("search").classes("text-blue-7 mt-1")
                                    with ui.column().classes("gap-0"):
                                        ui.label(insight["title"]).classes(
                                            "font-semibold"
                                        )
                                        ui.label(insight["text"]).classes(
                                            "text-slate-700"
                                        )

                        frame = pd.DataFrame(rows)
                        frame["date"] = pd.to_datetime(frame["date"])
                        with ui.row().classes("w-full gap-4 items-stretch flex-wrap"):
                            with ui.card().classes(
                                "gdh-card flex-1 min-w-[30rem]"
                            ):
                                ui.label("Sleep duration vs. need").classes(
                                    "text-lg font-semibold"
                                )
                                figure = go.Figure()
                                figure.add_trace(
                                    go.Bar(
                                        x=frame["date"],
                                        y=frame["sleep_hours"],
                                        name="Recorded sleep",
                                    )
                                )
                                figure.add_trace(
                                    go.Scatter(
                                        x=frame["date"],
                                        y=frame["sleep_need_hours"],
                                        name="Garmin sleep need",
                                        mode="lines+markers",
                                    )
                                )
                                figure.update_layout(
                                    yaxis_title="Hours",
                                    legend_orientation="h",
                                    margin=dict(l=40, r=20, t=20, b=40),
                                )
                                ui.plotly(figure).classes("w-full").style(
                                    "height: 25rem"
                                )

                            with ui.card().classes(
                                "gdh-card flex-1 min-w-[30rem]"
                            ):
                                ui.label("Sleep-stage composition").classes(
                                    "text-lg font-semibold"
                                )
                                stage_figure = go.Figure()
                                for key, label in (
                                    ("deep_min", "Deep"),
                                    ("rem_min", "REM"),
                                    ("light_min", "Light"),
                                    ("awake_min", "Awake"),
                                ):
                                    stage_figure.add_trace(
                                        go.Bar(
                                            x=frame["date"],
                                            y=frame[key],
                                            name=label,
                                        )
                                    )
                                stage_figure.update_layout(
                                    barmode="stack",
                                    yaxis_title="Minutes",
                                    legend_orientation="h",
                                    margin=dict(l=40, r=20, t=20, b=40),
                                )
                                ui.plotly(stage_figure).classes("w-full").style(
                                    "height: 25rem"
                                )

                        with ui.row().classes("w-full gap-4 items-stretch flex-wrap"):
                            with ui.card().classes(
                                "gdh-card flex-1 min-w-[30rem]"
                            ):
                                ui.label("Recovery signals").classes(
                                    "text-lg font-semibold"
                                )
                                recovery_figure = go.Figure()
                                for key, label in (
                                    ("sleep_score", "Sleep score"),
                                    ("readiness_score", "Training readiness"),
                                    ("body_battery_wake", "Body Battery at wake"),
                                    ("daily_stress", "Daily stress"),
                                ):
                                    recovery_figure.add_trace(
                                        go.Scatter(
                                            x=frame["date"],
                                            y=frame[key],
                                            name=label,
                                            mode="lines+markers",
                                        )
                                    )
                                recovery_figure.update_layout(
                                    yaxis_title="Garmin scale (0–100)",
                                    legend_orientation="h",
                                    margin=dict(l=40, r=20, t=20, b=40),
                                )
                                ui.plotly(recovery_figure).classes("w-full").style(
                                    "height: 25rem"
                                )

                            with ui.card().classes(
                                "gdh-card flex-1 min-w-[30rem]"
                            ):
                                ui.label("HRV and resting heart rate").classes(
                                    "text-lg font-semibold"
                                )
                                physiology_figure = make_subplots(
                                    specs=[[{"secondary_y": True}]]
                                )
                                physiology_figure.add_trace(
                                    go.Scatter(
                                        x=frame["date"],
                                        y=frame["hrv_nightly"],
                                        name="Nightly HRV",
                                        mode="lines+markers",
                                    ),
                                    secondary_y=False,
                                )
                                physiology_figure.add_trace(
                                    go.Scatter(
                                        x=frame["date"],
                                        y=frame["hrv_weekly"],
                                        name="7-day HRV",
                                        mode="lines",
                                    ),
                                    secondary_y=False,
                                )
                                physiology_figure.add_trace(
                                    go.Scatter(
                                        x=frame["date"],
                                        y=frame["resting_hr"],
                                        name="Resting HR",
                                        mode="lines+markers",
                                    ),
                                    secondary_y=True,
                                )
                                physiology_figure.update_yaxes(
                                    title_text="HRV (ms)", secondary_y=False
                                )
                                physiology_figure.update_yaxes(
                                    title_text="Resting HR (bpm)", secondary_y=True
                                )
                                physiology_figure.update_layout(
                                    legend_orientation="h",
                                    margin=dict(l=40, r=40, t=20, b=40),
                                )
                                ui.plotly(physiology_figure).classes("w-full").style(
                                    "height: 25rem"
                                )

                        if analysis["relationships"]:
                            with ui.card().classes("gdh-card w-full"):
                                ui.label("Exploratory relationships").classes(
                                    "text-xl font-semibold"
                                )
                                ui.label(
                                    "Pearson correlations summarize association, "
                                    "not cause and effect. At least five paired days "
                                    "are required."
                                ).classes("text-grey-7")
                                data_grid(
                                    analysis["relationships"], height="12rem"
                                )

                        with ui.card().classes("gdh-card w-full"):
                            ui.label("7-day comparison").classes(
                                "text-xl font-semibold"
                            )
                            data_grid(analysis["trends"], height="18rem")

                        latest_notes = [
                            ("Sleep feedback", latest_value("sleep_feedback")),
                            ("Sleep insight", latest_value("sleep_insight")),
                            ("HRV feedback", latest_value("hrv_feedback")),
                            (
                                "Readiness feedback",
                                latest_value("readiness_feedback"),
                            ),
                        ]
                        latest_notes = [item for item in latest_notes if item[1]]
                        if latest_notes:
                            with ui.card().classes("gdh-card w-full"):
                                ui.label("Latest Garmin context").classes(
                                    "text-xl font-semibold"
                                )
                                for label, value in latest_notes:
                                    ui.label(f"{label}: {value}").classes(
                                        "text-slate-700"
                                    )

                        table_fields = (
                            "date",
                            "sleep_hours",
                            "sleep_need_hours",
                            "sleep_score",
                            "deep_min",
                            "rem_min",
                            "awake_min",
                            "sleeping_hr",
                            "resting_hr",
                            "sleep_stress",
                            "daily_stress",
                            "body_battery_wake",
                            "body_battery_change",
                            "hrv_nightly",
                            "hrv_weekly",
                            "hrv_status",
                            "readiness_score",
                            "readiness_level",
                            "average_spo2",
                            "lowest_spo2",
                            "sleep_respiration",
                            "skin_temp_delta_c",
                            "activity_sessions",
                            "activity_load",
                        )
                        with ui.card().classes("gdh-card w-full"):
                            ui.label("Daily detail").classes(
                                "text-xl font-semibold"
                            )
                            data_grid(
                                [
                                    {key: row.get(key) for key in table_fields}
                                    for row in reversed(rows)
                                ],
                                height="30rem",
                            )
                        with ui.card().classes("gdh-card w-full"):
                            ui.label("Data coverage by source").classes(
                                "text-xl font-semibold"
                            )
                            data_grid(analysis["sources"], height="15rem")

                    async def execute_recovery_analysis() -> None:
                        analyze_recovery_button.disable()
                        try:
                            recovery_state["analysis"] = await run.io_bound(
                                analyze_sleep_recovery,
                                db_path,
                                recovery_start.value,
                                recovery_end.value,
                            )
                        except (OSError, sqlite3.Error, ValueError) as exc:
                            _notify_error(exc)
                            return
                        finally:
                            analyze_recovery_button.enable()
                        recovery_results.refresh()

                    analyze_recovery_button.on(
                        "click", execute_recovery_analysis
                    )
                    recovery_results()

                with ui.tab_panel(sql_tab):
                    sql_state: dict[str, list[dict]] = {"rows": []}
                    sql = ui.textarea("SQL", value="SELECT activity_type AS sport, COUNT(*) AS activities FROM activity GROUP BY activity_type ORDER BY activities DESC").props("outlined autogrow").classes("w-full")
                    limit = ui.number("Row limit", value=1000, min=1, max=5000).props("outlined").classes("w-40")

                    @ui.refreshable
                    def sql_results() -> None:
                        data_grid(sql_state["rows"], height="31rem")

                    def execute_sql() -> None:
                        try:
                            sql_state["rows"] = run_read_only_query(db_path, str(sql.value), limit=int(limit.value or 1000))
                        except (sqlite3.Error, ValueError) as exc:
                            _notify_error(exc)
                            return
                        sql_results.refresh()

                    ui.button("Run query", icon="play_arrow", on_click=execute_sql)
                    sql_results()
                with ui.tab_panel(mcp_tab):
                    ui.label("MCP calls run in a worker so the interface remains responsive.").classes("text-grey-7")
                    tool = ui.select(
                        ["garmin_schema", "garmin_query", "garmin_health_summary", "garmin_activities", "garmin_trends", "garmin_sync"],
                        value="garmin_schema",
                        label="Tool (type any garmin_mcp tool name)",
                    ).props("outlined use-input new-value-mode=add-unique").classes("w-96")
                    arguments = ui.textarea("Arguments JSON", value="{}").props("outlined autogrow").classes("w-full")
                    result_box = ui.textarea("Result", value="").props("outlined readonly").classes("w-full").style("height: 30rem")
                    run_button = ui.button("Run MCP tool", icon="play_arrow")

                    async def execute_mcp() -> None:
                        try:
                            parsed = json.loads(str(arguments.value or "{}"))
                            if not isinstance(parsed, dict):
                                raise ValueError("Arguments must be a JSON object")
                        except (json.JSONDecodeError, ValueError) as exc:
                            _notify_error(exc)
                            return
                        run_button.disable()
                        try:
                            result_box.value = await run.io_bound(
                                call_tool_via_sidecar,
                                str(tool.value),
                                parsed,
                                db_path,
                            )
                        except Exception as exc:
                            _notify_error(exc)
                        finally:
                            run_button.enable()

                    run_button.on("click", execute_mcp)

    @ui.page("/settings")
    def settings_page() -> None:
        render_shell("Settings", db_path, sandboxed=sandboxed)
        with ui.column().classes("gdh-page"):
            page_heading(
                "Interface Settings",
                "Choose display units and reusable defaults. Preferences are stored only in the local database.",
            )
            saved = interface_settings(db_path)
            sports = activity_sports(db_path)
            fields: dict[str, Any] = {}

            with ui.card().classes("gdh-card w-full"):
                ui.label("Units").classes("text-xl font-semibold")
                fields["unit_system"] = ui.radio(
                    {
                        "Imperial": "Miles",
                        "Metric": "Kilometres",
                    },
                    value=saved["unit_system"],
                ).props("inline")
                fields["activity_velocity_display"] = ui.radio(
                    {
                        "Pace": "Pace",
                        "Speed": "Speed",
                    },
                    value=saved["activity_velocity_display"],
                ).props("inline")
                ui.label(
                    "This changes presentation only. Garmin records, JSON exports, "
                    "database fields, and Codex plan validation retain their canonical units."
                ).classes("text-sm text-grey-7")

            with ui.card().classes("gdh-card w-full"):
                ui.label("Activities and charts").classes("text-xl font-semibold")
                with ui.grid(columns=2).classes("w-full gap-3"):
                    fields["activity_lookback_days"] = ui.number(
                        "Default activity history (days)",
                        value=saved["activity_lookback_days"],
                        min=7,
                        max=3650,
                    ).props("outlined")
                    fields["activity_row_limit"] = ui.number(
                        "Default activity table rows",
                        value=saved["activity_row_limit"],
                        min=25,
                        max=5000,
                    ).props("outlined")
                    fields["activity_default_sport"] = ui.select(
                        ["All", *sports],
                        value=(
                            saved["activity_default_sport"]
                            if saved["activity_default_sport"] in sports
                            else "All"
                        ),
                        label="Default activity sport",
                    ).props("outlined")
                    fields["chart_lookback_days"] = ui.number(
                        "Default chart history (days)",
                        value=saved["chart_lookback_days"],
                        min=7,
                        max=3650,
                    ).props("outlined")

            with ui.card().classes("gdh-card w-full"):
                ui.label("Dashboard and sync").classes("text-xl font-semibold")
                with ui.grid(columns=2).classes("w-full gap-3"):
                    fields["dashboard_item_limit"] = ui.number(
                        "Upcoming/recent dashboard rows",
                        value=saved["dashboard_item_limit"],
                        min=4,
                        max=20,
                    ).props("outlined")
                    fields["sync_lookback_days"] = ui.number(
                        "Default Garmin sync days (0 uses CLI default)",
                        value=saved["sync_lookback_days"],
                        min=0,
                        max=3650,
                    ).props("outlined")

            def field_values() -> dict[str, Any]:
                return {key: element.value for key, element in fields.items()}

            def save_preferences() -> None:
                try:
                    save_interface_settings(db_path, field_values())
                except (OSError, TypeError, ValueError) as exc:
                    _notify_error(exc)
                    return
                ui.notify(
                    "Interface settings saved. Other pages will use them when opened.",
                    type="positive",
                )

            def load_defaults() -> None:
                for key, value in INTERFACE_SETTING_DEFAULTS.items():
                    fields[key].value = value
                ui.notify("Recommended defaults loaded. Click Save settings to keep them.")

            with ui.row().classes("gap-3"):
                ui.button("Save settings", icon="save", on_click=save_preferences)
                ui.button("Load recommended defaults", on_click=load_defaults).props(
                    "outline"
                )

    @ui.page("/guide")
    def help_about_page() -> None:
        render_shell("Help & About", db_path, sandboxed=sandboxed)
        with ui.column().classes("gdh-page"):
            page_heading(
                "Help & About",
                "Get oriented, find support, and review application and "
                "open-source information.",
            )

            with ui.card().classes("gdh-card w-full bg-slate-900 text-white p-5"):
                with ui.row().classes("w-full items-center gap-4 flex-wrap"):
                    ui.icon("directions_run").classes("text-5xl text-blue-3")
                    with ui.column().classes("gap-1 flex-1 min-w-[18rem]"):
                        with ui.row().classes("items-center gap-3 flex-wrap"):
                            ui.label("Garmin Data Hub").classes(
                                "text-2xl font-semibold"
                            )
                            ui.badge(f"Version {__version__}", color="blue")
                        ui.label(
                            "A local-first desktop app for syncing and analysing "
                            "Garmin activity data, exploring training metrics, and "
                            "creating policy-validated training plans with optional "
                            "Codex coaching."
                        ).classes("text-slate-200")

            ui.label("Getting started").classes(
                "text-2xl font-semibold text-slate-900 mt-2"
            )
            ui.label(
                "Follow this workflow, or jump directly to the page you need."
            ).classes("text-slate-600")
            with ui.row().classes("w-full gap-4 items-stretch flex-wrap"):
                for step, title, text, link_label, route in HELP_WORKFLOW_SECTIONS:
                    with ui.card().classes(
                        "gdh-card flex-1 min-w-[20rem] max-w-full"
                    ):
                        with ui.row().classes("items-center gap-3"):
                            ui.badge(step, color="blue")
                            ui.label(title).classes("text-lg font-semibold")
                        ui.label(text).classes("text-slate-700 flex-1")
                        ui.link(link_label, route).classes("font-medium")

            ui.label("About Garmin Data Hub").classes(
                "text-2xl font-semibold text-slate-900 mt-2"
            )
            with ui.row().classes("w-full gap-4 items-stretch flex-wrap"):
                with ui.card().classes("gdh-card flex-1 min-w-[20rem]"):
                    with ui.row().classes("items-center gap-2"):
                        ui.icon("lock").classes("text-blue-7")
                        ui.label("Local by design").classes(
                            "text-lg font-semibold"
                        )
                    ui.label(
                        "The selected local SQLite database is the app's primary "
                        "store for Garmin-derived activity records, saved "
                        "preferences, and accepted plans. Network-backed features "
                        "run only when you initiate them—for example Garmin sync, "
                        "optional Codex setup, sign-in, or generation, viewing "
                        "online map tiles, and external links. On Windows, optional "
                        "saved Garmin credentials use Windows Credential Manager; "
                        "browser-session data and logs are separate local files."
                    ).classes("text-slate-700")
                    ui.label("Current database").classes(
                        "text-xs font-bold uppercase text-grey-6 mt-2"
                    )
                    ui.label(str(db_path)).classes(
                        "text-xs font-mono text-grey-8 break-all"
                    )
                    ui.badge(
                        "Sandbox snapshot" if sandboxed else "Local database",
                        color="amber" if sandboxed else "green",
                    )

                with ui.card().classes("gdh-card flex-1 min-w-[20rem]"):
                    with ui.row().classes("items-center gap-2"):
                        ui.icon("support").classes("text-blue-7")
                        ui.label("Project & support").classes(
                            "text-lg font-semibold"
                        )
                    ui.label(
                        "Use these links for source code, known updates, or a "
                        "problem that is not resolved by the workflow above."
                    ).classes("text-slate-700")
                    with ui.column().classes("gap-2"):
                        for label, target in ABOUT_LINKS:
                            ui.link(label, target, new_tab=True).classes(
                                "font-medium"
                            )

            with ui.card().classes("gdh-card w-full"):
                with ui.row().classes("items-center gap-2"):
                    ui.icon("balance").classes("text-blue-7")
                    ui.label("Open-source & legal").classes(
                        "text-lg font-semibold"
                    )
                ui.label(
                    "Garmin Data Hub project code is available under the MIT "
                    "License. Garmin Connect download support is powered by the "
                    "open-source garmin-givemydata project, licensed "
                    "AGPL-3.0-only. Packaged releases include the applicable "
                    "license texts, third-party notices, and source materials."
                ).classes("text-slate-700")
                ui.link(
                    "View the garmin-givemydata project",
                    "https://github.com/nrvim/garmin-givemydata",
                    new_tab=True,
                ).classes("font-medium")
                ui.label(
                    "Garmin Data Hub is not affiliated with or endorsed by Garmin."
                ).classes("text-slate-700")
                ui.label("Copyright © 2024 Garmin Data Hub").classes(
                    "text-sm text-grey-7"
                )
