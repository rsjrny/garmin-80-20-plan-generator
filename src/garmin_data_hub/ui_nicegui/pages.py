"""Core pages for the primary NiceGUI navigation surface."""

from __future__ import annotations

import json
import sqlite3
import subprocess
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from garmin_data_hub.mcp_sidecar_client import call_tool_via_sidecar
from garmin_data_hub.services.baseline_plan_builder import (
    BASELINE_DISTANCE_OPTIONS,
    BaselinePlanRequest,
    build_and_save_baseline,
    normalize_baseline_distance,
)
from garmin_data_hub.services.garmin_auth_session import BrowserSessionResetError
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
    plan_rows,
    planning_settings,
    repair_derived_metrics,
    run_read_only_query,
    save_interface_settings,
    save_planning_settings,
    validate_planning_settings,
)
from garmin_data_hub.ui_nicegui.layout import (
    data_grid,
    metric_card,
    page_heading,
    render_shell,
)


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


def _split_rows(rows: list[dict[str, Any]], unit_system: str) -> list[dict[str, Any]]:
    unit = distance_unit(unit_system)
    converted: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        meters = row.pop("distance_meters", None)
        kilometres = float(meters) / 1000 if meters is not None else None
        row[f"distance_{unit}"] = distance_from_km(kilometres, unit_system)
        converted.append(row)
    return converted


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

            with ui.row().classes("w-full gap-4 items-stretch"):
                with ui.card().classes("gdh-card flex-1 min-w-[420px]"):
                    ui.label("Upcoming plan").classes("text-xl font-semibold")
                    data_grid(
                        _distance_rows(data["upcoming"], unit_system, "distance_km"),
                        height="21rem",
                    )
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
                rows = _distance_rows(state["rows"], unit_system, "distance_km")
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
                    track_tab = ui.tab("Track")
                    raw_tab = ui.tab("All fields")
                with ui.tab_panels(tabs, value=overview_tab).classes("w-full"):
                    with ui.tab_panel(overview_tab):
                        data_grid([detail["metrics"]] if detail["metrics"] else [])
                    with ui.tab_panel(splits_tab):
                        data_grid(_split_rows(detail["splits"], unit_system))
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
            sports = activity_sports(db_path)
            with ui.card().classes("gdh-card w-full"):
                with ui.row().classes("items-end gap-3 flex-wrap"):
                    start = ui.input("Start date", value=(date.today() - timedelta(days=preferences["chart_lookback_days"])).isoformat()).props("outlined type=date").classes("w-48")
                    selected_sports = ui.select(sports, value=[], multiple=True, label="Sports (blank means all)").props("outlined use-chips").classes("w-96")
                    draw_button = ui.button("Update charts", icon="monitoring")

            @ui.refreshable
            def render_charts() -> None:
                frame = chart_dataframe(
                    db_path,
                    start_date=str(start.value),
                    sports=tuple(selected_sports.value or ()) or None,
                )
                if frame.empty:
                    ui.label("No activities match the selected filters.").classes("text-grey-7")
                    return
                frame["date"] = pd.to_datetime(frame["start_time_utc"], errors="coerce")
                frame = frame.dropna(subset=["date"])
                frame["distance"] = frame["total_distance_m"].fillna(0) / 1000
                if unit_system == "Imperial":
                    frame["distance"] *= 0.621371192237334
                frame["duration_hours"] = frame["total_elapsed_s"].fillna(0) / 3600
                speed_unit = "mph" if unit_system == "Imperial" else "km/h"
                speed_factor = 2.2369362920544 if unit_system == "Imperial" else 3.6
                frame["display_speed"] = frame["avg_speed_mps"] * speed_factor
                frame["week"] = frame["date"].dt.to_period("W").dt.start_time
                weekly = frame.groupby("week", as_index=False).agg(distance=("distance", "sum"), duration_hours=("duration_hours", "sum"), tss=("tss", "sum"))
                figures = (
                    px.bar(frame.groupby("sport", as_index=False).size(), x="sport", y="size", title="Activity distribution"),
                    px.bar(weekly, x="week", y="distance", title=f"Weekly distance ({unit})"),
                    px.line(weekly, x="week", y="duration_hours", markers=True, title="Weekly duration (hours)"),
                    px.scatter(frame, x="date", y="avg_hr_bpm", color="sport", hover_data=["distance", "duration_hours"], title="Average heart rate"),
                    px.scatter(frame, x="date", y="display_speed", color="sport", size="distance", title=f"Average speed ({speed_unit})"),
                    px.bar(weekly, x="week", y="tss", title="Weekly training stress"),
                )
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
        }
        with ui.column().classes("gdh-page"):
            page_heading(
                "Garmin Sync",
                "Sign in securely, complete Garmin MFA in Chrome when requested, and monitor the local sync log.",
            )
            with ui.card().classes("gdh-card w-full"):
                ui.label("Garmin Connect login").classes("text-lg font-semibold")
                ui.label(
                    "A saved password is never filled back into this page or added "
                    "to the sync command or log."
                ).classes("text-grey-7")
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
                with ui.row().classes("w-full items-center gap-3 flex-wrap"):
                    remember = ui.checkbox(
                        "Remember on this Windows account",
                        value=credential_store_available,
                    )
                    save_login_button = ui.button(
                        "Save login",
                        icon="key",
                    ).props("outline")
                    forget_login_button = ui.button(
                        "Forget saved login",
                        icon="delete_outline",
                    ).props("outline color=negative")
                    reset_browser_button = ui.button(
                        "Reset browser login",
                        icon="restart_alt",
                    ).props("outline")
                credential_label = ui.label().classes("font-medium")
                ui.label(
                    "Clearing Remember prevents password storage in Windows "
                    "Credential Manager, but Garmin's browser session can still persist."
                ).classes("text-grey-7")
                ui.label(
                    "When Garmin requests MFA, enter the code in the Chrome window that opens. Sync resumes automatically."
                ).classes("text-grey-7")
                ui.label(
                    "An existing Garmin browser session can take precedence over "
                    "new credentials. Reset browser login to force a fresh sign-in; "
                    "use a separate database when changing Garmin accounts."
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

            def refresh_credential_controls(*, enabled: bool = True) -> None:
                enabled = enabled and not bool(state["resetting_browser"])
                email.set_enabled(enabled)
                password.set_enabled(enabled)
                remember.set_enabled(enabled and credential_store_available)
                save_login_button.set_enabled(
                    enabled and credential_store_available
                )
                forget_login_button.set_enabled(
                    enabled
                    and credential_store_available
                    and bool(state["credential_may_exist"])
                )
                reset_browser_button.set_enabled(enabled)

            def refresh_credential_label() -> None:
                if state["saved_email"]:
                    credential_label.text = (
                        f"Saved securely in {credential_backend} for "
                        f"{state['saved_email']}."
                    )
                elif state["credential_problem"]:
                    if credential_store_available:
                        credential_label.text = (
                            "Could not read the saved Garmin login: "
                            f"{state['credential_problem']}. Enter a complete login "
                            "to replace it or choose Forget saved login."
                        )
                    else:
                        credential_label.text = (
                            f"Secure storage unavailable: {state['credential_problem']}. "
                            "Credentials can still be used once without being saved."
                        )
                else:
                    credential_label.text = (
                        f"No Garmin login is saved in {credential_backend}."
                    )

            def save_login() -> None:
                if not credential_store_available:
                    _notify_error(
                        state["credential_problem"]
                        or "Windows Credential Manager is unavailable"
                    )
                    return
                try:
                    credentials = save_credentials(
                        str(email.value or ""),
                        str(password.value or ""),
                    )
                except (CredentialStoreError, ValueError) as exc:
                    _notify_error(exc)
                    return
                state["saved_email"] = credentials.email
                state["credential_problem"] = None
                state["credential_may_exist"] = True
                email.value = credentials.email
                password.value = ""
                refresh_credential_label()
                refresh_credential_controls()
                ui.notify(
                    "Garmin login saved securely for this Windows account.",
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
                password.value = ""
                refresh_credential_label()
                refresh_credential_controls()
                message = (
                    "Saved Garmin login removed. Browser session data was left unchanged."
                    if removed
                    else "No saved Garmin login was found."
                )
                ui.notify(message, type="info")

            async def reset_browser_login() -> None:
                if job.snapshot().state in {"running", "cancelling"}:
                    _notify_error("Stop Garmin sync before resetting its browser login")
                    return
                if not await reset_browser_dialog:
                    return
                if job.snapshot().state in {"running", "cancelling"}:
                    _notify_error("Garmin sync started; stop it before resetting login")
                    return
                state["resetting_browser"] = True
                start_button.disable()
                refresh_credential_controls(enabled=False)
                try:
                    result = await run.io_bound(
                        job.reset_browser_session,
                    )
                except (BrowserSessionResetError, RuntimeError) as exc:
                    _notify_error(exc)
                else:
                    message = (
                        "Garmin browser login reset. The next sync will perform a fresh sign-in."
                        if result.removed_anything
                        else "No saved Garmin browser login was found."
                    )
                    ui.notify(message, type="positive")
                finally:
                    state["resetting_browser"] = False
                    poll_sync()

            def start_sync() -> None:
                saved_credentials = None
                try:
                    if not str(password.value or "") and credential_store_available:
                        saved_credentials = load_credentials()
                    credentials, entered = _resolve_sync_credentials(
                        email.value,
                        password.value,
                        saved_credentials,
                    )
                    if entered and bool(remember.value):
                        if not credential_store_available:
                            raise CredentialStoreUnavailable(
                                "Windows Credential Manager is unavailable; "
                                "clear Remember to use these credentials once"
                            )
                        credentials = save_credentials(
                            credentials.email,
                            credentials.password,
                        )
                        state["saved_email"] = credentials.email
                        state["credential_problem"] = None
                        state["credential_may_exist"] = True
                        email.value = credentials.email
                        refresh_credential_label()
                    job.start(
                        days=int(days.value or 0),
                        credentials=credentials,
                    )
                except (
                    CredentialStoreError,
                    OSError,
                    RuntimeError,
                    ValueError,
                ) as exc:
                    _notify_error(exc)
                    return
                password.value = ""
                state["handled"] = "running"
                state["handled_error"] = None
                start_button.disable()
                stop_button.enable()
                clear_button.disable()
                refresh_credential_controls(enabled=False)
                ui.notify(
                    "Garmin login window is opening. Complete MFA there if requested.",
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
                    ui.notify("Garmin sync completed successfully.", type="positive")
                elif snapshot.state == "failed":
                    _notify_error(snapshot.error or f"Sync exited with {snapshot.return_code}")
                elif snapshot.state == "cancelled":
                    ui.notify("Garmin sync stopped.", type="info")

            refresh_credential_label()
            poll_sync()
            ui.timer(1.0, poll_sync)

    @ui.page("/query")
    def query_page() -> None:
        render_shell("Data Query", db_path, sandboxed=sandboxed)
        with ui.column().classes("gdh-page"):
            page_heading("Data Query", "Run guarded read-only SQLite queries or advanced garmin_mcp tools.")
            tabs = ui.tabs().classes("w-full")
            with tabs:
                sql_tab = ui.tab("Read-only SQL")
                mcp_tab = ui.tab("MCP tool")
            with ui.tab_panels(tabs, value=sql_tab).classes("gdh-card w-full bg-white"):
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
                        "Imperial": "Miles and mph",
                        "Metric": "Kilometres and km/h",
                    },
                    value=saved["unit_system"],
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
    def guide_page() -> None:
        render_shell("Guide", db_path, sandboxed=sandboxed)
        with ui.column().classes("gdh-page"):
            page_heading("Garmin Data Hub Guide", "The core workflow in the NiceGUI desktop interface.")
            sections = (
                ("0. Set preferences", "Open Settings to choose miles or kilometres and save the default history windows, table size, sport, dashboard rows, and Garmin sync lookback used across the interface."),
                ("1. Sync Garmin", "Open Garmin Sync, choose an optional lookback, and run the account-authenticated garmin-givemydata workflow. Navigation remains usable while it runs."),
                ("2. Review history", "Activities provides filterable Garmin sessions, split/trackpoint inspection, and complete per-activity JSON downloads. Charts summarizes volume, intensity, heart rate, speed, and load."),
                ("3. Configure the goal", "Plan stores the athlete, event, schedule, nutrition context, and effective HR thresholds used as locked inputs for coaching."),
                ("4. Generate safely", "Codex Coach uses your signed-in Codex CLI. It excludes raw routes and identifiers, validates the structured response locally, and never writes a proposal automatically."),
                ("5. Review and apply", "Read the rationale, policy warnings, macros, and exact date-level database changes. Check the acknowledgement only when you want to apply the proposal."),
                ("6. Track compliance", "Compliance compares the active schedule with completed Garmin activities through today."),
            )
            for title, text in sections:
                with ui.card().classes("gdh-card w-full"):
                    ui.label(title).classes("text-xl font-semibold")
                    ui.label(text).classes("text-slate-700")
