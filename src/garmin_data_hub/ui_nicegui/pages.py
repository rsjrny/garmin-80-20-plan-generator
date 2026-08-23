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
from garmin_data_hub.services.athlete_metrics_service import (
    calculate_metrics_from_db_sources,
    clear_override_metrics,
    get_athlete_metrics,
    set_calculated_metrics,
    set_override_metrics,
)
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
                                    await route_map.initialized()
                                    await route_map.run_map_method(
                                        "fitBounds",
                                        coordinates,
                                        {"padding": [24, 24]},
                                    )

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
        with ui.column().classes("gdh-page"):
            page_heading(
                "Plan Configuration & Review",
                "Set locked coaching context, manage HR thresholds, and review the active schedule before asking Codex for changes.",
            )
            preferences = interface_settings(db_path)
            unit_system = preferences["unit_system"]
            settings = planning_settings(db_path)
            with ui.card().classes("gdh-card w-full"):
                ui.label("Event and schedule").classes("text-xl font-semibold")
                fields: dict[str, Any] = {}
                with ui.grid(columns=3).classes("w-full gap-3"):
                    fields["athlete_name"] = ui.input("Athlete name", value=settings["athlete_name"]).props("outlined")
                    fields["age"] = ui.number("Age", value=settings["age"], min=10, max=100).props("outlined")
                    distance_options = ["5K", "10K", "10 Miler", "Half Marathon", "20 Miler", "Marathon", "50K", "50 Mile", "100K", "100 Mile"]
                    if settings["distance"] not in distance_options:
                        distance_options.append(str(settings["distance"]))
                    fields["distance"] = ui.select(distance_options, value=settings["distance"], label="Race distance").props("outlined")
                    fields["event_name"] = ui.input("Event name", value=settings["event_name"]).props("outlined")
                    fields["plan_start"] = ui.input("Plan start", value=str(settings["plan_start"])).props("outlined type=date")
                    fields["event_date"] = ui.input("Event date", value=str(settings["event_date"])).props("outlined type=date")
                    fields["run_days_per_week"] = ui.number("Run days/week", value=settings["run_days_per_week"], min=1, max=7).props("outlined")
                    fields["long_run_day"] = ui.select(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"], value=settings["long_run_day"], label="Long-session day").props("outlined")
                    fields["sodium_mg_per_hour"] = ui.number("Sodium mg/hour (0 unknown)", value=settings["sodium_mg_per_hour"], min=0, max=3000).props("outlined")

                def save_settings() -> None:
                    try:
                        save_planning_settings(db_path, {key: element.value for key, element in fields.items()})
                    except (OSError, TypeError, ValueError) as exc:
                        _notify_error(exc)
                    else:
                        ui.notify("Planning settings saved locally.", type="positive")

                with ui.row().classes("gap-3"):
                    ui.button("Save settings", icon="save", on_click=save_settings)
                    ui.button("Open Codex Coach", icon="auto_awesome", on_click=lambda: ui.navigate.to("/coach"))

            with ui.card().classes("gdh-card w-full"):
                ui.label("Athlete thresholds").classes("text-xl font-semibold")
                metrics = get_athlete_metrics(db_path)
                with ui.row().classes("items-end gap-3 flex-wrap"):
                    hrmax = ui.number("HRmax override (0 clears)", value=metrics.get("hrmax_effective") or 0, min=0, max=250).props("outlined")
                    lthr = ui.number("LTHR override (0 clears)", value=metrics.get("lthr_effective") or 0, min=0, max=220).props("outlined")
                    years = ui.number("History years", value=5, min=1, max=50).props("outlined")

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
                        result = await run.io_bound(calculate_metrics_from_db_sources, db_path, int(years.value or 5))
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
                schedule = plan_rows(db_path)
                display_schedule = _distance_rows(
                    schedule, unit_system, "distance_km"
                )
                with ui.row().classes("w-full gap-3 flex-wrap"):
                    metric_card("Sessions", len(schedule))
                    metric_card("Start", schedule[0]["date"] if schedule else "-")
                    metric_card("End", schedule[-1]["date"] if schedule else "-")
                    metric_card("Strength", sum(1 for row in schedule if row.get("sport") == "strength"))
                plan_tabs = ui.tabs().classes("w-full")
                with plan_tabs:
                    workouts_tab = ui.tab("Workouts")
                    macros_tab = ui.tab("Daily macros")
                with ui.tab_panels(plan_tabs, value=workouts_tab).classes("w-full"):
                    with ui.tab_panel(workouts_tab):
                        data_grid(display_schedule, height="34rem")
                    with ui.tab_panel(macros_tab):
                        data_grid(nutrition_rows(db_path), height="34rem")

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
        state = {"handled": "idle"}
        with ui.column().classes("gdh-page"):
            page_heading("Garmin Sync", "Run garmin-givemydata without blocking navigation and monitor its local log.")
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

            def start_sync() -> None:
                try:
                    job.start(days=int(days.value or 0))
                except (OSError, RuntimeError, ValueError) as exc:
                    _notify_error(exc)
                    return
                state["handled"] = "running"
                start_button.disable()
                stop_button.enable()
                clear_button.disable()

            def stop_sync() -> None:
                try:
                    if job.cancel():
                        status.text = "Sync cancelled."
                except (OSError, subprocess.SubprocessError) as exc:
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

            def poll_sync() -> None:
                snapshot = job.snapshot()
                progress.value = snapshot.progress
                minutes, seconds = divmod(int(snapshot.elapsed_seconds), 60)
                status.text = f"{snapshot.state.title()} - {minutes}:{seconds:02d}"
                if log.value != snapshot.log:
                    log.value = snapshot.log
                if snapshot.state == "running":
                    return
                if state["handled"] == snapshot.state:
                    return
                state["handled"] = snapshot.state
                start_button.enable()
                stop_button.disable()
                clear_button.enable()
                if snapshot.state == "completed":
                    ui.notify("Garmin sync completed successfully.", type="positive")
                elif snapshot.state == "failed":
                    _notify_error(snapshot.error or f"Sync exited with {snapshot.return_code}")

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
