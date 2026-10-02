"""Accessible split/lap controls and a selection outline over the metric route."""
from __future__ import annotations

from html import escape
import json

from nicegui import ui

from garmin_data_hub.analytics.track_splits import distance_splits, interval_features, stored_laps
from garmin_data_hub.analytics.track_visuals import pace_label


def _duration(value):
    if value is None:
        return "—"
    seconds = round(value)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}:{minutes:02}:{seconds:02}" if hours else f"{minutes}:{seconds:02}"


def render_track_intervals(track, lap_rows, route_map, state, activity_key, sport):
    splits = distance_splits(track)
    laps = stored_laps(track, lap_rows)
    intervals = splits + laps
    by_id = {item["id"]: item for item in intervals}
    selections = state.setdefault("track_intervals", {})
    chosen = {"id": selections.get(activity_key, "none")}
    if chosen["id"] not in by_id:
        chosen["id"] = "none"
    options = {"none": "Whole route", **{item["id"]: item["label"] for item in intervals}}
    # Separate layers leave the T2 geometry and colors untouched.
    halo = route_map.generic_layer(name="geoJSON", args=[interval_features({}), {"style": {"color": "#ffffff", "weight": 12, "opacity": 1, "interactive": False}}])
    outline = route_map.generic_layer(name="geoJSON", args=[interval_features({}), {"style": {"color": "#172e50", "weight": 7, "opacity": 1, "interactive": False}}])
    divisor = 1609.344 if track["unit"] == "mi" else 1000
    elevation_factor, elevation_unit = (3.280839895, "ft") if track["unit"] == "mi" else (1, "m")
    cadence_unit = "rpm" if any(s in str(sport).lower() for s in ("cycl", "bik")) else "spm"

    def reading(value, unit, coverage=None):
        if value is None:
            return "—"
        label = f"{value:.0f} {unit}"
        if coverage is not None and coverage < .999:
            label += f" ({coverage:.0%} coverage)"
        return label

    def row(item):
        return {"id": item["id"], "interval": item["label"] + (" · Fastest full split" if item["fastest"] else ""),
                "distance": f"{item['distance_m']/divisor:.2f} {track['unit']}" if item["distance_m"] is not None else "—",
                "duration": _duration(item["duration_s"]),
                "pace": f"{pace_label(item['pace'])} /{track['unit']}" if item["pace"] is not None else "—",
                "hr": reading(item.get("average_hr"), "bpm", item.get("hr_coverage")),
                "max_hr": reading(item.get("max_hr"), "bpm"),
                "elevation": reading(item.get("elevation_change_m") * elevation_factor if item.get("elevation_change_m") is not None else None, elevation_unit),
                "cadence": reading(item.get("cadence"), cadence_unit, item.get("cadence_coverage")),
                "note": item["note"]}

    async def emphasize(zoom=False):
        if not route_map.is_initialized:
            return
        item = by_id.get(chosen["id"])
        features = interval_features(item or {})
        for layer in (halo, outline):
            layer.run_method("clearLayers")
            layer.run_method("addData", features)
            layer.run_method(":eachLayer", "layer => layer.bringToFront()")
        if zoom and item and item["paths"]:
            coordinates = [c for path in item["paths"] for c in path]
            route_map.run_map_method("fitBounds", coordinates, {"padding": [24, 24], "maxZoom": 16, "animate": False})

    async def select(event):
        if event.value not in options:
            return
        chosen["id"] = event.value
        selections[activity_key] = event.value
        selected_details.refresh()
        await emphasize(zoom=True)

    @ui.refreshable
    def selected_details():
        item = by_id.get(chosen["id"])
        with ui.column().classes("w-full gap-1").props('role="status" aria-live="polite"'):
            if item:
                values = row(item)
                ui.label(values["interval"]).classes("font-semibold")
                ui.label(f"{values['distance']} · {values['duration']} · Pace {values['pace']}").classes("text-sm")
                ui.label(f"Average HR {values['hr']} · Max HR {values['max_hr']} · Net elevation {values['elevation']} · Cadence {values['cadence']}").classes("text-sm")
                ui.label(item["note"]).classes("text-xs text-grey-7")
                if not item["paths"]:
                    ui.label("Route highlighting unavailable for this lap.").classes("text-sm")
            else:
                ui.label("Select a split or lap to outline its route and inspect measurements.").classes("text-sm")

    ui.label("Laps and distance splits").classes("text-lg font-semibold mt-3")
    ui.label(f"GPS splits use {track['unit']} and elapsed time, including stops and pauses. Stored laps use source totals. Only full GPS splits with reliable timing qualify as fastest.").classes("text-xs text-grey-7")
    ui.label("GPS sensor averages weight available readings by time; coverage excludes pauses and gaps. Net elevation needs complete endpoint readings. A dash means unavailable.").classes("text-xs text-grey-7")
    if not laps:
        ui.label("No stored laps are available for this activity.").classes("text-xs text-grey-7")
    with ui.row().classes("w-full items-center gap-2 flex-wrap"):
        selector = ui.select(options, value=chosen["id"], label="Split or lap", on_change=select).classes("w-full max-w-sm")

        def step(direction):
            keys = list(options)
            index = keys.index(chosen["id"])
            selector.set_value(keys[(index + direction) % len(keys)])

        ui.button("Previous interval", on_click=lambda: step(-1), icon="chevron_left").props("outline dense").set_enabled(bool(intervals))
        ui.button("Next interval", on_click=lambda: step(1), icon="chevron_right").props("outline dense").set_enabled(bool(intervals))
        ui.button("Clear interval", on_click=lambda: selector.set_value("none"), icon="clear").props("outline dense")
    selected_details()
    columns = [dict(name=key, label=label, field=key, align="left") for key, label in (
        ("interval", "Interval"), ("distance", "Distance"), ("duration", "Duration"), ("pace", "Pace"),
        ("hr", "Average HR"), ("max_hr", "Max HR"), ("elevation", "Net elevation"), ("cadence", "Cadence"), ("note", "Details"))]
    with ui.element("div").classes("w-full overflow-x-auto"):
        measurements = ui.table(columns=columns, rows=[row(item) for item in intervals], row_key="id", pagination=8).classes("w-full").props('dense flat bordered wrap-cells')

    markers = []
    # Sampling preserves UI responsiveness on exceptionally long routes/lap lists.
    marker_items = [s for s in splits if not s["partial"] and s["marker"]] + [lap for lap in laps if lap["marker"]]
    stride = max(1, (len(marker_items) + 199) // 200)
    shown = marker_items[::stride]
    if stride > 1:
        ui.label(f"Showing {len(shown)} of {len(marker_items)} boundary markers; every interval remains in the selector and table.").classes("text-xs")
    event_name = f"track_interval_{route_map.id}"
    route_map.on(event_name, lambda e: selector.set_value(e.args) if e.args in options else None)
    for item in shown:
        is_lap = item["kind"] != "GPS split"
        color = "#70489c" if is_lap else "#2455a4"
        marker = route_map.generic_layer(name="circleMarker", args=[item["marker"], {"radius": 7 if is_lap else 4, "color": color, "fillColor": "#ffffff", "fillOpacity": 1, "weight": 3 if is_lap else 2}])
        markers.append((marker, item))
    if marker_items:
        ui.label("Small blue circles mark full distance splits; larger purple circles mark stored lap starts. Select with the controls or tap a marker.").classes("text-xs text-grey-7")

    async def initialize():
        await route_map.initialized()
        # Quasar puts component attributes on a wrapper; name the actual table.
        ui.run_javascript(f'document.querySelector("#c{measurements.id} table")?.setAttribute("aria-label", "Lap and split measurements")')
        for marker, item in markers:
            marker.run_method("bindTooltip", escape(item["label"]), {"direction": "top"})
            marker.run_method(":on", "'click'", f"() => getElement({route_map.id}).$emit({json.dumps(event_name)}, {json.dumps(item['id'])})")
        await emphasize()

    return initialize
