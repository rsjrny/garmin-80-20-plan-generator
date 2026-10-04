"""Aligned charts with browser-local cursors and shared T3 interval selection."""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from nicegui import ui

from garmin_data_hub.analytics.track_charts import chart_model, chart_figure, interval_bounds, range_interval
from garmin_data_hub.analytics.track_visuals import METRIC_SPECS, number

_CLIENT = Path(__file__).with_suffix(".js").read_text(encoding="utf-8")


def render_track_charts(track, route_map, state, activity_key, sport, on_range):
    model = chart_model(track, sport)
    ui.label("Route measurements").classes("text-lg font-semibold mt-3")
    if not model["metrics"] or not model["axes"]:
        ui.label("Linked charts unavailable: insufficient route measurements.").classes("text-sm")
        return SimpleNamespace(initialize=lambda: _nothing(), select_interval=lambda item: None)
    axes = state.setdefault("track_axes", {})
    axis = {"value": axes.get(activity_key, "time" if "time" in model["axes"] else next(iter(model["axes"])))}
    if axis["value"] not in model["axes"]:
        axis["value"] = next(iter(model["axes"]))
    selected = {"item": None}
    ready = {"value": False}
    ui.label("Charts share a cursor and selected interval with the route. Drag horizontally to select a range, or enter bounds below. Hover shows sampled edge-end readings; tap pins the cursor.").classes("text-xs text-grey-7")
    ui.label("Pace is smoothed; gaps, pauses and missing readings stay empty. GPS distance excludes rejected edges, gaps and pauses. Chart drawing keeps local extrema; map cursors retain original route samples.").classes("text-xs text-grey-7")
    if "time" not in model["axes"]:
        ui.label("Elapsed-time charts unavailable: no reliable timestamped route samples.").classes("text-xs text-grey-7")

    async def initialize():
        await route_map.initialized()
        config = {"plot": plot.id, "map": route_map.id, "readout": readout.id, "rows": model["rows"],
                  "axis": axis["value"], "unit": track["unit"], "maximum": model["axes"][axis["value"]]["max"],
                  "selectable": [s["status"] != "sampling gap" for s in track["segments"]],
                  "metrics": [{"label": "Smoothed pace" if k == "pace" else METRIC_SPECS[k]["label"],
                               "unit": track["metrics"][k]["unit"]} for k in METRIC_SPECS]}
        await ui.run_javascript(_CLIENT.replace("CONFIG", json.dumps(config)), timeout=10)
        ready["value"] = True
        select_interval(selected["item"])

    def select_interval(item):
        selected["item"] = item
        if ready["value"]:
            bounds = interval_bounds(track, model, item, axis["value"])
            ui.run_javascript(f"window.gdhTrackCharts?.[{plot.id}]?.select({json.dumps(bounds)})")
            if bounds:
                start.set_value(round(bounds[0], 3))
                end.set_value(round(bounds[1], 3))
            elif item is None:
                start.set_value(0)
                end.set_value(round(model["axes"][axis["value"]]["max"], 3))
            alignment.set_text("Selected interval has no reliable chart alignment." if item and bounds is None else "")

    def figure_options(axis_name):
        options = chart_figure(track, model, axis_name)
        # The client controller resizes only displayed charts, including tab reveals.
        options["config"] = {**options.get("config", {}), "responsive": False}
        return options

    async def change_axis(event):
        if event.value not in model["axes"]:
            return
        ready["value"] = False
        await ui.run_javascript(f"window.gdhTrackCharts?.[{plot.id}]?.dispose(); delete window.gdhTrackCharts?.[{plot.id}]")
        axis["value"] = event.value
        axes[activity_key] = event.value
        maximum = model["axes"][event.value]["max"]
        for field, value in ((position, 0), (start, 0), (end, maximum)):
            field.set_value(round(value, 3))
        bounds_label.set_text(f"Positions in {'elapsed minutes' if event.value == 'time' else track['unit']}; activity range 0–{maximum:.3f}.")
        plot.update_figure(figure_options(event.value))
        await initialize()

    with ui.row().classes("w-full items-center gap-2 flex-wrap"):
        ui.select({k: v["label"] for k, v in model["axes"].items()}, value=axis["value"], label="Chart x-axis", on_change=change_axis).classes("w-full max-w-xs")
    plot = ui.plotly(figure_options(axis["value"])).classes("w-full min-w-0")
    readout = ui.label("Hover or tap the route or a chart, or inspect a position with the keyboard controls.").classes("text-sm w-full").props('role="status" aria-live="off"')
    # Hover is deliberately not a live announcement; keyboard inspection announces once.
    announcement = ui.label("").classes("sr-only").props('role="status" aria-live="polite"')
    alignment = ui.label("").classes("text-xs text-grey-7")
    maximum = model["axes"][axis["value"]]["max"]
    bounds_label = ui.label(f"Positions in {'elapsed minutes' if axis['value'] == 'time' else track['unit']}; activity range 0–{maximum:.3f}.").classes("text-xs text-grey-7")

    def inspect():
        value = number(position.value)
        if value is None or not 0 <= value <= model["axes"][axis["value"]]["max"]:
            feedback.set_text("Choose a cursor position within the activity.")
            return
        feedback.set_text("")
        ui.run_javascript(f"window.gdhTrackCharts?.[{plot.id}]?.inspect({value}); requestAnimationFrame(() => requestAnimationFrame(() => {{document.getElementById('c{announcement.id}').textContent = document.getElementById('c{readout.id}').textContent;}}))")

    def apply_range(axis_name, low, high):
        if axis_name != axis["value"]:
            return
        try:
            interval = range_interval(track, model, axis_name, low, high)
        except (ValueError, TypeError) as exc:
            feedback.set_text(str(exc))
            return
        start.set_value(round(low, 3))
        end.set_value(round(high, 3))
        feedback.set_text("")
        on_range(interval, {"axis": axis_name, "start": low, "end": high})

    with ui.row().classes("w-full items-end gap-2 flex-wrap"):
        position = ui.number("Cursor position", value=0, precision=3, format="%.3f", step=.1).classes("w-40")
        ui.button("Inspect position", on_click=inspect).props("outline dense")
        ui.button("Clear cursor", on_click=lambda: ui.run_javascript(f"window.gdhTrackCharts?.[{plot.id}]?.clear()" )).props("outline dense")
    with ui.row().classes("w-full items-end gap-2 flex-wrap"):
        start = ui.number("Range start", value=0, precision=3, format="%.3f").classes("w-40")
        end = ui.number("Range end", value=round(maximum, 3), precision=3).classes("w-40")
        ui.button("Select chart range", on_click=lambda: apply_range(axis["value"], start.value, end.value)).props("outline dense")
    feedback = ui.label("").classes("text-sm text-red-8").props('role="status" aria-live="polite"')
    plot.on("track-range", lambda e: apply_range(e.args.get("axis"), e.args.get("start"), e.args.get("end")))
    return SimpleNamespace(initialize=initialize, select_interval=select_interval)


async def _nothing():
    pass
