"""Local, data-driven presentation of arbitrary MCP tool responses."""

from __future__ import annotations

import json
import math
from datetime import date
from statistics import fmean


def label(key: str) -> str:
    return key.replace("_", " ").strip().capitalize()


def display(value) -> str:
    if value is None:
        return "Not reported"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, float):
        return f"{value:,.2f}" if math.isfinite(value) else "Not reported"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def result_sections(raw: str) -> list[dict]:
    """Preserve source labels and values without inventing health interpretations."""
    try:
        payload = json.loads(raw)
    except (ValueError, TypeError):
        return [{"kind": "text", "title": "Result", "text": raw or "No data returned."}]
    sections: list[dict] = []

    def visit(value, title="Overview", depth=0):
        if depth > 5:
            sections.append({"kind": "text", "title": title, "text": "Further detail is available in Raw JSON."})
        elif isinstance(value, dict):
            if not value:
                sections.append({"kind": "text", "title": title, "text": "No data returned."})
                return
            # Schema tools return one object per database table.
            if all(isinstance(item, dict) and "columns" in item and "row_count" in item for item in value.values()):
                visit([{"table": key, "row_count": item["row_count"], "columns": display(item["columns"])}
                       for key, item in value.items()], "Database tables", depth + 1)
                return
            fields = []
            for key, item in value.items():
                if not isinstance(item, (dict, list)):
                    if key.lower() in {"error", "warning", "message", "interpretation", "feedback", "insight"} and item:
                        sections.append({"kind": "text", "title": label(key), "text": display(item), "error": key.lower() == "error"})
                    else:
                        fields.append({"label": label(key), "value": display(item)})
            if fields:
                sections.append({"kind": "fields", "title": title, "fields": fields})
            for key, item in value.items():
                if isinstance(item, (dict, list)):
                    visit(item, label(key) if title == "Overview" else f"{title} / {label(key)}", depth + 1)
        elif isinstance(value, list):
            if not value:
                sections.append({"kind": "text", "title": title, "text": "No records returned."})
            elif all(isinstance(item, dict) for item in value):
                keys = list(dict.fromkeys(key for item in value for key in item))
                rows = [{key: item.get(key) for key in keys} for item in value[:1000]]
                sections.append({"kind": "table", "title": title, "rows": rows, "count": len(value)})
            else:
                sections.append({"kind": "text", "title": title, "text": "\n".join(display(item) for item in value[:100])})
        else:
            sections.append({"kind": "text", "title": title, "text": display(value)})

    visit(payload)
    return sections


def series_for(rows: list[dict]) -> dict:
    """Use only explicitly dated numeric records; exclude IDs and missing values."""
    date_key = next((key for key in ("calendar_date", "date", "activity_date", "day")
                     if any(key in row for row in rows)), None)
    if date_key is None:
        return {}
    dated = []
    for row in rows:
        try:
            day = date.fromisoformat(str(row.get(date_key)))
        except ValueError:
            continue
        dated.append((day.isoformat(), row))
    dated.sort(key=lambda item: item[0])
    keys = list(dict.fromkeys(key for _, row in dated for key in row))
    series = {}
    for key in keys:
        if key == "id" or key.endswith("_id"):
            continue
        points = [(day, row[key]) for day, row in dated
                  if isinstance(row.get(key), (int, float)) and not isinstance(row[key], bool)
                  and math.isfinite(row[key])]
        if points:
            values = [value for _, value in points]
            plot_points = [(day, row.get(key) if isinstance(row.get(key), (int, float))
                            and not isinstance(row[key], bool) and math.isfinite(row[key]) else None)
                           for day, row in dated]
            series[key] = {"points": points, "mean": fmean(values), "min": min(values),
                           "max": max(values), "latest": values[-1], "count": len(values), "plot_points": plot_points}
    return series


def render_mcp_result(tool_name: str, raw: str, *, is_error: bool = False) -> None:
    from nicegui import ui
    import plotly.graph_objects as go
    from garmin_data_hub.ui_nicegui.layout import data_grid

    ui.label(label(tool_name.removeprefix("garmin_"))).classes("text-xl font-semibold")
    if is_error:
        ui.label("The tool reported an error.").classes("text-negative")
    for section in result_sections(raw):
        if section["kind"] == "fields":
            ui.label(section["title"]).classes("font-semibold mt-2")
            with ui.element("dl").classes("w-full grid gap-x-6 gap-y-3").style("grid-template-columns: repeat(auto-fit, minmax(min(100%, 180px), 1fr))"):
                for field in section["fields"]:
                    with ui.column().classes("gap-1 min-w-0"):
                        ui.label(field["label"]).classes("text-sm text-grey-7")
                        ui.label(field["value"]).classes("font-medium whitespace-pre-wrap").style("overflow-wrap: anywhere")
        elif section["kind"] == "text":
            ui.label(section["title"]).classes("font-semibold mt-2")
            ui.label(section["text"]).classes("whitespace-pre-wrap w-full" + (" text-negative" if section.get("error") else "")).style("overflow-wrap: anywhere")
        else:
            _render_records(section, ui, go, data_grid)
    with ui.expansion("Raw JSON", icon="code").classes("w-full mt-3"):
        ui.code(raw, language="json").classes("w-full").style("max-height: 30rem; overflow: auto")


def _render_records(section, ui, go, data_grid):
    # A separate function keeps callbacks bound to their own table and selector.
    rows = section["rows"]
    with ui.expansion(f"{section['title']} ({section['count']:,} records)", icon="table_chart", value=True).classes("w-full"):
        if section["count"] > len(rows):
            ui.label(f"Showing the first {len(rows):,} records; statistics cover these records only.").classes("text-grey-7")
        series = series_for(rows)
        if series:
            metric = ui.select({key: label(key) for key in series}, value=next(iter(series)), label="Metric").props("outlined dense").classes("w-full")

            @ui.refreshable
            def chart():
                values = series[metric.value]
                points = values["points"]
                ui.label(f"{points[0][0]} to {points[-1][0]} | {values['count']} measurements").classes("text-sm text-grey-7")
                ui.label(f"Latest: {display(values['latest'])} | Average: {display(values['mean'])} | Range: {display(values['min'])} to {display(values['max'])}").classes("text-sm whitespace-normal")
                plot_points = values["plot_points"]
                figure = go.Figure(go.Scatter(x=[p[0] for p in plot_points], y=[p[1] for p in plot_points], mode="lines+markers", name=label(metric.value), connectgaps=False))
                days = list(dict.fromkeys(day for day, _ in plot_points))
                figure.update_layout(height=260, margin=dict(l=45, r=15, t=15, b=40), yaxis_title=label(metric.value), template="plotly_white", showlegend=False,
                                     xaxis=dict(tickvals=days[::max(1, math.ceil(len(days) / 6))], tickformat="%b %d"))
                ui.plotly(figure).classes("w-full min-w-0")

            metric.on_value_change(lambda _: chart.refresh())
            chart()
        formatted = [{label(key): display(value) if isinstance(value, (dict, list)) else value
                      for key, value in row.items()} for row in rows]
        data_grid(formatted, height="20rem")
