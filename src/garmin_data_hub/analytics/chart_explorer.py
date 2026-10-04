"""Chart drill-down identities, tab-state validation, and the complete Explorer catalog."""
from __future__ import annotations
from datetime import date
import math
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from .chart_overview import QUICK_RANGES, ZONES, numeric, period_range, pace_text

CATALOG = (
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


def chart_state(saved, sports, today):
    """Validate old/stale session values and recompute relative dates on arrival."""
    from .chart_performance import performance_state
    saved = saved if isinstance(saved, dict) else {}
    quick = saved.get("quick", "12 weeks")
    if quick not in QUICK_RANGES:
        quick = "12 weeks"
    first, last = period_range(quick if quick != "Custom" else "12 weeks", today)
    if quick == "Custom":
        try:
            first, last = date.fromisoformat(saved["start"]), date.fromisoformat(saved["end"])
            if first > last or last > today:
                raise ValueError
        except (KeyError, TypeError, ValueError):
            quick = "12 weeks"
            first, last = period_range(quick, today)
    sport = saved.get("sport", "All sports")
    if not isinstance(sport, str) or sport not in ["All sports", *sports]:
        sport = "All sports"
    distance_ok = sport != "All sports" and any(x in sport.lower() for x in ("run", "walk", "hik", "cycl", "bik"))
    selected = saved.get("charts", ["average_heart_rate"])
    if not isinstance(selected, list):
        selected = ["average_heart_rate"]
    return dict(quick=quick, start=first.isoformat(), end=last.isoformat(), sport=sport,
                load=saved.get("load") if saved.get("load") in ("tss", "trimp") else "tss",
                volume="Distance" if distance_ok and saved.get("volume") == "Distance" else "Time",
                intensity="Percent" if saved.get("intensity") == "Percent" else "Hours",
                section=saved.get("section") if saved.get("section") in ("Overview", "Explorer", "Plan comparison", "Performance") else "Overview",
                performance=performance_state(saved.get("performance")),
                plan=saved.get("plan") if isinstance(saved.get("plan"),str) else "All active plans",
                alignment="Plan weeks" if saved.get("alignment") == "Plan weeks" else "Calendar weeks",
                charts=[key for key, _ in CATALOG if key in selected])


def activity_targets(frame):
    return [dict(kind="activity", key=int(value)) if pd.notna(value) else None
            for value in frame.get("activity_id", pd.Series(index=frame.index, dtype=float))]


def tag(trace, targets):
    trace.meta = dict(targets=targets)


def resolve_click(figure, payload):
    """Resolve an event against server-owned trace identities, never date positions."""
    try:
        curve, point = payload["curveNumber"], payload["pointNumber"]
        if isinstance(curve, bool) or isinstance(point, bool) or not isinstance(curve, int) or not isinstance(point, int) or curve < 0 or point < 0:
            return None
        trace = figure.data[curve]
        return trace.meta["targets"][point] if trace.meta else None
    except (KeyError, TypeError, IndexError):
        return None


def tag_overview(cards, data):
    frame, weekly = data["current"], data["weekly"]
    for card in cards[:3]:
        fig = card["figure"]
        if fig is None:
            continue
        for trace in fig.data:
            if trace.type != "bar":
                continue
            sport = trace.name if card is cards[0] and trace.name in set(frame.sport) else None
            tag(trace, [dict(kind="week", key=w.date().isoformat(), sport=sport) for w in weekly.index])
    fig = cards[3]["figure"]
    if fig is None:
        return cards
    family = data["family"]
    subset = frame[frame.avg_speed_mps.gt(0)] if family == "velocity" else frame[frame.avg_power_w.gt(0)] if family == "power" else frame[frame.duration.notna()]
    subset = subset.sort_values("date")
    for trace in fig.data:
        if trace.name in {"Activities", "Normalized power"}:
            tag(trace, activity_targets(subset))
            # Preserve the formatted metric while adding recognizable source identity.
            labels = source_rows(subset, data)
            old = list(trace.customdata) if trace.customdata is not None else list(trace.y)
            trace.customdata = [[value, row["name"], row["id"], row["sport"], row["distance"], row["duration_min"], row["time_source"]] for value, row in zip(old, labels)]
            trace.hovertemplate = "%{x|%b %d, %Y}<br>%{customdata[1]} (ID %{customdata[2]})<br>%{customdata[3]}<br>Metric: %{customdata[0]}<br>Distance: %{customdata[4]} " + data["unit"] + "<br>Time: %{customdata[5]} min (%{customdata[6]})<extra></extra>"
        elif trace.name == "Daily activity count":
            tag(trace, [dict(kind="day", key=pd.Timestamp(day).date().isoformat()) for day in trace.x])
    return cards


def contributors(frame, target):
    if target["kind"] == "week":
        selected = frame[frame.week.eq(pd.Timestamp(target["key"]))]
    elif target["kind"] == "day":
        selected = frame[frame.date.eq(pd.Timestamp(target["key"]))]
    elif target["kind"] == "sport":
        selected = frame[frame.sport.eq(target["key"])]
    else:
        return frame.iloc[:0]
    if target.get("sport"):
        selected = selected[selected.sport.eq(target["sport"])]
    return selected.sort_values(["date", "activity_id"])


def source_rows(frame, data):
    """JSON-safe, unit-aware source table shared by charts and keyboard drill-down."""
    def number(value):
        return round(float(value), 2) if pd.notna(value) and math.isfinite(float(value)) else None
    rows = []
    for _, row in frame.iterrows():
        aid = int(row.activity_id) if pd.notna(row.get("activity_id")) else None
        name = row.get("activity_name")
        if not isinstance(name, str) or not name.strip():
            name = f"Activity {aid}" if aid is not None else "Activity"
        rows.append(dict(id=aid, name=name, date=row.date.date().isoformat(), sport=row.sport,
                         distance=number(row.distance), duration_min=number(row.duration*60),
                         time_source="elapsed fallback" if row.elapsed_fallback else "moving" if pd.notna(row.duration) else "missing",
                         load=number(row.load), load_source=data["load"],
                         avg_hr_bpm=number(row.get("avg_hr_bpm", float("nan"))),
                         avg_speed_mps=number(row.avg_speed_mps), avg_power_w=number(row.avg_power_w),
                         elevation=number(row.elevation), normalized_power_w=number(row.normalized_power_w),
                         aerobic_decoupling_pct=number(row.get("aerobic_decoupling_pct", float("nan"))),
                         hr_drift_pct=number(row.get("hr_drift_pct", float("nan"))),
                         hr_zones="usable" if row.zone_usable else "unavailable/stale"))
    return rows


def explorer_figures(data, selected, velocity_display="Pace"):
    """Build only selected catalog views using C1 missingness and calendar weeks."""
    frame, weekly = data["current"].copy(), data["weekly"]
    cards = []
    labels = dict(CATALOG)
    for key, _ in CATALOG:
        if key not in selected:
            continue
        fig = None
        note = "Click a point or aggregate to inspect its source activities. Blank measurements remain missing."
        if key == "activity_distribution" and not frame.empty:
            counts = frame.groupby("sport", as_index=False).size()
            fig = px.bar(counts, x="sport", y="size")
            tag(fig.data[0], [dict(kind="sport", key=s) for s in counts.sport])
        elif key in {"weekly_distance", "weekly_duration", "weekly_training_stress", "weekly_elevation", "longest_activity", "weekly_hr_zones"}:
            fields = {"weekly_distance": ("distance", data["unit"]), "weekly_duration": ("duration", "hours (moving preferred)"),
                      "weekly_training_stress": ("load", data["load"]), "weekly_elevation": ("elevation", data["elevation_unit"]),
                      "longest_activity": ("distance", data["unit"])}
            fig = go.Figure()
            columns = ZONES if key == "weekly_hr_zones" else [fields[key][0]]
            for column in columns:
                values = weekly[column]/3600 if column in ZONES else frame.groupby("week").distance.max().reindex(weekly.index).where(weekly.activities.gt(0),0) if key == "longest_activity" else weekly[column]
                if key == "longest_activity":
                    fig.add_scatter(x=weekly.week_label, y=values, mode="lines+markers", name="Longest distance", connectgaps=False)
                else:
                    fig.add_bar(x=weekly.week_label, y=values, name=column.replace("_s", "").replace("_", " "))
                tag(fig.data[-1], [dict(kind="week", key=w.date().isoformat()) for w in weekly.index])
            if key == "weekly_hr_zones" and not data["zone_count"]:
                fig = None
            else:
                fig.update_yaxes(title="hours of measured HR-zone time" if key == "weekly_hr_zones" else fields[key][1])
            note += " Monday weeks; * marks partial weeks. Observed totals exclude missing values."
        elif key in {"average_heart_rate", "average_velocity", "load_vs_duration", "drift_decoupling"}:
            if key == "average_velocity" and (data["sport"] == "All sports" or data["family"] != "velocity"):
                note = "Select a single running, walking, or hiking sport for comparable pace/speed."
            else:
                if key == "average_heart_rate":
                    frame["metric"] = numeric(frame, "avg_hr_bpm").where(numeric(frame, "avg_hr_bpm").gt(0))
                    x, y, unit = "date", "metric", "bpm"
                elif key == "average_velocity":
                    speed = frame.avg_speed_mps.where(frame.avg_speed_mps.gt(0))
                    frame["metric"] = speed*(2.236936292 if data["imperial"] else 3.6) if velocity_display == "Speed" else (1609.344 if data["imperial"] else 1000)/speed/60
                    x, y, unit = "date", "metric", ("mph" if data["imperial"] else "km/h") if velocity_display == "Speed" else "min/"+data["unit"]
                elif key == "load_vs_duration":
                    x, y, unit = "duration", "load", data["load"]
                else:
                    x, y, unit = "date", "metric", "%"
                fig = go.Figure()
                metric_fields = [c for c in ("aerobic_decoupling_pct", "hr_drift_pct") if c in frame] if key == "drift_decoupling" else [y]
                for field in metric_fields:
                    subset = frame.dropna(subset=[x,field]).sort_values("date")
                    if subset.empty:
                        continue
                    for sport, group in subset.groupby("sport"):
                        fig.add_scatter(x=group[x], y=group[field], mode="markers", name=f"{sport} · {field}" if key == "drift_decoupling" else sport,
                                        customdata=[[r["name"], r["id"], r["distance"], r["duration_min"], pace_text(v*60) if key == "average_velocity" and velocity_display != "Speed" else f"{v:.2f}"] for r,v in zip(source_rows(group,data), group[field])],
                                        hovertemplate="%{customdata[0]} (ID %{customdata[1]})<br>%{x}<br>%{customdata[4]} " + unit + "<br>Distance: %{customdata[2]} " + data["unit"] + "<br>Time: %{customdata[3]} min<extra></extra>")
                        tag(fig.data[-1], activity_targets(group))
                if not fig.data:
                    fig = None
                elif key == "average_velocity" and velocity_display != "Speed":
                    values = frame.metric.dropna()
                    ticks = [values.min()+(values.max()-values.min())*i/4 for i in range(5)]
                    fig.update_yaxes(autorange="reversed", tickvals=ticks, ticktext=[pace_text(v*60) for v in ticks])
                if fig is not None:
                    fig.update_yaxes(title=unit)
                    if key == "load_vs_duration":
                        fig.update_xaxes(title="hours (moving preferred)")
                if key == "drift_decoupling":
                    note += " Stored provenance-approved metrics; effort qualification is not assessed here."
        if fig is not None:
            fig.update_layout(template="plotly_white", height=320, margin=dict(l=45,r=15,t=20,b=65), barmode="stack", font=dict(size=12), legend=dict(orientation="h",y=-.25))
        cards.append(dict(title=labels[key], figure=fig, note=note))
    return cards
