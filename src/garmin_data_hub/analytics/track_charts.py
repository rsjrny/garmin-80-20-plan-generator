"""Bounded aligned charts over original validated route edges (no sensor interpolation)."""
from __future__ import annotations

import math

from .track_visuals import pace_label, prepare_overlays
from .track_splits import _counted, _pieces, _summarize

METRICS = ("pace", "heart_rate", "elevation", "cadence")


def chart_model(track, sport="running"):
    if "metrics" not in track:
        prepare_overlays(track, sport)
    segments = track["segments"]
    origin = next((s["time"] - s["elapsed"] for s in segments
                   if s["time"] is not None and s["elapsed"] is not None), None)
    divisor = 1609.344 if track["unit"] == "mi" else 1000
    # Compact cursor rows: distance, elapsed seconds, lat, lon, four values.
    rows = [[s["distance_m"] / divisor, s["elapsed"], *s["end"],
             *[s["overlays"][k]["value"] for k in METRICS]] for s in segments]
    axes = {}
    if track["distance_m"] > 0:
        axes["distance"] = {"label": f"GPS distance ({track['unit']})", "max": track["distance_m"] / divisor}
    times = [r[1] for r in rows if r[1] is not None and r[1] >= 0]
    if times and max(times) > 0:
        axes["time"] = {"label": "Elapsed time (min)", "max": max(times) / 60}
    return {"rows": rows, "origin": origin, "axes": axes,
            "metrics": {k: track["metrics"][k] for k in METRICS if track["metrics"][k]["available"]}}


def _sample(run, segments, metric, limit):
    """Keep endpoints plus local extrema; never sample across a quality break."""
    if len(run) <= limit:
        return run
    buckets = max(1, (limit - 2) // 2)
    result = {run[0], run[-1]}
    for bucket in range(buckets):
        values = run[bucket * len(run) // buckets:(bucket + 1) * len(run) // buckets]
        result.add(min(values, key=lambda i: segments[i]["overlays"][metric]["value"]))
        result.add(max(values, key=lambda i: segments[i]["overlays"][metric]["value"]))
    return sorted(result)


def chart_figure(track, model, axis="time", max_points=2400):
    if axis not in model["axes"]:
        axis = next(iter(model["axes"]), "distance")
    segments, traces, layout = track["segments"], [], {}
    metrics = model["metrics"]
    n = len(metrics)
    for position, (metric, config) in enumerate(metrics.items()):
        runs, run = [], []
        previous = None
        for i, s in enumerate(segments):
            row = model["rows"][i]
            valid = (s["overlays"][metric]["value"] is not None and s["status"] != "sampling gap"
                     and (axis != "time" or (row[1] is not None and s["start_time"] is not None)))
            contiguous = previous is None or (previous["end"] == s["start"] and previous["time"] == s["start_time"])
            if run and (not valid or not contiguous):
                runs.append(run)
                run = []
            if valid:
                run.append(i)
            previous = s
        if run:
            runs.append(run)
        x, y, identities = [], [], []
        retained = set(_sample([i for run in runs for i in run], segments, metric, max_points))
        for run in runs:
            shown = [i for i in run if i in retained]
            if not shown:
                continue
            for i in shown:
                row = model["rows"][i]
                x.append(row[1] / 60 if axis == "time" else row[0])
                y.append(row[4 + METRICS.index(metric)] / 60 if metric == "pace" else row[4 + METRICS.index(metric)])
                identities.append(i)
            x.append(None)
            y.append(None)
            identities.append(None)
        suffix = str(position + 1) if position else ""
        label = "Smoothed pace" if metric == "pace" else config["label"]
        traces.append({"type": "scatter", "mode": "lines+markers", "x": x, "y": y,
                       "customdata": identities, "name": label, "xaxis": f"x{suffix}", "yaxis": f"y{suffix}",
                       "connectgaps": False, "line": {"color": "#2455a4", "width": 1.7},
                       "marker": {"size": 3, "opacity": .5},
                       "hovertemplate": f"{label}: %{{y:.2f}} {config['unit']}<extra></extra>"})
        top = 1 - position / max(1, n)
        bottom = 1 - (position + 1) / max(1, n) + (.055 if n > 1 else 0)
        layout[f"xaxis{suffix}"] = {"domain": [0, 1], "anchor": f"y{suffix}", "matches": "x" if position else None,
                                   "range": [0, model["axes"][axis]["max"]], "showticklabels": position == n - 1,
                                   "title": {"text": model["axes"][axis]["label"] if position == n - 1 else ""}}
        layout[f"yaxis{suffix}"] = {"domain": [bottom, top], "anchor": f"x{suffix}",
                                   "title": {"text": f"{label}<br>({config['unit']})", "font": {"size": 11}},
                                   "fixedrange": True, "autorange": "reversed" if metric == "pace" else True}
        if metric == "pace":
            values = [value for value in y if value is not None]
            if values:
                low, high = min(values) * 60, max(values) * 60
                if high - low < 20:
                    center = (low + high) / 2
                    low, high = center - 10, center + 10
                step = max(5, round((high - low) / 20) * 5)
                low, high = math.floor(low / step) * step, math.ceil(high / step) * step
                ticks = list(range(max(0, int(low)), int(high) + 1, step))
                layout[f"yaxis{suffix}"].update(autorange=False, range=[(high + step * .15) / 60, max(0, low - step * .15) / 60],
                                              tickvals=[v / 60 for v in ticks], ticktext=[pace_label(v) for v in ticks])
    layout.update({"height": max(220, n * 155 + 60), "margin": {"l": 75, "r": 15, "t": 15, "b": 50},
                   "showlegend": False, "hovermode": "closest", "dragmode": "select", "selectdirection": "h",
                   "paper_bgcolor": "white", "plot_bgcolor": "#f8fafc", "font": {"size": 11},
                   "shapes": [], "uirevision": axis})
    return {"data": traces, "layout": layout,
            "config": {"displaylogo": False, "modeBarButtonsToRemove": ["lasso2d", "toImage"], "scrollZoom": False}}


def interval_bounds(track, model, interval, axis):
    """Exact clipped chart bounds; unknown source timing never gains invented bounds."""
    if not interval or not interval.get("paths") or interval.get("start") is None or interval.get("end") is None:
        return None
    source_axis = interval.get("axis", "distance" if interval["kind"] == "GPS split" else "time")
    start, end = interval["start"], interval["end"]
    if source_axis == axis:
        divisor = 60 if axis == "time" else (1609.344 if track["unit"] == "mi" else 1000)
        offset = model["origin"] if axis == "time" else 0
        return [(start - offset) / divisor, (end - offset) / divisor] if offset is not None else None
    pieces = _pieces(track, start, end, source_axis)
    if not pieces:
        return None
    converted = []
    for s, left, right in pieces:
        if axis == "time":
            if s["start_time"] is None or s["duration"] is None or model["origin"] is None:
                return None
            converted.extend((s["start_time"] + s["duration"] * f - model["origin"]) / 60 for f in (left, right))
        else:
            metres = _counted(s)
            converted.extend((s["distance_m"] - metres + metres * f) / (1609.344 if track["unit"] == "mi" else 1000) for f in (left, right))
    return [min(converted), max(converted)] if converted else None


def range_interval(track, model, axis, start, end):
    """Validate display units, clip real geometry and share T3 measurement semantics."""
    if axis not in model["axes"]:
        raise ValueError("This axis is unavailable.")
    from .track_visuals import number
    start, end = number(start), number(end)
    if (start is None or end is None or not 0 <= start < end <= model["axes"][axis]["max"] + .0005 + 1e-8
            or start >= model["axes"][axis]["max"]):
        raise ValueError("Choose a range within the activity, with end after start.")
    end = min(end, model["axes"][axis]["max"])
    divisor = 60 if axis == "time" else (1609.344 if track["unit"] == "mi" else 1000)
    offset = model["origin"] if axis == "time" else 0
    low, high = start * divisor + offset, end * divisor + offset
    pieces = _pieces(track, low, high, axis)
    summary = _summarize(track, pieces, low, high, axis)
    return dict(summary, id="range", label="Selected chart range", kind="Chart range", axis=axis,
                start=low, end=high, partial=False, fastest=False, marker=None,
                note="GPS range measurements" if summary["trustworthy"] else "Incomplete timing / GPS quality break; pace unavailable")
