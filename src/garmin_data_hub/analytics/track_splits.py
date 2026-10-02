"""Read-only interval analytics over the validated T1/T2 GPS track."""
from __future__ import annotations

import json

from .track_visuals import number, sensor_value, timestamp


def _counted(segment):
    return segment["metres"] if segment["status"] not in {"sampling gap", "paused"} else 0


def _coordinate(segment, fraction):
    return [a + (b - a) * fraction for a, b in zip(segment["start"], segment["end"])]


def _pieces(track, start, end, axis):
    """Clip each real edge; never join across a quality break."""
    pieces = []
    for segment in track["segments"]:
        if axis == "distance":
            high = segment["distance_m"]
            low = high - _counted(segment)
        else:
            low, high = segment["start_time"], segment["time"]
            if low is None or high is None:
                continue
        if high < start or low >= end:
            continue
        if high == low:
            # A stop or pause contributes elapsed time at an interior distance.
            if not start <= high < end:
                continue
            left, right = 0, 1
        else:
            left = max(0, (start - low) / (high - low))
            right = min(1, (end - low) / (high - low))
            if right <= left:
                continue
        pieces.append((segment, left, right))
    return pieces


def _summarize(track, pieces, start, end, axis):
    paths, path = [], []
    duration = 0.0
    metres = 0.0
    trustworthy = bool(pieces)
    prior = None
    readings = {key: [0.0, 0.0] for key in ("heart_rate_bpm", "cadence_spm")}
    max_hr = None
    altitude_start = altitude_end = None
    altitude_complete = True
    sensor_seconds = 0.0
    for segment, left, right in pieces:
        fraction = right - left
        dt = segment["duration"]
        if dt is None or dt <= 0 or segment["status"] == "sampling gap":
            trustworthy = False
        if prior is not None and (prior["end"] != segment["start"] or prior["time"] != segment["start_time"]):
            trustworthy = False
        prior = segment
        if dt is not None and dt > 0:
            duration += dt * fraction
        metres += _counted(segment) * fraction
        if segment["status"] == "sampling gap":
            altitude_complete = False
            if path:
                paths.append(path)
                path = []
            continue
        a, b = _coordinate(segment, left), _coordinate(segment, right)
        if path and path[-1] != a:
            paths.append(path)
            path = []
        if not path:
            path = [a]
        path.append(b)
        if dt is None or dt <= 0 or segment["status"] == "paused":
            altitude_complete = False
            continue
        weight = dt * fraction
        sensor_seconds += weight
        for field, sums in readings.items():
            value = sensor_value(segment["point"], field)
            if value is not None:
                sums[0] += value * weight
                sums[1] += weight
                if field == "heart_rate_bpm":
                    max_hr = value if max_hr is None else max(max_hr, value)
        elevation = sensor_value(segment["point"], "altitude_m")
        old_elevation = sensor_value(segment["start_point"], "altitude_m")
        if elevation is not None and old_elevation is not None:
            if altitude_start is None:
                altitude_start = old_elevation + (elevation - old_elevation) * left
            altitude_end = old_elevation + (elevation - old_elevation) * right
        else:
            altitude_complete = False
    if path:
        paths.append(path)
    # Time intervals must cover their requested endpoints and all intervening time.
    if axis == "time":
        trustworthy = trustworthy and abs(duration - (end - start)) < 0.01
    unit_m = 1609.344 if track["unit"] == "mi" else 1000
    return dict(paths=paths, duration_s=duration if trustworthy else None,
                distance_m=metres, pace=duration / metres * unit_m if trustworthy and metres > 0 else None,
                trustworthy=trustworthy,
                elevation_change_m=altitude_end-altitude_start if altitude_complete and altitude_start is not None and trustworthy else None,
                average_hr=readings["heart_rate_bpm"][0] / readings["heart_rate_bpm"][1] if readings["heart_rate_bpm"][1] else None,
                max_hr=max_hr,
                cadence=readings["cadence_spm"][0] / readings["cadence_spm"][1] if readings["cadence_spm"][1] else None,
                hr_coverage=readings["heart_rate_bpm"][1] / sensor_seconds if sensor_seconds else 0,
                cadence_coverage=readings["cadence_spm"][1] / sensor_seconds if sensor_seconds else 0)


def distance_splits(track):
    """Full preferred-unit splits plus an explicitly partial final interval."""
    total = track["distance_m"]
    if total <= 0:
        return []
    unit_m = 1609.344 if track["unit"] == "mi" else 1000
    splits = []
    start = 0.0
    while start < total - 0.01:
        end = min(total, start + unit_m)
        pieces = _pieces(track, start, end, "distance")
        summary = _summarize(track, pieces, start, end, "distance")
        partial = end - start < unit_m - 0.01
        index = len(splits) + 1
        splits.append(dict(summary, id=f"split:{index}", label=f"GPS split {index}" + (" (partial)" if partial else ""),
                           kind="GPS split", start=start, end=end, partial=partial, fastest=False,
                           marker=summary["paths"][-1][-1] if summary["paths"] else None,
                           note="Partial final distance" if partial else ""))
        if not summary["trustworthy"]:
            splits[-1]["note"] = "Incomplete timing / GPS quality break; pace unavailable"
        start = end
    eligible = [s for s in splits if not s["partial"] and s["pace"] is not None]
    if eligible:
        min(eligible, key=lambda s: s["pace"])["fastest"] = True
    return splits


def stored_laps(track, rows):
    """Use source timing/provenance only; never infer manual or timer/elapsed equivalence."""
    laps = []
    metadata = []
    for row in rows:
        try:
            raw = json.loads(row.get("raw_json") or "{}")
        except (ValueError, TypeError):
            raw = {}
        if not isinstance(raw, dict):
            raw = {}
        metadata.append(raw)
    starts = [timestamp(row.get("start_time_gmt")) if timestamp(row.get("start_time_gmt")) is not None
              else timestamp(raw.get("startTimeGMT")) for row, raw in zip(rows, metadata)]
    for i, (row, raw) in enumerate(zip(rows, metadata)):
        start = starts[i]
        trigger = str(raw.get("lapTrigger") or raw.get("lap_trigger") or "").lower()
        manual = trigger in {"manual", "manual_lap"}
        kind = "Manual lap" if manual else "Stored lap"
        elapsed = number(raw.get("elapsedDuration"))
        end = timestamp(raw.get("endTimeGMT"))
        if end is None and start is not None and elapsed is not None and elapsed > 0:
            end = start + elapsed
        if end is None and i + 1 < len(starts):
            end = starts[i + 1]
        aligned = start is not None and end is not None and end > start
        pieces = _pieces(track, start, end, "time") if aligned else []
        summary = _summarize(track, pieces, start, end, "time") if pieces else dict(paths=[], trustworthy=False)
        metres, duration = number(row.get("distance_meters")), number(row.get("duration_seconds"))
        if metres is not None and metres <= 0:
            metres = None
        if duration is not None and duration <= 0:
            duration = None
        divisor = 1609.344 if track["unit"] == "mi" else 1000
        laps.append(dict(id=f"lap:{i+1}", label=f"{kind} {row.get('split_number', i+1)}", kind=kind,
                         paths=summary["paths"], marker=summary["paths"][0][0] if summary["paths"] else None,
                         distance_m=metres, duration_s=duration, pace=duration/metres*divisor if duration and metres else None,
                         average_hr=sensor_value(row, "heart_rate_bpm"), max_hr=sensor_value(row, "max_hr"),
                         cadence=sensor_value(row, "cadence_spm"), hr_coverage=None, cadence_coverage=None,
                         elevation_change_m=(number(row.get("elevation_gain")) - number(row.get("elevation_loss")))
                         if number(row.get("elevation_gain")) is not None and number(row.get("elevation_loss")) is not None else None,
                         partial=False, fastest=False, trustworthy=summary["trustworthy"],
                         note=("Source lap totals; route aligned by elapsed timestamps" if summary["trustworthy"] else
                               "Source lap totals; route timing unavailable or incomplete") + ("; trigger unknown" if not trigger else "")))
    return laps


def interval_features(interval):
    return {"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry": {"type": "LineString", "coordinates": [[lon, lat] for lat, lon in path]},
         "properties": {}} for path in interval.get("paths", []) if len(path) > 1
    ]}
