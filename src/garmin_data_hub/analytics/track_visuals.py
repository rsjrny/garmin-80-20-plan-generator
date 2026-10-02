"""Pure, unit-aware route processing. Never connect across invalid GPS points."""
from __future__ import annotations

from bisect import bisect_right
from datetime import datetime, timezone
import math
from statistics import median

COLORS = ("#2455a4", "#168ba5", "#509568", "#bc9235", "#c27646")
NEUTRAL = "#808892"


def number(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def coordinate(point):
    lat, lon = number(point.get("lat_deg")), number(point.get("lon_deg"))
    if lat is None or lon is None or not -90 <= lat <= 90 or not -180 <= lon <= 180:
        return None
    return [lat, lon]


def timestamp(value):
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt.replace(tzinfo=timezone.utc).timestamp() if dt.tzinfo is None else dt.timestamp()
    except (ValueError, TypeError, OverflowError):
        return None


def distance(a, b):
    lat1, lat2 = map(math.radians, (a[0], b[0]))
    dlat, dlon = lat2-lat1, math.radians(b[1]-a[1])
    h = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    return 6371000 * 2 * math.asin(math.sqrt(min(1, h)))


def pace_label(seconds):
    seconds = round(seconds)
    return f"{seconds // 60}:{seconds % 60:02d}"


def process_track(points, sport="running", unit_system="metric", window_s=20):
    """Emit segments, legend, and disconnected fallback paths in stored order.

    Distances exclude rejected jumps and gaps. No sensor distance is fabricated.
    Explicit paused flags are supported although the current DB has no such field.
    """
    unit_m = 1609.344 if str(unit_system).lower() == "imperial" else 1000
    unit = "mi" if str(unit_system).lower() == "imperial" else "km"
    speed_limit = 35 if any(s in str(sport).lower() for s in ("cycl", "bik")) else 12
    segments, paths, path = [], [], []
    total = 0.0
    origin = next((t for p in points if (t := timestamp(p.get("timestamp_utc"))) is not None), None)
    previous, high_time = None, None
    for p in points:
        c, t = coordinate(p), timestamp(p.get("timestamp_utc"))
        if c is None:
            if path: paths.append(path)
            path, previous = [], None
            continue
        if previous is None:
            path = [c]
        else:
            old, a, start = previous
            dt = t-start if t is not None and start is not None else None
            metres = distance(a, c)
            status = "moving"
            if dt is None: status = "missing time"
            elif dt <= 0 or (high_time is not None and t <= high_time): status = "invalid time"
            elif dt > 60: status = "sampling gap"
            elif p.get("paused") or old.get("paused"): status = "paused"
            elif metres / dt > speed_limit: status = "GPS jump"
            elif metres / dt < 0.5: status = "stopped"
            if status in {"GPS jump", "invalid time", "sampling gap"}:
                if path: paths.append(path)
                path = [c]
            else:
                path.append(c)
            if status not in {"GPS jump", "invalid time"}:
                if status not in {"sampling gap", "paused"}: total += metres
                segments.append(dict(start=a, end=c, time=t, start_time=start, duration=dt,
                                     distance_m=total, metres=metres, status=status,
                                     pace=dt/metres*unit_m if status == "moving" else None,
                                     point=p, elapsed=t-origin if t is not None and origin is not None else None))
        if t is not None: high_time = max(high_time, t) if high_time is not None else t
        previous = p, c, t
    if path: paths.append(path)
    # Smooth contiguous valid runs only; sliding window is linear in track size.
    index = 0
    while index < len(segments):
        if segments[index]["status"] != "moving":
            index += 1
            continue
        end = index
        while (end < len(segments) and segments[end]["status"] == "moving"
               and (end == index or (segments[end-1]["end"] == segments[end]["start"]
                                    and segments[end-1]["time"] == segments[end]["start_time"]))): end += 1
        run = segments[index:end]
        left = right = 0
        seconds = metres = 0.0
        for s in run:
            while right < len(run) and run[right]["time"] <= s["time"]+window_s/2:
                seconds += run[right]["duration"]
                metres += run[right]["metres"]
                right += 1
            while left < right and run[left]["time"] < s["time"]-window_s/2:
                seconds -= run[left]["duration"]
                metres -= run[left]["metres"]
                left += 1
            s["pace"] = seconds/metres*unit_m
        index = end
    samples = sorted(s["pace"] for s in segments if s["pace"] is not None)
    boundaries = []
    if samples:
        low, high = samples[int((len(samples)-1)*.05)], samples[int((len(samples)-1)*.95)]
        center = median(samples)
        if high-low < 20: low, high = center-10, center+10
        boundaries = [max(1, round((low+(high-low)*i/5)/5)*5) for i in range(1,5)]
        for i in range(1,4): boundaries[i] = max(boundaries[i], boundaries[i-1]+5)
    for s in segments:
        s["band"] = bisect_right(boundaries, s["pace"]) if s["pace"] is not None else None
        s["color"] = COLORS[s["band"]] if s["band"] is not None else NEUTRAL
    labels = []
    if boundaries:
        labels = [f"< {pace_label(boundaries[0])}"]
        labels += [f"{pace_label(a)}–< {pace_label(b)}" for a,b in zip(boundaries,boundaries[1:])]
        labels += [f"≥ {pace_label(boundaries[-1])}"]
    return dict(segments=segments, paths=paths, boundaries=boundaries, labels=labels,
                unit=unit, colored=bool(samples), distance_m=total)


METRIC_SPECS = {
    "pace": {"label": "Pace", "field": None, "colors": COLORS},
    "heart_rate": {"label": "Heart rate", "field": "heart_rate_bpm", "colors": COLORS},
    "elevation": {"label": "Elevation", "field": "altitude_m", "colors": COLORS},
    "cadence": {"label": "Cadence", "field": "cadence_spm", "colors": COLORS},
}


def sensor_value(point, field):
    value = number(point.get(field))
    if value is not None and field != "altitude_m" and value <= 0:
        return None
    return value


def prepare_overlays(track, sport="running"):
    """Classify sensors once; switching metrics changes only client-side styles.

    No interpolation across sensor holes. Gap and pause geometry stays neutral.
    Sensors with missing timestamps can still be shown, as can stationary readings.
    """
    metrics = {}
    for key, spec in METRIC_SPECS.items():
        if key == "pace":
            unit = f"min/{track['unit']}"
            values = [s["pace"] for s in track["segments"]]
            boundaries, labels = track["boundaries"], track["labels"]
            direction = "faster → slower"
        else:
            unit = {"heart_rate": "bpm", "elevation": "ft" if track["unit"] == "mi" else "m", "cadence": "rpm" if any(x in str(sport).lower() for x in ("cycl", "bik")) else "spm"}[key]
            factor = 3.280839895 if unit == "ft" else 1
            values = [sensor_value(s["point"], spec["field"]) if s["status"] not in {"sampling gap", "paused"} else None for s in track["segments"]]
            values = [v*factor if v is not None else None for v in values]
            samples = sorted(v for v in values if v is not None)
            boundaries, labels = [], []
            if samples:
                # Elevation spans the full route; HR/cadence resist isolated spikes.
                low, high = (samples[0], samples[-1]) if key == "elevation" else (samples[int((len(samples)-1)*.05)], samples[int((len(samples)-1)*.95)])
                if high-low < 5: low, high = median(samples)-2.5, median(samples)+2.5
                boundaries = [round(low+(high-low)*i/5) for i in range(1,5)]
                for i in range(1,4): boundaries[i] = max(boundaries[i], boundaries[i-1]+1)
                labels = [f"< {boundaries[0]}"] + [f"{a}–< {b}" for a,b in zip(boundaries,boundaries[1:])] + [f"≥ {boundaries[-1]}"]
            direction = "low → high"
        count = sum(v is not None for v in values)
        metrics[key] = dict(label=spec["label"], unit=unit, colors=spec["colors"], boundaries=boundaries,
                            labels=labels, available=bool(count), count=count, total=len(values), direction=direction)
        for segment, value in zip(track["segments"], values):
            band = bisect_right(boundaries, value) if value is not None else None
            segment.setdefault("overlays", {})[key] = dict(value=value, band=band, color=spec["colors"][band] if band is not None else NEUTRAL)
    track["metrics"] = metrics
    return metrics


def route_features(track):
    """Partition geometry once on every metric's bands and missingness."""
    if "metrics" not in track:
        prepare_overlays(track)
    groups = []
    def signature(s):
        return tuple(s["overlays"][k]["band"] for k in METRIC_SPECS)
    for s in track["segments"]:
        if (groups and groups[-1][-1]["end"] == s["start"]
                and signature(groups[-1][-1]) == signature(s)
                and groups[-1][-1]["status"] == s["status"]
                and len(groups[-1]) < 120):
            groups[-1].append(s)
        else:
            groups.append([s])
    features = []
    for group in groups:
        first, last = group[0], group[-1]
        text = [f"Section: {last['status']}"]
        if first["elapsed"] is not None and last["elapsed"] is not None:
            text.append(f"Elapsed: {max(0, round(first['elapsed']-(first['duration'] or 0)))}–{round(last['elapsed'])} s")
        divisor = 1609.344 if track["unit"] == "mi" else 1000
        text.append(f"Cumulative GPS distance: {last['distance_m']/divisor:.2f} {track['unit']}")
        for key, config in track["metrics"].items():
            values = [s["overlays"][key]["value"] for s in group if s["overlays"][key]["value"] is not None]
            if values:
                value = pace_label(median(values)) if key == "pace" else f"{median(values):.0f}"
                label = "Smoothed pace" if key == "pace" else config["label"]
                text.append(f"{label}: {value} {config['unit']}")
        coords = [first["start"]]+[s["end"] for s in group]
        overlays = {key: dict(color=first["overlays"][key]["color"], missing=first["overlays"][key]["value"] is None) for key in METRIC_SPECS}
        features.append(dict(type="Feature", geometry=dict(type="LineString", coordinates=[[c[1],c[0]] for c in coords]),
                             properties=dict(color=first["color"], detail="<br>".join(text), overlays=overlays)))
    return dict(type="FeatureCollection", features=features)
