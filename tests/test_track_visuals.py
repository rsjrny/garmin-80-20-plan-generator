from datetime import datetime, timedelta, timezone
import math
import time

import pytest
from garmin_data_hub.analytics.track_visuals import process_track, route_features, COLORS, NEUTRAL


def points(n=100, step=1, metres=3):
    origin = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [dict(lat_deg=40+i*metres/111195, lon_deg=-74,
                 timestamp_utc=(origin+timedelta(seconds=i*step)).isoformat(),
                 heart_rate_bpm=140, altitude_m=20, cadence_spm=None) for i in range(n)]


def test_steady_units_and_details():
    metric = process_track(points())
    imperial = process_track(points(), unit_system="Imperial")
    assert metric["colored"]
    assert metric["segments"][20]["pace"] == pytest.approx(1000/3, rel=.001)
    assert imperial["segments"][20]["pace"] / metric["segments"][20]["pace"] == pytest.approx(1.609344)
    for track in (metric, imperial):
        assert len(track["boundaries"]) == 4
        assert len(track["labels"]) == 5
        assert all(s["color"] in COLORS for s in track["segments"])
    detail = route_features(metric)["features"][0]["properties"]["detail"]
    assert "Heart rate: 140" in detail and "Cadence" not in detail


def test_quality_breaks_and_missing_coordinates():
    track = points()
    track[10]["paused"] = True
    track[20]["lat_deg"] = track[19]["lat_deg"]
    track[30]["lat_deg"] = 80
    track[40]["timestamp_utc"] = track[39]["timestamp_utc"]
    track[50]["lat_deg"] = float("nan")
    track[60]["timestamp_utc"] = None
    result = process_track(track)
    statuses = {s["status"] for s in result["segments"]}
    assert {"paused", "stopped", "missing time"} <= statuses
    assert all(s["pace"] is None and s["color"] == NEUTRAL for s in result["segments"] if s["status"] != "moving")
    assert not any(s["end"] == [80,-74] for s in result["segments"])
    assert len(result["paths"]) > 1


def test_gap_does_not_inflate_distance_or_smoothed_pace():
    track = points(4)
    track[2]["timestamp_utc"] = "2026-01-01T00:02:00+00:00"
    track[3]["timestamp_utc"] = "2026-01-01T00:02:01+00:00"
    result = process_track(track)
    assert result["segments"][1]["status"] == "sampling gap"
    assert result["distance_m"] == pytest.approx(6, abs=.01)
    assert result["segments"][-1]["pace"] == pytest.approx(1000/3, rel=.001)


def test_empty_sparse_and_timestamp_fallback():
    assert not process_track([])["colored"]
    assert not process_track(points(1))["colored"]
    track = points(3)
    for p in track: p.pop("timestamp_utc")
    result = process_track(track)
    assert not result["colored"] and len(result["paths"][0]) == 3
    assert all(s["color"] == NEUTRAL for s in result["segments"])
    assert not process_track([dict(lat_deg=91,lon_deg=0)])["paths"]


def test_progression_smoothing_and_robust_bands():
    track = points(200)
    for i,p in enumerate(track):
        p["lat_deg"] = 40 + (min(i,100)*2+max(0,i-100)*4)/111195
    result = process_track(track)
    assert result["segments"][20]["band"] > result["segments"][150]["band"]
    assert result["segments"][98]["pace"] != result["segments"][20]["pace"]


def test_long_track_cost_and_geometry():
    raw = points(20000)
    start = time.perf_counter()
    result = process_track(raw)
    features = route_features(result)["features"]
    assert time.perf_counter()-start < 5
    assert len(features) < 200
    assert features[0]["geometry"]["coordinates"][0] == [-74,40]
    assert len(result["segments"]) == 19999


def test_sensor_overlays_partial_values_units_and_geometry():
    from garmin_data_hub.analytics.track_visuals import prepare_overlays
    raw = points(60)
    for i, p in enumerate(raw):
        p.update(heart_rate_bpm=None if i < 15 else 120+i,
                 altitude_m=-10+i, cadence_spm=0 if i < 30 else 160+i)
    track = process_track(raw, unit_system="Imperial")
    metrics = prepare_overlays(track)
    assert metrics["heart_rate"]["count"] == 45
    assert metrics["cadence"]["count"] == 30
    assert metrics["elevation"]["unit"] == "ft"
    assert track["segments"][0]["overlays"]["heart_rate"]["color"] == NEUTRAL
    assert track["segments"][0]["overlays"]["cadence"]["value"] is None
    assert track["segments"][0]["overlays"]["elevation"]["value"] == pytest.approx(-9*3.280839895)
    features = route_features(track)["features"]
    assert sum(len(f["geometry"]["coordinates"])-1 for f in features) == len(raw)-1
    assert "Cadence" not in features[0]["properties"]["detail"]
    assert "ft" in features[0]["properties"]["detail"]
    assert all(set(f["properties"]["overlays"]) == set(metrics) for f in features)
    for metric, config in metrics.items():
        for segment in track["segments"]:
            value = segment["overlays"][metric]["value"]
            if value is not None:
                assert segment["overlays"][metric]["band"] == sum(value >= boundary for boundary in config["boundaries"])


def test_missing_sensors_and_cycling_cadence():
    from garmin_data_hub.analytics.track_visuals import prepare_overlays
    raw = points(4)
    for p in raw:
        p.update(heart_rate_bpm=float("nan"), altitude_m=None, cadence_spm=-1)
    track = process_track(raw)
    metrics = prepare_overlays(track)
    assert [k for k,v in metrics.items() if v["available"]] == ["pace"]
    for p in raw: p["cadence_spm"] = 90
    metrics = prepare_overlays(process_track(raw), "cycling")
    assert metrics["cadence"]["unit"] == "rpm"


def test_sensor_without_timestamps_and_quality_breaks():
    from garmin_data_hub.analytics.track_visuals import prepare_overlays
    raw = points(4)
    for p in raw: p["timestamp_utc"] = None
    track = process_track(raw)
    metrics = prepare_overlays(track)
    assert not metrics["pace"]["available"]
    assert metrics["heart_rate"]["available"]
    raw = points(4)
    raw[2]["paused"] = True
    track = process_track(raw)
    prepare_overlays(track)
    assert all(s["overlays"]["heart_rate"]["value"] is None for s in track["segments"] if s["status"] == "paused")
