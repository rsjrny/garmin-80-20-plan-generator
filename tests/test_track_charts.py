import math
import time

import pytest

from garmin_data_hub.analytics.track_charts import chart_model, chart_figure, interval_bounds, range_interval
from garmin_data_hub.analytics.track_splits import distance_splits, stored_laps
from garmin_data_hub.analytics.track_visuals import process_track
from test_track_visuals import points


def test_shared_units_and_exact_split_mapping():
    for units, unit, factor in (("Metric", "km", 1), ("Imperial", "mi", 3.280839895)):
        track = process_track(points(1301), unit_system=units)
        model = chart_model(track)
        assert model["axes"]["time"]["max"] == pytest.approx(1300 / 60)
        assert model["axes"]["distance"]["label"] == f"GPS distance ({unit})"
        assert model["rows"][30][6] == pytest.approx(20 * factor)
        split = distance_splits(track)[0]
        assert interval_bounds(track, model, split, "distance") == pytest.approx([0, 1])
        unit_m = 1609.344 if unit == "mi" else 1000
        assert interval_bounds(track, model, split, "time") == pytest.approx([0, unit_m / 3 / 60], rel=.001)
        fig = chart_figure(track, model, "time")
        assert len(fig["data"]) == 3  # No cadence readings.
        assert fig["layout"]["yaxis"]["range"][0] > fig["layout"]["yaxis"]["range"][1]
        assert fig["data"][0]["y"][0] == pytest.approx(unit_m / 3 / 60, rel=.001)
        assert all(trace["connectgaps"] is False for trace in fig["data"])


def test_quality_breaks_and_partial_sensor_gaps_are_not_connected():
    raw = points(20)
    raw[7]["heart_rate_bpm"] = None
    raw[10]["lat_deg"] = None
    raw[15]["paused"] = True
    track = process_track(raw)
    model = chart_model(track)
    for axis in ("time", "distance"):
        fig = chart_figure(track, model, axis)
        hr = next(t for t in fig["data"] if t["name"] == "Heart rate")
        identities = hr["customdata"]
        assert identities.count(None) >= 4
        assert all(model["rows"][i][5] is not None for i in identities if i is not None)
        # Every rendered run is contiguous in source identity; omitted edges cannot be bridged.
        for a, b in zip(identities, identities[1:]):
            if a is not None and b is not None:
                assert b == a + 1
    selected = range_interval(track, model, "time", 0, 19 / 60)
    assert not selected["trustworthy"] and selected["pace"] is None
    assert len(selected["paths"]) == 2


def test_no_timestamps_still_supports_sensor_distance_charts():
    raw = points(20)
    for p in raw:
        p["timestamp_utc"] = None
    track = process_track(raw)
    model = chart_model(track)
    assert "time" not in model["axes"]
    fig = chart_figure(track, model)
    assert [t["name"] for t in fig["data"]] == ["Heart rate", "Elevation"]
    assert fig["layout"]["xaxis"]["title"]["text"] == ""  # Title belongs to bottom subplot.
    selected = range_interval(track, model, "distance", .01, .04)
    assert selected["paths"] and selected["duration_s"] is None
    assert interval_bounds(track, model, selected, "distance") == pytest.approx([.01, .04])
    with pytest.raises(ValueError, match="unavailable"):
        range_interval(track, model, "time", 0, .1)


def test_source_lap_elapsed_boundaries_do_not_use_timer_totals():
    raw = points(500)
    track = process_track(raw)
    model = chart_model(track)
    lap = stored_laps(track, [{"split_number":1, "start_time_gmt":raw[20]["timestamp_utc"],
                              "duration_seconds":60, "distance_meters":200,
                              "raw_json":'{"elapsedDuration":120,"lapTrigger":"manual"}'}])[0]
    assert lap["duration_s"] == 60
    assert interval_bounds(track, model, lap, "time") == pytest.approx([20/60, 140/60])
    assert interval_bounds(track, model, lap, "distance") == pytest.approx([.06, .42], rel=.001)
    unknown = stored_laps(track, [{"start_time_gmt":raw[20]["timestamp_utc"], "duration_seconds":60}])[0]
    assert interval_bounds(track, model, unknown, "time") is None
    outside = stored_laps(track, [{"start_time_gmt":"2027-01-01T00:00:00Z", "raw_json":'{"elapsedDuration":60}'}])[0]
    assert interval_bounds(track, model, outside, "time") is None


def test_numeric_range_exact_clipping_and_input_validation():
    track = process_track(points(1000))
    model = chart_model(track)
    interval = range_interval(track, model, "distance", .123, .789)
    assert interval["distance_m"] == pytest.approx(666)
    assert interval["duration_s"] == pytest.approx(222, rel=.001)
    assert interval_bounds(track, model, interval, "time") == pytest.approx([41/60, 263/60], rel=.001)
    maximum = model["axes"]["distance"]["max"]
    for low, high in ((maximum + .0001, maximum + .0002), (-1, 2), (1, 1), (2, 1), (0, 100), (math.nan, 1), (0, math.inf), (None, 1)):
        with pytest.raises(ValueError):
            range_interval(track, model, "distance", low, high)


def test_bounded_long_chart_keeps_spike_and_raw_cursor_identity():
    raw = points(20000)
    for p in raw:
        p["cadence_spm"] = 175
    raw[10001]["heart_rate_bpm"] = 210
    started = time.perf_counter()
    track = process_track(raw)
    model = chart_model(track)
    figure = chart_figure(track, model)
    assert time.perf_counter() - started < 5
    assert len(model["rows"]) == 19999
    assert len(figure["data"]) == 4
    hr = figure["data"][1]
    assert 210 in hr["y"] and 10000 in hr["customdata"]
    assert all(len(trace["x"]) <= 4800 for trace in figure["data"])
    # Pathologically sparse sensors are also bounded and stay disconnected.
    for p in raw[::2]:
        p["heart_rate_bpm"] = None
    track = process_track(raw)
    figure = chart_figure(track, chart_model(track), max_points=400)
    hr = figure["data"][1]
    assert len(hr["x"]) <= 800
    assert all(a is None or b is None for a, b in zip(hr["y"], hr["y"][1:]))


def test_stationary_timed_sensors_and_rounded_full_range():
    raw = points(100)
    for p in raw:
        p["lat_deg"] = 40
    track = process_track(raw)
    model = chart_model(track)
    assert list(model["axes"]) == ["time"]
    assert [t["name"] for t in chart_figure(track, model)["data"]] == ["Heart rate", "Elevation"]
    interval = range_interval(track, model, "time", 0, round(model["axes"]["time"]["max"], 3))
    assert interval["duration_s"] == 99 and interval["distance_m"] == 0
    assert interval["pace"] is None
    with pytest.raises(ValueError):
        range_interval(track, model, "time", 0, model["axes"]["time"]["max"] + .001)
