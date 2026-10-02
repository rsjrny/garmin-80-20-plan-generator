import json
import sqlite3

import pytest

from garmin_data_hub.analytics.track_splits import distance_splits, interval_features, stored_laps
from garmin_data_hub.analytics.track_visuals import process_track
from garmin_data_hub.db.queries import get_activity_laps
from test_track_visuals import points


@pytest.mark.parametrize("units,unit_m", [("Metric", 1000), ("Imperial", 1609.344)])
def test_exact_splits_units_partial_and_fastest(units, unit_m):
    raw = points(1501)
    for i, point in enumerate(raw):
        point["altitude_m"] = i / 10
    track = process_track(raw, unit_system=units)
    splits = distance_splits(track)
    assert len(splits) == (5 if units == "Metric" else 3)
    assert sum(s["distance_m"] for s in splits) == pytest.approx(track["distance_m"])
    assert all(s["distance_m"] == pytest.approx(unit_m) for s in splits[:-1])
    assert splits[-1]["partial"] and not splits[-1]["fastest"]
    assert sum(s["fastest"] for s in splits) == 1
    assert splits[0]["duration_s"] == pytest.approx(unit_m / 3, rel=.001)
    assert splits[0]["elevation_change_m"] == pytest.approx(unit_m / 30, rel=.001)
    assert splits[0]["paths"][-1][-1] == splits[1]["paths"][0][0]
    assert interval_features(splits[0])["features"][0]["geometry"]["coordinates"][0] == [-74, 40]


def test_stop_pause_and_partial_sensor_coverage():
    raw = points(501)
    # Ten stationary seconds midway through a single full kilometre.
    for i in range(200, len(raw)):
        raw[i]["lat_deg"] -= min(i - 199, 10) * 3 / 111195
        raw[i]["heart_rate_bpm"] = None if i < 300 else 150
    raw[250]["paused"] = True
    splits = distance_splits(process_track(raw))
    assert splits[0]["duration_s"] > 1000 / 3 + 9
    assert splits[0]["pace"] == pytest.approx(splits[0]["duration_s"])
    assert 0 < splits[0]["hr_coverage"] < 1
    assert splits[0]["cadence"] is None
    assert splits[0]["average_hr"] > 140


@pytest.mark.parametrize("failure", ["time", "gap", "jump", "coordinate"])
def test_quality_breaks_cannot_win_fastest_or_bridge_selection(failure):
    raw = points(901)
    if failure == "time":
        raw[200]["timestamp_utc"] = None
    elif failure == "gap":
        for point in raw[200:]:
            point["timestamp_utc"] = point["timestamp_utc"].replace("00:", "01:", 1)
    elif failure == "jump":
        raw[200]["lat_deg"] = 80
    else:
        raw[200]["lat_deg"] = None
    splits = distance_splits(process_track(raw))
    assert splits[0]["duration_s"] is None and splits[0]["pace"] is None
    assert not splits[0]["fastest"]
    assert splits[1]["fastest"]
    if failure != "time":
        assert len(splits[0]["paths"]) == 2
    assert all(abs(b[0] - a[0]) < .001 for path in splits[0]["paths"] for a, b in zip(path, path[1:]))


def test_empty_missing_time_and_exact_final_boundary():
    assert not distance_splits(process_track([]))
    raw = points(401)
    for point in raw:
        point["timestamp_utc"] = None
    splits = distance_splits(process_track(raw))
    assert all(s["pace"] is None for s in splits)
    assert not any(s["fastest"] for s in splits)
    track = process_track(points(1001))
    # Force an exact boundary without depending on haversine roundoff.
    scale = 1000 / track["distance_m"]
    for segment in track["segments"]:
        segment["distance_m"] *= scale
        segment["metres"] *= scale
    track["distance_m"] = 1000
    assert len(distance_splits(track)) == 1
    assert not distance_splits(track)[0]["partial"]


def test_lap_trigger_timing_source_totals_and_unknown_provenance():
    track = process_track(points(701))
    rows = [dict(split_number=1, start_time_gmt="2026-01-01T00:00:00Z", duration_seconds=250,
                 distance_meters=750, heart_rate_bpm=145, max_hr=170, cadence_spm=176,
                 elevation_gain=20, elevation_loss=5, raw_json=json.dumps({"lapTrigger": "manual", "elapsedDuration": 300})),
            dict(split_number=2, start_time_gmt="2026-01-01T00:05:00Z", duration_seconds=100,
                 distance_meters=300, raw_json='{}'),
            dict(split_number=3, start_time_gmt="2026-01-01T00:10:00Z", duration_seconds=100,
                 distance_meters=300, raw_json='{}')]
    laps = stored_laps(track, rows)
    assert laps[0]["kind"] == "Manual lap"
    assert laps[0]["duration_s"] == 250  # Source timer time, not the elapsed boundary.
    assert laps[0]["paths"][-1][-1] == laps[1]["paths"][0][0]
    assert laps[0]["elevation_change_m"] == 15
    assert laps[1]["kind"] == "Stored lap" and laps[1]["paths"]
    assert not laps[2]["paths"]  # Timer duration alone cannot locate a lap end.
    assert "trigger unknown" in laps[1]["note"]
    assert laps[0]["pace"] == pytest.approx(250 / 750 * 1000)
    assert not any(lap["fastest"] for lap in laps)


@pytest.mark.parametrize("raw", ["invalid", "[]", "null", None])
def test_unaligned_or_malformed_laps_keep_source_details(raw):
    laps = stored_laps(process_track(points(10)), [dict(split_number=1, raw_json=raw, distance_meters=30, duration_seconds=10)])
    assert laps[0]["distance_m"] == 30 and laps[0]["duration_s"] == 10
    assert not laps[0]["paths"] and laps[0]["kind"] == "Stored lap"


def test_lap_query_optional_columns_missing_table_and_activity_isolation():
    conn = sqlite3.connect(":memory:")
    assert get_activity_laps(conn, 202) == []
    conn.execute("CREATE TABLE activity_splits(activity_id, split_number, average_hr, avg_cadence)")
    conn.executemany("INSERT INTO activity_splits VALUES(?,?,?,?)", [(202, 2, 145, 175), (101, 1, 120, 170), (202, 1, None, None)])
    assert [lap["split_number"] for lap in get_activity_laps(conn, 202)] == [1, 2]
    assert get_activity_laps(conn, 202)[1]["heart_rate_bpm"] == 145
    conn.execute("ALTER TABLE activity_splits ADD COLUMN raw_json TEXT")
    conn.execute("UPDATE activity_splits SET raw_json = ? WHERE activity_id=202 AND split_number=1", ('{"lapTrigger":"manual"}',))
    assert get_activity_laps(conn, 202)[0]["raw_json"] == '{"lapTrigger":"manual"}'
    conn.close()


def test_old_schema_raw_start_timing_and_lap_quality_break():
    raw = points(401)
    rows = [dict(split_number=1, raw_json=json.dumps({"startTimeGMT": raw[0]["timestamp_utc"]})),
            dict(split_number=2, raw_json=json.dumps({"startTimeGMT": raw[300]["timestamp_utc"]}))]
    assert stored_laps(process_track(raw), rows)[0]["trustworthy"]
    raw[100]["lat_deg"] = None
    lap = stored_laps(process_track(raw), rows)[0]
    assert not lap["trustworthy"] and len(lap["paths"]) == 2
