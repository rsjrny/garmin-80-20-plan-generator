"""Exercise upstream FIT decoding through archive routing and persisted rows."""

from datetime import datetime
from types import SimpleNamespace
from zipfile import ZipFile

import pytest
from garmin_mcp import parse_activity_files as upstream
from garmin_data_hub.ingest import trackpoints


@pytest.mark.parametrize("filename", ["2026-09-01_1234567_Run.zip", "download.zip"],
                         ids=["archive-id", "member-id"])
def test_upstream_fit_records_survive_archive_ingestion(db_conn, tmp_path, monkeypatch, filename):
    archive = tmp_path / filename
    with ZipFile(archive, "w") as zipped:
        zipped.writestr("1234567_ACTIVITY.fit", b"isolated FIT decoder input")
    db_conn.execute(
        "INSERT INTO activity(activity_id,start_time_gmt,activity_type) VALUES(1234567,?,?)",
        ("2026-09-01T10:00:00", "running"),
    )
    db_conn.commit()

    class FitDecoder:
        def __init__(self, stream):
            assert stream.read() == b"isolated FIT decoder input"

        def get_messages(self, message_type):
            assert message_type == "record"
            values = {
                "timestamp": datetime(2026, 9, 1, 10),
                "position_lat": 2**29, "position_long": -(2**30),
                "altitude": 12, "enhanced_altitude": 15,
                "distance": 100, "speed": 2, "enhanced_speed": 3,
                "heart_rate": 145, "cadence": 175, "power": 210, "temperature": 20,
            }
            return [[SimpleNamespace(name=name, value=value) for name, value in values.items()]]

    # Replace only binary FIT decoding; upstream conversion, ID routing, ZIP
    # reading, targeting and SQLite insertion all execute normally.
    monkeypatch.setattr(upstream, "FitFile", FitDecoder)
    result = trackpoints.ingest_trackpoints_from_fit_archives(db_conn, tmp_path)
    assert result["updated_activity_ids"] == [1234567]
    assert result["ingested_points"] == 1
    assert result["errors"] == 0
    row = db_conn.execute(
        """SELECT activity_id,seq,timestamp_utc,latitude,longitude,altitude_m,
                  distance_m,speed_mps,heart_rate_bpm,cadence,power_w,temperature_c
           FROM activity_trackpoints"""
    ).fetchone()
    assert tuple(row) == (1234567, 0, "2026-09-01T10:00:00", 45, -90, 15,
                          100, 3, 145, 175, 210, 20)
