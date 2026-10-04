from __future__ import annotations

import pytest

from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path


@pytest.fixture()
def db_conn(tmp_path):
    db_path = tmp_path / "garmin.db"
    conn = connect_sqlite(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS activity (
            activity_id INTEGER PRIMARY KEY,
            start_time_gmt TEXT,
            elapsed_duration_seconds REAL,
            moving_duration_seconds REAL,
            average_speed REAL,
            max_hr INTEGER,
            training_stress_score REAL,
            average_hr REAL,
            activity_type TEXT,
            avg_power REAL,
            max_power REAL,
            norm_power REAL,
            intensity_factor REAL,
            avg_cadence REAL,
            elevation_gain REAL,
            elevation_loss REAL,
            min_elevation REAL,
            max_elevation REAL,
            aerobic_training_effect REAL,
            anaerobic_training_effect REAL,
            min_temperature REAL,
            max_temperature REAL
        )
        """
    )
    apply_schema(conn, schema_sql_path())
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture()
def ui_database(db_conn):
    """An isolated app database for page-action tests."""
    from pathlib import Path

    db_conn.commit()
    return Path(db_conn.execute("PRAGMA database_list").fetchone()[2])


@pytest.fixture()
def browser_artifacts(tmp_path, request):
    """Keep screenshots out of tracked reports; CI can retain them explicitly."""
    import hashlib
    import os
    from pathlib import Path

    configured = os.environ.get("GARMIN_TEST_ARTIFACTS")
    if configured:
        suffix = hashlib.sha256(request.node.nodeid.encode()).hexdigest()[:10]
        directory = Path(configured) / (request.node.name + "-" + suffix)
    else:
        directory = tmp_path / "browser-artifacts"
    directory.mkdir(parents=True, exist_ok=True)
    return directory
