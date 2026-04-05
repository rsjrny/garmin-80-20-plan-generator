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
            average_speed REAL,
            max_hr INTEGER,
            training_stress_score REAL,
            average_hr REAL,
            activity_type TEXT
        )
        """
    )
    apply_schema(conn, schema_sql_path())
    try:
        yield conn
    finally:
        conn.close()
