from __future__ import annotations
from pathlib import Path
import sqlite3


def _fix_trackpoint_cascade(conn: sqlite3.Connection) -> None:
    """Remove ON DELETE CASCADE from activity_trackpoint if present.

    garmin-givemydata uses INSERT OR REPLACE on the activity table, which
    internally does DELETE + INSERT and fires the cascade, wiping all
    trackpoints on every sync.  This migration recreates the table without
    the cascade so existing trackpoint data is preserved.
    """
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='activity_trackpoint'"
    ).fetchone()
    if row is None:
        return  # table doesn't exist yet; schema.sql will create it correctly
    ddl: str = row[0] or ""
    if "ON DELETE CASCADE" not in ddl.upper():
        return  # already fixed

    conn.executescript(
        """
        PRAGMA foreign_keys=OFF;

        CREATE TABLE activity_trackpoint_new (
          activity_id    INTEGER NOT NULL,
          seq            INTEGER NOT NULL,
          timestamp_utc  TEXT NOT NULL,
          latitude       REAL,
          longitude      REAL,
          altitude_m     REAL,
          distance_m     REAL,
          speed_mps      REAL,
          heart_rate_bpm INTEGER,
          cadence        INTEGER,
          power_w        INTEGER,
          temperature_c  REAL,
          PRIMARY KEY (activity_id, seq),
          FOREIGN KEY (activity_id) REFERENCES activity(activity_id)
        );

        INSERT INTO activity_trackpoint_new
          SELECT activity_id, seq, timestamp_utc, latitude, longitude,
                 altitude_m, distance_m, speed_mps, heart_rate_bpm,
                 cadence, power_w, temperature_c
          FROM activity_trackpoint;

        DROP TABLE activity_trackpoint;
        ALTER TABLE activity_trackpoint_new RENAME TO activity_trackpoint;

        CREATE INDEX IF NOT EXISTS idx_activity_trackpoint_activity_time
          ON activity_trackpoint(activity_id, timestamp_utc);

        CREATE INDEX IF NOT EXISTS idx_activity_trackpoint_latlon
          ON activity_trackpoint(latitude, longitude);

        PRAGMA foreign_keys=ON;
    """
    )
    conn.commit()


def apply_schema(conn: sqlite3.Connection, schema_path: Path) -> None:
    _fix_trackpoint_cascade(conn)
    conn.executescript(schema_path.read_text(encoding="utf-8"))
    conn.commit()
