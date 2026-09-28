"""Green characterizations retained by the Phase 3M.4B.2 design.

These tests cover FIT behavior and changed/new safety that already exist.  The
new multi-format and historical-reconciliation requirements are deliberately
kept in ``test_phase3m4b2_contracts.py`` so they remain visibly red until the
production implementation lands.
"""

from __future__ import annotations

import sqlite3
import subprocess
from pathlib import Path

import pytest

from garmin_data_hub import cli_backup_ingest
from garmin_data_hub import paths as hub_paths
from garmin_data_hub.analytics import post_sync_refresh
from garmin_data_hub.db import migrate as db_migrate
from garmin_data_hub.ingest import trackpoints


def _row(seq: int, timestamp: str) -> tuple:
    return (
        seq,
        timestamp,
        38.9,
        -77.0,
        123.0,
        float(seq),
        3.0,
        140,
        82,
        210,
        18.0,
    )


def _database(path: Path, activity_id: int, *, with_old_row: bool = False) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE activity (
                activity_id INTEGER PRIMARY KEY,
                start_time_gmt TEXT,
                activity_type TEXT
            );
            CREATE TABLE activity_trackpoints (
                activity_id INTEGER NOT NULL,
                seq INTEGER NOT NULL,
                timestamp_utc TEXT NOT NULL,
                latitude REAL,
                longitude REAL,
                altitude_m REAL,
                distance_m REAL,
                speed_mps REAL,
                heart_rate_bpm INTEGER,
                cadence INTEGER,
                power_w INTEGER,
                temperature_c REAL,
                PRIMARY KEY (activity_id, seq)
            );
            """
        )
        conn.execute(
            "INSERT INTO activity(activity_id, start_time_gmt, activity_type) "
            "VALUES (?, '2035-01-02T03:04:05Z', 'running')",
            (activity_id,),
        )
        if with_old_row:
            conn.execute(
                "INSERT INTO activity_trackpoints(activity_id, seq, timestamp_utc) "
                "VALUES (?, 99, 'old-complete-row')",
                (activity_id,),
            )


def _archive(directory: Path, activity_id: int, payload: bytes = b"fixture") -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"2035-01-02_{activity_id}_synthetic.zip"
    path.write_bytes(payload)
    return path


def test_01_fit_success_contract_is_preserved(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    activity_id = 9_100_001
    db_path = tmp_path / "garmin.db"
    archive = _archive(tmp_path / "fit", activity_id)
    _database(db_path, activity_id)
    monkeypatch.setattr(
        trackpoints,
        "parse_trackpoints_from_fit_archive",
        lambda _path: (activity_id, [_row(0, "2035-01-02T03:04:05")]),
    )

    with sqlite3.connect(db_path) as conn:
        summary = trackpoints.ingest_trackpoints_from_fit_archives(
            conn,
            archive.parent,
            replace_existing=True,
            archive_paths=[archive],
        )
        stored = conn.execute(
            "SELECT seq, timestamp_utc FROM activity_trackpoints "
            "WHERE activity_id=?",
            (activity_id,),
        ).fetchall()

    assert summary["errors"] == 0
    assert summary["updated_activity_ids"] == [activity_id]
    assert stored == [(0, "2035-01-02T03:04:05")]


def test_02_fit_recognized_zero_records_remains_a_safe_skip(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    activity_id = 9_100_002
    db_path = tmp_path / "garmin.db"
    archive = _archive(tmp_path / "fit", activity_id)
    _database(db_path, activity_id, with_old_row=True)
    monkeypatch.setattr(
        trackpoints,
        "parse_trackpoints_from_fit_archive",
        lambda _path: (activity_id, []),
    )

    with sqlite3.connect(db_path) as conn:
        summary = trackpoints.ingest_trackpoints_from_fit_archives(
            conn,
            archive.parent,
            replace_existing=True,
            archive_paths=[archive],
        )
        stored = conn.execute(
            "SELECT seq, timestamp_utc FROM activity_trackpoints "
            "WHERE activity_id=?",
            (activity_id,),
        ).fetchall()

    assert summary["errors"] == 0
    assert summary["skipped_no_records"] == 1
    assert summary["updated_activity_ids"] == []
    assert stored == [(99, "old-complete-row")]


def test_17_changed_new_parser_exception_is_fatal_and_preserves_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    activity_id = 9_100_017
    db_path = tmp_path / "garmin.db"
    archive = _archive(tmp_path / "fit", activity_id)
    _database(db_path, activity_id, with_old_row=True)

    def fail(_path: Path):
        raise RuntimeError("synthetic parser failure")

    monkeypatch.setattr(trackpoints, "parse_trackpoints_from_fit_archive", fail)

    with sqlite3.connect(db_path) as conn:
        summary = trackpoints.ingest_trackpoints_from_fit_archives(
            conn,
            archive.parent,
            replace_existing=True,
            archive_paths=[archive],
        )
        stored = conn.execute(
            "SELECT seq, timestamp_utc FROM activity_trackpoints "
            "WHERE activity_id=?",
            (activity_id,),
        ).fetchall()

    assert summary["errors"] == 1
    assert summary["updated_activity_ids"] == []
    assert stored == [(99, "old-complete-row")]


def test_19_failed_replacement_rolls_back_partial_rows_atomically(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    activity_id = 9_100_019
    db_path = tmp_path / "garmin.db"
    archive = _archive(tmp_path / "fit", activity_id)
    _database(db_path, activity_id, with_old_row=True)
    monkeypatch.setattr(
        trackpoints,
        "parse_trackpoints_from_fit_archive",
        lambda _path: (
            activity_id,
            [
                _row(0, "2035-01-02T03:04:05"),
                _row(0, "2035-01-02T03:04:06"),
            ],
        ),
    )

    with sqlite3.connect(db_path) as conn:
        summary = trackpoints.ingest_trackpoints_from_fit_archives(
            conn,
            archive.parent,
            replace_existing=True,
            archive_paths=[archive],
        )
        stored = conn.execute(
            "SELECT seq, timestamp_utc FROM activity_trackpoints "
            "WHERE activity_id=?",
            (activity_id,),
        ).fetchall()

    assert summary["errors"] == 1
    assert stored == [(99, "old-complete-row")]


def test_33_changed_new_activity_still_enters_metric_refresh_targets(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    activity_id = 9_100_033
    db_path = tmp_path / "garmin.db"
    fit_dir = tmp_path / "fit"
    archive = _archive(fit_dir, activity_id, b"old")
    _database(db_path, activity_id, with_old_row=True)
    captured: dict[str, object] = {}

    monkeypatch.setattr(cli_backup_ingest, "ensure_app_dirs", lambda: None)
    monkeypatch.setattr(
        cli_backup_ingest, "_find_givemydata_cmd", lambda: ["synthetic-upstream"]
    )
    monkeypatch.setattr(db_migrate, "apply_schema", lambda _conn, _schema: None)
    monkeypatch.setattr(hub_paths, "schema_sql_path", lambda: Path("schema.sql"))
    monkeypatch.setattr(
        trackpoints,
        "parse_trackpoints_from_fit_archive",
        lambda _path: (activity_id, [_row(0, "2035-01-02T03:04:05")]),
    )

    def upstream(command, **_kwargs):
        archive.write_bytes(b"new-and-longer")
        return subprocess.CompletedProcess(command, 0)

    def refresh(_conn, **kwargs):
        captured.update(kwargs)
        return {"errors": 0}

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", upstream)
    monkeypatch.setattr(post_sync_refresh, "refresh_post_sync_tables", refresh)

    assert cli_backup_ingest.run_sync(db_path, days=7) == 0
    assert captured["activity_ids"] == [activity_id]
