"""Focused regressions for the Phase 3A.4 acceptance corrections."""

from __future__ import annotations

import sqlite3
import subprocess
from pathlib import Path

import pytest

from garmin_data_hub import cli_backup_ingest


def _create_trackpoint_database(db_path: Path, activity_id: int | None = None) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE activity (
                activity_id INTEGER PRIMARY KEY,
                start_time_gmt TEXT
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
        if activity_id is not None:
            conn.execute(
                "INSERT INTO activity(activity_id, start_time_gmt) VALUES (?, ?)",
                (activity_id, "2026-09-27T08:00:00Z"),
            )


def _patch_before_ingestion_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_backup_ingest, "ensure_app_dirs", lambda: None)
    monkeypatch.setattr(
        cli_backup_ingest,
        "_find_givemydata_cmd",
        lambda: ["upstream"],
    )
    monkeypatch.setattr(
        "garmin_data_hub.db.migrate.apply_schema",
        lambda _conn, _schema: None,
    )
    monkeypatch.setattr(
        "garmin_data_hub.paths.schema_sql_path",
        lambda: Path("schema.sql"),
    )
    monkeypatch.setattr(
        cli_backup_ingest,
        "ingest_trackpoints_from_fit_archives",
        lambda *_args, **_kwargs: pytest.fail(
            "archive integrity validation must precede ingestion"
        ),
    )
    monkeypatch.setattr(
        "garmin_data_hub.analytics.post_sync_refresh.refresh_post_sync_tables",
        lambda *_args, **_kwargs: pytest.fail(
            "archive integrity failure must precede derived refresh"
        ),
    )


def test_preexisting_archive_disappearing_before_post_enumeration_fails_sync(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys,
) -> None:
    db_path = tmp_path / "garmin.db"
    fit_dir = tmp_path / "fit"
    archive = fit_dir / "2026-09-27_43000001_existing.zip"
    _create_trackpoint_database(db_path)
    fit_dir.mkdir()
    archive.write_bytes(b"known before upstream")
    _patch_before_ingestion_failure(monkeypatch)

    def fake_upstream(command, **_kwargs):
        archive.unlink()
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fake_upstream)

    result = cli_backup_ingest.run_sync(db_path)
    output = capsys.readouterr().out

    assert result != 0
    assert "disappeared during synchronization" in output
    assert str(archive) in output
    assert "[SUCCESS] Sync complete" not in output


def test_snapshot_directory_enumeration_error_is_observable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fit_dir = tmp_path / "fit"
    fit_dir.mkdir()
    real_iterdir = Path.iterdir

    def fail_fit_enumeration(path: Path):
        if path == fit_dir:
            raise PermissionError("injected enumeration failure")
        return real_iterdir(path)

    monkeypatch.setattr(Path, "iterdir", fail_fit_enumeration)

    with pytest.raises(PermissionError, match="injected enumeration failure"):
        cli_backup_ingest._snapshot_fit_archives(fit_dir)


def test_changed_and_unchanged_duplicate_ids_fail_before_pass_partitioning(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys,
) -> None:
    activity_id = 43_000_002
    _patch_before_ingestion_failure(monkeypatch)
    pending_new_archive: list[Path] = []

    def fake_upstream(command, **_kwargs):
        pending_new_archive.pop(0).write_bytes(b"created by upstream")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fake_upstream)

    results: list[int] = []
    for case_name, unchanged_name, changed_name in (
        (
            "unchanged-first",
            f"2026-09-26_{activity_id}_alpha.zip",
            f"2026-09-27_{activity_id}_zeta.zip",
        ),
        (
            "changed-first",
            f"2026-09-28_{activity_id}_zeta.zip",
            f"2026-09-27_{activity_id}_alpha.zip",
        ),
    ):
        case_dir = tmp_path / case_name
        db_path = case_dir / "garmin.db"
        fit_dir = case_dir / "fit"
        unchanged_archive = fit_dir / unchanged_name
        changed_archive = fit_dir / changed_name
        case_dir.mkdir()
        _create_trackpoint_database(db_path, activity_id)
        fit_dir.mkdir()
        unchanged_archive.write_bytes(b"unchanged pre-sync archive")
        pending_new_archive.append(changed_archive)

        results.append(cli_backup_ingest.run_sync(db_path))

    output = capsys.readouterr().out

    assert results == [2, 2]
    assert output.count("ambiguous FIT archives detected") == 2
    assert "[SUCCESS] Sync complete" not in output
