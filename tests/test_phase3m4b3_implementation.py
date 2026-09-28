"""Surgical regressions for the 3M.4B.3 implementation."""

from __future__ import annotations

import sqlite3
import subprocess
import zipfile
from pathlib import Path

import pytest

from garmin_data_hub import cli_archive_reconcile, cli_backup_ingest
from garmin_data_hub import paths as hub_paths
from garmin_data_hub.analytics import post_sync_refresh
from garmin_data_hub.db import queries
from garmin_data_hub.db import migrate as db_migrate
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.ingest import archive_parser, reconciliation, trackpoints
from garmin_data_hub.paths import schema_sql_path


ACTIVITY_ID = 9_300_001
START_UTC = "2036-02-03T04:05:06Z"


def _database(path: Path, *, with_points: bool = False) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE activity (
              activity_id INTEGER PRIMARY KEY,
              start_time_gmt TEXT,
              elapsed_duration_seconds REAL,
              activity_type TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO activity VALUES (?, ?, 600.0, 'running')",
            (ACTIVITY_ID, START_UTC),
        )
        apply_schema(conn, schema_sql_path())
        if with_points:
            conn.execute(
                "INSERT INTO activity_trackpoints(activity_id, seq, timestamp_utc) "
                "VALUES (?, 99, 'old-row')",
                (ACTIVITY_ID,),
            )


def _archive(directory: Path, payload: str, *, suffix: str = "one") -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"2036-02-03_{ACTIVITY_ID}_{suffix}.zip"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr(f"{ACTIVITY_ID}_UNKNOWN.gpx", payload)
    return path


def _gpx(*, with_points: bool = True) -> str:
    points = (
        f"""
        <trkpt lat="39.1" lon="-76.2"><ele>100</ele><time>{START_UTC}</time></trkpt>
        <trkpt lat="39.2" lon="-76.3"><ele>101</ele><time>2036-02-03T04:05:07Z</time></trkpt>
        """
        if with_points
        else ""
    )
    return f"""
    <gpx xmlns="http://www.topografix.com/GPX/1/1" version="1.1" creator="test">
      <trk><trkseg>{points}</trkseg></trk>
    </gpx>
    """


def test_strict_parser_exception_is_fatal_preserves_rows_and_writes_no_ledger(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path, with_points=True)
    archive = _archive(tmp_path / "fit", _gpx())
    monkeypatch.setattr(
        trackpoints,
        "parse_activity_archive",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("parser boundary")),
    )

    with sqlite3.connect(db_path) as conn:
        summary = trackpoints.ingest_trackpoints_from_archives(
            conn,
            archive.parent,
            replace_existing=True,
            archive_paths=[archive],
        )
        points = conn.execute(
            "SELECT seq, timestamp_utc FROM activity_trackpoints"
        ).fetchall()
        ledger_count = conn.execute(
            "SELECT COUNT(*) FROM archive_reconciliation"
        ).fetchone()[0]

    assert summary["errors"] == 1
    assert summary["result_statuses"][ACTIVITY_ID] == "parser_error"
    assert points == [(99, "old-row")]
    assert ledger_count == 0


def test_historical_parser_error_is_retryable_without_duplicate_ledger_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path)
    archive = _archive(tmp_path / "fit", _gpx())
    monkeypatch.setattr(
        reconciliation,
        "parse_activity_archive",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("transient parser")),
    )

    with sqlite3.connect(db_path) as conn:
        first = reconciliation.reconcile_historical_archives(
            conn, archive.parent, archive_paths=[archive]
        )
        second = reconciliation.reconcile_historical_archives(
            conn, archive.parent, archive_paths=[archive]
        )
        rows = conn.execute(
            "SELECT status FROM archive_reconciliation"
        ).fetchall()

    assert first["errors"] == second["errors"] == 1
    assert first["attempted_archives"] == second["attempted_archives"] == 1
    assert rows == [("parser_error",)]


def test_ledger_success_failure_rolls_back_inserted_trackpoints(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path)
    archive = _archive(tmp_path / "fit", _gpx())
    real_insert = reconciliation._insert_ledger

    def fail_success_ledger(conn, **kwargs):
        if kwargs["status"] == "ingested":
            raise sqlite3.OperationalError("ledger unavailable")
        return real_insert(conn, **kwargs)

    monkeypatch.setattr(reconciliation, "_insert_ledger", fail_success_ledger)

    with sqlite3.connect(db_path) as conn:
        summary = reconciliation.reconcile_historical_archives(
            conn, archive.parent, archive_paths=[archive]
        )
        points = conn.execute(
            "SELECT COUNT(*) FROM activity_trackpoints"
        ).fetchone()[0]
        ledger = conn.execute(
            "SELECT status FROM archive_reconciliation"
        ).fetchall()

    assert summary["errors"] == 1
    assert points == 0
    assert ledger == [("write_error",)]


def test_duplicate_historical_archives_are_durable_ambiguities_without_parsing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path)
    first = _archive(tmp_path / "fit", _gpx(), suffix="one")
    second = _archive(tmp_path / "fit", _gpx(), suffix="two")
    monkeypatch.setattr(
        reconciliation,
        "parse_activity_archive",
        lambda *_args: pytest.fail("ambiguous archives must not be parsed"),
    )

    with sqlite3.connect(db_path) as conn:
        summary = reconciliation.reconcile_historical_archives(
            conn, first.parent, archive_paths=[first, second]
        )
        statuses = conn.execute(
            "SELECT status FROM archive_reconciliation ORDER BY archive_identity"
        ).fetchall()

    assert summary["attempted_archives"] == 2
    assert summary["warnings"] == 2
    assert statuses == [("ambiguous",), ("ambiguous",)]


def test_unchanged_terminal_fingerprint_does_not_reparse(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path)
    tcx = f"""
    <TrainingCenterDatabase xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
      <Activities><Activity Sport="Running"><Id>{START_UTC}</Id>
        <Lap StartTime="{START_UTC}"><TotalTimeSeconds>600</TotalTimeSeconds></Lap>
      </Activity></Activities>
    </TrainingCenterDatabase>
    """
    fit_dir = tmp_path / "fit"
    fit_dir.mkdir()
    archive = fit_dir / f"2036-02-03_{ACTIVITY_ID}_zero.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr(f"{ACTIVITY_ID}_UNKNOWN.tcx", tcx)

    with sqlite3.connect(db_path) as conn:
        first = reconciliation.reconcile_historical_archives(
            conn, fit_dir, archive_paths=[archive]
        )
        monkeypatch.setattr(
            reconciliation,
            "parse_activity_archive",
            lambda *_args: pytest.fail("unchanged terminal source was reparsed"),
        )
        second = reconciliation.reconcile_historical_archives(
            conn, fit_dir, archive_paths=[archive]
        )

    assert first["resolved_no_records"] == 1
    assert second["attempted_archives"] == 0
    assert second["unchanged_terminal"] == 1


def test_schema_migration_never_invokes_archive_parser(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        reconciliation,
        "parse_activity_archive",
        lambda *_args: pytest.fail("migration invoked archive parser"),
    )
    conn = sqlite3.connect(tmp_path / "migration.db")
    try:
        conn.execute("CREATE TABLE activity(activity_id INTEGER PRIMARY KEY)")
        apply_schema(conn, schema_sql_path())
        assert conn.execute(
            "SELECT COUNT(*) FROM schema_migrations WHERE version=8"
        ).fetchone()[0] == 1
    finally:
        conn.close()


def test_default_sync_refresh_is_targeted_not_global(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "garmin.db"
    captured: dict[str, object] = {}
    monkeypatch.setattr(cli_backup_ingest, "ensure_app_dirs", lambda: None)
    monkeypatch.setattr(
        cli_backup_ingest, "_find_givemydata_cmd", lambda: ["synthetic-upstream"]
    )
    monkeypatch.setattr(
        cli_backup_ingest.subprocess,
        "run",
        lambda command, **_kwargs: subprocess.CompletedProcess(command, 0),
    )
    monkeypatch.setattr(db_migrate, "apply_schema", lambda _conn, _schema: None)
    monkeypatch.setattr(hub_paths, "schema_sql_path", lambda: Path("schema.sql"))
    monkeypatch.setattr(
        cli_backup_ingest,
        "ingest_trackpoints_from_archives",
        lambda *_args, **_kwargs: {
            "errors": 0,
            "updated_activity_ids": [ACTIVITY_ID],
            "target_activity_ids": [ACTIVITY_ID],
            "ingested_activities": 1,
        },
    )
    monkeypatch.setattr(
        cli_backup_ingest,
        "reconcile_historical_archives",
        lambda *_args, **_kwargs: {
            "errors": 0,
            "warnings": 0,
            "updated_activity_ids": [],
            "ingested_activities": 0,
        },
    )

    def refresh(_conn, **kwargs):
        captured.update(kwargs)
        return {"errors": 0}

    monkeypatch.setattr(post_sync_refresh, "refresh_post_sync_tables", refresh)

    assert cli_backup_ingest.run_sync(db_path) == 0
    assert captured["activity_ids"] == [ACTIVITY_ID]
    assert captured["start_ts_iso"] is None


def test_normal_sync_requires_explicit_reconciliation_baseline(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path)
    archive = _archive(tmp_path / "fit", _gpx())
    monkeypatch.setattr(
        reconciliation,
        "parse_activity_archive",
        lambda *_args: pytest.fail("normal sync launched the initial backlog sweep"),
    )

    with sqlite3.connect(db_path) as conn:
        summary = cli_backup_ingest._run_historical_archive_reconciliation(
            conn, archive.parent, [archive]
        )

    assert summary["baseline_required"] is True
    assert summary["warnings"] == 1
    assert summary["attempted_archives"] == 0


def test_current_baseline_allows_incremental_historical_reconciliation(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path)
    archive = _archive(tmp_path / "fit", _gpx())

    with sqlite3.connect(db_path) as conn:
        queries.set_setting(
            conn,
            reconciliation.BASELINE_SETTING_KEY,
            {"algorithm_version": archive_parser.PARSER_ALGORITHM_VERSION},
        )
        summary = cli_backup_ingest._run_historical_archive_reconciliation(
            conn, archive.parent, [archive]
        )

    assert summary["ingested_activities"] == 1
    assert summary["updated_activity_ids"] == [ACTIVITY_ID]


def test_missing_ingested_rows_are_an_integrity_error_not_silent_replay(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path)
    archive = _archive(tmp_path / "fit", _gpx())

    with sqlite3.connect(db_path) as conn:
        first = reconciliation.reconcile_historical_archives(
            conn, archive.parent, archive_paths=[archive]
        )
        conn.execute(
            "DELETE FROM activity_trackpoints WHERE activity_id=?", (ACTIVITY_ID,)
        )
        conn.commit()
        second = reconciliation.reconcile_historical_archives(
            conn, archive.parent, archive_paths=[archive]
        )
        remaining = conn.execute(
            "SELECT COUNT(*) FROM activity_trackpoints WHERE activity_id=?",
            (ACTIVITY_ID,),
        ).fetchone()[0]

    assert first["ingested_activities"] == 1
    assert second["errors"] == 1
    assert second["status_counts"] == {"integrity_error": 1}
    assert second["updated_activity_ids"] == []
    assert remaining == 0


def test_maintenance_dry_run_does_not_install_schema_or_write(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE activity (
              activity_id INTEGER PRIMARY KEY,
              start_time_gmt TEXT,
              elapsed_duration_seconds REAL,
              activity_type TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE activity_trackpoints (
              activity_id INTEGER NOT NULL,
              seq INTEGER NOT NULL,
              timestamp_utc TEXT,
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
            )
            """
        )
        conn.execute(
            "INSERT INTO activity VALUES (?, ?, 600.0, 'running')",
            (ACTIVITY_ID, START_UTC),
        )
        conn.commit()
    archive = _archive(tmp_path / "fit", _gpx())

    summary = cli_archive_reconcile.run_reconciliation(
        db_path, archive.parent, apply=False
    )

    with sqlite3.connect(db_path) as conn:
        objects = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        point_count = conn.execute(
            "SELECT COUNT(*) FROM activity_trackpoints"
        ).fetchone()[0]
    assert summary["ingested_activities"] == 1
    assert "schema_migrations" not in objects
    assert "archive_reconciliation" not in objects
    assert point_count == 0


def test_recordless_gpx_segment_is_ambiguous(tmp_path: Path) -> None:
    archive = _archive(tmp_path, _gpx(with_points=False))
    result = archive_parser.parse_activity_archive(
        archive,
        archive_parser.ActivityIdentity(
            activity_id=ACTIVITY_ID,
            start_time_utc=START_UTC,
            duration_s=600.0,
            activity_type="running",
        ),
    )

    assert result.status is archive_parser.ArchiveParseStatus.AMBIGUOUS
    assert result.reason_code == "unresolved_empty_gpx_segment"


def test_unexpected_fit_parser_exception_crosses_canonical_boundary(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / f"2036-02-03_{ACTIVITY_ID}_fit.zip"
    with zipfile.ZipFile(path, "w") as bundle:
        bundle.writestr(f"{ACTIVITY_ID}_ACTIVITY.fit", b"synthetic-fit")
    monkeypatch.setattr(
        archive_parser,
        "_track_rows_from_fit_bytes",
        lambda _payload: (_ for _ in ()).throw(RuntimeError("decoder defect")),
    )

    with pytest.raises(RuntimeError, match="decoder defect"):
        archive_parser.parse_activity_archive(
            path,
            archive_parser.ActivityIdentity(
                activity_id=ACTIVITY_ID,
                start_time_utc=START_UTC,
                duration_s=600.0,
                activity_type="running",
            ),
        )
