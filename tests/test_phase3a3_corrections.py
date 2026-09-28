"""Focused regressions for the Phase 3A.3 acceptance corrections."""

from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

from garmin_data_hub import cli_backup_ingest
from garmin_data_hub.analytics import post_sync_refresh
from garmin_data_hub.ingest import trackpoints


def _trackpoint_row(seq: int, timestamp: str) -> tuple:
    return (seq, timestamp, None, None, None, None, None, None, None, None, None)


def _trackpoint_database(
    db_path: Path,
    activity_ids: list[int],
    *,
    populated_ids: set[int] | None = None,
) -> None:
    populated_ids = populated_ids or set()
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
        conn.executemany(
            "INSERT INTO activity(activity_id, start_time_gmt) VALUES (?, ?)",
            [(activity_id, "2026-09-27T08:00:00Z") for activity_id in activity_ids],
        )
        conn.executemany(
            "INSERT INTO activity_trackpoints(activity_id, seq, timestamp_utc) "
            "VALUES (?, 99, 'old-complete-row')",
            [(activity_id,) for activity_id in sorted(populated_ids)],
        )


def _patch_post_sync_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_backup_ingest, "ensure_app_dirs", lambda: None)
    monkeypatch.setattr(
        cli_backup_ingest, "_find_givemydata_cmd", lambda: ["upstream"]
    )
    monkeypatch.setattr(
        "garmin_data_hub.db.migrate.apply_schema", lambda _conn, _schema: None
    )
    monkeypatch.setattr(
        "garmin_data_hub.paths.schema_sql_path", lambda: Path("schema.sql")
    )
    monkeypatch.setattr(
        "garmin_data_hub.analytics.post_sync_refresh.refresh_post_sync_tables",
        lambda _conn, **_kwargs: {"errors": 0},
    )


@pytest.mark.parametrize(
    "extra_args",
    [
        ["--rebuild-trackpoints"],
        ["--rebuild-t"],
        ["--json-import", "payload.json"],
        ["--json-import=payload.json"],
        [cli_backup_ingest._BUNDLED_GIVEMYDATA_FLAG],
        ["--status"],
        ["--help"],
        ["--unknown-upstream-utility"],
    ],
)
def test_unsafe_or_unknown_upstream_arguments_are_rejected_before_source_spawn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    extra_args: list[str],
) -> None:
    monkeypatch.setattr(
        cli_backup_ingest,
        "ensure_app_dirs",
        lambda: pytest.fail("argument rejection must precede application setup"),
    )
    monkeypatch.setattr(
        cli_backup_ingest,
        "_find_givemydata_cmd",
        lambda: pytest.fail("unsafe arguments reached upstream command discovery"),
    )

    assert (
        cli_backup_ingest.run_sync(
            tmp_path / "garmin.db",
            extra_args=extra_args,
        )
        == 2
    )


@pytest.mark.parametrize(
    "arguments",
    [
        ["--rebuild-trackpoints"],
        ["--rebuild-t"],
        ["--json-import", "payload.json"],
        ["--status"],
    ],
)
def test_unsafe_modes_are_rejected_before_frozen_upstream_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    arguments: list[str],
) -> None:
    fake_upstream = ModuleType("garmin_givemydata")
    fake_upstream.main = lambda: pytest.fail("unsafe frozen dispatch reached upstream")
    monkeypatch.setitem(sys.modules, "garmin_givemydata", fake_upstream)

    assert cli_backup_ingest._run_bundled_givemydata(arguments) == 2


@pytest.mark.parametrize(
    "arguments",
    [
        ["--days", "14"],
        ["--since", "2026-09-01"],
        ["--profile", "health"],
        ["--full"],
    ],
)
def test_supported_frozen_arguments_keep_exactly_one_no_trackpoints(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    arguments: list[str],
) -> None:
    from seleniumbase.core import browser_launcher

    seen_argv: list[str] = []
    fake_upstream = ModuleType("garmin_givemydata")
    fake_upstream.main = lambda: seen_argv.extend(sys.argv)
    monkeypatch.setitem(sys.modules, "garmin_givemydata", fake_upstream)
    monkeypatch.setenv("GARMIN_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(browser_launcher, "override_driver_dir", lambda _path: None)

    assert cli_backup_ingest._run_bundled_givemydata(arguments) == 0
    assert seen_argv.count("--no-trackpoints") == 1
    assert all(argument in seen_argv for argument in arguments)


def test_real_inner_athlete_profile_failure_is_counted(
    db_conn,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_conn.execute("DROP TABLE activity")
    monkeypatch.setattr(
        post_sync_refresh.queries,
        "refresh_persisted_activity_metrics",
        lambda *_args, **_kwargs: {
            "target_activities": 0,
            "rows_upserted": 0,
            "zones_updated": 0,
            "errors": 0,
        },
    )

    summary = post_sync_refresh.refresh_post_sync_tables(db_conn)

    assert summary["athlete_profile_errors"] == 1
    assert summary["errors"] == 1


def test_no_recent_athlete_evidence_remains_nonfatal(db_conn) -> None:
    summary = post_sync_refresh.refresh_post_sync_tables(db_conn)

    assert summary["athlete_profile_errors"] == 0
    assert summary["errors"] == 0


def test_corrupt_explicit_replacement_is_error_and_preserves_old_rows(
    tmp_path: Path,
) -> None:
    activity_id = 31_000_001
    db_path = tmp_path / "garmin.db"
    archive = tmp_path / f"2026-09-27_{activity_id}_run.zip"
    _trackpoint_database(db_path, [activity_id], populated_ids={activity_id})
    archive.write_bytes(b"not a ZIP archive")

    with sqlite3.connect(db_path) as conn:
        summary = trackpoints.ingest_trackpoints_from_fit_archives(
            conn,
            tmp_path,
            replace_existing=True,
            archive_paths=[archive],
        )
        rows = conn.execute(
            "SELECT seq, timestamp_utc FROM activity_trackpoints "
            "WHERE activity_id = ?",
            (activity_id,),
        ).fetchall()

    assert summary["errors"] == 1
    assert summary["skipped_no_fit"] == 0
    assert rows == [(99, "old-complete-row")]


def test_parser_failure_sentinel_is_error_for_explicit_target(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    activity_id = 31_000_002
    db_path = tmp_path / "garmin.db"
    archive = tmp_path / f"2026-09-27_{activity_id}_run.zip"
    _trackpoint_database(db_path, [activity_id])
    archive.touch()
    monkeypatch.setattr(
        trackpoints,
        "parse_trackpoints_from_fit_archive",
        lambda _path: (None, []),
    )

    with sqlite3.connect(db_path) as conn:
        summary = trackpoints.ingest_trackpoints_from_fit_archives(
            conn,
            tmp_path,
            replace_existing=False,
            archive_paths=[archive],
        )

    assert summary["errors"] == 1
    assert summary["skipped_no_fit"] == 0


def test_valid_empty_explicit_archive_remains_a_no_records_skip(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    activity_id = 31_000_003
    db_path = tmp_path / "garmin.db"
    archive = tmp_path / f"2026-09-27_{activity_id}_run.zip"
    _trackpoint_database(db_path, [activity_id])
    archive.touch()
    monkeypatch.setattr(
        trackpoints,
        "parse_trackpoints_from_fit_archive",
        lambda _path: (activity_id, []),
    )

    with sqlite3.connect(db_path) as conn:
        summary = trackpoints.ingest_trackpoints_from_fit_archives(
            conn,
            tmp_path,
            replace_existing=False,
            archive_paths=[archive],
        )

    assert summary["errors"] == 0
    assert summary["skipped_no_records"] == 1


def test_corrupt_changed_archive_prevents_terminal_sync_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys,
) -> None:
    activity_id = 31_000_004
    db_path = tmp_path / "garmin.db"
    archive = tmp_path / "fit" / f"2026-09-27_{activity_id}_run.zip"
    _trackpoint_database(db_path, [activity_id], populated_ids={activity_id})
    archive.parent.mkdir()
    archive.write_bytes(b"old identity")
    _patch_post_sync_success(monkeypatch)

    def fake_upstream(command, **_kwargs):
        archive.write_bytes(b"new but corrupt ZIP identity")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fake_upstream)

    assert cli_backup_ingest.run_sync(db_path) != 0
    assert "[SUCCESS] Sync complete" not in capsys.readouterr().out
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT seq, timestamp_utc FROM activity_trackpoints "
            "WHERE activity_id = ?",
            (activity_id,),
        ).fetchall() == [(99, "old-complete-row")]


def test_disappearing_explicit_target_is_an_ingestion_error(tmp_path: Path) -> None:
    activity_id = 31_000_005
    db_path = tmp_path / "garmin.db"
    missing = tmp_path / f"2026-09-27_{activity_id}_run.zip"
    _trackpoint_database(db_path, [activity_id])

    with sqlite3.connect(db_path) as conn:
        summary = trackpoints.ingest_trackpoints_from_fit_archives(
            conn,
            tmp_path,
            replace_existing=True,
            archive_paths=[missing],
        )

    assert summary["errors"] == 1


def test_disappearing_changed_target_errors_without_a_matching_db_row(
    tmp_path: Path,
) -> None:
    activity_id = 31_000_009
    db_path = tmp_path / "garmin.db"
    missing = tmp_path / f"2026-09-27_{activity_id}_run.zip"
    _trackpoint_database(db_path, [])

    with sqlite3.connect(db_path) as conn:
        summary = trackpoints.ingest_trackpoints_from_fit_archives(
            conn,
            tmp_path,
            replace_existing=True,
            archive_paths=[missing],
        )

    assert summary["errors"] == 1
    assert summary["target_activity_ids"] == [activity_id]


def test_non_file_not_found_snapshot_stat_error_is_observable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    archive = tmp_path / "fit" / "2026-09-27_31000006_run.zip"
    archive.parent.mkdir()
    archive.touch()
    real_stat = Path.stat

    def fail_archive_stat(path: Path, *args, **kwargs):
        if path == archive:
            raise PermissionError("injected stat failure")
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", fail_archive_stat)

    with pytest.raises(PermissionError, match="injected stat failure"):
        cli_backup_ingest._snapshot_fit_archives(archive.parent)


def test_post_sync_snapshot_does_not_ignore_disappearing_archive(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    archive = tmp_path / "fit" / "2026-09-27_31000008_run.zip"
    archive.parent.mkdir()
    archive.touch()
    real_stat = Path.stat

    def disappear_during_stat(path: Path, *args, **kwargs):
        if path == archive:
            raise FileNotFoundError("injected disappearance")
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", disappear_during_stat)

    assert cli_backup_ingest._snapshot_fit_archives(archive.parent) == {}
    with pytest.raises(FileNotFoundError, match="injected disappearance"):
        cli_backup_ingest._snapshot_fit_archives(
            archive.parent,
            fail_on_disappearing_archive=True,
        )


def test_post_sync_snapshot_failure_prevents_terminal_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys,
) -> None:
    db_path = tmp_path / "garmin.db"
    _trackpoint_database(db_path, [])
    _patch_post_sync_success(monkeypatch)
    monkeypatch.setattr(
        cli_backup_ingest.subprocess,
        "run",
        lambda command, **_kwargs: subprocess.CompletedProcess(command, 0),
    )
    snapshots = iter([{}, PermissionError("post-sync stat failure")])
    monkeypatch.setattr(
        cli_backup_ingest,
        "_snapshot_fit_archives",
        lambda _fit_dir, **_kwargs: next(snapshots),
    )

    assert cli_backup_ingest.run_sync(db_path) != 0
    assert "[SUCCESS] Sync complete" not in capsys.readouterr().out


def test_duplicate_activity_archives_are_rejected_independent_of_input_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    activity_id = 31_000_007
    db_path = tmp_path / "garmin.db"
    first = tmp_path / f"2026-09-27_{activity_id}_alpha.zip"
    second = tmp_path / f"2026-09-27_{activity_id}_beta.zip"
    _trackpoint_database(db_path, [activity_id])
    first.touch()
    second.touch()
    monkeypatch.setattr(
        trackpoints,
        "parse_trackpoints_from_fit_archive",
        lambda _path: pytest.fail("ambiguous duplicate must not be parsed"),
    )

    summaries = []
    for paths in ([first, second], [second, first]):
        with sqlite3.connect(db_path) as conn:
            summaries.append(
                trackpoints.ingest_trackpoints_from_fit_archives(
                    conn,
                    tmp_path,
                    replace_existing=False,
                    archive_paths=paths,
                )
            )

    assert summaries[0] == summaries[1]
    assert summaries[0]["errors"] == 1
    assert summaries[0]["ingested_activities"] == 0
