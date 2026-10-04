"""Regression contracts for canonical sync, archive refresh and failure propagation."""

from __future__ import annotations

import sqlite3
import subprocess
from pathlib import Path

import pytest

from garmin_data_hub import cli_backup_ingest
from garmin_data_hub.db import queries
from garmin_data_hub.ingest import trackpoints


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _trackpoint_row(seq: int, timestamp: str, *, speed: float = 3.0) -> tuple:
    return (
        seq,
        timestamp,
        None,
        None,
        None,
        float(seq),
        speed,
        140,
        80,
        200,
        18.0,
    )


def _activity_id(path: Path) -> int:
    return int(path.stem.split("_")[1])


def _create_trackpoint_database(
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
                PRIMARY KEY (activity_id, seq),
                FOREIGN KEY (activity_id) REFERENCES activity(activity_id)
            );
            """
        )
        conn.executemany(
            "INSERT INTO activity(activity_id, start_time_gmt) VALUES (?, ?)",
            [
                (activity_id, f"2026-09-{(index % 20) + 1:02d}T08:00:00Z")
                for index, activity_id in enumerate(activity_ids)
            ],
        )
        conn.executemany(
            """
            INSERT INTO activity_trackpoints(
                activity_id, seq, timestamp_utc, speed_mps
            ) VALUES (?, 99, 'old-complete-row', 2.5)
            """,
            [(activity_id,) for activity_id in sorted(populated_ids)],
        )


def _archive(fit_dir: Path, activity_id: int, payload: str = "unchanged") -> Path:
    fit_dir.mkdir(parents=True, exist_ok=True)
    path = fit_dir / f"2026-09-01_{activity_id}_contract.zip"
    path.write_text(payload, encoding="utf-8")
    return path


def _patch_schema_and_refresh(
    monkeypatch: pytest.MonkeyPatch,
    *,
    refresh_summary: dict[str, int] | None = None,
) -> None:
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
        lambda _conn, **_kwargs: refresh_summary or {"errors": 0},
    )


def _record_real_ingestion(monkeypatch: pytest.MonkeyPatch):
    real_ingest = trackpoints.ingest_trackpoints_from_fit_archives
    calls: list[dict[str, object]] = []

    def recording_ingest(conn, fit_dir, **kwargs):
        result = real_ingest(conn, fit_dir, **kwargs)
        calls.append({"fit_dir": Path(fit_dir), "kwargs": dict(kwargs), "result": result})
        return result

    monkeypatch.setattr(
        trackpoints, "ingest_trackpoints_from_fit_archives", recording_ingest
    )
    # Support either a future module-level import or a local import in run_sync().
    monkeypatch.setattr(
        cli_backup_ingest,
        "ingest_trackpoints_from_fit_archives",
        recording_ingest,
        raising=False,
    )
    return calls




@pytest.mark.parametrize(
    ("days", "extra_args", "expected_option", "expected_value"),
    [
        (14, None, "--days", "14"),
        (None, ["--since", "2026-09-01"], "--since", "2026-09-01"),
    ],
)
def test_canonical_upstream_invocation_disables_trackpoints_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    days,
    extra_args,
    expected_option,
    expected_value,
):
    _patch_schema_and_refresh(monkeypatch)
    recorded: dict[str, object] = {}

    def fake_run(command, **kwargs):
        recorded["command"] = list(command)
        recorded["kwargs"] = kwargs
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fake_run)
    db_path = tmp_path / "selected" / "garmin.db"

    assert (
        cli_backup_ingest.run_sync(
            db_path,
            days=days,
            extra_args=extra_args,
        )
        == 0
    )
    command = recorded["command"]
    assert command.count("--no-trackpoints") == 1
    assert command[command.index(expected_option) + 1] == expected_value
    assert recorded["kwargs"]["env"]["GARMIN_DATA_DIR"] == str(db_path.parent)


def test_changed_archive_pass_parses_only_new_or_changed_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    unchanged_ids = [20_000_000 + index for index in range(12)]
    changed_id = 21_000_001
    new_id = 21_000_002
    all_ids = [*unchanged_ids, changed_id, new_id]
    db_path = tmp_path / "garmin.db"
    fit_dir = tmp_path / "fit"
    _create_trackpoint_database(
        db_path,
        all_ids,
        populated_ids={*unchanged_ids, changed_id},
    )
    for activity_id in unchanged_ids:
        _archive(fit_dir, activity_id)
    changed_path = _archive(fit_dir, changed_id, "before")
    new_path = fit_dir / f"2026-09-27_{new_id}_new.zip"

    parsed: list[Path] = []

    def fake_parse(path: Path):
        parsed.append(Path(path))
        activity_id = _activity_id(Path(path))
        return activity_id, [_trackpoint_row(0, f"new-{activity_id}")]

    monkeypatch.setattr(trackpoints, "parse_trackpoints_from_fit_archive", fake_parse)
    calls = _record_real_ingestion(monkeypatch)
    _patch_schema_and_refresh(monkeypatch)

    def fake_upstream(command, **_kwargs):
        changed_path.write_text("after-with-different-size", encoding="utf-8")
        new_path.write_text("new archive", encoding="utf-8")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fake_upstream)

    assert cli_backup_ingest.run_sync(db_path, days=7) == 0
    assert set(parsed) == {changed_path, new_path}
    changed_calls = [
        call
        for call in calls
        if call["kwargs"].get("replace_existing") is True
        and call["kwargs"].get("archive_paths") is not None
    ]
    assert len(changed_calls) == 1
    assert set(map(Path, changed_calls[0]["kwargs"]["archive_paths"])) == {
        changed_path,
        new_path,
    }


def test_changed_existing_archive_replaces_rows_and_reports_updated_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    activity_id = 22_000_001
    db_path = tmp_path / "garmin.db"
    fit_dir = tmp_path / "fit"
    _create_trackpoint_database(db_path, [activity_id], populated_ids={activity_id})
    archive = _archive(fit_dir, activity_id, "old archive")
    calls = _record_real_ingestion(monkeypatch)
    _patch_schema_and_refresh(monkeypatch)
    monkeypatch.setattr(
        trackpoints,
        "parse_trackpoints_from_fit_archive",
        lambda _path: (activity_id, [_trackpoint_row(0, "corrected")]),
    )

    def fake_upstream(command, **_kwargs):
        archive.write_text("corrected archive with changed identity", encoding="utf-8")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fake_upstream)

    assert cli_backup_ingest.run_sync(db_path) == 0
    changed = [
        call
        for call in calls
        if call["kwargs"].get("replace_existing") is True
    ]
    assert len(changed) == 1
    assert changed[0]["result"]["updated_activity_ids"] == [activity_id]
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT seq, timestamp_utc FROM activity_trackpoints "
            "WHERE activity_id = ?",
            (activity_id,),
        ).fetchall() == [(0, "corrected")]


def test_unchanged_archive_left_by_cancelled_run_is_repaired_by_backlog_pass(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    populated_ids = {23_000_001, 23_000_002, 23_000_003}
    missing_id = 23_000_004
    db_path = tmp_path / "garmin.db"
    fit_dir = tmp_path / "fit"
    _create_trackpoint_database(
        db_path,
        [*sorted(populated_ids), missing_id],
        populated_ids=populated_ids,
    )
    for activity_id in [*sorted(populated_ids), missing_id]:
        _archive(fit_dir, activity_id, "archive left from run 1")

    parsed: list[int] = []

    def fake_parse(path: Path):
        activity_id = _activity_id(Path(path))
        parsed.append(activity_id)
        return activity_id, [_trackpoint_row(0, "recovered")]

    monkeypatch.setattr(trackpoints, "parse_trackpoints_from_fit_archive", fake_parse)
    calls = _record_real_ingestion(monkeypatch)
    _patch_schema_and_refresh(monkeypatch)
    monkeypatch.setattr(
        cli_backup_ingest.subprocess,
        "run",
        lambda command, **_kwargs: subprocess.CompletedProcess(command, 0),
    )

    assert cli_backup_ingest.run_sync(db_path) == 0
    assert parsed == [missing_id]
    backlog_calls = [
        call
        for call in calls
        if call["kwargs"].get("replace_existing") is False
    ]
    assert len(backlog_calls) == 1
    assert backlog_calls[0]["result"]["updated_activity_ids"] == [missing_id]


def test_canonical_trackpoint_failure_is_atomic_and_prevents_sync_success(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys
):
    failed_id = 24_000_001
    successful_id = 24_000_002
    db_path = tmp_path / "garmin.db"
    fit_dir = tmp_path / "fit"
    _create_trackpoint_database(
        db_path,
        [failed_id, successful_id],
        populated_ids={failed_id, successful_id},
    )
    failed_archive = fit_dir / f"2026-09-27_{failed_id}_failed.zip"
    successful_archive = fit_dir / f"2026-09-27_{successful_id}_success.zip"
    calls = _record_real_ingestion(monkeypatch)
    _patch_schema_and_refresh(monkeypatch)

    def fake_upstream(command, **_kwargs):
        fit_dir.mkdir(exist_ok=True)
        failed_archive.write_text("failed replacement", encoding="utf-8")
        successful_archive.write_text("successful replacement", encoding="utf-8")
        return subprocess.CompletedProcess(command, 0)

    def fake_parse(path: Path):
        activity_id = _activity_id(Path(path))
        if activity_id == failed_id:
            return activity_id, [
                _trackpoint_row(1, "partial"),
                _trackpoint_row(1, "duplicate"),
            ]
        return activity_id, [_trackpoint_row(0, "valid-replacement")]

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fake_upstream)
    monkeypatch.setattr(trackpoints, "parse_trackpoints_from_fit_archive", fake_parse)

    result = cli_backup_ingest.run_sync(db_path)
    output = capsys.readouterr().out

    assert result != 0
    assert "[SUCCESS] Sync complete" not in output
    changed_result = next(
        call["result"]
        for call in calls
        if call["kwargs"].get("replace_existing") is True
    )
    assert changed_result["errors"] == 1
    assert changed_result["updated_activity_ids"] == [successful_id]
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT seq, timestamp_utc FROM activity_trackpoints "
            "WHERE activity_id = ?",
            (failed_id,),
        ).fetchall() == [(99, "old-complete-row")]
        assert conn.execute(
            "SELECT seq, timestamp_utc FROM activity_trackpoints "
            "WHERE activity_id = ?",
            (successful_id,),
        ).fetchall() == [(0, "valid-replacement")]


def test_post_sync_semantic_order_places_both_ingestion_passes_before_refresh(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys
):
    events: list[str] = []
    _patch_schema_and_refresh(monkeypatch)

    def fake_upstream(command, **_kwargs):
        events.append("upstream")
        return subprocess.CompletedProcess(command, 0)

    def fake_schema(_conn, _schema):
        events.append("schema")

    def fake_ingest(_conn, _fit_dir, **kwargs):
        events.append("changed_trackpoints" if kwargs.get("replace_existing") else "backlog")
        return {"errors": 0, "updated_activity_ids": []}

    def fake_refresh(_conn, **_kwargs):
        events.extend(["athlete_profile", "persisted_metrics"])
        return {"errors": 0}

    class FakeConnection:
        def close(self):
            pass

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fake_upstream)
    monkeypatch.setattr("garmin_data_hub.db.migrate.apply_schema", fake_schema)
    monkeypatch.setattr(
        "garmin_data_hub.db.sqlite.connect_sqlite", lambda _path: FakeConnection()
    )
    monkeypatch.setattr(
        trackpoints, "ingest_trackpoints_from_fit_archives", fake_ingest
    )
    monkeypatch.setattr(
        cli_backup_ingest,
        "ingest_trackpoints_from_fit_archives",
        fake_ingest,
        raising=False,
    )
    monkeypatch.setattr(
        "garmin_data_hub.analytics.post_sync_refresh.refresh_post_sync_tables",
        fake_refresh,
    )

    assert cli_backup_ingest.run_sync(tmp_path / "garmin.db") == 0
    assert events == [
        "upstream",
        "schema",
        "changed_trackpoints",
        "backlog",
        "athlete_profile",
        "persisted_metrics",
    ]
    assert capsys.readouterr().out.rstrip().endswith("[SUCCESS] Sync complete")


def test_targeted_trackpoint_error_propagates_to_overall_sync_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys
):
    _patch_schema_and_refresh(monkeypatch)
    monkeypatch.setattr(
        cli_backup_ingest.subprocess,
        "run",
        lambda command, **_kwargs: subprocess.CompletedProcess(command, 0),
    )

    def failing_ingest(*_args, **_kwargs):
        return {"errors": 1, "updated_activity_ids": []}

    monkeypatch.setattr(
        trackpoints, "ingest_trackpoints_from_fit_archives", failing_ingest
    )
    monkeypatch.setattr(
        cli_backup_ingest,
        "ingest_trackpoints_from_fit_archives",
        failing_ingest,
        raising=False,
    )

    assert cli_backup_ingest.run_sync(tmp_path / "garmin.db") != 0
    assert "[SUCCESS] Sync complete" not in capsys.readouterr().out


def test_athlete_profile_failure_propagates_to_overall_sync_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys
):
    import garmin_data_hub.analytics.post_sync_refresh as post_sync_refresh

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    real_refresh_post_sync_tables = post_sync_refresh.refresh_post_sync_tables
    _patch_schema_and_refresh(monkeypatch)
    monkeypatch.setattr(
        cli_backup_ingest.subprocess,
        "run",
        lambda command, **_kwargs: subprocess.CompletedProcess(command, 0),
    )
    monkeypatch.setattr("garmin_data_hub.db.sqlite.connect_sqlite", lambda _path: conn)
    monkeypatch.setattr(post_sync_refresh.queries, "get_effective_lthr", lambda _conn: None)
    monkeypatch.setattr(
        post_sync_refresh.queries,
        "refresh_persisted_activity_metrics",
        lambda _conn, **_kwargs: {
            "target_activities": 0,
            "rows_upserted": 0,
            "zones_updated": 0,
            "errors": 0,
        },
    )
    monkeypatch.setattr(post_sync_refresh.queries, "list_activities_needing_metrics", lambda _conn: [])
    monkeypatch.setattr(
        "garmin_data_hub.analytics.post_sync_refresh.refresh_post_sync_tables",
        real_refresh_post_sync_tables,
    )

    assert cli_backup_ingest.run_sync(tmp_path / "garmin.db") != 0
    assert "[SUCCESS] Sync complete" not in capsys.readouterr().out


def test_refresh_targets_union_of_date_window_and_changed_trackpoint_ids(
    db_conn, monkeypatch: pytest.MonkeyPatch
):
    old_changed_id = 25_000_001
    recent_id = 25_000_002
    db_conn.executemany(
        "INSERT INTO activity(activity_id, start_time_gmt) VALUES (?, ?)",
        [
            (old_changed_id, "2020-01-01T08:00:00Z"),
            (recent_id, "2026-09-20T08:00:00Z"),
        ],
    )
    db_conn.commit()
    monkeypatch.setattr(queries, "_refresh_scalar_activity_metrics", lambda *_args: None)
    monkeypatch.setattr(queries, "_refresh_trackpoint_derived_metrics", lambda *_args: None)

    summary = queries.refresh_persisted_activity_metrics(
        db_conn,
        activity_ids=[old_changed_id],
        start_ts_iso="2026-09-01T00:00:00Z",
        lthr=None,
    )

    assert summary["errors"] == 0
    assert summary["target_activities"] == 2
    assert {
        row[0]
        for row in db_conn.execute(
            "SELECT activity_id FROM activity_metrics "
            "WHERE activity_id IN (?, ?)",
            (old_changed_id, recent_id),
        )
    } == {old_changed_id, recent_id}










def test_frozen_cli_entrypoint_imports_application_trackpoint_adapter():
    cli_source = (
        PROJECT_ROOT / "src" / "garmin_data_hub" / "cli_backup_ingest.py"
    ).read_text(encoding="utf-8")
    build_source = (PROJECT_ROOT / "packaging" / "build.ps1").read_text(
        encoding="utf-8"
    )

    assert "garmin_data_hub.ingest.trackpoints" in cli_source
    assert '"--collect-all", "garmin_mcp"' in build_source


def test_frozen_canonical_dispatch_disables_bundled_upstream_trackpoints(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    _patch_schema_and_refresh(monkeypatch)
    executable = tmp_path / "cli_backup_ingest.exe"
    expected_prefix = [
        str(executable),
        cli_backup_ingest._BUNDLED_GIVEMYDATA_FLAG,
    ]
    recorded: dict[str, object] = {}
    monkeypatch.setattr(
        cli_backup_ingest, "_find_givemydata_cmd", lambda: expected_prefix.copy()
    )

    def fake_run(command, **kwargs):
        recorded["command"] = list(command)
        recorded["kwargs"] = kwargs
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fake_run)
    db_path = tmp_path / "data" / "garmin.db"

    assert cli_backup_ingest.run_sync(db_path, days=7) == 0
    command = recorded["command"]
    assert command[:2] == expected_prefix
    assert command.count("--no-trackpoints") == 1
    assert recorded["kwargs"]["env"]["GARMIN_DATA_DIR"] == str(db_path.parent)
