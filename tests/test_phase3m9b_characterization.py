"""Phase 3M.9B characterization contracts for the production archive universe.

The fixtures are synthetic.  Counts and lane shape intentionally mirror the
accepted production history (133 scoped terminals plus 22 later FIT no-record
terminals) without naming or reading any live archive.
"""

from __future__ import annotations

import importlib.metadata
import json
import sqlite3
import subprocess
import tomllib
import zipfile
from pathlib import Path

import pytest

from garmin_data_hub import cli_backup_ingest
from garmin_data_hub.analytics import post_sync_refresh
from garmin_data_hub.db import queries
from garmin_data_hub.db import migrate as db_migrate
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.ingest import reconciliation, trackpoints
from garmin_data_hub.ingest.archive_parser import (
    PARSER_ALGORITHM_VERSION,
    ArchiveFormat,
    ArchiveParseResult,
    ArchiveParseStatus,
)
from garmin_data_hub.ingest.fingerprint import sha256_file, stat_signature
from garmin_data_hub.paths import schema_sql_path


START_UTC = "2048-01-02T03:04:05Z"
SCOPED_GPX_COUNT = 53
SCOPED_TCX_COUNT = 80
SCOPED_COUNT = SCOPED_GPX_COUNT + SCOPED_TCX_COUNT
OUTSIDE_FIT_COUNT = 22
COMPLETE_TERMINAL_COUNT = SCOPED_COUNT + OUTSIDE_FIT_COUNT
ORDINARY_POPULATED_COUNT = 3
FIRST_ACTIVITY_ID = 39_000_000
PINNED_UPSTREAM_VERSION = "0.1.12"
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _database(path: Path, activity_ids: list[int]) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE activity (
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
        conn.executemany(
            """
            INSERT INTO activity (
              activity_id, start_time_gmt, elapsed_duration_seconds,
              moving_duration_seconds, average_speed, max_hr, average_hr,
              activity_type, avg_cadence
            ) VALUES (?, ?, 600.0, 590.0, 3.0, 180, 150, 'running', 86.0)
            """,
            [(activity_id, START_UTC) for activity_id in activity_ids],
        )
        apply_schema(conn, schema_sql_path())


def test_repository_and_current_environment_require_exact_upstream_version() -> None:
    with (PROJECT_ROOT / "pyproject.toml").open("rb") as source:
        metadata = tomllib.load(source)
    dependencies = metadata["project"]["dependencies"]

    assert f"garmin-givemydata=={PINNED_UPSTREAM_VERSION}" in dependencies
    assert importlib.metadata.version("garmin-givemydata") == PINNED_UPSTREAM_VERSION


def _archive(
    fit_dir: Path,
    activity_id: int,
    extension: str,
    *,
    payload: bytes | None = None,
) -> Path:
    fit_dir.mkdir(parents=True, exist_ok=True)
    path = fit_dir / f"2048-01-02_{activity_id}_synthetic.zip"
    member_kind = "ACTIVITY" if extension == "fit" else "UNKNOWN"
    content = payload if payload is not None else f"synthetic-{activity_id}".encode()
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr(f"{activity_id}_{member_kind}.{extension}", content)
    return path


def _record_terminal(
    conn: sqlite3.Connection,
    fit_dir: Path,
    archive: Path,
    activity_id: int,
    *,
    status: str,
    archive_format: str,
) -> None:
    source_size, source_mtime_ns = stat_signature(archive)
    reconciliation._insert_ledger(
        conn,
        archive_identity=archive.relative_to(fit_dir).as_posix().casefold(),
        target_activity_id=activity_id,
        archive_format=archive_format,
        source_size=source_size,
        source_mtime_ns=source_mtime_ns,
        source_sha256=sha256_file(archive),
        algorithm_version=PARSER_ALGORITHM_VERSION,
        status=status,
        reason_code="synthetic_terminal",
        identity_evidence={"target_activity_id": activity_id},
        trackpoint_count=0,
        attempted_at_utc="2048-01-02T04:00:00.000000Z",
    )


def _scoped_baseline(conn: sqlite3.Connection) -> None:
    queries.set_setting(
        conn,
        reconciliation.BASELINE_SETTING_KEY,
        {
            "algorithm_version": PARSER_ALGORITHM_VERSION,
            "candidate_archives": SCOPED_COUNT,
            "accounted_archives": SCOPED_COUNT,
            "terminal_archives": SCOPED_COUNT,
            "unresolved_archives": 0,
            "scope": "synthetic-scoped-133",
        },
    )


def _baseline_raw(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "SELECT value FROM app_settings WHERE key=?",
        (reconciliation.BASELINE_SETTING_KEY,),
    ).fetchone()
    assert row is not None
    return str(row[0])


def _durable_state(conn: sqlite3.Connection) -> dict[str, list[tuple[object, ...]]]:
    return {
        "ledger": conn.execute(
            "SELECT * FROM archive_reconciliation ORDER BY reconciliation_id"
        ).fetchall(),
        "trackpoints": conn.execute(
            "SELECT * FROM activity_trackpoints ORDER BY activity_id, seq"
        ).fetchall(),
        "metrics": conn.execute(
            "SELECT * FROM activity_metrics ORDER BY activity_id"
        ).fetchall(),
    }


def _no_records(activity_id: int) -> ArchiveParseResult:
    return ArchiveParseResult(
        status=ArchiveParseStatus.RECOGNIZED_NO_RECORDS,
        archive_format=ArchiveFormat.FIT,
        activity_id=activity_id,
        rows=[],
        reason_code="activity_has_no_trackpoints",
        identity_evidence={"target_activity_id": activity_id},
    )


def _mismatch() -> ArchiveParseResult:
    return ArchiveParseResult(
        status=ArchiveParseStatus.MISMATCH,
        archive_format=ArchiveFormat.FIT,
        activity_id=None,
        rows=[],
        reason_code="activity_id_mismatch",
        identity_evidence={},
    )


def _build_full_universe(
    tmp_path: Path,
    *,
    terminal_outside_fit: bool,
) -> tuple[Path, Path, list[Path], list[Path], list[Path]]:
    scoped_ids = list(range(FIRST_ACTIVITY_ID, FIRST_ACTIVITY_ID + SCOPED_COUNT))
    outside_ids = list(
        range(
            FIRST_ACTIVITY_ID + SCOPED_COUNT,
            FIRST_ACTIVITY_ID + COMPLETE_TERMINAL_COUNT,
        )
    )
    populated_ids = list(
        range(
            FIRST_ACTIVITY_ID + COMPLETE_TERMINAL_COUNT,
            FIRST_ACTIVITY_ID + COMPLETE_TERMINAL_COUNT + ORDINARY_POPULATED_COUNT,
        )
    )
    all_ids = [*scoped_ids, *outside_ids, *populated_ids]
    db_path = tmp_path / "garmin.db"
    fit_dir = tmp_path / "fit"
    _database(db_path, all_ids)

    scoped_archives: list[Path] = []
    for index, activity_id in enumerate(scoped_ids):
        extension = "gpx" if index < SCOPED_GPX_COUNT else "tcx"
        scoped_archives.append(_archive(fit_dir, activity_id, extension))
    outside_archives = [
        _archive(fit_dir, activity_id, "fit") for activity_id in outside_ids
    ]
    populated_archives = [
        _archive(fit_dir, activity_id, "fit") for activity_id in populated_ids
    ]

    with sqlite3.connect(db_path) as conn:
        for index, (activity_id, archive) in enumerate(
            zip(scoped_ids, scoped_archives, strict=True)
        ):
            _record_terminal(
                conn,
                fit_dir,
                archive,
                activity_id,
                status=("resolved_no_records" if index % 2 == 0 else "mismatch"),
                archive_format=("gpx" if index < SCOPED_GPX_COUNT else "tcx"),
            )
        if terminal_outside_fit:
            for activity_id, archive in zip(
                outside_ids, outside_archives, strict=True
            ):
                _record_terminal(
                    conn,
                    fit_dir,
                    archive,
                    activity_id,
                    status="resolved_no_records",
                    archive_format="fit",
                )
        for activity_id in populated_ids:
            conn.execute(
                """
                INSERT INTO activity_trackpoints(activity_id, seq, timestamp_utc)
                VALUES (?, 0, ?)
                """,
                (activity_id, START_UTC),
            )
        conn.commit()
        _scoped_baseline(conn)

    return (
        db_path,
        fit_dir,
        scoped_archives,
        outside_archives,
        populated_archives,
    )


def _build_terminal_universe(
    tmp_path: Path,
    *,
    special_index: int = 0,
    special_status: str = "resolved_no_records",
) -> tuple[Path, Path, list[Path], list[int]]:
    activity_ids = list(
        range(FIRST_ACTIVITY_ID, FIRST_ACTIVITY_ID + COMPLETE_TERMINAL_COUNT)
    )
    db_path = tmp_path / "garmin.db"
    fit_dir = tmp_path / "fit"
    _database(db_path, activity_ids)
    archives = [_archive(fit_dir, activity_id, "fit") for activity_id in activity_ids]
    with sqlite3.connect(db_path) as conn:
        for index, (activity_id, archive) in enumerate(
            zip(activity_ids, archives, strict=True)
        ):
            _record_terminal(
                conn,
                fit_dir,
                archive,
                activity_id,
                status=(special_status if index == special_index else "resolved_no_records"),
                archive_format="fit",
            )
        conn.commit()
        _scoped_baseline(conn)
    return db_path, fit_dir, archives, activity_ids


def test_scoped_133_baseline_exposes_22_unledgered_fit_candidates_then_155_is_zero_work(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path, fit_dir, scoped, outside, populated = _build_full_universe(
        tmp_path,
        terminal_outside_fit=False,
    )
    outside_set = set(outside)
    parsed: list[Path] = []

    def parse_only_outside(path: Path, target) -> ArchiveParseResult:
        assert path in outside_set, "ledgered/populated history entered parsing"
        parsed.append(path)
        return _no_records(target.activity_id)

    monkeypatch.setattr(reconciliation, "parse_activity_archive", parse_only_outside)
    full_universe = [*scoped, *outside, *populated]

    with sqlite3.connect(db_path) as conn:
        baseline_before = _baseline_raw(conn)
        points_before = conn.execute(
            "SELECT * FROM activity_trackpoints ORDER BY activity_id, seq"
        ).fetchall()
        first = cli_backup_ingest._run_historical_archive_reconciliation(
            conn, fit_dir, full_universe
        )
        ledger_after_first = conn.execute(
            "SELECT COUNT(*) FROM archive_reconciliation"
        ).fetchone()[0]

        monkeypatch.setattr(
            reconciliation,
            "parse_activity_archive",
            lambda *_args: pytest.fail("complete 155 state was reparsed"),
        )
        state_before_second = _durable_state(conn)
        second = cli_backup_ingest._run_historical_archive_reconciliation(
            conn, fit_dir, full_universe
        )
        state_after_second = _durable_state(conn)
        baseline_after = _baseline_raw(conn)
        points_after = conn.execute(
            "SELECT * FROM activity_trackpoints ORDER BY activity_id, seq"
        ).fetchall()

    assert len(parsed) == OUTSIDE_FIT_COUNT
    assert set(parsed) == outside_set
    assert first["candidate_archives"] == len(full_universe)
    assert first["attempted_archives"] == OUTSIDE_FIT_COUNT
    assert first["resolved_no_records"] == OUTSIDE_FIT_COUNT
    assert first["unchanged_terminal"] == SCOPED_COUNT
    assert first["status_counts"]["already_populated"] == ORDINARY_POPULATED_COUNT
    assert ledger_after_first == COMPLETE_TERMINAL_COUNT
    assert second["attempted_archives"] == 0
    assert second["unchanged_terminal"] == COMPLETE_TERMINAL_COUNT
    assert second["status_counts"]["already_populated"] == ORDINARY_POPULATED_COUNT
    assert state_after_second == state_before_second
    assert baseline_after == baseline_before
    assert points_after == points_before


def test_normal_sync_path_with_complete_155_state_has_no_historical_or_global_work(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path, _fit_dir, _scoped, _outside, _populated = _build_full_universe(
        tmp_path,
        terminal_outside_fit=True,
    )
    captured: dict[str, object] = {}
    upstream_commands: list[list[str]] = []

    monkeypatch.setattr(cli_backup_ingest, "ensure_app_dirs", lambda: None)
    monkeypatch.setattr(
        cli_backup_ingest,
        "_find_givemydata_cmd",
        lambda: ["environment-local-garmin-givemydata"],
    )

    def fake_upstream(command: list[str], **_kwargs):
        upstream_commands.append(command)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fake_upstream)
    monkeypatch.setattr(db_migrate, "apply_schema", lambda _conn, _schema: None)
    monkeypatch.setattr(
        reconciliation,
        "parse_activity_archive",
        lambda *_args: pytest.fail("zero-work normal sync invoked historical parsing"),
    )
    original_historical = cli_backup_ingest._run_historical_archive_reconciliation

    def capture_historical(conn, fit_dir, archive_paths):
        result = original_historical(conn, fit_dir, archive_paths)
        captured["historical"] = result
        return result

    monkeypatch.setattr(
        cli_backup_ingest,
        "_run_historical_archive_reconciliation",
        capture_historical,
    )

    def fake_refresh(_conn, **kwargs):
        captured["refresh"] = kwargs
        return {
            "target_activities": 0,
            "rows_upserted": 0,
            "zones_updated": 0,
            "errors": 0,
        }

    monkeypatch.setattr(post_sync_refresh, "refresh_post_sync_tables", fake_refresh)

    with sqlite3.connect(db_path) as conn:
        baseline_before = _baseline_raw(conn)
        state_before = _durable_state(conn)

    assert cli_backup_ingest.run_sync(db_path) == 0

    with sqlite3.connect(db_path) as conn:
        baseline_after = _baseline_raw(conn)
        state_after = _durable_state(conn)

    historical = captured["historical"]
    refresh = captured["refresh"]
    assert isinstance(historical, dict)
    assert isinstance(refresh, dict)
    assert historical["attempted_archives"] == 0
    assert historical["unchanged_terminal"] == COMPLETE_TERMINAL_COUNT
    assert refresh["activity_ids"] == []
    assert refresh["start_ts_iso"] is None
    assert upstream_commands[0].count("--no-trackpoints") == 1
    assert baseline_after == baseline_before
    assert state_after == state_before


@pytest.mark.parametrize(
    ("status", "expected_warning"),
    [
        ("resolved_no_records", False),
        ("mismatch", True),
        ("malformed", True),
        ("unsupported", True),
        ("ambiguous", True),
    ],
)
def test_unchanged_recordless_and_quarantine_terminals_skip_without_parsing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    status: str,
    expected_warning: bool,
) -> None:
    activity_id = FIRST_ACTIVITY_ID
    db_path = tmp_path / "garmin.db"
    fit_dir = tmp_path / "fit"
    _database(db_path, [activity_id])
    archive = _archive(fit_dir, activity_id, "fit")
    with sqlite3.connect(db_path) as conn:
        _record_terminal(
            conn,
            fit_dir,
            archive,
            activity_id,
            status=status,
            archive_format="fit",
        )
        conn.commit()
        monkeypatch.setattr(
            reconciliation,
            "parse_activity_archive",
            lambda *_args: pytest.fail("unchanged terminal archive was parsed"),
        )
        summary = reconciliation.reconcile_historical_archives(
            conn, fit_dir, archive_paths=[archive]
        )

    assert summary["attempted_archives"] == 0
    assert summary["unchanged_terminal"] == 1
    assert summary["baseline_warnings"] == int(expected_warning)


@pytest.mark.parametrize("initial_status", ["resolved_no_records", "mismatch"])
def test_one_changed_terminal_fingerprint_retries_without_resurrecting_other_154(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    initial_status: str,
) -> None:
    db_path, fit_dir, archives, activity_ids = _build_terminal_universe(
        tmp_path,
        special_status=initial_status,
    )
    changed = archives[0]
    _archive(
        fit_dir,
        activity_ids[0],
        "fit",
        payload=f"changed-{initial_status}-payload".encode(),
    )
    parsed: list[int] = []

    def parse_changed(path: Path, target) -> ArchiveParseResult:
        assert path == changed
        parsed.append(target.activity_id)
        return (
            _no_records(target.activity_id)
            if initial_status == "resolved_no_records"
            else _mismatch()
        )

    monkeypatch.setattr(reconciliation, "parse_activity_archive", parse_changed)
    with sqlite3.connect(db_path) as conn:
        summary = cli_backup_ingest._run_historical_archive_reconciliation(
            conn, fit_dir, archives
        )
        ledger_count = conn.execute(
            "SELECT COUNT(*) FROM archive_reconciliation"
        ).fetchone()[0]

    assert parsed == [activity_ids[0]]
    assert summary["attempted_archives"] == 1
    assert summary["unchanged_terminal"] == COMPLETE_TERMINAL_COUNT - 1
    assert ledger_count == COMPLETE_TERMINAL_COUNT + 1


def test_complete_155_baseline_does_not_suppress_new_historical_candidate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path, fit_dir, archives, _activity_ids = _build_terminal_universe(tmp_path)
    new_activity_id = FIRST_ACTIVITY_ID + COMPLETE_TERMINAL_COUNT
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO activity (
              activity_id, start_time_gmt, elapsed_duration_seconds,
              moving_duration_seconds, average_speed, max_hr, average_hr,
              activity_type, avg_cadence
            ) VALUES (?, ?, 600.0, 590.0, 3.0, 180, 150, 'running', 86.0)
            """,
            (new_activity_id, START_UTC),
        )
        conn.commit()
        baseline_before = _baseline_raw(conn)
    new_archive = _archive(fit_dir, new_activity_id, "fit")
    parsed: list[int] = []

    def parse_new(_path: Path, target) -> ArchiveParseResult:
        parsed.append(target.activity_id)
        return _no_records(target.activity_id)

    monkeypatch.setattr(reconciliation, "parse_activity_archive", parse_new)
    with sqlite3.connect(db_path) as conn:
        summary = cli_backup_ingest._run_historical_archive_reconciliation(
            conn, fit_dir, [*archives, new_archive]
        )
        baseline_after = _baseline_raw(conn)
        ledger_count = conn.execute(
            "SELECT COUNT(*) FROM archive_reconciliation"
        ).fetchone()[0]

    assert parsed == [new_activity_id]
    assert summary["attempted_archives"] == 1
    assert summary["unchanged_terminal"] == COMPLETE_TERMINAL_COUNT
    assert summary["resolved_no_records"] == 1
    assert ledger_count == COMPLETE_TERMINAL_COUNT + 1
    assert baseline_after == baseline_before
    assert json.loads(baseline_after)["candidate_archives"] == SCOPED_COUNT


@pytest.mark.parametrize(
    "outcome",
    ["malformed", "unsupported", "mismatch", "ambiguous", "parser_exception"],
)
def test_matching_historical_terminal_never_weakens_changed_new_strictness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    outcome: str,
) -> None:
    activity_id = FIRST_ACTIVITY_ID
    db_path = tmp_path / "garmin.db"
    fit_dir = tmp_path / "fit"
    _database(db_path, [activity_id])
    archive = _archive(fit_dir, activity_id, "fit")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO activity_trackpoints(activity_id, seq, timestamp_utc) "
            "VALUES (?, 99, 'old-row')",
            (activity_id,),
        )
        _record_terminal(
            conn,
            fit_dir,
            archive,
            activity_id,
            status="mismatch",
            archive_format="fit",
        )
        conn.commit()

    if outcome == "parser_exception":
        monkeypatch.setattr(
            trackpoints,
            "parse_activity_archive",
            lambda *_args: (_ for _ in ()).throw(RuntimeError("synthetic parser error")),
        )
    else:
        status = ArchiveParseStatus(outcome)
        monkeypatch.setattr(
            trackpoints,
            "parse_activity_archive",
            lambda *_args: ArchiveParseResult(
                status=status,
                archive_format=ArchiveFormat.FIT,
                activity_id=None,
                rows=[],
                reason_code=f"synthetic_{outcome}",
                identity_evidence={},
            ),
        )

    with sqlite3.connect(db_path) as conn:
        summary = cli_backup_ingest._run_changed_archive_ingestion(
            conn, fit_dir, [archive]
        )
        rows = conn.execute(
            "SELECT seq, timestamp_utc FROM activity_trackpoints WHERE activity_id=?",
            (activity_id,),
        ).fetchall()
        ledger_count = conn.execute(
            "SELECT COUNT(*) FROM archive_reconciliation"
        ).fetchone()[0]

    assert summary["errors"] == 1
    assert summary["result_statuses"][activity_id] == (
        "parser_error" if outcome == "parser_exception" else outcome
    )
    assert rows == [(99, "old-row")]
    assert ledger_count == 1


def test_valid_changed_new_ingestion_is_atomic_even_with_matching_terminal_ledger(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    activity_id = FIRST_ACTIVITY_ID
    db_path = tmp_path / "garmin.db"
    fit_dir = tmp_path / "fit"
    _database(db_path, [activity_id])
    archive = _archive(fit_dir, activity_id, "fit")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO activity_trackpoints(activity_id, seq, timestamp_utc) "
            "VALUES (?, 99, 'old-row')",
            (activity_id,),
        )
        _record_terminal(
            conn,
            fit_dir,
            archive,
            activity_id,
            status="mismatch",
            archive_format="fit",
        )
        conn.commit()

    parsed_rows = [
        (0, START_UTC, 39.0, -77.0, None, None, None, 140, 82, None, None),
        (
            1,
            "2048-01-02T03:04:06Z",
            39.1,
            -77.1,
            None,
            None,
            None,
            141,
            83,
            None,
            None,
        ),
    ]
    monkeypatch.setattr(
        trackpoints,
        "parse_activity_archive",
        lambda *_args: ArchiveParseResult(
            status=ArchiveParseStatus.PARSED,
            archive_format=ArchiveFormat.FIT,
            activity_id=activity_id,
            rows=parsed_rows,
            identity_evidence={"target_activity_id": activity_id},
        ),
    )

    with sqlite3.connect(db_path) as conn:
        summary = cli_backup_ingest._run_changed_archive_ingestion(
            conn, fit_dir, [archive]
        )
        rows = conn.execute(
            "SELECT seq, timestamp_utc FROM activity_trackpoints WHERE activity_id=? "
            "ORDER BY seq",
            (activity_id,),
        ).fetchall()
        ledger_count = conn.execute(
            "SELECT COUNT(*) FROM archive_reconciliation"
        ).fetchone()[0]

    assert summary["errors"] == 0
    assert summary["updated_activity_ids"] == [activity_id]
    assert rows == [(0, START_UTC), (1, "2048-01-02T03:04:06Z")]
    assert ledger_count == 1
