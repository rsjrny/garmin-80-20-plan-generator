"""Phase 3M.6A contracts for durable baseline-marker semantics."""

from __future__ import annotations

import json
import sqlite3
import zipfile
from pathlib import Path

import pytest

from garmin_data_hub import cli_archive_reconcile
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.ingest import reconciliation
from garmin_data_hub.ingest.reconciliation import BASELINE_SETTING_KEY
from garmin_data_hub.paths import schema_sql_path


ACTIVITY_ID = 9_600_001
SECOND_ACTIVITY_ID = 9_600_002
MISSING_ACTIVITY_ID = 9_699_999
START_UTC = "2046-03-04T05:06:07Z"


def _insert_activity(conn: sqlite3.Connection, activity_id: int) -> None:
    conn.execute(
        """
        INSERT INTO activity (
          activity_id, start_time_gmt, elapsed_duration_seconds,
          moving_duration_seconds, average_speed, max_hr, average_hr,
          activity_type, avg_cadence
        ) VALUES (?, ?, 600.0, 590.0, 3.0, 180, 150, 'running', 86.0)
        """,
        (activity_id, START_UTC),
    )


def _database(path: Path, *activity_ids: int) -> None:
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
        for activity_id in activity_ids:
            _insert_activity(conn, activity_id)
        apply_schema(conn, schema_sql_path())


def _gpx() -> str:
    return f"""
    <gpx xmlns="http://www.topografix.com/GPX/1/1" version="1.1" creator="test">
      <trk><trkseg>
        <trkpt lat="39.1" lon="-76.2"><time>{START_UTC}</time></trkpt>
        <trkpt lat="39.2" lon="-76.3"><time>2046-03-04T05:06:08Z</time></trkpt>
      </trkseg></trk>
    </gpx>
    """


def _recordless_tcx() -> str:
    return f"""
    <TrainingCenterDatabase
      xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
      <Activities><Activity Sport="Running"><Id>{START_UTC}</Id>
        <Lap StartTime="{START_UTC}"><TotalTimeSeconds>600</TotalTimeSeconds></Lap>
      </Activity></Activities>
    </TrainingCenterDatabase>
    """


def _archive(
    directory: Path,
    activity_id: int,
    payload: str,
    *,
    extension: str = "gpx",
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"2046-03-04_{activity_id}_synthetic.zip"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr(f"{activity_id}_UNKNOWN.{extension}", payload)
    return path


def _baseline_row(conn: sqlite3.Connection) -> tuple[str, dict[str, object]] | None:
    row = conn.execute(
        "SELECT value FROM app_settings WHERE key=?", (BASELINE_SETTING_KEY,)
    ).fetchone()
    return (str(row[0]), json.loads(row[0])) if row else None


def _durable_rows(conn: sqlite3.Connection) -> dict[str, list[tuple[object, ...]]]:
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


def test_identical_no_work_rerun_preserves_warning_baseline_and_all_durable_state(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path, ACTIVITY_ID, SECOND_ACTIVITY_ID)
    fit_dir = tmp_path / "fit"
    _archive(fit_dir, ACTIVITY_ID, _gpx())
    _archive(fit_dir, SECOND_ACTIVITY_ID, "<broken>")

    first = cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    with sqlite3.connect(db_path) as conn:
        baseline_before = _baseline_row(conn)
        state_before = _durable_rows(conn)

    second = cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    with sqlite3.connect(db_path) as conn:
        baseline_after = _baseline_row(conn)
        state_after = _durable_rows(conn)

    assert first["warnings"] == 1
    assert second["warnings"] == 0
    assert second["baseline_warnings"] == 1
    assert second["attempted_archives"] == 0
    assert second["unchanged_terminal"] == 2
    assert baseline_before is not None
    assert baseline_before[1]["warnings"] == 1
    assert baseline_after == baseline_before
    assert state_after == state_before


def test_identical_no_work_rerun_preserves_zero_warning_completion_timestamp(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path, ACTIVITY_ID)
    fit_dir = tmp_path / "fit"
    _archive(fit_dir, ACTIVITY_ID, _recordless_tcx(), extension="tcx")

    first = cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    with sqlite3.connect(db_path) as conn:
        before = _baseline_row(conn)
    second = cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    with sqlite3.connect(db_path) as conn:
        after = _baseline_row(conn)

    assert first["warnings"] == second["warnings"] == 0
    assert second["baseline_warnings"] == 0
    assert second["attempted_archives"] == 0
    assert before is not None
    assert before[1]["warnings"] == 0
    assert after == before


def test_changed_fingerprint_reestablishes_marker_and_durable_warning_state(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path, ACTIVITY_ID)
    fit_dir = tmp_path / "fit"
    _archive(fit_dir, ACTIVITY_ID, "<broken>")

    cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    with sqlite3.connect(db_path) as conn:
        before = _baseline_row(conn)
    _archive(fit_dir, ACTIVITY_ID, _recordless_tcx(), extension="tcx")

    second = cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    with sqlite3.connect(db_path) as conn:
        after = _baseline_row(conn)
        ledger = conn.execute(
            "SELECT status FROM archive_reconciliation ORDER BY reconciliation_id"
        ).fetchall()

    assert before is not None and after is not None
    assert before[1]["warnings"] == 1
    assert second["attempted_archives"] == 1
    assert second["resolved_no_records"] == 1
    assert after[1]["warnings"] == 0
    assert after[1]["completed_at_utc"] != before[1]["completed_at_utc"]
    assert ledger == [("malformed",), ("resolved_no_records",)]


def test_changed_fingerprint_updates_marker_when_aggregate_state_is_equal(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path, ACTIVITY_ID)
    fit_dir = tmp_path / "fit"
    _archive(fit_dir, ACTIVITY_ID, "<broken>")

    cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    with sqlite3.connect(db_path) as conn:
        before = _baseline_row(conn)
    _archive(fit_dir, ACTIVITY_ID, "<different-broken-payload>")

    second = cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    with sqlite3.connect(db_path) as conn:
        after = _baseline_row(conn)

    assert before is not None and after is not None
    assert second["attempted_archives"] == 1
    assert before[1]["warnings"] == after[1]["warnings"] == 1
    assert after[1]["completed_at_utc"] != before[1]["completed_at_utc"]


def test_new_eligible_candidate_reestablishes_complete_baseline(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path, ACTIVITY_ID)
    fit_dir = tmp_path / "fit"
    _archive(fit_dir, ACTIVITY_ID, _recordless_tcx(), extension="tcx")

    cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    with sqlite3.connect(db_path) as conn:
        before = _baseline_row(conn)
        _insert_activity(conn, SECOND_ACTIVITY_ID)
        conn.commit()
    _archive(fit_dir, SECOND_ACTIVITY_ID, _recordless_tcx(), extension="tcx")

    second = cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    with sqlite3.connect(db_path) as conn:
        after = _baseline_row(conn)

    assert before is not None and after is not None
    assert second["attempted_archives"] == 1
    assert second["candidate_archives"] == 2
    assert after[1]["candidate_archives"] == 2
    assert after[1]["completed_at_utc"] != before[1]["completed_at_utc"]


def test_zero_attempt_accounting_change_still_reestablishes_marker(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path, ACTIVITY_ID)
    fit_dir = tmp_path / "fit"
    _archive(fit_dir, ACTIVITY_ID, _recordless_tcx(), extension="tcx")

    cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    with sqlite3.connect(db_path) as conn:
        before = _baseline_row(conn)
        _insert_activity(conn, SECOND_ACTIVITY_ID)
        conn.execute(
            "INSERT INTO activity_trackpoints(activity_id, seq, timestamp_utc) "
            "VALUES (?, 0, ?)",
            (SECOND_ACTIVITY_ID, START_UTC),
        )
        conn.commit()
    _archive(fit_dir, SECOND_ACTIVITY_ID, _gpx())

    second = cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    with sqlite3.connect(db_path) as conn:
        after = _baseline_row(conn)

    assert before is not None and after is not None
    assert second["attempted_archives"] == 0
    assert second["candidate_archives"] == 2
    assert second["accounted_archives"] == 2
    assert after[1]["candidate_archives"] == 2
    assert after[1]["completed_at_utc"] != before[1]["completed_at_utc"]


def test_zero_attempt_same_count_candidate_replacement_reestablishes_marker(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path, ACTIVITY_ID, SECOND_ACTIVITY_ID)
    fit_dir = tmp_path / "fit"
    original = _archive(
        fit_dir, ACTIVITY_ID, _recordless_tcx(), extension="tcx"
    )

    cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    with sqlite3.connect(db_path) as conn:
        before = _baseline_row(conn)
        conn.execute(
            "INSERT INTO activity_trackpoints(activity_id, seq, timestamp_utc) "
            "VALUES (?, 0, ?)",
            (SECOND_ACTIVITY_ID, START_UTC),
        )
        conn.commit()

    original.unlink()
    _archive(fit_dir, SECOND_ACTIVITY_ID, _gpx())

    second = cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    with sqlite3.connect(db_path) as conn:
        after = _baseline_row(conn)

    assert before is not None and after is not None
    assert second["attempted_archives"] == 0
    assert second["candidate_archives"] == 1
    assert second["accounted_archives"] == 1
    assert after[1]["candidate_archives"] == 1
    assert after[1]["completed_at_utc"] != before[1]["completed_at_utc"]


def test_previously_unresolved_candidate_can_establish_baseline_when_terminal(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path, ACTIVITY_ID)
    fit_dir = tmp_path / "fit"
    _archive(fit_dir, MISSING_ACTIVITY_ID, _gpx())

    first = cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    with sqlite3.connect(db_path) as conn:
        assert _baseline_row(conn) is None
        _insert_activity(conn, MISSING_ACTIVITY_ID)
        conn.commit()

    second = cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    with sqlite3.connect(db_path) as conn:
        baseline = _baseline_row(conn)

    assert first["baseline_complete"] is False
    assert second["baseline_complete"] is True
    assert second["attempted_archives"] == 1
    assert baseline is not None
    assert baseline[1]["candidate_archives"] == 1
    assert baseline[1]["terminal_archives"] == 1


def test_incomplete_operational_result_does_not_create_completion_marker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path, ACTIVITY_ID)
    fit_dir = tmp_path / "fit"
    _archive(fit_dir, ACTIVITY_ID, _gpx())
    monkeypatch.setattr(
        reconciliation,
        "sha256_file",
        lambda _path: (_ for _ in ()).throw(OSError("offline fingerprint failure")),
    )

    summary = cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    with sqlite3.connect(db_path) as conn:
        baseline = _baseline_row(conn)

    assert summary["errors"] == 1
    assert summary["baseline_complete"] is False
    assert baseline is None


def test_incomplete_rerun_invalidates_existing_complete_marker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path, ACTIVITY_ID)
    fit_dir = tmp_path / "fit"
    _archive(fit_dir, ACTIVITY_ID, _recordless_tcx(), extension="tcx")

    cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    monkeypatch.setattr(
        reconciliation,
        "sha256_file",
        lambda _path: (_ for _ in ()).throw(OSError("offline fingerprint failure")),
    )

    summary = cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    with sqlite3.connect(db_path) as conn:
        baseline = _baseline_row(conn)
        current = reconciliation.reconciliation_baseline_is_current(conn)

    assert summary["errors"] == 1
    assert summary["baseline_complete"] is False
    assert baseline is None
    assert current is False


def test_marker_write_failure_invalidates_prior_marker_and_propagates(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path, ACTIVITY_ID)
    fit_dir = tmp_path / "fit"
    _archive(fit_dir, ACTIVITY_ID, "<broken>")

    cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)
    _archive(fit_dir, ACTIVITY_ID, _recordless_tcx(), extension="tcx")

    raw = sqlite3.connect(db_path)
    raw.row_factory = sqlite3.Row

    class FailBaselineCommit:
        def __init__(self) -> None:
            self.fail_next_commit = False

        @property
        def in_transaction(self) -> bool:
            return raw.in_transaction

        def execute(self, sql: str, params: tuple[object, ...] = ()):
            result = raw.execute(sql, params)
            if (
                sql == cli_archive_reconcile.queries.UPSERT_SETTING_SQL
                and params
                and params[0] == BASELINE_SETTING_KEY
            ):
                self.fail_next_commit = True
            return result

        def commit(self) -> None:
            if self.fail_next_commit:
                self.fail_next_commit = False
                raw.rollback()
                raise sqlite3.OperationalError(
                    "synthetic marker commit failure"
                )
            raw.commit()

        def rollback(self) -> None:
            raw.rollback()

        def close(self) -> None:
            raw.close()

        def __getattr__(self, name: str):
            return getattr(raw, name)

    wrapped = FailBaselineCommit()
    monkeypatch.setattr(
        cli_archive_reconcile, "connect_sqlite", lambda _path: wrapped
    )

    with pytest.raises(
        sqlite3.OperationalError, match="synthetic marker commit failure"
    ):
        cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)

    with sqlite3.connect(db_path) as conn:
        baseline = _baseline_row(conn)
        ledger = conn.execute(
            "SELECT status FROM archive_reconciliation ORDER BY reconciliation_id"
        ).fetchall()

    assert baseline is None
    assert ledger == [("malformed",), ("resolved_no_records",)]


def test_ledger_write_failure_rolls_back_points_and_never_creates_marker(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path, ACTIVITY_ID)
    fit_dir = tmp_path / "fit"
    _archive(fit_dir, ACTIVITY_ID, _gpx())
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TRIGGER reject_reconciliation_ledger
            BEFORE INSERT ON archive_reconciliation
            BEGIN
              SELECT RAISE(ABORT, 'synthetic ledger failure');
            END
            """
        )
        conn.commit()

    with pytest.raises(sqlite3.IntegrityError, match="synthetic ledger failure"):
        cli_archive_reconcile.run_reconciliation(db_path, fit_dir, apply=True)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM activity_trackpoints").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM archive_reconciliation").fetchone()[0] == 0
        assert _baseline_row(conn) is None
