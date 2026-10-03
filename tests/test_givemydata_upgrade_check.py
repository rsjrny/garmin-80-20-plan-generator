"""Safety and compatibility regressions for the isolated upgrade checker."""
from __future__ import annotations

import contextlib
import importlib.util
from pathlib import Path
import sqlite3
import sys

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check_givemydata_upgrade.py"
spec = importlib.util.spec_from_file_location("upgrade_checker", SCRIPT)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


@pytest.fixture
def source(tmp_path):
    from garmin_mcp.db import init_db
    from garmin_data_hub.db.migrate import apply_schema
    directory = tmp_path / "production"
    directory.mkdir()
    database = directory / "garmin.db"
    with contextlib.closing(sqlite3.connect(database)) as conn:
        conn.row_factory = sqlite3.Row
        init_db(conn)
        apply_schema(conn)
        conn.execute(
            "INSERT INTO activity(activity_id, activity_type, start_time_gmt, "
            "distance_meters, elapsed_duration_seconds, average_hr, max_hr) "
            "VALUES (123, 'running', '2026-01-02T12:00:00', 5000, 1800, 140, 165)"
        )
        conn.execute(
            "INSERT INTO planned_workout(planned_workout_id, scheduled_date, workout_name) "
            "VALUES (1, '2026-01-03', 'Preserve this plan')"
        )
        conn.commit()
    return database


def fake_candidate(monkeypatch, transform=None):
    calls = []

    def run(python, mode, database, result_file, **kwargs):
        from garmin_mcp.db import init_db
        calls.append((mode, database))
        with contextlib.closing(sqlite3.connect(database)) as conn:
            conn.row_factory = sqlite3.Row
            init_db(conn)
            if mode == "sync":
                conn.execute("INSERT INTO sync_log(status) VALUES ('ok')")
            if transform:
                transform(conn, database)
            conn.commit()
        return {"version": "0.1.13", "status": "completed"}

    monkeypatch.setattr(checker, "run_worker", run)
    return calls


def test_real_offline_audit_preserves_source_and_exercises_app(source, tmp_path):
    original = source.read_bytes()
    run, report = checker.audit(source, Path(sys.executable), tmp_path / "audit")
    assert report["status"] == "checks_passed_review_required", report["problems"]
    assert not report["version_changed"]
    assert not report["live_sync_completed"]
    assert source.read_bytes() == original
    assert report["offline_app_smoke"]["checks"]
    assert (run / "backup.sqlite").is_file()
    assert (run / "report.md").is_file()
    assert all(c["status"] == "passed" for c in report["offline_app_smoke"]["checks"])


def test_fresh_schema_catches_removal_hidden_by_existing_tables(source, tmp_path, monkeypatch):
    original = source.read_bytes()

    def transform(conn, database):
        if database.parent.name == "fresh":
            conn.execute("ALTER TABLE activity DROP COLUMN distance_meters")

    calls = fake_candidate(monkeypatch, transform)
    run, report = checker.audit(source, Path(sys.executable), tmp_path / "audit", sync=True)
    assert report["status"] == "failed"
    assert "distance_meters" in report["fresh_schema_diff"]["changed"]["activity"]["removed_columns"]
    assert "Incompatible table change: activity" in report["problems"]
    assert not any(mode == "sync" for mode, _ in calls)
    assert source.read_bytes() == original
    assert (run / "report.json").is_file()


def test_app_data_loss_is_detected_before_live_sync(source, tmp_path, monkeypatch):
    def transform(conn, database):
        if database.parent.name == "candidate":
            conn.execute("DELETE FROM planned_workout")

    calls = fake_candidate(monkeypatch, transform)
    _, report = checker.audit(source, Path(sys.executable), tmp_path / "audit", sync=True)
    assert "Candidate changed or removed app-owned rows" in report["problems"]
    assert report["status"] == "failed"
    assert not any(mode == "sync" for mode, _ in calls)
    with contextlib.closing(checker.read_only(source)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM planned_workout").fetchone()[0] == 1


def test_activity_identity_loss_is_detected(source, tmp_path, monkeypatch):
    def transform(conn, database):
        if database.parent.name == "candidate":
            conn.execute("DELETE FROM activity")
    fake_candidate(monkeypatch, transform)
    _, report = checker.audit(source, Path(sys.executable), tmp_path / "audit")
    assert "Candidate removed existing activity identities: 1" in report["problems"]
    assert report["status"] == "failed"


def test_failed_worker_reports_failure_and_retains_backup(source, tmp_path, monkeypatch):
    def fail(*args, **kwargs):
        raise RuntimeError("password=must-not-appear")
    monkeypatch.setattr(checker, "run_worker", fail)
    run, report = checker.audit(source, Path(sys.executable), tmp_path / "audit")
    assert report["status"] == "failed"
    assert (run / "backup.sqlite").is_file()
    assert "must-not-appear" not in (run / "report.json").read_text()
    assert "fresh candidate initialization" in report["problems"][0]


def test_successful_candidate_live_sync_is_followed_by_app_checks(source, tmp_path, monkeypatch):
    calls = fake_candidate(monkeypatch)
    _, report = checker.audit(source, Path(sys.executable), tmp_path / "audit", sync=True)
    assert report["status"] == "checks_passed_review_required", report["problems"]
    assert report["version_changed"]
    assert report["live_sync_completed"]
    assert "post_sync_app_smoke" in report
    assert [mode for mode, _ in calls] == ["init", "init", "sync"]


def test_output_cannot_contain_the_source_database(source):
    with pytest.raises(ValueError, match="separate"):
        checker.audit(source, Path(sys.executable), source.parent)


def test_backup_includes_committed_wal_data(tmp_path):
    source = tmp_path / "wal.sqlite"
    destination = tmp_path / "backup.sqlite"
    with contextlib.closing(sqlite3.connect(source)) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")
        conn.execute("CREATE TABLE evidence(value TEXT)")
        conn.execute("INSERT INTO evidence VALUES ('committed')")
        conn.commit()
        assert Path(str(source) + "-wal").stat().st_size > 0
        checker.backup(source, destination)
        with contextlib.closing(checker.read_only(destination)) as copied:
            assert copied.execute("SELECT value FROM evidence").fetchone() == ("committed",)


def test_failure_after_live_sync_cannot_report_success(source, tmp_path, monkeypatch):
    def run(python, mode, database, result_file, **kwargs):
        from garmin_mcp.db import init_db
        with contextlib.closing(sqlite3.connect(database)) as conn:
            conn.row_factory = sqlite3.Row
            init_db(conn)
            if mode == "sync":
                conn.execute("INSERT INTO sync_log(status) VALUES ('ok')")
                conn.execute("UPDATE planned_workout SET workout_name='Unexpected overwrite'")
            conn.commit()
        return {"version": "0.1.13"}
    monkeypatch.setattr(checker, "run_worker", run)
    _, report = checker.audit(source, Path(sys.executable), tmp_path / "audit", sync=True)
    assert report["live_sync_completed"]
    assert report["status"] == "failed"
    assert "Candidate changed or removed app-owned rows" in report["problems"]
    with contextlib.closing(checker.read_only(source)) as conn:
        assert conn.execute("SELECT workout_name FROM planned_workout").fetchone()[0] == "Preserve this plan"


def test_removed_required_column_cannot_hide_behind_ui_fallback(source, tmp_path, monkeypatch):
    def transform(conn, database):
        conn.execute("ALTER TABLE activity DROP COLUMN distance_meters")
    fake_candidate(monkeypatch, transform)
    _, report = checker.audit(source, Path(sys.executable), tmp_path / "audit")
    assert report["status"] == "failed"
    assert "required activity columns" in report["offline_app_smoke"]["problems"]


def test_candidate_version_drift_fails_before_sync(source, tmp_path, monkeypatch):
    calls = []
    def run(python, mode, database, result_file, **kwargs):
        from garmin_mcp.db import init_db
        calls.append(mode)
        with contextlib.closing(sqlite3.connect(database)) as conn:
            conn.row_factory = sqlite3.Row
            init_db(conn)
        return {"version": "0.1.13" if database.parent.name == "fresh" else "0.1.14"}
    monkeypatch.setattr(checker, "run_worker", run)
    _, report = checker.audit(source, Path(sys.executable), tmp_path / "audit", sync=True)
    assert report["status"] == "failed"
    assert calls == ["init", "init"]
    assert not report["live_sync_completed"]


@pytest.mark.parametrize("failure", ["missing_sync_record", "logged_error"])
def test_zero_exit_is_insufficient_sync_evidence(source, tmp_path, monkeypatch, failure):
    def transform(conn, database):
        if failure == "missing_sync_record":
            conn.execute("DELETE FROM sync_log")
    fake_candidate(monkeypatch, transform)
    original = checker.run_worker

    def run(*args, **kwargs):
        result = original(*args, **kwargs)
        if args[1] == "sync" and failure == "logged_error":
            result["logged_errors"] = 1
        return result

    monkeypatch.setattr(checker, "run_worker", run)
    _, report = checker.audit(source, Path(sys.executable), tmp_path / "audit", sync=True)
    assert report["live_sync_completed"]
    assert report["status"] == "failed"
    expected = ("Candidate did not record a successful sync"
                if failure == "missing_sync_record" else "Candidate logged errors during sync")
    assert expected in report["problems"]
