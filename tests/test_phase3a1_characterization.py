"""Passing characterizations for the Phase 3A canonical-sync boundary.

These tests deliberately exercise the installed, pinned upstream dependency as
well as application behavior that is already correct.  Required Phase 3A
behavior that production does not implement yet lives in
``test_phase3a1_contracts.py`` and is intentionally red until Phase 3A.2.
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from datetime import date
from importlib.metadata import version
from pathlib import Path

import pytest

from garmin_data_hub import cli_backup_ingest
from garmin_data_hub.mcp_sidecar_client import call_readonly_tools
from garmin_data_hub.services.garmin_credentials import GarminCredentials
from garmin_data_hub.ui_nicegui import data as nicegui_data


PINNED_UPSTREAM_VERSION = "0.1.12"


def _trackpoint_row(seq: int, timestamp: str) -> tuple:
    return (seq, timestamp, None, None, None, None, None, None, None, None, None)


def _trackpoint_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
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
        )
        """
    )


def _patch_successful_post_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeConnection:
        def close(self) -> None:
            pass

    monkeypatch.setattr(cli_backup_ingest, "ensure_app_dirs", lambda: None)
    monkeypatch.setattr(
        "garmin_data_hub.db.sqlite.connect_sqlite", lambda _path: FakeConnection()
    )
    monkeypatch.setattr(
        "garmin_data_hub.db.migrate.apply_schema", lambda _conn, _schema: None
    )
    monkeypatch.setattr(
        "garmin_data_hub.analytics.post_sync_refresh.refresh_post_sync_tables",
        lambda _conn, **_kwargs: {"errors": 0},
    )
    monkeypatch.setattr(
        "garmin_data_hub.paths.schema_sql_path", lambda: Path("schema.sql")
    )


def test_pinned_upstream_can_commit_a_failed_replacement_when_later_work_succeeds():
    """Characterize the exact 0.1.12 deletion/partial-insert defect."""
    assert version("garmin-givemydata") == PINNED_UPSTREAM_VERSION, (
        "Re-audit the upstream transaction contract before changing the pinned version"
    )

    from garmin_mcp.db import save_to_db

    conn = sqlite3.connect(":memory:")
    try:
        _trackpoint_table(conn)
        conn.execute(
            "INSERT INTO activity_trackpoints(activity_id, seq, timestamp_utc) "
            "VALUES (1, 99, 'old-complete-row')"
        )
        conn.commit()

        duplicate_sequence = [
            _trackpoint_row(1, "new-partial-row"),
            _trackpoint_row(1, "duplicate-that-fails"),
        ]
        assert (
            save_to_db(
                conn,
                "activity_trackpoints",
                duplicate_sequence,
                cal_date="1",
            )
            == 0
        )
        assert conn.in_transaction is True
        assert conn.execute(
            "SELECT seq, timestamp_utc FROM activity_trackpoints "
            "WHERE activity_id = 1"
        ).fetchall() == [(1, "new-partial-row")]

        assert (
            save_to_db(
                conn,
                "activity_trackpoints",
                [_trackpoint_row(1, "activity-2-valid")],
                cal_date="2",
            )
            == 1
        )
        assert conn.in_transaction is False

        # A rollback after the later commit cannot restore Activity 1.
        conn.rollback()
        assert conn.execute(
            "SELECT seq, timestamp_utc FROM activity_trackpoints "
            "WHERE activity_id = 1"
        ).fetchall() == [(1, "new-partial-row")]
        assert conn.execute(
            "SELECT seq, timestamp_utc FROM activity_trackpoints "
            "WHERE activity_id = 2"
        ).fetchall() == [(1, "activity-2-valid")]
    finally:
        conn.close()


def test_pinned_upstream_no_trackpoints_switch_prevents_new_fit_processing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    """Behaviorally guard the explicit extension seam selected by Phase 3A."""
    assert version("garmin-givemydata") == PINNED_UPSTREAM_VERSION, (
        "Re-audit --no-trackpoints before changing the pinned version"
    )

    import garmin_givemydata as upstream

    activity_id = 12_345_678
    parser_calls: list[Path] = []

    class FakeConnection:
        def close(self) -> None:
            pass

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def login(self) -> bool:
            return True

        def fetch_all(self, **_kwargs) -> None:
            pass

        def download_file(self, _api_path: str) -> bytes:
            return b"synthetic FIT archive payload"

        def close(self) -> None:
            pass

    def fake_query(_conn, sql: str, _params=None):
        if "FROM activity_splits" in sql:
            return []
        if "sqlite_master" in sql:
            return []
        if "FROM activity WHERE start_time_local IS NOT NULL" in sql:
            return [
                {
                    "activity_id": activity_id,
                    "activity_name": "Contract Run",
                    "start_time_local": "2026-09-27 08:00:00",
                }
            ]
        raise AssertionError(f"Unexpected upstream query: {sql}")

    status = {
        "exists": True,
        "rows": 1,
        "last_date": date.today().isoformat(),
        "first_date": date.today().isoformat(),
    }
    monkeypatch.setattr(upstream, "DATA_DIR", tmp_path)
    monkeypatch.setattr(upstream, "PROFILE_DIR", tmp_path / "browser_profile")
    monkeypatch.setattr(upstream, "SESSION_FILE", tmp_path / "garmin_session.json")
    monkeypatch.setattr(upstream, "GarminClient", FakeClient)
    monkeypatch.setattr(upstream, "load_env", lambda: None)
    monkeypatch.setattr(upstream, "get_db_status", lambda: dict(status))
    monkeypatch.setattr(upstream, "get_connection", lambda: FakeConnection())
    monkeypatch.setattr(upstream, "init_db", lambda _conn: None)
    monkeypatch.setattr(upstream, "db_query", fake_query)
    monkeypatch.setattr(upstream, "_log_sync", lambda *_args: None)
    monkeypatch.setattr(
        upstream,
        "_save_trackpoints_from_fit",
        lambda _conn, path: parser_calls.append(Path(path)) or ("ingested", 1),
    )
    monkeypatch.setenv("GARMIN_EMAIL", "contract@example.com")
    monkeypatch.setenv("GARMIN_PASSWORD", "not-a-real-password")
    monkeypatch.setattr(
        sys,
        "argv",
        ["garmin-givemydata", "--no-trackpoints"],
    )

    upstream.main()

    assert len(list((tmp_path / "fit").glob("*.zip"))) == 1
    assert parser_calls == []


def test_user_supplied_no_trackpoints_is_not_duplicated(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    """Keep exact-once forwarding safe while Phase 3A adds the default flag."""
    _patch_successful_post_sync(monkeypatch)
    recorded: dict[str, object] = {}
    monkeypatch.setattr(
        cli_backup_ingest, "_find_givemydata_cmd", lambda: ["upstream"]
    )

    def fake_run(command, **kwargs):
        recorded["command"] = list(command)
        recorded["kwargs"] = kwargs
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fake_run)
    db_path = tmp_path / "garmin.db"

    assert (
        cli_backup_ingest.run_sync(
            db_path,
            days=14,
            extra_args=["--no-trackpoints"],
        )
        == 0
    )
    command = recorded["command"]
    assert isinstance(command, list)
    assert command.count("--no-trackpoints") == 1
    assert command[command.index("--days") + 1] == "14"
    assert recorded["kwargs"]["env"]["GARMIN_DATA_DIR"] == str(tmp_path)


def test_upstream_process_failure_skips_all_application_post_sync_work(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    events: list[str] = []
    monkeypatch.setattr(cli_backup_ingest, "ensure_app_dirs", lambda: None)
    monkeypatch.setattr(
        cli_backup_ingest, "_find_givemydata_cmd", lambda: ["upstream"]
    )

    def fail_upstream(command, **_kwargs):
        events.append("upstream")
        raise subprocess.CalledProcessError(17, command)

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fail_upstream)
    monkeypatch.setattr(
        "garmin_data_hub.db.sqlite.connect_sqlite",
        lambda _path: pytest.fail("post-sync connection opened after upstream failure"),
    )

    assert cli_backup_ingest.run_sync(tmp_path / "garmin.db") == 17
    assert events == ["upstream"]


def test_required_metric_refresh_error_prevents_terminal_success(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys
):
    _patch_successful_post_sync(monkeypatch)
    monkeypatch.setattr(
        cli_backup_ingest, "_find_givemydata_cmd", lambda: ["upstream"]
    )
    monkeypatch.setattr(
        cli_backup_ingest.subprocess,
        "run",
        lambda command, **_kwargs: subprocess.CompletedProcess(command, 0),
    )
    monkeypatch.setattr(
        "garmin_data_hub.analytics.post_sync_refresh.refresh_post_sync_tables",
        lambda _conn, **_kwargs: {"errors": 1},
    )

    assert cli_backup_ingest.run_sync(tmp_path / "garmin.db") != 0
    assert "[SUCCESS] Sync complete" not in capsys.readouterr().out


def test_readonly_garmin_sync_status_does_not_write_or_start_a_job(tmp_path: Path):
    from garmin_mcp.db import get_connection, init_db

    db_path = tmp_path / "custom-status.sqlite"
    conn = get_connection(str(db_path))
    try:
        init_db(conn)
    finally:
        conn.close()
    before = db_path.read_bytes()

    output = call_readonly_tools(
        db_path,
        [("garmin_sync", {"refresh": False})],
    )["garmin_sync"]

    payload = json.loads(output["text"])
    assert output["is_error"] is False
    assert payload["sync_state"]["running"] is False
    assert db_path.read_bytes() == before


def test_database_scoped_sync_job_registry_shares_identity_and_snapshot(
    tmp_path: Path,
):
    db_path = tmp_path / "garmin.db"
    first = nicegui_data.get_sync_job(db_path)
    second = nicegui_data.get_sync_job(db_path.resolve())

    assert first is second
    assert first.snapshot() == second.snapshot()


def test_shared_job_rejects_duplicate_start_and_cancels_the_same_process_tree(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    spawned: list[object] = []

    class FakeProcess:
        pid = 24_681

        def __init__(self):
            self.return_code: int | None = None

        def poll(self):
            return self.return_code

    class FakeTree:
        warning = None

        def __init__(self):
            self.terminate_calls: list[tuple[object, float]] = []
            self.close_calls = 0

        def terminate(self, process, *, timeout: float) -> int:
            self.terminate_calls.append((process, timeout))
            process.return_code = -9
            return -9

        def close_after_exit(self) -> None:
            self.close_calls += 1

    process = FakeProcess()
    process_tree = FakeTree()

    def fake_popen(_command, **_kwargs):
        spawned.append(process)
        return process

    db_path = tmp_path / "garmin.db"
    sync_page_job = nicegui_data.get_sync_job(db_path)
    data_query_job = nicegui_data.get_sync_job(db_path.resolve())
    monkeypatch.setattr(nicegui_data.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(
        nicegui_data, "attach_process_tree", lambda _process: process_tree
    )
    monkeypatch.setattr(sync_page_job, "_start_monitor_locked", lambda *_args: None)

    sync_page_job.start(days=7)
    with pytest.raises(RuntimeError, match="already running"):
        data_query_job.start(days=7)

    assert spawned == [process]
    assert data_query_job.snapshot().state == "running"
    assert sync_page_job.cancel(wait=True) is True
    terminal = data_query_job.snapshot()
    assert process_tree.terminate_calls == [
        (process, sync_page_job._STOP_TIMEOUT_SECONDS)
    ]
    assert process_tree.close_calls == 1
    assert terminal.state == "cancelled"
    assert terminal.return_code == -9
    assert terminal.state != "completed"


def test_sync_job_keeps_credentials_out_of_argv_and_logged_command(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    captured: dict[str, object] = {}

    class FakeProcess:
        pid = 24680

        def poll(self):
            return None

    class FakeTree:
        warning = None

    def fake_popen(command, **kwargs):
        captured["command"] = list(command)
        captured["environment"] = dict(kwargs["env"])
        return FakeProcess()

    job = nicegui_data.SyncJob(tmp_path / "garmin.db")
    monkeypatch.setattr(nicegui_data.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(nicegui_data, "attach_process_tree", lambda _process: FakeTree())
    monkeypatch.setattr(job, "_start_monitor_locked", lambda *_args: None)
    secret = "phase3a-secret-must-not-leak"

    job.start(
        days=7,
        credentials=GarminCredentials("athlete@example.com", secret),
    )

    command = captured["command"]
    environment = captured["environment"]
    log_text = job.log_path.read_text(encoding="utf-8")
    assert isinstance(command, list)
    assert isinstance(environment, dict)
    assert environment["GARMIN_PASSWORD"] == secret
    assert all(secret not in argument for argument in command)
    assert secret not in log_text
