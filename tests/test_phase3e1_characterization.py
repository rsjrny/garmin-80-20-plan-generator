"""Green characterizations for the Phase 3E privacy boundary.

These tests preserve the supported canonical-sync behavior while Phase 3E
hardens logging, output, and child-environment handling.  They use only
synthetic credentials, injected subprocesses, and temporary files.
"""

from __future__ import annotations

import importlib.metadata
import subprocess
import sys
from pathlib import Path

import pytest

from garmin_data_hub import cli_backup_ingest
from garmin_data_hub import paths as hub_paths
from garmin_data_hub.analytics import post_sync_refresh
from garmin_data_hub.db import migrate as db_migrate
from garmin_data_hub.db import sqlite as db_sqlite
from garmin_data_hub.services.garmin_credentials import (
    GarminCredentials,
    load_credentials,
)
from garmin_data_hub.ui_nicegui import data as nicegui_data


SYNTHETIC_USER = "SYNTHETIC_PHASE3E_USER@example.invalid"
SYNTHETIC_PASSWORD = "SYNTHETIC_PHASE3E_PASSWORD_DO_NOT_LOG"
SYNTHETIC_ACCESS_TOKEN = "SYNTHETIC_PHASE3E_ACCESS_TOKEN_DO_NOT_LOG"
SYNTHETIC_REFRESH_TOKEN = "SYNTHETIC_PHASE3E_REFRESH_TOKEN_DO_NOT_LOG"
SYNTHETIC_UNRELATED_SECRET = "DO_NOT_INHERIT_PHASE3E"
PINNED_UPSTREAM_VERSION = "0.1.12"


class _FakeConnection:
    def close(self) -> None:
        pass


def _patch_successful_post_sync(
    monkeypatch: pytest.MonkeyPatch,
    *,
    events: list[str] | None = None,
) -> None:
    recorded = events if events is not None else []
    monkeypatch.setattr(cli_backup_ingest, "ensure_app_dirs", lambda: None)
    monkeypatch.setattr(
        cli_backup_ingest,
        "_snapshot_fit_archives",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        db_sqlite,
        "connect_sqlite",
        lambda _path: recorded.append("connect") or _FakeConnection(),
    )
    monkeypatch.setattr(
        db_migrate,
        "apply_schema",
        lambda _conn, _schema: recorded.append("schema"),
    )
    monkeypatch.setattr(
        cli_backup_ingest,
        "_run_changed_archive_ingestion",
        lambda _conn, _fit_dir, _paths: recorded.append("archive_ingestion")
        or {"errors": 0, "updated_activity_ids": []},
    )
    monkeypatch.setattr(
        cli_backup_ingest,
        "_run_historical_archive_reconciliation",
        lambda _conn, _fit_dir, _paths: recorded.append("reconciliation")
        or {"errors": 0, "warnings": 0, "updated_activity_ids": []},
    )
    monkeypatch.setattr(
        post_sync_refresh,
        "refresh_post_sync_tables",
        lambda _conn, **_kwargs: recorded.append("profile_metric_threshold_refresh")
        or {"errors": 0},
    )
    monkeypatch.setattr(
        hub_paths,
        "schema_sql_path",
        lambda: Path("synthetic-schema.sql"),
    )


def test_canonical_runtime_remains_pinned_isolated_and_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[list[str], dict[str, object]]] = []
    monkeypatch.setattr(cli_backup_ingest.sys, "frozen", False, raising=False)
    _patch_successful_post_sync(monkeypatch)
    for name, value in (
        ("GARMIN_EMAIL", SYNTHETIC_USER),
        ("GARMIN_PASSWORD", SYNTHETIC_PASSWORD),
        ("PHASE3E_ACCESS_TOKEN", SYNTHETIC_ACCESS_TOKEN),
        ("PHASE3E_REFRESH_TOKEN", SYNTHETIC_REFRESH_TOKEN),
        ("PHASE3E_UNRELATED_PARENT_SECRET", SYNTHETIC_UNRELATED_SECRET),
    ):
        monkeypatch.setenv(name, value)

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess:
        calls.append((list(command), dict(kwargs)))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fake_run)

    assert importlib.metadata.version("garmin-givemydata") == PINNED_UPSTREAM_VERSION
    assert cli_backup_ingest.run_sync(
        tmp_path / "garmin.db",
        days=14,
        extra_args=["--profile", "health", "--no-trackpoints"],
    ) == 0

    assert len(calls) == 1
    command, kwargs = calls[0]
    assert command[0] == sys.executable
    assert "-I" in command and "-u" in command
    assert command.count("--no-trackpoints") == 1
    assert command[command.index("--days") + 1] == "14"
    assert command[command.index("--profile") + 1] == "health"
    assert kwargs["check"] is True
    assert all(
        sensitive not in argument
        for sensitive in (
            SYNTHETIC_USER,
            SYNTHETIC_PASSWORD,
            SYNTHETIC_ACCESS_TOKEN,
            SYNTHETIC_REFRESH_TOKEN,
            SYNTHETIC_UNRELATED_SECRET,
        )
        for argument in command
    )


@pytest.mark.parametrize(
    "arguments",
    [
        ["--rebuild-trackpoints"],
        ["--import-json", "synthetic.json"],
        ["--db", "private.sqlite"],
        [cli_backup_ingest._BUNDLED_GIVEMYDATA_FLAG],
    ],
)
def test_canonical_runtime_rejects_private_rebuild_and_import_forwarding(
    arguments: list[str],
) -> None:
    with pytest.raises(ValueError):
        cli_backup_ingest._validate_upstream_sync_args(arguments)


def test_credentials_come_from_injected_application_store_not_argv(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    credentials = GarminCredentials(SYNTHETIC_USER, SYNTHETIC_PASSWORD)
    captured: dict[str, object] = {}

    class FakeStore:
        backend_name = "Synthetic approved credential store"

        def __init__(self) -> None:
            self.load_calls = 0

        def load(self) -> GarminCredentials:
            self.load_calls += 1
            return credentials

        def save(self, _credentials: GarminCredentials) -> None:
            raise AssertionError("characterization unexpectedly saved credentials")

        def delete(self) -> bool:
            raise AssertionError("characterization unexpectedly deleted credentials")

    class FakeProcess:
        pid = 31_001

        def poll(self) -> None:
            return None

    class FakeTree:
        warning = None

    store = FakeStore()
    approved = load_credentials(store=store)
    assert approved is credentials

    def fake_popen(command: list[str], **kwargs: object) -> FakeProcess:
        captured["command"] = list(command)
        captured["environment"] = dict(kwargs["env"])
        return FakeProcess()

    job = nicegui_data.SyncJob(tmp_path / "garmin.db")
    monkeypatch.setattr(nicegui_data.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(nicegui_data, "attach_process_tree", lambda _process: FakeTree())
    monkeypatch.setattr(job, "_start_monitor_locked", lambda *_args: None)

    job.start(days=7, credentials=approved)

    command = captured["command"]
    environment = captured["environment"]
    assert store.load_calls == 1
    assert environment["GARMIN_EMAIL"] == SYNTHETIC_USER
    assert environment["GARMIN_PASSWORD"] == SYNTHETIC_PASSWORD
    assert all(SYNTHETIC_USER not in item for item in command)
    assert all(SYNTHETIC_PASSWORD not in item for item in command)
    persisted_command = job.log_path.read_text(encoding="utf-8")
    assert SYNTHETIC_USER not in persisted_command
    assert SYNTHETIC_PASSWORD not in persisted_command


def test_successful_sync_preserves_archive_ingest_reconcile_and_refresh_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    events: list[str] = []
    _patch_successful_post_sync(monkeypatch, events=events)
    monkeypatch.setattr(
        cli_backup_ingest,
        "_find_givemydata_cmd",
        lambda: [sys.executable, "-I", "-u", "-m", "garmin_givemydata"],
    )

    def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess:
        events.append("upstream")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fake_run)

    assert cli_backup_ingest.run_sync(tmp_path / "garmin.db") == 0
    assert events == [
        "upstream",
        "connect",
        "schema",
        "archive_ingestion",
        "reconciliation",
        "profile_metric_threshold_refresh",
    ]
    assert capsys.readouterr().out.rstrip().endswith("[SUCCESS] Sync complete")


def test_upstream_failure_propagates_without_false_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(cli_backup_ingest, "ensure_app_dirs", lambda: None)
    monkeypatch.setattr(
        cli_backup_ingest,
        "_snapshot_fit_archives",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        cli_backup_ingest,
        "_find_givemydata_cmd",
        lambda: [sys.executable, "-I", "-u", "-m", "garmin_givemydata"],
    )

    def fail_worker(command: list[str], **_kwargs: object) -> None:
        raise subprocess.CalledProcessError(23, command)

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fail_worker)
    monkeypatch.setattr(
        db_sqlite,
        "connect_sqlite",
        lambda _path: pytest.fail("worker failure reached app-owned ingestion"),
    )

    assert cli_backup_ingest.run_sync(tmp_path / "garmin.db") == 23
    output = capsys.readouterr().out
    assert "failed (exit 23)" in output
    assert "[OK] Sync completed" not in output
    assert "[SUCCESS] Sync complete" not in output
