"""Intentional RED contracts for Phase 3M.9B runtime enforcement.

These tests describe the missing production behavior.  They are intentionally
kept separate from the green characterization module so they can be run and
reported as the implementation boundary for the next phase.
"""

from __future__ import annotations

import importlib.metadata
import shutil
import sys
from pathlib import Path

import pytest

from garmin_data_hub import cli_backup_ingest
from garmin_data_hub.db import sqlite as db_sqlite


PINNED_UPSTREAM_VERSION = "0.1.12"


def test_source_runtime_uses_current_python_not_global_path_launcher(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    global_launcher = tmp_path / "global" / "garmin-givemydata.exe"
    monkeypatch.setattr(cli_backup_ingest.sys, "frozen", False, raising=False)
    monkeypatch.setattr(shutil, "which", lambda _name: str(global_launcher))

    command = cli_backup_ingest._find_givemydata_cmd()

    assert command == [sys.executable, "-I", "-u", "-m", "garmin_givemydata"], (
        "source execution must bind garmin-givemydata to the same Python "
        "environment as Garmin Data Hub"
    )


def test_version_mismatch_fails_before_process_launch_or_writable_database(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    launched: list[list[str]] = []
    opened_writable: list[Path] = []
    monkeypatch.setattr(cli_backup_ingest, "ensure_app_dirs", lambda: None)
    monkeypatch.setattr(
        cli_backup_ingest,
        "_find_givemydata_cmd",
        lambda: [sys.executable, "-m", "garmin_givemydata"],
    )
    monkeypatch.setattr(
        importlib.metadata,
        "version",
        lambda name: "0.1.13" if name == "garmin-givemydata" else "unknown",
    )
    monkeypatch.setattr(
        cli_backup_ingest.subprocess,
        "run",
        lambda command, **_kwargs: launched.append(command),
    )

    def writable_connect(path: Path):
        opened_writable.append(Path(path))
        raise AssertionError("runtime mismatch reached writable DB open")

    monkeypatch.setattr(db_sqlite, "connect_sqlite", writable_connect)
    db_path = tmp_path / "data" / "garmin.db"

    result = cli_backup_ingest.run_sync(db_path)
    output = capsys.readouterr().out.casefold()

    assert result != 0
    assert launched == []
    assert opened_writable == []
    assert PINNED_UPSTREAM_VERSION in output
    assert "0.1.13" in output
    assert "version" in output and ("mismatch" in output or "requires" in output)


def test_missing_environment_dependency_never_falls_back_to_global_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    global_launcher = tmp_path / "global" / "garmin-givemydata.exe"
    launched: list[list[str]] = []
    opened_writable: list[Path] = []
    monkeypatch.setattr(cli_backup_ingest, "ensure_app_dirs", lambda: None)
    monkeypatch.setattr(shutil, "which", lambda _name: str(global_launcher))

    def missing(name: str) -> str:
        if name == "garmin-givemydata":
            raise importlib.metadata.PackageNotFoundError(name)
        return "unknown"

    monkeypatch.setattr(importlib.metadata, "version", missing)
    monkeypatch.setattr(
        cli_backup_ingest.subprocess,
        "run",
        lambda command, **_kwargs: launched.append(command),
    )

    def writable_connect(path: Path):
        opened_writable.append(Path(path))
        raise AssertionError("missing dependency reached writable DB open")

    monkeypatch.setattr(db_sqlite, "connect_sqlite", writable_connect)
    db_path = tmp_path / "data" / "garmin.db"

    result = cli_backup_ingest.run_sync(db_path)
    output = capsys.readouterr().out.casefold()

    assert result != 0
    assert launched == []
    assert opened_writable == []
    assert str(global_launcher).casefold() not in output
    assert "same python environment" in output or "not installed" in output
