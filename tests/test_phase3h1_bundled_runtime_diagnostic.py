"""Contracts for the non-network bundled Garmin runtime diagnostic."""

from __future__ import annotations

import importlib.metadata
import socket
import sys
from pathlib import Path
from types import ModuleType

import pytest

from garmin_data_hub import cli_backup_ingest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _install_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    version: str = "0.1.12",
) -> list[str]:
    imported: list[str] = []

    def fake_import_module(name: str) -> ModuleType:
        imported.append(name)
        return ModuleType(name)

    monkeypatch.setattr(cli_backup_ingest.importlib, "import_module", fake_import_module)
    monkeypatch.setattr(
        cli_backup_ingest.importlib.metadata,
        "version",
        lambda name: version
        if name == cli_backup_ingest._GIVEMYDATA_DISTRIBUTION
        else pytest.fail(f"unexpected distribution lookup: {name}"),
    )
    return imported


def test_diagnostic_succeeds_for_exact_bundled_runtime(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    imported = _install_runtime(monkeypatch)

    assert cli_backup_ingest._check_bundled_givemydata_runtime() == 0
    assert imported == ["garmin_givemydata"]
    output = capsys.readouterr().out
    assert "runtime available" in output.lower()
    assert "0.1.12" in output


def test_diagnostic_fails_truthfully_when_runtime_module_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def missing_module(_name: str) -> ModuleType:
        raise ModuleNotFoundError("garmin_givemydata")

    monkeypatch.setattr(cli_backup_ingest.importlib, "import_module", missing_module)

    assert cli_backup_ingest._check_bundled_givemydata_runtime() != 0
    assert "not available" in capsys.readouterr().err.lower()


def test_diagnostic_fails_truthfully_for_wrong_runtime_version(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _install_runtime(monkeypatch, version="0.1.13")

    assert cli_backup_ingest._check_bundled_givemydata_runtime() != 0
    diagnostic = capsys.readouterr().err
    assert "version mismatch" in diagnostic.lower()
    assert "0.1.12" in diagnostic
    assert "0.1.13" in diagnostic


def test_diagnostic_has_no_credentials_network_sync_or_state_side_effects(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_runtime(monkeypatch)
    database = tmp_path / "garmin.db"
    archive = tmp_path / "activity.zip"
    database.write_bytes(b"synthetic database sentinel")
    archive.write_bytes(b"synthetic archive sentinel")
    before = {path: path.read_bytes() for path in (database, archive)}

    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("diagnostic crossed a forbidden runtime boundary")

    monkeypatch.setattr(cli_backup_ingest, "_transient_worker_credentials", forbidden)
    monkeypatch.setattr(cli_backup_ingest, "run_sync", forbidden)
    monkeypatch.setattr(cli_backup_ingest, "default_db_path", forbidden)
    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.chdir(tmp_path)

    assert cli_backup_ingest._check_bundled_givemydata_runtime() == 0
    assert {path: path.read_bytes() for path in (database, archive)} == before
    assert not (tmp_path / "debug.log").exists()


def test_main_routes_diagnostic_before_sync_argument_processing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cli_backup_ingest,
        "_check_bundled_givemydata_runtime",
        lambda: 7,
    )
    monkeypatch.setattr(
        cli_backup_ingest,
        "_validate_upstream_sync_args",
        lambda _args: pytest.fail("diagnostic entered sync argument processing"),
    )
    monkeypatch.setattr(
        cli_backup_ingest.sys,
        "argv",
        [
            "cli_backup_ingest.exe",
            cli_backup_ingest._BUNDLED_GIVEMYDATA_DIAGNOSTIC_FLAG,
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        cli_backup_ingest.main()

    assert exc_info.value.code == 7


def test_normal_worker_still_rejects_help() -> None:
    assert cli_backup_ingest._run_bundled_givemydata(["--help"]) == 2


def test_canonical_build_uses_diagnostic_not_worker_help_probe() -> None:
    script = (PROJECT_ROOT / "packaging" / "build.ps1").read_text(encoding="utf-8")

    assert (
        'Invoke-PackagedSmokeTest -Executable $ExpectedCliExePath '
        '-Arguments @("--_check-bundled-givemydata")'
    ) in script
    assert '@("--_run-bundled-givemydata", "--help")' not in script
