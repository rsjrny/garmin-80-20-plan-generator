"""Focused regressions for deterministic garmin-givemydata execution."""

from __future__ import annotations

import importlib.metadata
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

from garmin_data_hub import cli_backup_ingest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_source_runtime_preserves_python_path_with_spaces_as_one_argument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    python = r"C:\Program Files\Garmin Data Hub\python.exe"
    monkeypatch.setattr(cli_backup_ingest.sys, "frozen", False, raising=False)
    monkeypatch.setattr(cli_backup_ingest.sys, "executable", python)

    assert cli_backup_ingest._find_givemydata_cmd() == [
        python,
        "-I",
        "-u",
        "-m",
        "garmin_givemydata",
    ]


def test_source_runtime_cannot_be_shadowed_by_sync_working_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    marker = tmp_path / "shadow-module-ran"
    (tmp_path / "garmin_givemydata.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(marker)!r}).write_text('executed', encoding='utf-8')\n",
        encoding="utf-8",
    )
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    environment = os.environ.copy()
    environment.pop("GARMIN_EMAIL", None)
    environment.pop("GARMIN_PASSWORD", None)
    environment.pop("PYTHONPATH", None)
    environment["GARMIN_DATA_DIR"] = str(data_dir)
    monkeypatch.setattr(cli_backup_ingest.sys, "frozen", False, raising=False)

    subprocess.run(
        [*cli_backup_ingest._find_givemydata_cmd(), "--help"],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
    )

    assert not marker.exists(), (
        "the checked environment-local distribution was replaced at execution "
        "time by a same-named module in the sync working directory"
    )


def test_source_runtime_rejects_metadata_from_outside_isolated_environment(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(cli_backup_ingest.sys, "frozen", False, raising=False)
    monkeypatch.setattr(importlib.metadata, "version", lambda _name: "0.1.12")
    monkeypatch.setattr(
        cli_backup_ingest,
        "_isolated_givemydata_runtime_version",
        lambda: "0.1.13",
    )

    assert cli_backup_ingest._validate_givemydata_runtime_version() is False
    output = capsys.readouterr().out
    assert "checked 0.1.12" in output
    assert "selected 0.1.13" in output


def test_version_mismatch_precedes_all_writable_sync_setup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        importlib.metadata,
        "version",
        lambda _name: "0.1.13",
    )
    monkeypatch.setattr(
        cli_backup_ingest,
        "ensure_app_dirs",
        lambda: pytest.fail("version mismatch reached application directory setup"),
    )
    monkeypatch.setattr(
        cli_backup_ingest,
        "_snapshot_fit_archives",
        lambda *_args, **_kwargs: pytest.fail(
            "version mismatch reached archive inspection"
        ),
    )
    monkeypatch.setattr(
        cli_backup_ingest.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail("version mismatch launched upstream"),
    )

    assert cli_backup_ingest.run_sync(tmp_path / "data" / "garmin.db") == 1


def test_frozen_worker_checks_bundled_version_before_writable_setup_or_import(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "not-created"
    fake_upstream = ModuleType("garmin_givemydata")
    fake_upstream.main = lambda: pytest.fail("version mismatch reached authentication")
    monkeypatch.setenv("GARMIN_DATA_DIR", str(data_dir))
    monkeypatch.setitem(sys.modules, "garmin_givemydata", fake_upstream)
    monkeypatch.setattr(
        importlib.metadata,
        "version",
        lambda _name: "0.1.13",
    )

    assert cli_backup_ingest._run_bundled_givemydata([]) == 1
    assert not data_dir.exists()


def test_frozen_build_copies_selected_distribution_metadata() -> None:
    script = (PROJECT_ROOT / "packaging" / "build.ps1").read_text(encoding="utf-8")

    assert '"--copy-metadata", "garmin-givemydata"' in script
