"""Regression tests for the frozen Garmin sync self-dispatch contract."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

from garmin_data_hub import cli_backup_ingest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _fake_givemydata(main) -> ModuleType:
    module = ModuleType("garmin_givemydata")
    module.main = main
    return module


def test_frozen_cli_dispatches_givemydata_through_its_own_executable(
    monkeypatch, tmp_path
):
    executable = tmp_path / "cli_backup_ingest.exe"
    monkeypatch.setattr(cli_backup_ingest.sys, "frozen", True, raising=False)
    monkeypatch.setattr(cli_backup_ingest.sys, "executable", str(executable))

    assert cli_backup_ingest._find_givemydata_cmd() == [
        str(executable),
        cli_backup_ingest._BUNDLED_GIVEMYDATA_FLAG,
    ]


def test_bundled_givemydata_forwards_arguments_and_restores_sys_argv(
    monkeypatch, tmp_path
):
    from seleniumbase.core import browser_launcher

    seen_argv: list[str] = []
    seen_driver_dirs: list[str] = []

    def fake_main():
        seen_argv.extend(sys.argv)

    original_argv = ["cli_backup_ingest.exe", "--outer-option"]
    monkeypatch.setenv("GARMIN_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(cli_backup_ingest.sys, "argv", original_argv)
    monkeypatch.setattr(
        browser_launcher,
        "override_driver_dir",
        lambda path: seen_driver_dirs.append(path),
    )
    monkeypatch.setitem(
        sys.modules,
        "garmin_givemydata",
        _fake_givemydata(fake_main),
    )

    result = cli_backup_ingest._run_bundled_givemydata(
        ["--days", "14", "--visible"]
    )

    assert result == 0
    assert seen_argv == [
        "garmin-givemydata",
        "--days",
        "14",
        "--visible",
        "--no-trackpoints",
    ]
    assert seen_driver_dirs == [str(tmp_path / "drivers")]
    assert (tmp_path / "drivers").is_dir()
    assert cli_backup_ingest.sys.argv is original_argv


@pytest.mark.parametrize(
    ("exit_code", "expected_result"),
    [(None, 0), (7, 7), ("invalid command line", 1)],
)
def test_bundled_givemydata_normalizes_system_exit(
    monkeypatch, tmp_path, exit_code, expected_result
):
    from seleniumbase.core import browser_launcher

    def fake_main():
        raise SystemExit(exit_code)

    original_argv = ["cli_backup_ingest.exe"]
    monkeypatch.setenv("GARMIN_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(cli_backup_ingest.sys, "argv", original_argv)
    monkeypatch.setattr(browser_launcher, "override_driver_dir", lambda _path: None)
    monkeypatch.setitem(
        sys.modules,
        "garmin_givemydata",
        _fake_givemydata(fake_main),
    )

    assert cli_backup_ingest._run_bundled_givemydata([]) == expected_result
    assert cli_backup_ingest.sys.argv is original_argv


def test_main_routes_private_givemydata_flag_before_app_argument_parsing(monkeypatch):
    received: list[str] = []

    def fake_dispatch(arguments: list[str]) -> int:
        received.extend(arguments)
        return 9

    monkeypatch.setattr(
        cli_backup_ingest,
        "_run_bundled_givemydata",
        fake_dispatch,
    )
    monkeypatch.setattr(
        cli_backup_ingest.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail(
            "private dispatch flag reached the normal sync subprocess path"
        ),
    )
    monkeypatch.setattr(
        cli_backup_ingest.sys,
        "argv",
        [
            "cli_backup_ingest.exe",
            cli_backup_ingest._BUNDLED_GIVEMYDATA_FLAG,
            "--profile",
            "health",
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        cli_backup_ingest.main()

    assert exc_info.value.code == 9
    assert received == ["--profile", "health"]


def test_cli_pyinstaller_bundle_contains_sync_runtime_not_venv_launcher():
    script = (PROJECT_ROOT / "packaging" / "build.ps1").read_text(
        encoding="utf-8"
    )
    cli_arguments_match = re.search(
        r"\$pyinstallerArgsCli\s*=\s*@\((.*?)\n\)",
        script,
        flags=re.DOTALL,
    )
    assert cli_arguments_match, "CLI PyInstaller argument block was not found"
    cli_arguments = cli_arguments_match.group(1)

    for module_name in (
        "garmin_givemydata",
        "garmin_client",
        "garmin_mcp",
        "seleniumbase",
    ):
        inclusion = re.compile(
            rf'"--(?:hidden-import|collect-all|collect-submodules)"\s*,\s*"{module_name}"'
        )
        assert inclusion.search(cli_arguments), (
            f"CLI PyInstaller build does not explicitly include {module_name}"
        )

    assert "garmin-givemydata.exe" not in script.lower(), (
        "build.ps1 must not copy the environment-specific pip launcher"
    )


def test_run_sync_keeps_upstream_sync_in_a_child_process(monkeypatch, tmp_path):
    expected_command = [
        str(tmp_path / "cli_backup_ingest.exe"),
        cli_backup_ingest._BUNDLED_GIVEMYDATA_FLAG,
    ]
    recorded: dict[str, object] = {}

    monkeypatch.setattr(cli_backup_ingest, "ensure_app_dirs", lambda: None)
    monkeypatch.setattr(
        cli_backup_ingest,
        "_find_givemydata_cmd",
        lambda: expected_command.copy(),
    )

    def fake_run(command, **kwargs):
        recorded["command"] = command
        recorded["kwargs"] = kwargs
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fake_run)

    class FakeConnection:
        def close(self):
            recorded["closed"] = True

    monkeypatch.setattr(
        "garmin_data_hub.db.sqlite.connect_sqlite",
        lambda _path: FakeConnection(),
    )
    monkeypatch.setattr(
        "garmin_data_hub.db.migrate.apply_schema",
        lambda _conn, _schema: None,
    )
    monkeypatch.setattr(
        "garmin_data_hub.analytics.post_sync_refresh.refresh_post_sync_tables",
        lambda _conn, **_kwargs: {"errors": 0},
    )
    monkeypatch.setattr(
        "garmin_data_hub.paths.schema_sql_path",
        lambda: tmp_path / "schema.sql",
    )

    db_path = tmp_path / "data" / "garmin.db"
    assert (
        cli_backup_ingest.run_sync(
            db_path,
            days=14,
            visible=True,
            extra_args=["--profile", "health"],
        )
        == 0
    )

    assert recorded["command"] == [
        *expected_command,
        "--days",
        "14",
        "--visible",
        "--profile",
        "health",
        "--no-trackpoints",
    ]
    assert recorded["kwargs"]["check"] is True
    assert recorded["kwargs"]["cwd"] == db_path.parent
    assert recorded["kwargs"]["env"]["GARMIN_DATA_DIR"] == str(db_path.parent)
    assert recorded["closed"] is True
