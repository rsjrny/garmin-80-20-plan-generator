"""Phase 3E privacy contracts, including intentional RED tests.

The red contracts describe audited privacy behavior that production does not
yet provide.  The harness never authenticates, starts a browser, contacts a
network service, opens a live database, or reads a real credential.
"""

from __future__ import annotations

import contextlib
import logging
import os
import socket
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Callable

import pytest

from garmin_data_hub import cli_backup_ingest
from garmin_data_hub import paths as hub_paths
from garmin_data_hub.analytics import post_sync_refresh
from garmin_data_hub.db import migrate as db_migrate
from garmin_data_hub.db import sqlite as db_sqlite


SYNTHETIC_USER = "SYNTHETIC_PHASE3E_USER@example.invalid"
SYNTHETIC_DISPLAY_NAME = "SYNTHETIC_PHASE3E_DISPLAY_NAME_DO_NOT_LOG"
SYNTHETIC_PASSWORD = "SYNTHETIC_PHASE3E_PASSWORD_DO_NOT_LOG"
SYNTHETIC_ACCESS_TOKEN = "SYNTHETIC_PHASE3E_ACCESS_TOKEN_DO_NOT_LOG"
SYNTHETIC_REFRESH_TOKEN = "SYNTHETIC_PHASE3E_REFRESH_TOKEN_DO_NOT_LOG"
SYNTHETIC_COOKIE = "SYNTHETIC_PHASE3E_COOKIE_DO_NOT_LOG"
SYNTHETIC_SESSION = "SYNTHETIC_PHASE3E_SESSION_DO_NOT_LOG"
SYNTHETIC_UNRELATED_SECRET = "DO_NOT_INHERIT_PHASE3E"

SENSITIVE_VALUES = (
    SYNTHETIC_USER,
    SYNTHETIC_DISPLAY_NAME,
    SYNTHETIC_PASSWORD,
    SYNTHETIC_ACCESS_TOKEN,
    SYNTHETIC_REFRESH_TOKEN,
    SYNTHETIC_COOKIE,
    SYNTHETIC_SESSION,
)


class _FakeConnection:
    def close(self) -> None:
        pass


@pytest.fixture(autouse=True)
def _restore_process_logging() -> None:
    """Keep intentional forced-root scenarios isolated from other tests."""
    root = logging.getLogger()
    # A pre-existing Phase 3A characterization invokes upstream in-process and
    # leaves its temporary debug.log handler behind.  Remove that test artifact
    # before recording the parent state this module promises to preserve.
    for handler in tuple(root.handlers):
        if (
            isinstance(handler, logging.FileHandler)
            and Path(handler.baseFilename).name.casefold() == "debug.log"
        ):
            root.removeHandler(handler)
            with contextlib.suppress(Exception):
                handler.close()
    original_handlers = list(root.handlers)
    original_level = root.level
    original_disabled = logging.root.manager.disable
    named_levels = {
        name: logging.getLogger(name).level
        for name in (
            "garmin_client.client",
            "selenium.webdriver.remote.remote_connection",
        )
    }
    yield
    for handler in list(root.handlers):
        if handler not in original_handlers:
            root.removeHandler(handler)
            with contextlib.suppress(Exception):
                handler.close()
    root.handlers = original_handlers
    root.setLevel(original_level)
    logging.disable(original_disabled)
    for name, level in named_levels.items():
        logging.getLogger(name).setLevel(level)


def _install_fake_upstream(
    monkeypatch: pytest.MonkeyPatch,
    data_dir: Path,
    main: Callable[[], object],
) -> None:
    from seleniumbase.core import browser_launcher

    module = ModuleType("garmin_givemydata")
    module.main = main
    monkeypatch.setitem(sys.modules, "garmin_givemydata", module)
    monkeypatch.setenv("GARMIN_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GARMIN_EMAIL", SYNTHETIC_USER)
    monkeypatch.setenv("GARMIN_PASSWORD", SYNTHETIC_PASSWORD)
    monkeypatch.setattr(
        cli_backup_ingest,
        "_validate_givemydata_runtime_version",
        lambda: True,
    )
    monkeypatch.setattr(browser_launcher, "override_driver_dir", lambda _path: None)


def _configure_upstream_debug_log(debug_path: Path) -> None:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(levelname)s %(name)s: %(message)s",
        handlers=[logging.FileHandler(debug_path, mode="a")],
        force=True,
    )


def _patch_run_sync_after_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_backup_ingest, "ensure_app_dirs", lambda: None)
    monkeypatch.setattr(
        cli_backup_ingest,
        "_snapshot_fit_archives",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        cli_backup_ingest,
        "_run_changed_archive_ingestion",
        lambda *_args, **_kwargs: {"errors": 0, "updated_activity_ids": []},
    )
    monkeypatch.setattr(
        cli_backup_ingest,
        "_run_historical_archive_reconciliation",
        lambda *_args, **_kwargs: {
            "errors": 0,
            "warnings": 0,
            "updated_activity_ids": [],
        },
    )
    monkeypatch.setattr(db_sqlite, "connect_sqlite", lambda _path: _FakeConnection())
    monkeypatch.setattr(db_migrate, "apply_schema", lambda _conn, _schema: None)
    monkeypatch.setattr(
        post_sync_refresh,
        "refresh_post_sync_tables",
        lambda _conn, **_kwargs: {"errors": 0},
    )
    monkeypatch.setattr(
        hub_paths,
        "schema_sql_path",
        lambda: Path("synthetic-schema.sql"),
    )


def test_worker_prevents_debug_log_creation_not_delete_after_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    debug_path = tmp_path / "debug.log"
    attempted_file_handlers: list[Path] = []
    original_file_handler = logging.FileHandler

    def monitored_file_handler(filename: object, *args: object, **kwargs: object):
        attempted_file_handlers.append(Path(filename).resolve())
        return original_file_handler(filename, *args, **kwargs)

    monkeypatch.setattr(logging, "FileHandler", monitored_file_handler)

    def fake_main() -> int:
        _configure_upstream_debug_log(debug_path)
        logging.getLogger("garmin_client.client").info(
            "Display name: %s", SYNTHETIC_DISPLAY_NAME
        )
        for handler in logging.getLogger().handlers:
            handler.flush()
        return 0

    _install_fake_upstream(monkeypatch, tmp_path, fake_main)

    assert cli_backup_ingest._run_bundled_givemydata([]) == 0
    assert debug_path.resolve() not in attempted_file_handlers, (
        "the privacy boundary must prevent opening debug.log, not remove it later"
    )
    assert not debug_path.exists()


def test_worker_restores_forced_global_debug_handler_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = logging.getLogger()
    original_handlers = tuple(root.handlers)
    original_level = root.level

    def fake_main() -> int:
        _configure_upstream_debug_log(tmp_path / "debug.log")
        logging.getLogger("garmin_client.client").debug(
            "sensitive display %s", SYNTHETIC_DISPLAY_NAME
        )
        return 0

    _install_fake_upstream(monkeypatch, tmp_path, fake_main)

    assert cli_backup_ingest._run_bundled_givemydata([]) == 0
    assert tuple(root.handlers) == original_handlers
    assert root.level == original_level
    assert not any(
        isinstance(handler, logging.FileHandler)
        and Path(handler.baseFilename).name.casefold() == "debug.log"
        for handler in root.handlers
    )


def test_worker_does_not_persist_verbose_selenium_payloads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    debug_path = tmp_path / "debug.log"
    monkeypatch.setenv("SE_DEBUG", "1")

    def fake_main() -> int:
        _configure_upstream_debug_log(debug_path)
        selenium_logger = logging.getLogger(
            "selenium.webdriver.remote.remote_connection"
        )
        if os.environ.get("SE_DEBUG"):
            selenium_logger.setLevel(logging.DEBUG)
        selenium_logger.debug(
            "POST /actions payload={'text': '%s'}", SYNTHETIC_PASSWORD
        )
        selenium_logger.debug(
            "GET /cookie response={'value': '%s', 'session': '%s'}",
            SYNTHETIC_COOKIE,
            SYNTHETIC_SESSION,
        )
        for handler in logging.getLogger().handlers:
            handler.flush()
        return 0

    _install_fake_upstream(monkeypatch, tmp_path, fake_main)

    assert cli_backup_ingest._run_bundled_givemydata([]) == 0
    persisted = debug_path.read_text(encoding="utf-8") if debug_path.exists() else ""
    assert SYNTHETIC_PASSWORD not in persisted
    assert SYNTHETIC_COOKIE not in persisted
    assert SYNTHETIC_SESSION not in persisted
    assert not debug_path.exists()


def test_worker_redacts_identifiers_passwords_tokens_and_sessions_in_output(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_main() -> int:
        print(
            "download diagnostic: "
            f"user={SYNTHETIC_USER} display={SYNTHETIC_DISPLAY_NAME} "
            f"password={SYNTHETIC_PASSWORD}"
        )
        print(
            f"Authorization: Bearer {SYNTHETIC_ACCESS_TOKEN}; "
            f"access_token={SYNTHETIC_ACCESS_TOKEN}; "
            f"refresh_token={SYNTHETIC_REFRESH_TOKEN}; "
            f"cookie={SYNTHETIC_COOKIE}; session={SYNTHETIC_SESSION}",
            file=sys.stderr,
        )
        print(f"repeat={SYNTHETIC_PASSWORD}/{SYNTHETIC_PASSWORD}")
        print("download diagnostic retained")
        return 0

    _install_fake_upstream(monkeypatch, tmp_path, fake_main)

    assert cli_backup_ingest._run_bundled_givemydata([]) == 0
    captured = capsys.readouterr()
    diagnostic = captured.out + captured.err
    assert all(value not in diagnostic for value in SENSITIVE_VALUES)
    assert "download diagnostic" in diagnostic
    assert "download diagnostic retained" in diagnostic
    assert "REDACT" in diagnostic.upper()


def test_worker_redacts_quoted_multiword_identifiers_and_sensitive_headers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    multiword_display_name = "SYNTHETIC PHASE3E DISPLAY NAME"
    quoted_cookie = "SYNTHETIC PHASE3E COOKIE WITH SPACE"
    second_cookie = "SYNTHETIC_PHASE3E_SECOND_COOKIE"

    def fake_main() -> int:
        print(f"Display name: {multiword_display_name}")
        print(f"{{'displayName': '{multiword_display_name}'}}")
        print(
            f'{{"Authorization": "Bearer {SYNTHETIC_ACCESS_TOKEN}"}}',
            file=sys.stderr,
        )
        print(
            f"Cookie: SID={quoted_cookie}; CSRF={second_cookie}",
            file=sys.stderr,
        )
        print(f"cookie='{quoted_cookie}'", file=sys.stderr)
        return 0

    _install_fake_upstream(monkeypatch, tmp_path, fake_main)

    assert cli_backup_ingest._run_bundled_givemydata([]) == 0
    captured = capsys.readouterr()
    diagnostic = captured.out + captured.err
    assert multiword_display_name not in diagnostic
    assert SYNTHETIC_ACCESS_TOKEN not in diagnostic
    assert quoted_cookie not in diagnostic
    assert second_cookie not in diagnostic
    assert "REDACT" in diagnostic.upper()


def test_short_known_sensitive_values_do_not_destroy_ordinary_diagnostics() -> None:
    diagnostic = (
        "Downloaded 3 data files; Updated activity 24539563069; "
        "garmin-givemydata 0.1.12; Authentication failed; HTTP status 401"
    )

    assert cli_backup_ingest._sanitize_diagnostic_text(
        diagnostic,
        known_sensitive_values=("a", "data", "data"),
    ) == diagnostic


def test_duplicate_unicode_known_sensitive_values_are_redacted() -> None:
    unicode_secret = "秘密🔒"
    diagnostic = f"authentication failed for {unicode_secret}; again {unicode_secret}"

    sanitized = cli_backup_ingest._sanitize_diagnostic_text(
        diagnostic,
        known_sensitive_values=(unicode_secret, unicode_secret),
    )

    assert unicode_secret not in sanitized
    assert sanitized.count(cli_backup_ingest._REDACTED) == 2


def test_worker_redacts_sensitive_exception_text_but_propagates_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_main() -> None:
        raise SystemExit(
            "authentication failed for "
            f"{SYNTHETIC_USER}: password={SYNTHETIC_PASSWORD}; "
            f"Authorization: Bearer {SYNTHETIC_ACCESS_TOKEN}; "
            f"refresh_token={SYNTHETIC_REFRESH_TOKEN}; "
            f"cookie={SYNTHETIC_COOKIE}; session={SYNTHETIC_SESSION}"
        )

    _install_fake_upstream(monkeypatch, tmp_path, fake_main)

    result = cli_backup_ingest._run_bundled_givemydata([])
    captured = capsys.readouterr()
    diagnostic = captured.out + captured.err
    assert result != 0
    assert "authentication failed for" in diagnostic
    assert all(value not in diagnostic for value in SENSITIVE_VALUES)
    assert "REDACT" in diagnostic.upper()
    assert "[SUCCESS]" not in diagnostic


def test_output_sanitizer_preserves_ordinary_empty_and_already_redacted_text(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_main() -> int:
        print("ordinary progress: 3 archives processed")
        print("")
        print("authentication failed for <REDACTED>", file=sys.stderr)
        return 0

    _install_fake_upstream(monkeypatch, tmp_path, fake_main)

    assert cli_backup_ingest._run_bundled_givemydata([]) == 0
    captured = capsys.readouterr()
    diagnostic = captured.out + captured.err
    assert "ordinary progress: 3 archives processed" in diagnostic
    assert "authentication failed for <REDACTED>" in diagnostic


def test_unrelated_parent_secret_is_not_inherited_by_garmin_worker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured_environment: dict[str, str] = {}
    _patch_run_sync_after_worker(monkeypatch)
    monkeypatch.setenv("PHASE3E_UNRELATED_PARENT_SECRET", SYNTHETIC_UNRELATED_SECRET)
    monkeypatch.setenv("SE_DEBUG", "1")
    monkeypatch.setenv("GARMIN_EMAIL", SYNTHETIC_USER)
    monkeypatch.setenv("GARMIN_PASSWORD", SYNTHETIC_PASSWORD)

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess:
        captured_environment.update(kwargs["env"])
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fake_run)

    assert cli_backup_ingest.run_sync(tmp_path / "garmin.db") == 0
    assert "PHASE3E_UNRELATED_PARENT_SECRET" not in captured_environment
    assert SYNTHETIC_UNRELATED_SECRET not in captured_environment.values()
    assert "SE_DEBUG" not in captured_environment


def test_source_sync_arguments_cannot_bypass_controlled_privacy_worker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    launched_commands: list[list[str]] = []
    _patch_run_sync_after_worker(monkeypatch)
    monkeypatch.setattr(cli_backup_ingest.sys, "frozen", False, raising=False)
    monkeypatch.setattr(
        cli_backup_ingest,
        "_validate_givemydata_runtime_version",
        lambda: True,
    )

    def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess:
        launched_commands.append(list(command))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fake_run)

    assert cli_backup_ingest.run_sync(
        tmp_path / "garmin.db",
        days=7,
        visible=True,
        extra_args=["--profile", "health"],
    ) == 0

    assert len(launched_commands) == 1
    command = launched_commands[0]
    assert command[:5] == [
        sys.executable,
        "-I",
        "-u",
        "-m",
        "garmin_data_hub.cli_backup_ingest",
    ]
    assert cli_backup_ingest._BUNDLED_GIVEMYDATA_FLAG in command
    assert command.count("--no-trackpoints") == 1
    assert command[command.index("--days") + 1] == "7"
    assert command[command.index("--profile") + 1] == "health"
    assert "--visible" in command


def test_worker_credentials_are_not_inherited_by_upstream_descendants(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    descendant_environments: list[str] = []

    def fake_main() -> int:
        completed = subprocess.run(
            [
                sys.executable,
                "-I",
                "-c",
                (
                    "import os; "
                    "print(repr((os.environ.get('GARMIN_EMAIL'), "
                    "os.environ.get('GARMIN_PASSWORD'))))"
                ),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        descendant_environments.append(completed.stdout)
        return 0

    _install_fake_upstream(monkeypatch, tmp_path, fake_main)

    assert cli_backup_ingest._run_bundled_givemydata([]) == 0
    inherited = "".join(descendant_environments)
    assert SYNTHETIC_USER not in inherited
    assert SYNTHETIC_PASSWORD not in inherited


def test_required_runtime_and_garmin_environment_reaches_worker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured_environment: dict[str, str] = {}
    _patch_run_sync_after_worker(monkeypatch)
    required_platform = {
        "PATH": os.environ.get("PATH", "synthetic-path"),
        "SystemRoot": os.environ.get("SystemRoot", r"C:\Windows"),
        "TEMP": str(tmp_path / "temp"),
    }
    for name, value in required_platform.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("GARMIN_EMAIL", SYNTHETIC_USER)
    monkeypatch.setenv("GARMIN_PASSWORD", SYNTHETIC_PASSWORD)

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess:
        captured_environment.update(kwargs["env"])
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", fake_run)

    assert cli_backup_ingest.run_sync(tmp_path / "garmin.db") == 0
    assert captured_environment["GARMIN_DATA_DIR"] == str(tmp_path)
    assert captured_environment["GARMIN_EMAIL"] == SYNTHETIC_USER
    assert captured_environment["GARMIN_PASSWORD"] == SYNTHETIC_PASSWORD
    assert captured_environment["PYTHONUNBUFFERED"] == "1"
    casefolded_environment = {
        name.casefold(): value for name, value in captured_environment.items()
    }
    for name, value in required_platform.items():
        assert casefolded_environment[name.casefold()] == value


@pytest.mark.parametrize("exit_code", [0, 19])
def test_sensitive_worker_output_is_sanitized_before_sync_log_persistence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    exit_code: int,
) -> None:
    durable_log = tmp_path / f"synthetic-sync-{exit_code}.log"

    def fake_main() -> int:
        outcome = "success detail" if exit_code == 0 else "failure detail"
        print(f"{outcome}: user={SYNTHETIC_USER}")
        print(
            f"subprocess diagnostic: password={SYNTHETIC_PASSWORD} "
            f"access_token={SYNTHETIC_ACCESS_TOKEN}",
            file=sys.stderr,
        )
        print(
            f"exception summary: refresh_token={SYNTHETIC_REFRESH_TOKEN} "
            f"cookie={SYNTHETIC_COOKIE} session={SYNTHETIC_SESSION}"
        )
        return exit_code

    _install_fake_upstream(monkeypatch, tmp_path, fake_main)

    with durable_log.open("w", encoding="utf-8") as stream:
        with contextlib.redirect_stdout(stream), contextlib.redirect_stderr(stream):
            result = cli_backup_ingest._run_bundled_givemydata([])

    persisted = durable_log.read_text(encoding="utf-8")
    assert result == exit_code
    assert all(value not in persisted for value in SENSITIVE_VALUES)
    assert "detail" in persisted
    assert "subprocess diagnostic" in persisted
    assert "exception summary" in persisted


def test_privacy_worker_setup_needs_no_network_browser_or_download(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    invoked: list[str] = []

    def forbidden_network(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("privacy worker attempted external network access")

    monkeypatch.setattr(socket, "create_connection", forbidden_network)

    def fake_main() -> int:
        invoked.append("synthetic upstream")
        return 0

    _install_fake_upstream(monkeypatch, tmp_path, fake_main)

    assert cli_backup_ingest._run_bundled_givemydata([]) == 0
    assert invoked == ["synthetic upstream"]


def test_privacy_bootstrap_failure_is_sanitized_and_nonzero(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    @contextlib.contextmanager
    def fail_privacy_bootstrap(_known_sensitive_values: tuple[str, ...]):
        raise RuntimeError(
            f"privacy bootstrap failed: password={SYNTHETIC_PASSWORD}"
        )
        yield  # pragma: no cover

    _install_fake_upstream(monkeypatch, tmp_path, lambda: 0)
    monkeypatch.setattr(
        cli_backup_ingest,
        "_upstream_privacy_boundary",
        fail_privacy_bootstrap,
    )

    assert cli_backup_ingest._run_bundled_givemydata([]) != 0
    captured = capsys.readouterr()
    diagnostic = captured.out + captured.err
    assert "privacy bootstrap failed" in diagnostic
    assert SYNTHETIC_PASSWORD not in diagnostic
    assert "REDACT" in diagnostic.upper()


def test_unexpected_upstream_result_is_a_truthful_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_main() -> object:
        return object()

    _install_fake_upstream(monkeypatch, tmp_path, fake_main)

    assert cli_backup_ingest._run_bundled_givemydata([]) != 0
    captured = capsys.readouterr()
    diagnostic = captured.out + captured.err
    assert "unsupported result" in diagnostic.lower()
    assert "object at" not in diagnostic.lower()
