#!/usr/bin/env python3
"""
Standalone CLI tool for Garmin data sync using garmin-givemydata.

Invokes garmin-givemydata to download all activity and health data directly
into the canonical SQLite DB, then applies app-internal schema extensions.

Usage examples:
    python -m garmin_data_hub.cli_backup_ingest --visible
    python -m garmin_data_hub.cli_backup_ingest --visible --days 30
"""

import sys
import argparse
import contextlib
import importlib.metadata
import logging
import subprocess
import multiprocessing
import os
import re
import stat
import sysconfig
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

from garmin_data_hub.ingest.trackpoints import (
    _activity_id_from_zip_filename,
    ingest_trackpoints_from_archives,
    ingest_trackpoints_from_fit_archives,
)
from garmin_data_hub.ingest.reconciliation import (
    activity_id_for_archive,
    baseline_required_summary,
    reconcile_historical_archives,
    reconciliation_baseline_is_current,
)
from garmin_data_hub.paths import default_db_path, ensure_app_dirs

logger = logging.getLogger(__name__)

_BUNDLED_GIVEMYDATA_FLAG = "--_run-bundled-givemydata"
_BUNDLED_GIVEMYDATA_DIAGNOSTIC_FLAG = "--_check-bundled-givemydata"
_GIVEMYDATA_DISTRIBUTION = "garmin-givemydata"
_SUPPORTED_GIVEMYDATA_VERSION = "0.1.12"
_ORIGINAL_CANONICAL_INGESTER = ingest_trackpoints_from_archives
_ORIGINAL_COMPATIBILITY_INGESTER = ingest_trackpoints_from_fit_archives
_ORIGINAL_HISTORICAL_RECONCILER = reconcile_historical_archives

_REDACTED = "<REDACTED>"
_MIN_KNOWN_SENSITIVE_VALUE_BYTES = 8
_DISPLAY_NAME_PATTERN = re.compile(
    r"(?i)(?P<prefix>['\"]?display(?:[_ -]?name)?['\"]?\s*[:=]\s*)"
    r"(?:(?P<quote>['\"])(?P<quoted_value>[^\r\n]*?)(?P=quote)|"
    r"(?P<value>[^\r\n;,}\]]+))"
)
_SENSITIVE_FIELD_PATTERN = re.compile(
    r"(?i)(?P<prefix>['\"]?(?:user(?:name)?|email|display(?:[_ -]?name)?|"
    r"password|access[_ -]?token|refresh[_ -]?token|cookie|"
    r"session(?:[_ -]?id)?)['\"]?\s*[:=]\s*)"
    r"(?:(?P<quote>['\"])(?P<quoted_value>[^\r\n]*?)(?P=quote)|"
    r"(?P<value>[^\s;,'\"}\]]+))"
)
_BEARER_TOKEN_PATTERN = re.compile(
    r"(?i)(?P<prefix>['\"]?authorization['\"]?\s*[:=]\s*)"
    r"(?P<quote>['\"]?)(?P<scheme>bearer\s+)"
    r"(?P<value>[^\s;,'\"}\]]+)(?P=quote)"
)
_COOKIE_HEADER_PATTERN = re.compile(
    r"(?im)(?P<prefix>\b(?:set-cookie|cookie)\s*:\s*)[^\r\n]*"
)
_COOKIE_RESPONSE_VALUE_PATTERN = re.compile(
    r"(?i)(?P<prefix>/cookie\b[^\r\n]*?['\"]?value['\"]?\s*:\s*)"
    r"(?:(?P<quote>['\"])(?P<quoted_value>[^\r\n]*?)(?P=quote)|"
    r"(?P<value>[^\s;,'\"}\]]+))"
)

# Keep the subprocess usable as a Windows/Python/browser worker without copying
# arbitrary application or shell secrets into it.  Credentials are limited to
# the two names used by the approved application credential transport.
_WORKER_ENVIRONMENT_ALLOWLIST = (
    "ALLUSERSPROFILE",
    "APPDATA",
    "COMSPEC",
    "HOMEDRIVE",
    "HOMEPATH",
    "LOCALAPPDATA",
    "NUMBER_OF_PROCESSORS",
    "OS",
    "PATH",
    "PATHEXT",
    "PROCESSOR_ARCHITECTURE",
    "PROCESSOR_IDENTIFIER",
    "PROCESSOR_LEVEL",
    "PROCESSOR_REVISION",
    "PROGRAMDATA",
    "PROGRAMFILES",
    "PROGRAMFILES(X86)",
    "PROGRAMW6432",
    "SYSTEMDRIVE",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "USERDOMAIN",
    "USERNAME",
    "USERPROFILE",
    "WINDIR",
    "LANG",
    "LC_ALL",
    "PYTHONIOENCODING",
    "PYTHONUTF8",
    "SSL_CERT_DIR",
    "SSL_CERT_FILE",
    "REQUESTS_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    "GARMIN_EMAIL",
    "GARMIN_PASSWORD",
)


def _run_changed_archive_ingestion(conn, fit_dir: Path, archive_paths: list[Path]):
    """Use the canonical strict lane while preserving injectable legacy seams."""
    if (
        ingest_trackpoints_from_fit_archives
        is not _ORIGINAL_COMPATIBILITY_INGESTER
        and ingest_trackpoints_from_archives is _ORIGINAL_CANONICAL_INGESTER
    ):
        # Older Phase 3A callers inject the compatibility adapter directly.
        return ingest_trackpoints_from_fit_archives(
            conn,
            fit_dir,
            replace_existing=True,
            archive_paths=archive_paths,
        )
    return ingest_trackpoints_from_archives(
        conn,
        fit_dir,
        replace_existing=True,
        archive_paths=archive_paths,
    )


def _run_historical_archive_reconciliation(
    conn,
    fit_dir: Path,
    archive_paths: list[Path],
):
    """Run durable reconciliation, retaining the Phase 3A injection seam."""
    if (
        ingest_trackpoints_from_fit_archives
        is not _ORIGINAL_COMPATIBILITY_INGESTER
        and reconcile_historical_archives is _ORIGINAL_HISTORICAL_RECONCILER
    ):
        return ingest_trackpoints_from_fit_archives(
            conn,
            fit_dir,
            replace_existing=False,
            archive_paths=archive_paths,
        )
    if not archive_paths:
        return reconcile_historical_archives(
            conn,
            fit_dir,
            archive_paths=archive_paths,
        )
    if (
        reconcile_historical_archives is _ORIGINAL_HISTORICAL_RECONCILER
        and not reconciliation_baseline_is_current(conn)
    ):
        print(
            "[NOTICE] Historical archive reconciliation baseline is not "
            "established; run garmin-reconcile-trackpoints --apply"
        )
        excluded_activity_ids = [
            activity_id
            for path in archive_paths
            if (activity_id := activity_id_for_archive(path)) is not None
        ]
        return baseline_required_summary(
            candidate_archives=len(archive_paths),
            excluded_activity_ids=excluded_activity_ids,
        )
    return reconcile_historical_archives(
        conn,
        fit_dir,
        archive_paths=archive_paths,
    )


class _SyncArgumentParser(argparse.ArgumentParser):
    """Argument validator that reports errors without exiting the application."""

    def error(self, message: str) -> None:
        raise ValueError(message)


def _validate_upstream_sync_args(args: list[str] | None) -> list[str]:
    """Allow only upstream options used for ordinary Garmin synchronization.

    Upstream utility modes are intentionally excluded.  In particular,
    ``--rebuild-trackpoints`` bypasses upstream's ``--no-trackpoints`` switch.
    Disabling argparse abbreviation also prevents shortened utility options from
    resolving to an unsafe upstream mode.
    """
    validated = list(args or [])
    parser = _SyncArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("--days", type=int)
    parser.add_argument("--since")
    parser.add_argument(
        "--profile",
        choices=("all", "health", "activities", "sleep"),
    )
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--save-raw", action="store_true")
    parser.add_argument("--no-files", action="store_true")
    parser.add_argument("--no-trackpoints", action="store_true")
    parser.add_argument("--fit-only", action="store_true")
    parser.add_argument("--latest", action="store_true")
    parser.add_argument("--date")
    parser.add_argument("--visible", action="store_true")
    parser.parse_args(validated)
    return validated


def _with_no_trackpoints(args: list[str]) -> list[str]:
    """Return validated upstream arguments with exactly one safety switch."""
    return [argument for argument in args if argument != "--no-trackpoints"] + [
        "--no-trackpoints"
    ]


def _snapshot_fit_archives(
    fit_dir: Path,
    *,
    fail_on_disappearing_archive: bool = False,
) -> dict[Path, tuple[int, int]]:
    """Return lightweight identities without opening or parsing FIT archives."""
    snapshot: dict[Path, tuple[int, int]] = {}
    try:
        fit_dir_stat = fit_dir.stat()
    except FileNotFoundError:
        return snapshot
    if not stat.S_ISDIR(fit_dir_stat.st_mode):
        raise NotADirectoryError(f"FIT archive path is not a directory: {fit_dir}")
    try:
        archive_paths = sorted(
            (
                path
                for path in fit_dir.iterdir()
                if path.suffix.lower() == ".zip"
            ),
            key=lambda path: str(path).casefold(),
        )
    except FileNotFoundError:
        if fail_on_disappearing_archive:
            raise
        return snapshot
    for archive_path in archive_paths:
        try:
            archive_stat = archive_path.stat()
        except FileNotFoundError:
            if fail_on_disappearing_archive:
                raise
            continue
        snapshot[archive_path] = (
            archive_stat.st_size,
            archive_stat.st_mtime_ns,
        )
    return snapshot


def _ambiguous_changed_archive_ids(
    archive_paths: list[Path],
    changed_archive_paths: list[Path],
) -> dict[int, list[Path]]:
    """Return duplicate activity archives relevant to the changed pass.

    Unchanged-only backlog duplicates remain guarded by the ingester.  Indexing
    the complete post-sync filename universe here prevents a changed archive
    and its unchanged counterpart from being hidden in separate passes.
    """
    changed_paths = set(changed_archive_paths)
    archives_by_activity: dict[int, list[Path]] = {}
    for archive_path in archive_paths:
        activity_id = _activity_id_from_zip_filename(archive_path)
        if activity_id is not None:
            archives_by_activity.setdefault(int(activity_id), []).append(archive_path)

    return {
        activity_id: sorted(paths, key=lambda path: str(path).casefold())
        for activity_id, paths in archives_by_activity.items()
        if len(paths) > 1 and any(path in changed_paths for path in paths)
    }


def _clear_stale_chrome_profile_locks(profile_dir: Path) -> None:
    """Best-effort cleanup of stale Chromium profile lock files.

    A previous crash can leave lock artifacts that cause Playwright to fail with
    "Opening in existing browser session" for the same user-data-dir.
    """
    lock_names = ["SingletonLock", "SingletonCookie", "SingletonSocket"]
    removed = 0
    for name in lock_names:
        path = profile_dir / name
        if not path.exists():
            continue
        try:
            path.unlink()
            removed += 1
        except OSError:
            # If the file is in use, Chrome is still attached to this profile.
            pass

    if removed:
        print(f"[INFO] Cleared {removed} stale browser profile lock file(s)")


def _sanitize_diagnostic_text(
    text: str | None,
    *,
    known_sensitive_values: tuple[str, ...] = (),
) -> str:
    """Redact reachable Garmin authentication/session diagnostic forms."""
    if not text:
        return "" if text is None else text

    sanitized = str(text)
    for value in sorted(
        {
            value
            for value in known_sensitive_values
            if len(value.encode("utf-8", errors="surrogatepass"))
            >= _MIN_KNOWN_SENSITIVE_VALUE_BYTES
            and not value.isspace()
        },
        key=len,
        reverse=True,
    ):
        sanitized = sanitized.replace(value, _REDACTED)

    sanitized = _BEARER_TOKEN_PATTERN.sub(
        lambda match: (
            f"{match.group('prefix')}{match.group('quote')}"
            f"{match.group('scheme')}{_REDACTED}{match.group('quote')}"
        ),
        sanitized,
    )
    sanitized = _COOKIE_HEADER_PATTERN.sub(
        lambda match: f"{match.group('prefix')}{_REDACTED}",
        sanitized,
    )
    sanitized = _COOKIE_RESPONSE_VALUE_PATTERN.sub(
        lambda match: (
            f"{match.group('prefix')}{match.group('quote') or ''}"
            f"{_REDACTED}{match.group('quote') or ''}"
        ),
        sanitized,
    )
    sanitized = _DISPLAY_NAME_PATTERN.sub(
        lambda match: (
            f"{match.group('prefix')}{match.group('quote') or ''}"
            f"{_REDACTED}{match.group('quote') or ''}"
        ),
        sanitized,
    )
    return _SENSITIVE_FIELD_PATTERN.sub(
        lambda match: (
            f"{match.group('prefix')}{match.group('quote') or ''}"
            f"{_REDACTED}{match.group('quote') or ''}"
        ),
        sanitized,
    )


class _SanitizingTextStream:
    """Line-buffered stream that sanitizes text before forwarding it."""

    def __init__(self, stream, known_sensitive_values: tuple[str, ...]) -> None:
        self._stream = stream
        self._known_sensitive_values = known_sensitive_values
        self._pending = ""

    def write(self, text: str) -> int:
        if not text:
            return 0
        self._pending += str(text)
        lines = self._pending.splitlines(keepends=True)
        self._pending = ""
        if lines and not lines[-1].endswith(("\n", "\r")):
            self._pending = lines.pop()
        for line in lines:
            self._stream.write(
                _sanitize_diagnostic_text(
                    line,
                    known_sensitive_values=self._known_sensitive_values,
                )
            )
        return len(text)

    def flush(self) -> None:
        if self._pending:
            self._stream.write(
                _sanitize_diagnostic_text(
                    self._pending,
                    known_sensitive_values=self._known_sensitive_values,
                )
            )
            self._pending = ""
        self._stream.flush()

    def __getattr__(self, name: str):
        return getattr(self._stream, name)


@contextlib.contextmanager
def _transient_worker_credentials(credentials: dict[str, str]):
    """Expose credentials to pinned upstream without descendant inheritance."""
    environment = os.environ
    original_get = environment.get
    missing = object()
    original_instance_get = vars(environment).get("get", missing)
    original_values = {
        name: environment[name] if name in environment else missing
        for name in credentials
    }
    credential_lookup = {
        name.casefold(): value for name, value in credentials.items() if value
    }

    def credential_aware_get(name: str, default=None):
        value = credential_lookup.get(str(name).casefold(), missing)
        return original_get(name, default) if value is missing else value

    try:
        # Empty inherited values also prevent pinned upstream's legacy .env
        # loader from replacing application-provided credentials on setdefault.
        for name in credentials:
            environment[name] = ""
        environment.get = credential_aware_get
        yield
    finally:
        if original_instance_get is missing:
            del environment.get
        else:
            environment.get = original_instance_get
        for name, value in original_values.items():
            if value is missing:
                environment.pop(name, None)
            else:
                environment[name] = value


@contextlib.contextmanager
def _upstream_privacy_boundary(known_sensitive_values: tuple[str, ...]):
    """Contain upstream output and global logging changes inside the worker."""
    root_logger = logging.getLogger()
    original_root_handlers = tuple(root_logger.handlers)
    original_root_level = root_logger.level
    original_logging_disable = logging.root.manager.disable
    protected_logger_names = (
        "garmin_client.client",
        "selenium.webdriver.remote.remote_connection",
    )
    original_named_logger_state = {
        name: (
            logging.getLogger(name).level,
            logging.getLogger(name).disabled,
            logging.getLogger(name).propagate,
        )
        for name in protected_logger_names
    }
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    sanitized_stdout = _SanitizingTextStream(
        original_stdout,
        known_sensitive_values,
    )
    sanitized_stderr = _SanitizingTextStream(
        original_stderr,
        known_sensitive_values,
    )
    original_basic_config = logging.basicConfig
    original_file_handler = logging.FileHandler
    original_se_debug = os.environ.pop("SE_DEBUG", None)

    def discard_file_handler(*_args, **_kwargs) -> logging.Handler:
        return logging.NullHandler()

    def privacy_safe_basic_config(**kwargs) -> None:
        safe_kwargs = dict(kwargs)
        safe_kwargs.pop("filename", None)
        safe_kwargs.pop("filemode", None)
        safe_kwargs.pop("handlers", None)
        safe_kwargs.pop("force", None)
        requested_level = safe_kwargs.get("level", logging.INFO)
        if isinstance(requested_level, str):
            requested_level = logging.getLevelNamesMapping().get(
                requested_level.upper(),
                logging.INFO,
            )
        safe_kwargs["level"] = max(int(requested_level), logging.INFO)
        safe_kwargs["stream"] = sys.stderr
        original_basic_config(**safe_kwargs)

    try:
        sys.stdout = sanitized_stdout
        sys.stderr = sanitized_stderr
        logging.FileHandler = discard_file_handler
        logging.basicConfig = privacy_safe_basic_config
        logging.getLogger(
            "selenium.webdriver.remote.remote_connection"
        ).setLevel(logging.WARNING)
        yield
    finally:
        sanitized_stdout.flush()
        sanitized_stderr.flush()
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        logging.basicConfig = original_basic_config
        logging.FileHandler = original_file_handler
        if original_se_debug is None:
            os.environ.pop("SE_DEBUG", None)
        else:
            os.environ["SE_DEBUG"] = original_se_debug

        for handler in tuple(root_logger.handlers):
            if handler not in original_root_handlers:
                root_logger.removeHandler(handler)
                with contextlib.suppress(Exception):
                    handler.close()
        root_logger.handlers = list(original_root_handlers)
        root_logger.setLevel(original_root_level)
        logging.disable(original_logging_disable)
        for name, (level, disabled, propagate) in original_named_logger_state.items():
            named_logger = logging.getLogger(name)
            named_logger.setLevel(level)
            named_logger.disabled = disabled
            named_logger.propagate = propagate


def _build_garmin_worker_environment(data_dir: Path) -> dict[str, str]:
    """Construct the minimal practical environment for the Garmin worker."""
    environment = {
        name: value
        for name, value in os.environ.items()
        if name.upper() in _WORKER_ENVIRONMENT_ALLOWLIST
    }
    environment["GARMIN_DATA_DIR"] = str(data_dir)
    environment["PYTHONUNBUFFERED"] = "1"
    return environment


def _find_givemydata_cmd() -> list[str]:
    """Return a runnable garmin-givemydata command for this environment.

    A frozen build reuses this executable with an internal dispatch flag.  Pip's
    Windows console launcher cannot be copied into a release because it embeds
    the absolute path to the build virtual environment.  Source and installed
    execution use the current interpreter in isolated mode so neither PATH nor
    the sync working directory can select another environment's code.  ``-u``
    preserves the unbuffered logging behavior that isolated mode would
    otherwise ignore from ``PYTHONUNBUFFERED``.
    """
    if getattr(sys, "frozen", False):
        return [sys.executable, _BUNDLED_GIVEMYDATA_FLAG]

    return [sys.executable, "-I", "-u", "-m", "garmin_givemydata"]


def _controlled_givemydata_worker_cmd(command: list[str]) -> list[str]:
    """Route source execution through this module's privacy worker."""
    source_entry = [sys.executable, "-I", "-u", "-m", "garmin_givemydata"]
    if command[: len(source_entry)] == source_entry:
        return [
            sys.executable,
            "-I",
            "-u",
            "-m",
            "garmin_data_hub.cli_backup_ingest",
            _BUNDLED_GIVEMYDATA_FLAG,
            *command[len(source_entry) :],
        ]
    return list(command)


def _isolated_givemydata_runtime_version() -> str:
    """Read upstream metadata only from this interpreter's package roots."""
    package_roots = {
        path
        for scheme in ("purelib", "platlib")
        if (path := sysconfig.get_path(scheme))
    }
    canonical_name = re.sub(r"[-_.]+", "-", _GIVEMYDATA_DISTRIBUTION).casefold()
    matches = [
        distribution
        for distribution in importlib.metadata.distributions(
            path=sorted(package_roots)
        )
        if re.sub(
            r"[-_.]+",
            "-",
            str(distribution.metadata.get("Name", "")),
        ).casefold()
        == canonical_name
    ]
    if not matches:
        raise importlib.metadata.PackageNotFoundError(_GIVEMYDATA_DISTRIBUTION)
    versions = {distribution.version for distribution in matches}
    if len(versions) != 1:
        raise RuntimeError(
            "multiple garmin-givemydata versions exist in the selected runtime"
        )
    return versions.pop()


def _validate_givemydata_runtime_version() -> bool:
    """Require the supported upstream version in the selected environment."""
    try:
        installed_version = importlib.metadata.version(_GIVEMYDATA_DISTRIBUTION)
    except importlib.metadata.PackageNotFoundError:
        print(
            "[ERROR] garmin-givemydata is not installed in the same Python "
            "environment as Garmin Data Hub "
            f"(required version {_SUPPORTED_GIVEMYDATA_VERSION})."
        )
        return False
    except Exception as exc:
        print(f"[ERROR] Could not determine garmin-givemydata version: {exc}")
        return False

    if installed_version != _SUPPORTED_GIVEMYDATA_VERSION:
        print(
            "[ERROR] garmin-givemydata version mismatch in the selected "
            f"runtime: requires {_SUPPORTED_GIVEMYDATA_VERSION}, "
            f"found {installed_version}."
        )
        return False

    if not getattr(sys, "frozen", False):
        try:
            isolated_version = _isolated_givemydata_runtime_version()
        except importlib.metadata.PackageNotFoundError:
            print(
                "[ERROR] garmin-givemydata is not installed in the isolated "
                "Python runtime selected for execution "
                f"(required version {_SUPPORTED_GIVEMYDATA_VERSION})."
            )
            return False
        except Exception as exc:
            print(
                "[ERROR] Could not determine isolated garmin-givemydata "
                f"version: {exc}"
            )
            return False
        if isolated_version != installed_version:
            print(
                "[ERROR] garmin-givemydata metadata does not match the selected "
                f"isolated runtime: checked {installed_version}, "
                f"selected {isolated_version}."
            )
            return False
    return True


def _check_bundled_givemydata_runtime() -> int:
    """Verify the pinned upstream runtime without entering sync machinery."""
    try:
        importlib.import_module("garmin_givemydata")
    except Exception:
        print(
            "[ERROR] Bundled Garmin runtime is not available.",
            file=sys.stderr,
        )
        return 1

    try:
        installed_version = importlib.metadata.version(_GIVEMYDATA_DISTRIBUTION)
    except importlib.metadata.PackageNotFoundError:
        print(
            "[ERROR] Bundled Garmin runtime metadata is not available.",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "[ERROR] Bundled Garmin runtime metadata could not be read.",
            file=sys.stderr,
        )
        return 1

    if installed_version != _SUPPORTED_GIVEMYDATA_VERSION:
        print(
            "[ERROR] Bundled Garmin runtime version mismatch: "
            f"requires {_SUPPORTED_GIVEMYDATA_VERSION}, found {installed_version}.",
            file=sys.stderr,
        )
        return 1

    print(
        "[OK] Bundled Garmin runtime available: "
        f"garmin-givemydata {installed_version}."
    )
    return 0


def _run_bundled_givemydata(args: list[str]) -> int:
    """Run garmin-givemydata inside the controlled privacy worker boundary."""
    try:
        args = _with_no_trackpoints(_validate_upstream_sync_args(args))
    except ValueError as exc:
        print(f"[ERROR] Unsupported upstream sync argument: {exc}", file=sys.stderr)
        return 2

    if not _validate_givemydata_runtime_version():
        return 1

    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(errors="replace")

    worker_credentials = {
        name: value
        for name in ("GARMIN_EMAIL", "GARMIN_PASSWORD")
        if (value := os.environ.get(name))
    }
    known_sensitive_values = tuple(worker_credentials.values())
    original_argv = sys.argv
    result: object = 0
    sys.argv = ["garmin-givemydata", *args]
    try:
        with _upstream_privacy_boundary(known_sensitive_values):
            with _transient_worker_credentials(worker_credentials):
                try:
                    data_dir = Path(
                        os.environ.get("GARMIN_DATA_DIR", default_db_path().parent)
                    )
                    driver_dir = data_dir / "drivers"
                    driver_dir.mkdir(parents=True, exist_ok=True)

                    # SeleniumBase otherwise downloads Chrome drivers into its
                    # installed package directory, which is read-only under
                    # Program Files.
                    from seleniumbase.core import browser_launcher

                    browser_launcher.override_driver_dir(str(driver_dir))

                    # Import only after logging and output protections are active:
                    # the pinned upstream currently initializes its debug handler
                    # in main(), but the boundary remains safe if that moves to
                    # import time in a compatible build.
                    from garmin_givemydata import main as givemydata_main

                    result = givemydata_main()
                except SystemExit as exc:
                    if exc.code is None:
                        result = 0
                    elif isinstance(exc.code, int):
                        result = exc.code
                    else:
                        print(exc.code, file=sys.stderr)
                        result = 1
                except Exception:
                    traceback.print_exc(file=sys.stderr)
                    result = 1
    except Exception:
        diagnostic = _sanitize_diagnostic_text(
            traceback.format_exc(),
            known_sensitive_values=known_sensitive_values,
        )
        print(diagnostic, file=sys.stderr, end="")
        result = 1
    finally:
        sys.argv = original_argv

    if result is None:
        return 0
    if type(result) is int:
        return result
    print(
        "[ERROR] garmin-givemydata returned an unsupported result.",
        file=sys.stderr,
    )
    return 1


def run_sync(
    db_path: Path,
    days: int | None = None,
    visible: bool = False,
    chrome: bool = False,
    extra_args: list[str] | None = None,
    rebuild_derived_metrics: bool = False,
    rebuild_derived_metrics_all: bool = False,
    derived_metrics_only: bool = False,
) -> int:
    """Invoke garmin-givemydata to sync data into db_path.

    Returns process exit code (0 = success).
    """
    if db_path.name != "garmin.db":
        print(
            f"[ERROR] Unsupported synchronization database filename "
            f"'{db_path.name}'; the filename must be exactly 'garmin.db'."
        )
        return 2

    try:
        validated_extra_args = _validate_upstream_sync_args(extra_args)
    except ValueError as exc:
        print(f"[ERROR] Unsupported upstream sync argument: {exc}")
        return 2

    cmd = None
    if not derived_metrics_only:
        cmd = _find_givemydata_cmd()
        if not _validate_givemydata_runtime_version():
            return 1

    ensure_app_dirs()

    print("=" * 60)
    print("Garmin Data Sync  (garmin-givemydata)")
    print("=" * 60)
    print(f"Database: {db_path}")
    if days:
        print(f"Days:     {days}")
    print()

    data_dir = db_path.parent

    env = _build_garmin_worker_environment(data_dir)
    if chrome:
        _clear_stale_chrome_profile_locks(data_dir / "browser_profile")

    if cmd and days is not None:
        cmd.extend(["--days", str(days)])

    if cmd and visible:
        cmd.append("--visible")

    if cmd and chrome:
        print(
            "[INFO] --chrome requested, but upstream garmin-givemydata no longer accepts "
            "that flag; continuing with default browser mode"
        )

    if cmd and validated_extra_args:
        cmd.extend(validated_extra_args)

    if cmd:
        cmd = _controlled_givemydata_worker_cmd(cmd)
        cmd = _with_no_trackpoints(cmd)

    sync_cwd = data_dir
    fit_dir = data_dir / "fit"
    try:
        pre_sync_archives = (
            _snapshot_fit_archives(fit_dir) if not derived_metrics_only else {}
        )
    except OSError as exc:
        print(f"[ERROR] Could not inspect FIT archives before sync: {exc}")
        return 2

    if derived_metrics_only:
        print("[SKIP] Garmin download disabled (--derived-metrics-only)")
    else:
        try:
            subprocess.run(cmd, check=True, cwd=sync_cwd, env=env)
            print("[OK] Sync completed")
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] garmin-givemydata failed (exit {e.returncode})")
            if chrome:
                print(
                    "[HINT] If you see 'Opening in existing browser session', close all Chrome windows\n"
                    "       that are using GarminDataHub/browser_profile and try again."
                )
            return e.returncode
        except FileNotFoundError as e:
            print(f"[ERROR] Could not launch garmin-givemydata: {e}")
            return 1

    # Apply app-internal schema extensions (athlete_profile, activity_metrics, etc.)
    conn = None
    try:
        from garmin_data_hub.db.sqlite import connect_sqlite
        from garmin_data_hub.db.migrate import apply_schema
        from garmin_data_hub.analytics.post_sync_refresh import refresh_post_sync_tables
        from garmin_data_hub.paths import schema_sql_path

        conn = connect_sqlite(db_path)
        apply_schema(conn, schema_sql_path())
        print("[OK] App schema applied")

        updated_activity_ids: set[int] = set()
        historical_excluded_ids: set[int] = set()
        historical_warnings = 0
        if not derived_metrics_only:
            post_sync_archives = _snapshot_fit_archives(
                fit_dir,
                fail_on_disappearing_archive=True,
            )
            disappeared_archives = sorted(
                set(pre_sync_archives).difference(post_sync_archives),
                key=lambda path: str(path).casefold(),
            )
            if disappeared_archives:
                raise RuntimeError(
                    "FIT archive(s) disappeared during synchronization: "
                    + ", ".join(str(path) for path in disappeared_archives)
                )
            changed_archives = [
                path
                for path, identity in post_sync_archives.items()
                if pre_sync_archives.get(path) != identity
            ]
            ambiguous_archives = _ambiguous_changed_archive_ids(
                list(post_sync_archives),
                changed_archives,
            )
            if ambiguous_archives:
                details = "; ".join(
                    f"activity {activity_id}: "
                    + ", ".join(str(path) for path in paths)
                    for activity_id, paths in sorted(ambiguous_archives.items())
                )
                raise RuntimeError(f"ambiguous FIT archives detected: {details}")
            changed_summary = _run_changed_archive_ingestion(
                conn, fit_dir, changed_archives
            )
            updated_activity_ids.update(
                int(activity_id)
                for activity_id in changed_summary.get("updated_activity_ids", [])
            )

            changed_archive_set = set(changed_archives)
            changed_activity_ids = {
                int(activity_id)
                for activity_id in changed_summary.get("target_activity_ids", [])
            }
            backlog_archives = [
                path
                for path in post_sync_archives
                if path not in changed_archive_set
                and _activity_id_from_zip_filename(path) not in changed_activity_ids
            ]
            backlog_summary = _run_historical_archive_reconciliation(
                conn, fit_dir, backlog_archives
            )
            updated_activity_ids.update(
                int(activity_id)
                for activity_id in backlog_summary.get("updated_activity_ids", [])
            )
            historical_excluded_ids.update(
                int(activity_id)
                for activity_id in backlog_summary.get("excluded_activity_ids", [])
            )
            trackpoint_errors = int(changed_summary.get("errors", 0) or 0) + int(
                backlog_summary.get("errors", 0) or 0
            )
            historical_warnings = int(backlog_summary.get("warnings", 0) or 0)
            if trackpoint_errors:
                raise RuntimeError(
                    f"trackpoint ingestion encountered {trackpoint_errors} error(s)"
                )
            print(
                "[OK] Trackpoints ingested: "
                f"changed={changed_summary.get('ingested_activities', 0)}, "
                f"backlog={backlog_summary.get('ingested_activities', 0)}"
            )

        refresh_ids = None
        start_ts_iso = None

        if rebuild_derived_metrics_all:
            # Full rebuild across all activities.
            refresh_ids = None
            start_ts_iso = None
        elif rebuild_derived_metrics:
            # Rebuild over selected time window when provided, else all activities.
            if days and int(days) > 0:
                refresh_ids = sorted(updated_activity_ids)
                start_ts_iso = (
                    datetime.now(timezone.utc) - timedelta(days=int(days) + 1)
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            refresh_ids = sorted(updated_activity_ids)
            if days and int(days) > 0:
                start_ts_iso = (
                    datetime.now(timezone.utc) - timedelta(days=int(days) + 1)
                ).strftime("%Y-%m-%dT%H:%M:%SZ")

        refresh_summary = refresh_post_sync_tables(
            conn,
            activity_ids=refresh_ids,
            start_ts_iso=start_ts_iso,
            excluded_activity_ids=sorted(historical_excluded_ids),
        )
        if refresh_summary.get("errors", 0) > 0:
            raise RuntimeError("derived-table refresh encountered errors")
        print(
            "[OK] Derived tables refreshed: "
            f"targets={refresh_summary.get('target_activities', 0)}, "
            f"upserted={refresh_summary.get('rows_upserted', 0)}, "
            f"zones={refresh_summary.get('zones_updated', 0)}"
        )

    except Exception as e:
        logger.exception("Post-sync update failed")
        print(f"[ERROR] Post-sync update failed: {e}")
        return 2
    finally:
        if conn is not None:
            conn.close()

    if historical_warnings:
        print("[PARTIAL] Sync complete with historical reconciliation warnings")
    else:
        print("[SUCCESS] Sync complete")
    return 0


def main():
    # This packaging-only check is intentionally dispatched before the normal
    # worker and its sync-argument allowlist. It imports no authentication,
    # browser, database, archive, or synchronization path.
    if (
        len(sys.argv) > 1
        and sys.argv[1] == _BUNDLED_GIVEMYDATA_DIAGNOSTIC_FLAG
    ):
        raise SystemExit(_check_bundled_givemydata_runtime())

    if len(sys.argv) > 1 and sys.argv[1] == _BUNDLED_GIVEMYDATA_FLAG:
        raise SystemExit(_run_bundled_givemydata(sys.argv[2:]))

    if getattr(sys, "frozen", False) and "pyi_splash" in sys.modules:
        return

    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=os.environ.get("GARMIN_DATA_HUB_LOG_LEVEL", "INFO").upper(),
            format="[%(levelname)s] %(name)s: %(message)s",
        )

    parser = argparse.ArgumentParser(
        prog="garmin-sync",
        description="Sync Garmin Connect data into local SQLite via garmin-givemydata",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python -m garmin_data_hub.cli_backup_ingest --visible
    python -m garmin_data_hub.cli_backup_ingest --visible --days 30
        """,
    )

    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="Custom database path (default: %%LOCALAPPDATA%%/GarminDataHub/garmin.db)",
    )

    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Number of days to sync (default: garmin-givemydata default)",
    )

    parser.add_argument(
        "--visible",
        action="store_true",
        help="Show browser window during login (recommended)",
    )

    parser.add_argument(
        "--chrome",
        action="store_true",
        help="Legacy compatibility flag (upstream garmin-givemydata ignores it)",
    )

    parser.add_argument(
        "--rebuild-derived-metrics",
        action="store_true",
        help=(
            "Rebuild persisted derived metrics after sync. Uses --days window when provided; "
            "otherwise rebuilds all activities."
        ),
    )

    parser.add_argument(
        "--rebuild-derived-metrics-all",
        action="store_true",
        help="Force a full rebuild of persisted derived metrics across all activities",
    )

    parser.add_argument(
        "--derived-metrics-only",
        action="store_true",
        help="Skip Garmin download and only run schema + derived-metrics refresh",
    )

    args, extra = parser.parse_known_args()

    if args.rebuild_derived_metrics_all:
        args.rebuild_derived_metrics = True

    db_path = Path(args.db) if args.db else default_db_path()

    rc = run_sync(
        db_path=db_path,
        days=args.days,
        visible=args.visible,
        chrome=args.chrome,
        extra_args=extra or None,
        rebuild_derived_metrics=args.rebuild_derived_metrics,
        rebuild_derived_metrics_all=args.rebuild_derived_metrics_all,
        derived_metrics_only=args.derived_metrics_only,
    )

    sys.exit(rc)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
