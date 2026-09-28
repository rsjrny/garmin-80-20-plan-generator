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
import logging
import subprocess
import multiprocessing
import os
import stat
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
_ORIGINAL_CANONICAL_INGESTER = ingest_trackpoints_from_archives
_ORIGINAL_COMPATIBILITY_INGESTER = ingest_trackpoints_from_fit_archives
_ORIGINAL_HISTORICAL_RECONCILER = reconcile_historical_archives


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


def _find_givemydata_cmd() -> list[str] | None:
    """Return a runnable garmin-givemydata command for this environment.

    A frozen build reuses this executable with an internal dispatch flag.  Pip's
    Windows console launcher cannot be copied into a release because it embeds
    the absolute path to the build virtual environment.
    """
    import shutil

    if getattr(sys, "frozen", False):
        return [sys.executable, _BUNDLED_GIVEMYDATA_FLAG]

    found = shutil.which("garmin-givemydata")
    if found:
        return [found]

    print("[ERROR] 'garmin-givemydata' not found.")
    print("[INFO]  Install it: pip install garmin-givemydata")
    return None


def _run_bundled_givemydata(args: list[str]) -> int:
    """Run the packaged garmin-givemydata entry point with isolated arguments."""
    try:
        args = _with_no_trackpoints(_validate_upstream_sync_args(args))
    except ValueError as exc:
        print(f"[ERROR] Unsupported upstream sync argument: {exc}", file=sys.stderr)
        return 2

    original_argv = sys.argv
    sys.argv = ["garmin-givemydata", *args]
    try:
        for stream in (sys.stdout, sys.stderr):
            reconfigure = getattr(stream, "reconfigure", None)
            if callable(reconfigure):
                reconfigure(errors="replace")

        data_dir = Path(os.environ.get("GARMIN_DATA_DIR", default_db_path().parent))
        driver_dir = data_dir / "drivers"
        driver_dir.mkdir(parents=True, exist_ok=True)

        # SeleniumBase otherwise downloads Chrome drivers into its installed
        # package directory, which is read-only under Program Files.
        from seleniumbase.core import browser_launcher

        browser_launcher.override_driver_dir(str(driver_dir))

        from garmin_givemydata import main as givemydata_main

        result = givemydata_main()
    except SystemExit as exc:
        if exc.code is None:
            return 0
        if isinstance(exc.code, int):
            return exc.code
        print(exc.code, file=sys.stderr)
        return 1
    finally:
        sys.argv = original_argv

    return int(result) if isinstance(result, int) else 0


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

    ensure_app_dirs()

    print("=" * 60)
    print("Garmin Data Sync  (garmin-givemydata)")
    print("=" * 60)
    print(f"Database: {db_path}")
    if days:
        print(f"Days:     {days}")
    print()

    cmd = None
    if not derived_metrics_only:
        cmd = _find_givemydata_cmd()
        if not cmd:
            return 1

    data_dir = db_path.parent

    env = os.environ.copy()
    env["GARMIN_DATA_DIR"] = str(data_dir)
    env["PYTHONUNBUFFERED"] = "1"
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
