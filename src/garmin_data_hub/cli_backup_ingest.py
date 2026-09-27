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
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_BUNDLED_GIVEMYDATA_FLAG = "--_run-bundled-givemydata"

from garmin_data_hub.paths import default_db_path, ensure_app_dirs


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

    if cmd and extra_args:
        cmd.extend(extra_args)

    sync_cwd = data_dir

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
    try:
        from garmin_data_hub.db.sqlite import connect_sqlite
        from garmin_data_hub.db.migrate import apply_schema
        from garmin_data_hub.analytics.post_sync_refresh import refresh_post_sync_tables
        from garmin_data_hub.paths import schema_sql_path

        conn = connect_sqlite(db_path)
        apply_schema(conn, schema_sql_path())
        print("[OK] App schema applied")

        refresh_ids = None
        start_ts_iso = None

        if rebuild_derived_metrics_all:
            # Full rebuild across all activities.
            refresh_ids = None
            start_ts_iso = None
        elif rebuild_derived_metrics:
            # Rebuild over selected time window when provided, else all activities.
            if days and int(days) > 0:
                start_ts_iso = (
                    datetime.now(timezone.utc) - timedelta(days=int(days) + 1)
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            if days and int(days) > 0:
                start_ts_iso = (
                    datetime.now(timezone.utc) - timedelta(days=int(days) + 1)
                ).strftime("%Y-%m-%dT%H:%M:%SZ")

        refresh_summary = refresh_post_sync_tables(
            conn,
            activity_ids=refresh_ids,
            start_ts_iso=start_ts_iso,
        )
        if refresh_summary.get("errors", 0) > 0:
            raise RuntimeError("derived-table refresh encountered errors")
        print(
            "[OK] Derived tables refreshed: "
            f"targets={refresh_summary.get('target_activities', 0)}, "
            f"upserted={refresh_summary.get('rows_upserted', 0)}, "
            f"zones={refresh_summary.get('zones_updated', 0)}"
        )

        conn.close()
    except Exception as e:
        logger.exception("Post-sync update failed")
        print(f"[ERROR] Post-sync update failed: {e}")
        return 2

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
