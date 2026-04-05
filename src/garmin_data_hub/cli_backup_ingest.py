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


def _snapshot_fit_archives(fit_dir: Path) -> dict[str, tuple[int, int]]:
    if not fit_dir.exists():
        return {}

    snapshot: dict[str, tuple[int, int]] = {}
    for zip_path in fit_dir.glob("*.zip"):
        try:
            stat = zip_path.stat()
        except OSError:
            continue
        snapshot[zip_path.name] = (stat.st_mtime_ns, stat.st_size)
    return snapshot


def _find_changed_fit_archives(
    fit_dir: Path,
    before: dict[str, tuple[int, int]],
) -> list[Path]:
    if not fit_dir.exists():
        return []

    changed: list[Path] = []
    for zip_path in fit_dir.glob("*.zip"):
        try:
            stat = zip_path.stat()
        except OSError:
            continue
        signature = (stat.st_mtime_ns, stat.st_size)
        if before.get(zip_path.name) != signature:
            changed.append(zip_path)
    return changed


def _find_givemydata_cmd() -> list[str] | None:
    """Locate the garmin-givemydata executable."""
    import shutil

    exe_dir = None
    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).parent

    if exe_dir:
        for candidate in [
            exe_dir / "garmin-givemydata.exe",
            exe_dir / "_internal" / "garmin-givemydata.exe",
        ]:
            if candidate.exists():
                return [str(candidate)]

    found = shutil.which("garmin-givemydata")
    if found:
        return [found]

    print("[ERROR] 'garmin-givemydata' not found.")
    print("[INFO]  Install it: pip install garmin-givemydata")
    if exe_dir:
        print(f"[INFO]  Or bundle 'garmin-givemydata.exe' alongside: {exe_dir}")
    return None


def run_sync(
    db_path: Path,
    days: int | None = None,
    visible: bool = False,
    chrome: bool = False,
    extra_args: list[str] | None = None,
    skip_trackpoints: bool = False,
    rebuild_trackpoints: bool = False,
    trackpoints_max: int | None = None,
    rebuild_derived_metrics: bool = False,
    rebuild_derived_metrics_all: bool = False,
    derived_metrics_only: bool = False,
) -> int:
    """Invoke garmin-givemydata to sync data into db_path.

    Returns process exit code (0 = success).
    """
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
    if db_path.name != "garmin.db":
        print(
            f"[WARN] garmin-givemydata writes to 'garmin.db' in GARMIN_DATA_DIR; "
            f"custom filename '{db_path.name}' is ignored."
        )

    env = os.environ.copy()
    env["GARMIN_DATA_DIR"] = str(data_dir)
    env["PYTHONUNBUFFERED"] = "1"
    if chrome:
        _clear_stale_chrome_profile_locks(data_dir / "browser_profile")
    fit_dir = data_dir / "fit"
    fit_snapshot = _snapshot_fit_archives(fit_dir)

    if cmd and days is not None:
        cmd.extend(["--days", str(days)])

    if cmd and visible:
        cmd.append("--visible")

    if cmd and chrome:
        cmd.append("--chrome")

    if cmd and extra_args:
        cmd.extend(extra_args)

    exe_dir = Path(sys.executable).parent if getattr(sys, "frozen", False) else None

    if derived_metrics_only:
        print("[SKIP] Garmin download disabled (--derived-metrics-only)")
    else:
        try:
            subprocess.run(cmd, check=True, cwd=exe_dir, env=env)
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
        from garmin_data_hub.ingest.trackpoints import (
            ingest_trackpoints_from_fit_archives,
        )
        from garmin_data_hub.paths import schema_sql_path

        conn = connect_sqlite(data_dir / "garmin.db")
        apply_schema(conn, schema_sql_path())
        print("[OK] App schema applied")

        trackpoint_summary = None
        if derived_metrics_only:
            print("[SKIP] Trackpoint ingestion disabled (--derived-metrics-only)")
        elif skip_trackpoints:
            print("[SKIP] Trackpoint ingestion disabled (--skip-trackpoints)")
        else:
            changed_fit_archives = _find_changed_fit_archives(fit_dir, fit_snapshot)
            if changed_fit_archives:
                print(
                    f"[trackpoints] checking {len(changed_fit_archives)} new/updated FIT archive(s)"
                )
            elif not rebuild_trackpoints:
                print("[SKIP] No new FIT archives downloaded")
            trackpoint_summary = ingest_trackpoints_from_fit_archives(
                conn,
                fit_dir,
                replace_existing=rebuild_trackpoints,
                max_activities=trackpoints_max,
                archive_paths=None if rebuild_trackpoints else changed_fit_archives,
            )
            print(
                "[OK] Trackpoints: "
                f"targets={trackpoint_summary['target_activities']}, "
                f"archives={trackpoint_summary['matched_archives']}, "
                f"activities={trackpoint_summary['ingested_activities']}, "
                f"points={trackpoint_summary['ingested_points']}, "
                f"errors={trackpoint_summary['errors']}"
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
                start_ts_iso = (
                    datetime.now(timezone.utc) - timedelta(days=int(days) + 1)
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            if trackpoint_summary:
                candidate_ids = trackpoint_summary.get("target_activity_ids") or []
                refresh_ids = [int(v) for v in candidate_ids if v is not None]

            if not refresh_ids and days and int(days) > 0:
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
        help="Use Chrome engine for login",
    )

    parser.add_argument(
        "--skip-trackpoints",
        action="store_true",
        help="Skip FIT trackpoint extraction into activity_trackpoint",
    )

    parser.add_argument(
        "--rebuild-trackpoints",
        action="store_true",
        help="Rebuild trackpoints for all activities (deletes and reinserts per activity)",
    )

    parser.add_argument(
        "--trackpoints-max",
        type=int,
        default=None,
        help="Optional cap on number of activities to ingest trackpoints for",
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
        skip_trackpoints=args.skip_trackpoints,
        rebuild_trackpoints=args.rebuild_trackpoints,
        trackpoints_max=args.trackpoints_max,
        rebuild_derived_metrics=args.rebuild_derived_metrics,
        rebuild_derived_metrics_all=args.rebuild_derived_metrics_all,
        derived_metrics_only=args.derived_metrics_only,
    )

    sys.exit(rc)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
