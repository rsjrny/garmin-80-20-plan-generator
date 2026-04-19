from __future__ import annotations

import sqlite3
import zipfile
from collections.abc import Iterable
from pathlib import Path

from garmin_mcp.parse_activity_files import (
    _activity_id_from_zip_filename as _parse_activity_id_from_zip_filename,
)
from garmin_mcp.parse_activity_files import (
    _extract_activity_id_from_member as _parse_activity_id_from_member,
)
from garmin_mcp.parse_activity_files import (
    _track_rows_from_fit_bytes as _parse_track_rows_from_fit_bytes,
)
from garmin_mcp.parse_activity_files import parse_trackpoints_from_fit_archive


def _extract_activity_id_from_member(name: str) -> int | None:
    return _parse_activity_id_from_member(name)


def _get_target_activity_ids(
    conn: sqlite3.Connection,
    activity_ids: list[int],
    replace_existing: bool,
) -> list[int]:
    if not activity_ids:
        return []

    placeholders = ", ".join("?" for _ in activity_ids)
    if replace_existing:
        rows = conn.execute(
            f"""
            SELECT activity_id
            FROM activity
            WHERE activity_id IN ({placeholders})
            ORDER BY start_time_gmt DESC, activity_id DESC
            """,
            activity_ids,
        ).fetchall()
    else:
        rows = conn.execute(
            f"""
            SELECT a.activity_id
            FROM activity a
            LEFT JOIN activity_trackpoints t ON t.activity_id = a.activity_id
            WHERE a.activity_id IN ({placeholders})
              AND t.activity_id IS NULL
            ORDER BY a.start_time_gmt DESC, a.activity_id DESC
            """,
            activity_ids,
        ).fetchall()
    return [int(r[0]) for r in rows if r and r[0] is not None]


def _candidate_archive_paths(
    fit_dir: Path,
    archive_paths: Iterable[Path] | None,
) -> list[Path]:
    if archive_paths is None:
        return sorted(fit_dir.glob("*.zip"))

    unique_paths: dict[Path, None] = {}
    for archive_path in archive_paths:
        path = Path(archive_path)
        if path.suffix.lower() != ".zip" or not path.exists():
            continue
        unique_paths[path] = None
    return list(unique_paths)


def _activity_id_from_zip_filename(zip_path: Path) -> int | None:
    """Extract activity ID from garmin-givemydata ZIP filename.

    Expected format: YYYY-MM-DD_<activity_id>_<name>.zip
    Tries all numeric groups >= 7 digits (activity IDs are large ints).
    """
    return _parse_activity_id_from_zip_filename(zip_path)


def _track_rows_from_fit_bytes(fit_blob: bytes) -> list[tuple]:
    return _parse_track_rows_from_fit_bytes(fit_blob)


def ingest_trackpoints_from_fit_archives(
    conn: sqlite3.Connection,
    fit_dir: Path,
    *,
    replace_existing: bool = False,
    max_activities: int | None = None,
    archive_paths: Iterable[Path] | None = None,
) -> dict[str, object]:
    """Ingest per-record GPS trackpoints from downloaded FIT zip archives.

    Expected archive naming pattern is the garmin-givemydata format:
    YYYY-MM-DD_<activity_id>_<name>.zip containing <activity_id>_ACTIVITY.fit.
    """
    summary = {
        "target_activities": 0,
        "matched_archives": 0,
        "ingested_activities": 0,
        "ingested_points": 0,
        "skipped_no_fit": 0,
        "skipped_no_records": 0,
        "errors": 0,
        "target_activity_ids": [],
        "updated_activity_ids": [],
    }

    if not fit_dir.exists():
        return summary

    archive_by_activity: dict[int, Path] = {}
    candidate_archives = _candidate_archive_paths(fit_dir, archive_paths)
    if not candidate_archives:
        return summary

    # Fast path: extract activity ID from ZIP filename (no file open needed).
    # Falls back to scanning ZIP member names only when filename yields nothing.
    fallback_zips: list[Path] = []
    for zip_path in candidate_archives:
        activity_id = _activity_id_from_zip_filename(zip_path)
        if activity_id is not None:
            if activity_id not in archive_by_activity:
                archive_by_activity[activity_id] = zip_path
        else:
            fallback_zips.append(zip_path)

    # Fallback: open ZIPs where filename parsing was inconclusive.
    for zip_path in fallback_zips:
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                for member in zf.namelist():
                    activity_id = _extract_activity_id_from_member(member)
                    if activity_id is None:
                        continue
                    if activity_id not in archive_by_activity:
                        archive_by_activity[activity_id] = zip_path
                    break
        except Exception:
            continue

    target_ids = _get_target_activity_ids(
        conn,
        list(archive_by_activity),
        replace_existing,
    )
    if max_activities is not None and max_activities > 0:
        target_ids = target_ids[:max_activities]

    summary["target_activities"] = len(target_ids)
    summary["target_activity_ids"] = list(target_ids)
    summary["matched_archives"] = len(archive_by_activity)
    if not target_ids:
        return summary

    total = len(target_ids)
    matched = len(archive_by_activity)
    print(f"[trackpoints] {matched}/{total} activities matched to archives", flush=True)

    BATCH_SIZE = 25
    batch_count = 0

    for idx, activity_id in enumerate(target_ids, 1):
        zip_path = archive_by_activity.get(activity_id)
        if not zip_path:
            continue

        try:
            parsed_activity_id, rows = parse_trackpoints_from_fit_archive(zip_path)
            if parsed_activity_id is None:
                summary["skipped_no_fit"] += 1
                continue
            if parsed_activity_id != activity_id:
                summary["errors"] += 1
                print(
                    f"[trackpoints] {idx}/{total} {activity_id}: "
                    f"ERROR archive activity mismatch ({parsed_activity_id})",
                    flush=True,
                )
                continue
            if not rows:
                summary["skipped_no_records"] += 1
                print(
                    f"[trackpoints] {idx}/{total} {activity_id}: no records", flush=True
                )
                continue

            conn.execute(
                "DELETE FROM activity_trackpoints WHERE activity_id = ?",
                (activity_id,),
            )
            conn.executemany(
                """
                INSERT INTO activity_trackpoints (
                    activity_id,
                    seq,
                    timestamp_utc,
                    latitude,
                    longitude,
                    altitude_m,
                    distance_m,
                    speed_mps,
                    heart_rate_bpm,
                    cadence,
                    power_w,
                    temperature_c
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [(activity_id, *row) for row in rows],
            )

            summary["ingested_activities"] += 1
            summary["ingested_points"] += len(rows)
            summary["updated_activity_ids"].append(activity_id)
            batch_count += 1
            print(
                f"[trackpoints] {idx}/{total} {activity_id}: {len(rows)} pts",
                flush=True,
            )

            if batch_count >= BATCH_SIZE:
                conn.commit()
                batch_count = 0

        except Exception as exc:
            summary["errors"] += 1
            print(f"[trackpoints] {idx}/{total} {activity_id}: ERROR {exc}", flush=True)

    conn.commit()
    print(
        f"[trackpoints] done: {summary['ingested_activities']} activities, "
        f"{summary['ingested_points']} points, {summary['errors']} errors",
        flush=True,
    )
    return summary
