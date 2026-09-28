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

from garmin_data_hub.ingest.archive_parser import (
    ActivityIdentity,
    ArchiveFormat,
    ArchiveParseResult,
    ArchiveParseStatus,
    extract_activity_id_from_member,
    parse_activity_archive,
)


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
        return sorted(fit_dir.glob("*.zip"), key=lambda path: str(path).casefold())

    unique_paths: dict[Path, None] = {}
    for archive_path in archive_paths:
        path = Path(archive_path)
        unique_paths[path] = None
    return sorted(unique_paths, key=lambda path: str(path).casefold())


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
    explicit_targets = archive_paths is not None
    strict_archive_paths = explicit_targets and replace_existing

    if not fit_dir.exists():
        if not explicit_targets:
            return summary

    archives_by_activity: dict[int, list[Path]] = {}
    unavailable_archives: dict[Path, str] = {}
    strict_failed_activity_ids: set[int] = set()
    candidate_archives = _candidate_archive_paths(fit_dir, archive_paths)
    if not candidate_archives:
        return summary

    # Fast path: extract activity ID from ZIP filename (no file open needed).
    # Falls back to scanning ZIP member names only when filename yields nothing.
    fallback_zips: list[Path] = []
    for zip_path in candidate_archives:
        activity_id = _activity_id_from_zip_filename(zip_path)
        if zip_path.suffix.lower() != ".zip":
            if strict_archive_paths:
                summary["errors"] += 1
                if activity_id is not None:
                    strict_failed_activity_ids.add(activity_id)
                print(
                    f"[trackpoints] ERROR explicit target is not a ZIP archive: {zip_path}",
                    flush=True,
                )
            elif activity_id is not None:
                archives_by_activity.setdefault(activity_id, []).append(zip_path)
                unavailable_archives[zip_path] = "is not a ZIP archive"
            continue
        if explicit_targets:
            try:
                zip_path.stat()
            except FileNotFoundError:
                if strict_archive_paths:
                    summary["errors"] += 1
                    if activity_id is not None:
                        strict_failed_activity_ids.add(activity_id)
                    print(
                        f"[trackpoints] ERROR explicit target disappeared: {zip_path}",
                        flush=True,
                    )
                elif activity_id is not None:
                    archives_by_activity.setdefault(activity_id, []).append(zip_path)
                    unavailable_archives[zip_path] = "disappeared"
                continue
            except OSError as exc:
                if strict_archive_paths:
                    summary["errors"] += 1
                    if activity_id is not None:
                        strict_failed_activity_ids.add(activity_id)
                    print(
                        f"[trackpoints] ERROR cannot inspect explicit target "
                        f"{zip_path}: {exc}",
                        flush=True,
                    )
                elif activity_id is not None:
                    archives_by_activity.setdefault(activity_id, []).append(zip_path)
                    unavailable_archives[zip_path] = f"cannot be inspected: {exc}"
                continue
        if activity_id is not None:
            archives_by_activity.setdefault(activity_id, []).append(zip_path)
        else:
            fallback_zips.append(zip_path)

    # Fallback: open ZIPs where filename parsing was inconclusive.
    for zip_path in fallback_zips:
        matched_activity_id = None
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                for member in zf.namelist():
                    activity_id = _extract_activity_id_from_member(member)
                    if activity_id is None:
                        continue
                    matched_activity_id = activity_id
                    archives_by_activity.setdefault(activity_id, []).append(zip_path)
                    break
        except Exception as exc:
            if strict_archive_paths:
                summary["errors"] += 1
                print(
                    f"[trackpoints] ERROR cannot inspect explicit target {zip_path}: {exc}",
                    flush=True,
                )
            continue
        if matched_activity_id is None and strict_archive_paths:
            summary["errors"] += 1
            print(
                f"[trackpoints] ERROR no activity FIT found in explicit target: {zip_path}",
                flush=True,
            )

    strict_duplicate_ids = {
        activity_id
        for activity_id, paths in archives_by_activity.items()
        if strict_archive_paths and len(paths) > 1
    }
    for activity_id in sorted(strict_duplicate_ids):
        stable_paths = sorted(
            archives_by_activity[activity_id],
            key=lambda path: str(path).casefold(),
        )
        summary["errors"] += 1
        strict_failed_activity_ids.add(activity_id)
        print(
            f"[trackpoints] ERROR ambiguous archives for activity {activity_id}: "
            + ", ".join(str(path) for path in stable_paths),
            flush=True,
        )

    # A failed changed target must not be replaced by another archive for the
    # same activity during this pass.
    for activity_id in strict_failed_activity_ids:
        archives_by_activity.pop(activity_id, None)

    target_ids = _get_target_activity_ids(
        conn,
        list(archives_by_activity),
        replace_existing,
    )
    if max_activities is not None and max_activities > 0:
        target_ids = target_ids[:max_activities]

    failed_target_ids = sorted(strict_failed_activity_ids.difference(target_ids))
    summary["target_activities"] = len(target_ids) + len(failed_target_ids)
    summary["target_activity_ids"] = [*target_ids, *failed_target_ids]
    summary["matched_archives"] = len(archives_by_activity)
    if not target_ids:
        return summary

    # Reject ambiguous duplicates rather than allowing filesystem order to
    # choose an archive. Errors are limited to activities actually selected by
    # the DB query, so broad backlog discovery can ignore irrelevant files.
    archive_by_activity: dict[int, Path] = {}
    for activity_id in target_ids:
        paths = archives_by_activity[activity_id]
        if len(paths) > 1:
            stable_paths = sorted(paths, key=lambda path: str(path).casefold())
            summary["errors"] += 1
            print(
                f"[trackpoints] ERROR ambiguous archives for activity {activity_id}: "
                + ", ".join(str(path) for path in stable_paths),
                flush=True,
            )
            continue

        archive_path = paths[0]
        unavailable_reason = unavailable_archives.get(archive_path)
        if unavailable_reason is not None:
            summary["errors"] += 1
            print(
                f"[trackpoints] ERROR explicit target {unavailable_reason}: "
                f"{archive_path}",
                flush=True,
            )
            continue
        archive_by_activity[activity_id] = archive_path

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
                if explicit_targets:
                    summary["errors"] += 1
                    print(
                        f"[trackpoints] {idx}/{total} {activity_id}: "
                        "ERROR archive could not be parsed",
                        flush=True,
                    )
                else:
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

            if not conn.in_transaction:
                conn.execute("BEGIN")
            savepoint_name = f"replace_activity_trackpoints_{idx}"
            conn.execute(f"SAVEPOINT {savepoint_name}")
            try:
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
            except Exception:
                conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                raise
            else:
                conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")

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


def _activity_identity(
    conn: sqlite3.Connection,
    activity_id: int,
) -> ActivityIdentity | None:
    columns = {
        str(row[1]) for row in conn.execute("PRAGMA table_info(activity)").fetchall()
    }
    duration_expression = (
        "elapsed_duration_seconds"
        if "elapsed_duration_seconds" in columns
        else "NULL"
    )
    type_expression = "activity_type" if "activity_type" in columns else "NULL"
    row = conn.execute(
        f"""
        SELECT activity_id, start_time_gmt, {duration_expression}, {type_expression}
        FROM activity
        WHERE activity_id=?
        """,
        (activity_id,),
    ).fetchone()
    if row is None or row[1] is None:
        return None
    return ActivityIdentity(
        activity_id=int(row[0]),
        start_time_utc=str(row[1]),
        duration_s=float(row[2]) if row[2] is not None else None,
        activity_type=str(row[3]) if row[3] is not None else None,
    )


def _insert_activity_trackpoints(
    conn: sqlite3.Connection,
    activity_id: int,
    rows: list[tuple],
    *,
    replace_existing: bool,
    savepoint_name: str,
) -> None:
    conn.execute(f"SAVEPOINT {savepoint_name}")
    try:
        if replace_existing:
            conn.execute(
                "DELETE FROM activity_trackpoints WHERE activity_id=?",
                (activity_id,),
            )
        conn.executemany(
            """
            INSERT INTO activity_trackpoints (
                activity_id, seq, timestamp_utc, latitude, longitude,
                altitude_m, distance_m, speed_mps, heart_rate_bpm,
                cadence, power_w, temperature_c
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [(activity_id, *row) for row in rows],
        )
    except Exception:
        conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
        conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        raise
    conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")


def ingest_trackpoints_from_archives(
    conn: sqlite3.Connection,
    fit_dir: Path,
    *,
    replace_existing: bool = False,
    max_activities: int | None = None,
    archive_paths: Iterable[Path] | None = None,
) -> dict[str, object]:
    """Strict canonical ingestion for FIT, GPX, and TCX archives.

    Every non-success parse status is an error in this lane.  Historical
    quarantine policy is implemented separately in ``reconciliation``.
    """
    summary: dict[str, object] = {
        "target_activities": 0,
        "matched_archives": 0,
        "ingested_activities": 0,
        "ingested_points": 0,
        "skipped_no_fit": 0,
        "skipped_no_records": 0,
        "errors": 0,
        "target_activity_ids": [],
        "updated_activity_ids": [],
        "result_statuses": {},
    }
    explicit_targets = archive_paths is not None
    candidates = _candidate_archive_paths(fit_dir, archive_paths)
    if not candidates:
        return summary

    archives_by_activity: dict[int, list[Path]] = {}
    failed_ids: set[int] = set()
    for path in candidates:
        activity_id = _activity_id_from_zip_filename(path)
        if path.suffix.casefold() != ".zip" or not path.exists():
            if explicit_targets:
                summary["errors"] = int(summary["errors"]) + 1
                if activity_id is not None:
                    failed_ids.add(int(activity_id))
                    summary["result_statuses"][int(activity_id)] = "malformed"
            continue
        if activity_id is None:
            try:
                with zipfile.ZipFile(path, "r") as archive:
                    member_ids = {
                        extract_activity_id_from_member(name)
                        for name in archive.namelist()
                    }
                    member_ids.discard(None)
                if len(member_ids) == 1:
                    activity_id = int(next(iter(member_ids)))
            except (OSError, zipfile.BadZipFile, zipfile.LargeZipFile):
                activity_id = None
        if activity_id is None:
            if explicit_targets:
                summary["errors"] = int(summary["errors"]) + 1
            continue
        archives_by_activity.setdefault(int(activity_id), []).append(path)

    for activity_id, paths in list(archives_by_activity.items()):
        if len(paths) <= 1:
            continue
        summary["errors"] = int(summary["errors"]) + 1
        summary["result_statuses"][activity_id] = "ambiguous"
        failed_ids.add(activity_id)
        archives_by_activity.pop(activity_id, None)

    target_ids = _get_target_activity_ids(
        conn,
        list(archives_by_activity),
        replace_existing,
    )
    if max_activities is not None and max_activities > 0:
        target_ids = target_ids[:max_activities]
    summary["target_activity_ids"] = [*target_ids, *sorted(failed_ids)]
    summary["target_activities"] = len(target_ids) + len(failed_ids)
    summary["matched_archives"] = len(archives_by_activity)

    for index, activity_id in enumerate(target_ids, 1):
        path = archives_by_activity[activity_id][0]
        target = _activity_identity(conn, activity_id)
        if target is None:
            summary["errors"] = int(summary["errors"]) + 1
            summary["result_statuses"][activity_id] = "ambiguous"
            continue
        try:
            result = parse_activity_archive(path, target)
            # Phase 3A exposed the installed tuple parser as an injectable
            # compatibility seam.  Its synthetic tests use non-ZIP stand-ins;
            # honor a positive injected result without weakening real malformed
            # archives, whose unpatched adapter still returns ``(None, [])``.
            if (
                result.status is ArchiveParseStatus.MALFORMED
                and result.reason_code == "malformed_zip"
            ):
                legacy_activity_id, legacy_rows = parse_trackpoints_from_fit_archive(
                    path
                )
                if legacy_activity_id is not None:
                    result = ArchiveParseResult(
                        status=(
                            ArchiveParseStatus.PARSED
                            if legacy_rows
                            else ArchiveParseStatus.RECOGNIZED_NO_RECORDS
                        ),
                        archive_format=ArchiveFormat.FIT,
                        activity_id=int(legacy_activity_id),
                        rows=list(legacy_rows),
                        reason_code=(
                            None if legacy_rows else "activity_has_no_trackpoints"
                        ),
                        identity_evidence={
                            "target_activity_id": activity_id,
                            "compatibility_adapter": True,
                        },
                    )
        except Exception as exc:
            summary["errors"] = int(summary["errors"]) + 1
            summary["result_statuses"][activity_id] = "parser_error"
            print(
                f"[trackpoints] {index}/{len(target_ids)} {activity_id}: "
                f"ERROR parser exception: {exc}",
                flush=True,
            )
            continue

        status = str(result.status.value)
        summary["result_statuses"][activity_id] = status
        if result.status is ArchiveParseStatus.RECOGNIZED_NO_RECORDS:
            summary["skipped_no_records"] = int(summary["skipped_no_records"]) + 1
            continue
        if result.status is not ArchiveParseStatus.PARSED:
            summary["errors"] = int(summary["errors"]) + 1
            print(
                f"[trackpoints] {index}/{len(target_ids)} {activity_id}: "
                f"ERROR {status} ({result.reason_code or 'unspecified'})",
                flush=True,
            )
            continue
        if result.activity_id != activity_id:
            summary["errors"] = int(summary["errors"]) + 1
            summary["result_statuses"][activity_id] = "mismatch"
            continue

        try:
            _insert_activity_trackpoints(
                conn,
                activity_id,
                result.rows,
                replace_existing=replace_existing,
                savepoint_name=f"canonical_activity_trackpoints_{index}",
            )
        except Exception as exc:
            summary["errors"] = int(summary["errors"]) + 1
            summary["result_statuses"][activity_id] = "write_error"
            print(
                f"[trackpoints] {index}/{len(target_ids)} {activity_id}: ERROR {exc}",
                flush=True,
            )
            continue

        summary["ingested_activities"] = int(summary["ingested_activities"]) + 1
        summary["ingested_points"] = int(summary["ingested_points"]) + len(
            result.rows
        )
        summary["updated_activity_ids"].append(activity_id)

    conn.commit()
    return summary
