"""Durable reconciliation for historical zero-trackpoint archives."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import zipfile
from collections import Counter
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

from garmin_data_hub.db import queries
from garmin_data_hub.ingest.archive_parser import (
    PARSER_ALGORITHM_VERSION,
    ArchiveFormat,
    ArchiveParseStatus,
    extract_activity_id_from_member,
    parse_activity_archive,
)
from garmin_data_hub.ingest.fingerprint import sha256_file, stat_signature
from garmin_data_hub.ingest.trackpoints import (
    _activity_id_from_zip_filename,
    _activity_identity,
    _candidate_archive_paths,
)


TERMINAL_STATUSES = {
    "ingested",
    "resolved_no_records",
    "unsupported",
    "malformed",
    "mismatch",
    "ambiguous",
}
WARNING_TERMINAL_STATUSES = {
    "unsupported",
    "malformed",
    "mismatch",
    "ambiguous",
}
BASELINE_SETTING_KEY = "archive_reconciliation_baseline"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _archive_identity(fit_dir: Path, archive_path: Path) -> str:
    root = Path(fit_dir).resolve()
    path = Path(archive_path).resolve()
    try:
        relative = path.relative_to(root)
    except ValueError:
        relative = Path(path.name)
    return relative.as_posix().casefold()


def _activity_id_for_archive(path: Path) -> int | None:
    activity_id = _activity_id_from_zip_filename(path)
    if activity_id is not None:
        return int(activity_id)
    try:
        with zipfile.ZipFile(path, "r") as archive:
            ids = {
                extract_activity_id_from_member(name) for name in archive.namelist()
            }
    except (OSError, zipfile.BadZipFile, zipfile.LargeZipFile):
        return None
    ids.discard(None)
    return int(next(iter(ids))) if len(ids) == 1 else None


def activity_id_for_archive(path: Path) -> int | None:
    """Return safe routing evidence for baseline gating and reconciliation."""
    return _activity_id_for_archive(path)


def _matching_ledger_row(
    conn: sqlite3.Connection,
    *,
    archive_identity: str,
    target_activity_id: int,
    source_size: int,
    source_mtime_ns: int,
    source_sha256: str,
    algorithm_version: str,
) -> sqlite3.Row | tuple | None:
    return conn.execute(
        """
        SELECT status
        FROM archive_reconciliation
        WHERE archive_identity=?
          AND target_activity_id=?
          AND source_size=?
          AND source_mtime_ns=?
          AND source_sha256=?
          AND algorithm_version=?
        ORDER BY reconciliation_id DESC
        LIMIT 1
        """,
        (
            archive_identity,
            target_activity_id,
            source_size,
            source_mtime_ns,
            source_sha256,
            algorithm_version,
        ),
    ).fetchone()


def _latest_ledger_row(
    conn: sqlite3.Connection,
    *,
    archive_identity: str,
    target_activity_id: int,
    algorithm_version: str,
) -> sqlite3.Row | tuple | None:
    return conn.execute(
        """
        SELECT status
        FROM archive_reconciliation
        WHERE archive_identity=?
          AND target_activity_id=?
          AND algorithm_version=?
        ORDER BY reconciliation_id DESC
        LIMIT 1
        """,
        (archive_identity, target_activity_id, algorithm_version),
    ).fetchone()


def _insert_ledger(
    conn: sqlite3.Connection,
    *,
    archive_identity: str,
    target_activity_id: int | None,
    archive_format: str,
    source_size: int,
    source_mtime_ns: int,
    source_sha256: str,
    algorithm_version: str,
    status: str,
    reason_code: str | None,
    identity_evidence: dict[str, object],
    trackpoint_count: int,
    attempted_at_utc: str,
) -> None:
    if target_activity_id is None:
        existing = conn.execute(
            """
            SELECT reconciliation_id
            FROM archive_reconciliation
            WHERE archive_identity=?
              AND target_activity_id IS NULL
              AND source_size=?
              AND source_mtime_ns=?
              AND source_sha256=?
              AND algorithm_version=?
            ORDER BY reconciliation_id DESC
            LIMIT 1
            """,
            (
                archive_identity,
                source_size,
                source_mtime_ns,
                source_sha256,
                algorithm_version,
            ),
        ).fetchone()
        if existing is not None:
            conn.execute(
                """
                UPDATE archive_reconciliation
                SET archive_format=?, status=?, reason_code=?,
                    identity_evidence_json=?, trackpoint_count=?,
                    attempted_at_utc=?, completed_at_utc=?
                WHERE reconciliation_id=?
                """,
                (
                    archive_format,
                    status,
                    reason_code,
                    json.dumps(
                        identity_evidence, sort_keys=True, separators=(",", ":")
                    ),
                    int(trackpoint_count),
                    attempted_at_utc,
                    _utc_now(),
                    int(existing[0]),
                ),
            )
            return
    conn.execute(
        """
        INSERT INTO archive_reconciliation (
          archive_identity, target_activity_id, archive_format,
          source_size, source_mtime_ns, source_sha256, algorithm_version,
          status, reason_code, identity_evidence_json, trackpoint_count,
          attempted_at_utc, completed_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (
          archive_identity, target_activity_id, source_size, source_mtime_ns,
          source_sha256, algorithm_version
        ) DO UPDATE SET
          archive_format=excluded.archive_format,
          status=excluded.status,
          reason_code=excluded.reason_code,
          identity_evidence_json=excluded.identity_evidence_json,
          trackpoint_count=excluded.trackpoint_count,
          attempted_at_utc=excluded.attempted_at_utc,
          completed_at_utc=excluded.completed_at_utc
        """,
        (
            archive_identity,
            target_activity_id,
            archive_format,
            source_size,
            source_mtime_ns,
            source_sha256,
            algorithm_version,
            status,
            reason_code,
            json.dumps(identity_evidence, sort_keys=True, separators=(",", ":")),
            int(trackpoint_count),
            attempted_at_utc,
            _utc_now(),
        ),
    )


def _point_count(conn: sqlite3.Connection, activity_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM activity_trackpoints WHERE activity_id=?",
        (activity_id,),
    ).fetchone()
    return int(row[0] or 0) if row else 0


def _summary() -> dict[str, object]:
    return {
        "candidate_archives": 0,
        "accounted_archives": 0,
        "attempted_archives": 0,
        "terminal_archives": 0,
        "unresolved_archives": 0,
        "unchanged_terminal": 0,
        "ingested_activities": 0,
        "ingested_points": 0,
        "resolved_no_records": 0,
        "warnings": 0,
        "baseline_warnings": 0,
        "errors": 0,
        "updated_activity_ids": [],
        "excluded_activity_ids": [],
        "status_counts": {},
        "baseline_complete": False,
        "candidate_state_sha256": "",
    }


def reconciliation_baseline_is_current(
    conn: sqlite3.Connection,
    algorithm_version: str = PARSER_ALGORITHM_VERSION,
) -> bool:
    """Return whether the explicit maintenance sweep established this version."""
    marker = queries.get_setting(conn, BASELINE_SETTING_KEY, {})
    return bool(
        isinstance(marker, dict)
        and marker.get("algorithm_version") == algorithm_version
    )


def baseline_required_summary(
    *,
    candidate_archives: int = 0,
    excluded_activity_ids: Iterable[int] = (),
) -> dict[str, object]:
    """Describe a deliberately skipped implicit historical sweep."""
    summary = _summary()
    summary["candidate_archives"] = int(candidate_archives)
    summary["unresolved_archives"] = int(candidate_archives)
    summary["excluded_activity_ids"] = sorted(
        {int(activity_id) for activity_id in excluded_activity_ids}
    )
    summary["warnings"] = 1
    summary["baseline_required"] = True
    summary["status_counts"] = {"baseline_required": 1}
    return summary


def _increment_status(summary: dict[str, object], status: str) -> None:
    counts = Counter(summary.get("status_counts", {}))
    counts[status] += 1
    summary["status_counts"] = dict(counts)


def _account_candidate(
    summary: dict[str, object],
    *,
    terminal: bool,
    unresolved: bool = False,
) -> None:
    summary["accounted_archives"] = int(summary["accounted_archives"]) + 1
    if terminal:
        summary["terminal_archives"] = int(summary["terminal_archives"]) + 1
    if unresolved:
        summary["unresolved_archives"] = int(summary["unresolved_archives"]) + 1


def _exclude_activity(summary: dict[str, object], activity_id: int) -> None:
    excluded = summary["excluded_activity_ids"]
    if isinstance(excluded, list) and activity_id not in excluded:
        excluded.append(activity_id)


def reconciliation_baseline_is_complete(summary: dict[str, object]) -> bool:
    """Return whether every baseline candidate has a safe deliberate outcome."""
    candidates = int(summary.get("candidate_archives", 0) or 0)
    accounted = int(summary.get("accounted_archives", 0) or 0)
    terminal = int(summary.get("terminal_archives", 0) or 0)
    unresolved = int(summary.get("unresolved_archives", 0) or 0)
    errors = int(summary.get("errors", 0) or 0)
    return (
        accounted == candidates
        and terminal == accounted
        and unresolved == 0
        and errors == 0
    )


def _finalize_summary(summary: dict[str, object]) -> dict[str, object]:
    excluded = summary.get("excluded_activity_ids", [])
    if isinstance(excluded, list):
        summary["excluded_activity_ids"] = sorted(
            {int(activity_id) for activity_id in excluded}
        )
    summary["baseline_complete"] = reconciliation_baseline_is_complete(summary)
    return summary


def _candidate_state_sha256(fit_dir: Path, candidates: list[Path]) -> str:
    """Fingerprint the candidate universe without opening archive payloads."""
    state: list[tuple[str, int | None, int | None]] = []
    for path in candidates:
        try:
            source_size, source_mtime_ns = stat_signature(path)
        except OSError:
            source_size, source_mtime_ns = None, None
        state.append(
            (
                _archive_identity(fit_dir, path),
                source_size,
                source_mtime_ns,
            )
        )
    payload = json.dumps(state, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _record_unresolved_candidate(
    conn: sqlite3.Connection,
    fit_dir: Path,
    path: Path,
    summary: dict[str, object],
    *,
    algorithm_version: str,
    apply: bool,
    reason_code: str,
    candidate_activity_id: int | None,
) -> None:
    """Record a candidate that cannot safely be bound to an activity row."""
    summary["attempted_archives"] = int(summary["attempted_archives"]) + 1
    evidence: dict[str, object] = {
        "target_activity_id": None,
        "candidate_activity_id": candidate_activity_id,
    }
    try:
        source_size, source_mtime_ns = stat_signature(path)
        source_sha256 = sha256_file(path)
    except OSError:
        summary["errors"] = int(summary["errors"]) + 1
        _increment_status(summary, "fingerprint_error")
        _account_candidate(summary, terminal=False, unresolved=True)
        return

    if apply:
        _insert_ledger(
            conn,
            archive_identity=_archive_identity(fit_dir, path),
            target_activity_id=None,
            archive_format=ArchiveFormat.UNKNOWN.value,
            source_size=source_size,
            source_mtime_ns=source_mtime_ns,
            source_sha256=source_sha256,
            algorithm_version=algorithm_version,
            status="ambiguous",
            reason_code=reason_code,
            identity_evidence=evidence,
            trackpoint_count=0,
            attempted_at_utc=_utc_now(),
        )
        conn.commit()
    summary["errors"] = int(summary["errors"]) + 1
    _increment_status(summary, "ambiguous")
    _account_candidate(summary, terminal=False, unresolved=True)


def reconcile_historical_archives(
    conn: sqlite3.Connection,
    fit_dir: Path,
    *,
    archive_paths: Iterable[Path] | None = None,
    algorithm_version: str = PARSER_ALGORITHM_VERSION,
    apply: bool = True,
    force: bool = False,
) -> dict[str, object]:
    """Reconcile eligible historical archives without replacing existing rows."""
    summary = _summary()
    candidates = _candidate_archive_paths(Path(fit_dir), archive_paths)
    summary["candidate_archives"] = len(candidates)
    summary["candidate_state_sha256"] = _candidate_state_sha256(
        Path(fit_dir), candidates
    )
    if not candidates:
        return _finalize_summary(summary)
    ledger_available = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='archive_reconciliation'"
    ).fetchone() is not None
    if apply and not ledger_available:
        raise RuntimeError("archive reconciliation schema is not installed")

    grouped: dict[int, list[Path]] = {}
    unroutable: list[Path] = []
    for path in candidates:
        activity_id = _activity_id_for_archive(path)
        if activity_id is not None:
            grouped.setdefault(activity_id, []).append(path)
        else:
            unroutable.append(path)

    for path in unroutable:
        _record_unresolved_candidate(
            conn,
            Path(fit_dir),
            path,
            summary,
            algorithm_version=algorithm_version,
            apply=apply,
            reason_code="unresolved_activity_id",
            candidate_activity_id=None,
        )

    for activity_id in sorted(grouped):
        paths = sorted(grouped[activity_id], key=lambda path: str(path).casefold())
        target = _activity_identity(conn, activity_id)
        if target is None:
            activity_exists = conn.execute(
                "SELECT 1 FROM activity WHERE activity_id=?", (activity_id,)
            ).fetchone() is not None
            reason_code = (
                "missing_activity_identity" if activity_exists else "activity_not_found"
            )
            for path in paths:
                _record_unresolved_candidate(
                    conn,
                    Path(fit_dir),
                    path,
                    summary,
                    algorithm_version=algorithm_version,
                    apply=apply,
                    reason_code=reason_code,
                    candidate_activity_id=activity_id,
                )
            continue

        duplicate = len(paths) > 1
        for path_index, path in enumerate(paths, 1):
            try:
                source_size, source_mtime_ns = stat_signature(path)
            except OSError:
                summary["errors"] = int(summary["errors"]) + 1
                _increment_status(summary, "fingerprint_error")
                _exclude_activity(summary, activity_id)
                _account_candidate(summary, terminal=False, unresolved=True)
                continue

            identity = _archive_identity(Path(fit_dir), path)
            current_point_count = _point_count(conn, activity_id)
            latest = (
                _latest_ledger_row(
                    conn,
                    archive_identity=identity,
                    target_activity_id=activity_id,
                    algorithm_version=algorithm_version,
                )
                if ledger_available
                else None
            )
            if current_point_count > 0 and latest is None:
                # This is an ordinary already-populated historical activity,
                # not a reconciliation target. Avoid hashing or parsing it.
                _increment_status(summary, "already_populated")
                _account_candidate(summary, terminal=True)
                continue
            try:
                source_sha256 = sha256_file(path)
            except OSError:
                summary["errors"] = int(summary["errors"]) + 1
                _increment_status(summary, "fingerprint_error")
                _exclude_activity(summary, activity_id)
                _account_candidate(summary, terminal=False, unresolved=True)
                continue
            existing = (
                _matching_ledger_row(
                    conn,
                    archive_identity=identity,
                    target_activity_id=activity_id,
                    source_size=source_size,
                    source_mtime_ns=source_mtime_ns,
                    source_sha256=source_sha256,
                    algorithm_version=algorithm_version,
                )
                if ledger_available
                else None
            )
            if (
                not force
                and existing is not None
                and str(existing[0]) in TERMINAL_STATUSES
            ):
                if str(existing[0]) == "ingested" and current_point_count == 0:
                    # The durable decision says rows were committed, but the
                    # target is empty now.  Replaying silently could conceal
                    # deletion/corruption; require an explicit investigation.
                    summary["errors"] = int(summary["errors"]) + 1
                    _increment_status(summary, "integrity_error")
                    _exclude_activity(summary, activity_id)
                    _account_candidate(summary, terminal=False, unresolved=True)
                    continue
                summary["unchanged_terminal"] = int(
                    summary["unchanged_terminal"]
                ) + 1
                if str(existing[0]) in WARNING_TERMINAL_STATUSES:
                    summary["baseline_warnings"] = int(
                        summary["baseline_warnings"]
                    ) + 1
                if str(existing[0]) != "ingested":
                    _exclude_activity(summary, activity_id)
                _account_candidate(summary, terminal=True)
                continue
            if current_point_count > 0:
                # A populated target is outside the historical lane. A changed
                # archive will be handled by the strict changed/new lane when
                # observed during sync; never append a second historical series.
                _increment_status(summary, "already_populated")
                _account_candidate(summary, terminal=True)
                continue

            attempted_at = _utc_now()
            summary["attempted_archives"] = int(summary["attempted_archives"]) + 1

            if duplicate:
                status = "ambiguous"
                evidence = {
                    "target_activity_id": activity_id,
                    "archive_path_count": len(paths),
                }
                if apply:
                    _insert_ledger(
                        conn,
                        archive_identity=identity,
                        target_activity_id=activity_id,
                        archive_format="unknown",
                        source_size=source_size,
                        source_mtime_ns=source_mtime_ns,
                        source_sha256=source_sha256,
                        algorithm_version=algorithm_version,
                        status=status,
                        reason_code="multiple_archives_for_activity",
                        identity_evidence=evidence,
                        trackpoint_count=0,
                        attempted_at_utc=attempted_at,
                    )
                    conn.commit()
                summary["warnings"] = int(summary["warnings"]) + 1
                summary["baseline_warnings"] = int(
                    summary["baseline_warnings"]
                ) + 1
                _increment_status(summary, status)
                _exclude_activity(summary, activity_id)
                _account_candidate(summary, terminal=True)
                continue

            try:
                result = parse_activity_archive(path, target)
            except Exception as exc:
                if apply:
                    _insert_ledger(
                        conn,
                        archive_identity=identity,
                        target_activity_id=activity_id,
                        archive_format="unknown",
                        source_size=source_size,
                        source_mtime_ns=source_mtime_ns,
                        source_sha256=source_sha256,
                        algorithm_version=algorithm_version,
                        status="parser_error",
                        reason_code=type(exc).__name__,
                        identity_evidence={"target_activity_id": activity_id},
                        trackpoint_count=0,
                        attempted_at_utc=attempted_at,
                    )
                    conn.commit()
                summary["errors"] = int(summary["errors"]) + 1
                _increment_status(summary, "parser_error")
                _exclude_activity(summary, activity_id)
                _account_candidate(summary, terminal=False, unresolved=True)
                continue

            status = str(result.status.value)
            archive_format = str(result.archive_format.value)
            if result.status is ArchiveParseStatus.PARSED:
                if not apply:
                    summary["ingested_activities"] = int(
                        summary["ingested_activities"]
                    ) + 1
                    summary["ingested_points"] = int(summary["ingested_points"]) + len(
                        result.rows
                    )
                    summary["updated_activity_ids"].append(activity_id)
                    _increment_status(summary, "ingested")
                    _account_candidate(summary, terminal=True)
                    continue

                savepoint = f"historical_reconciliation_{activity_id}_{path_index}"
                conn.execute(f"SAVEPOINT {savepoint}")
                try:
                    conn.executemany(
                        """
                        INSERT INTO activity_trackpoints (
                          activity_id, seq, timestamp_utc, latitude, longitude,
                          altitude_m, distance_m, speed_mps, heart_rate_bpm,
                          cadence, power_w, temperature_c
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [(activity_id, *row) for row in result.rows],
                    )
                    _insert_ledger(
                        conn,
                        archive_identity=identity,
                        target_activity_id=activity_id,
                        archive_format=archive_format,
                        source_size=source_size,
                        source_mtime_ns=source_mtime_ns,
                        source_sha256=source_sha256,
                        algorithm_version=algorithm_version,
                        status="ingested",
                        reason_code=None,
                        identity_evidence=result.identity_evidence,
                        trackpoint_count=len(result.rows),
                        attempted_at_utc=attempted_at,
                    )
                except Exception as exc:
                    conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                    conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                    _insert_ledger(
                        conn,
                        archive_identity=identity,
                        target_activity_id=activity_id,
                        archive_format=archive_format,
                        source_size=source_size,
                        source_mtime_ns=source_mtime_ns,
                        source_sha256=source_sha256,
                        algorithm_version=algorithm_version,
                        status="write_error",
                        reason_code=type(exc).__name__,
                        identity_evidence=result.identity_evidence,
                        trackpoint_count=0,
                        attempted_at_utc=attempted_at,
                    )
                    conn.commit()
                    summary["errors"] = int(summary["errors"]) + 1
                    _increment_status(summary, "write_error")
                    _exclude_activity(summary, activity_id)
                    _account_candidate(summary, terminal=False, unresolved=True)
                    continue
                conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                conn.commit()
                summary["ingested_activities"] = int(
                    summary["ingested_activities"]
                ) + 1
                summary["ingested_points"] = int(summary["ingested_points"]) + len(
                    result.rows
                )
                summary["updated_activity_ids"].append(activity_id)
                _increment_status(summary, "ingested")
                _account_candidate(summary, terminal=True)
                continue

            if result.status is ArchiveParseStatus.RECOGNIZED_NO_RECORDS:
                ledger_status = "resolved_no_records"
                if apply:
                    _insert_ledger(
                        conn,
                        archive_identity=identity,
                        target_activity_id=activity_id,
                        archive_format=archive_format,
                        source_size=source_size,
                        source_mtime_ns=source_mtime_ns,
                        source_sha256=source_sha256,
                        algorithm_version=algorithm_version,
                        status=ledger_status,
                        reason_code=result.reason_code,
                        identity_evidence=result.identity_evidence,
                        trackpoint_count=0,
                        attempted_at_utc=attempted_at,
                    )
                    conn.commit()
                summary["resolved_no_records"] = int(
                    summary["resolved_no_records"]
                ) + 1
                _increment_status(summary, ledger_status)
                _exclude_activity(summary, activity_id)
                _account_candidate(summary, terminal=True)
                continue

            if status not in {"unsupported", "malformed", "mismatch", "ambiguous"}:
                status = "parser_error"
            if apply:
                _insert_ledger(
                    conn,
                    archive_identity=identity,
                    target_activity_id=activity_id,
                    archive_format=(
                        archive_format
                        if archive_format
                        in {
                            ArchiveFormat.FIT.value,
                            ArchiveFormat.GPX.value,
                            ArchiveFormat.TCX.value,
                        }
                        else ArchiveFormat.UNKNOWN.value
                    ),
                    source_size=source_size,
                    source_mtime_ns=source_mtime_ns,
                    source_sha256=source_sha256,
                    algorithm_version=algorithm_version,
                    status=status,
                    reason_code=result.reason_code,
                    identity_evidence=result.identity_evidence,
                    trackpoint_count=0,
                    attempted_at_utc=attempted_at,
                )
                conn.commit()
            if status == "parser_error":
                summary["errors"] = int(summary["errors"]) + 1
                _account_candidate(summary, terminal=False, unresolved=True)
            else:
                summary["warnings"] = int(summary["warnings"]) + 1
                summary["baseline_warnings"] = int(
                    summary["baseline_warnings"]
                ) + 1
                _account_candidate(summary, terminal=True)
            _exclude_activity(summary, activity_id)
            _increment_status(summary, status)

    return _finalize_summary(summary)
