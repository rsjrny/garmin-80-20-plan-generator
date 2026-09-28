"""Read-only archive classification harness for Phase 3M.5A.

The harness calls only the side-effect-free archive parser.  It opens SQLite
with ``mode=ro&immutable=1`` and never invokes reconciliation or ingestion.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from garmin_data_hub.ingest.archive_parser import (
    PARSER_ALGORITHM_VERSION,
    ActivityIdentity,
    ArchiveParseStatus,
    parse_activity_archive,
)
from garmin_data_hub.ingest.reconciliation import activity_id_for_archive


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _timestamps_are_nondecreasing(values: list[str]) -> bool:
    instants = [
        datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
        for value in values
    ]
    return all(
        current >= previous
        for previous, current in zip(instants, instants[1:])
    )


def _previous_outcomes(path: Path | None) -> dict[int, dict[str, Any]]:
    if path is None:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        int(item["target_activity_id"]): item
        for item in payload.get("outcomes", [])
    }


def _activity_identity(
    conn: sqlite3.Connection,
    activity_id: int,
) -> tuple[ActivityIdentity, str | None] | None:
    row = conn.execute(
        """
        SELECT activity_id, start_time_gmt, start_time_local,
               elapsed_duration_seconds, activity_type
        FROM activity
        WHERE activity_id=?
        """,
        (activity_id,),
    ).fetchone()
    if row is None or row[1] is None:
        return None
    return (
        ActivityIdentity(
            activity_id=int(row[0]),
            start_time_utc=str(row[1]),
            duration_s=float(row[3]) if row[3] is not None else None,
            activity_type=str(row[4]) if row[4] is not None else None,
        ),
        str(row[2]) if row[2] is not None else None,
    )


def classify(
    db_path: Path,
    archive_dir: Path,
    previous_outcomes_path: Path | None = None,
) -> dict[str, Any]:
    """Classify every ZIP without mutating the database or archives."""
    db_path = db_path.resolve()
    archive_dir = archive_dir.resolve()
    archives = sorted(archive_dir.glob("*.zip"), key=lambda item: item.name.casefold())
    previous = _previous_outcomes(previous_outcomes_path)
    before = {
        path: (path.stat().st_size, path.stat().st_mtime_ns, _sha256(path))
        for path in archives
    }
    db_before = (db_path.stat().st_size, db_path.stat().st_mtime_ns)
    outcomes: list[dict[str, Any]] = []

    uri = db_path.as_uri() + "?mode=ro&immutable=1"
    with sqlite3.connect(uri, uri=True) as conn:
        for archive_path in archives:
            activity_id = activity_id_for_archive(archive_path)
            prior = previous.get(activity_id) if activity_id is not None else None
            base: dict[str, Any] = {
                "archive_filename": archive_path.name,
                "archive_sha256": before[archive_path][2],
                "activity_id": activity_id,
                "previous_status": (
                    prior.get("semantic_identity_outcome") if prior else None
                ),
                "previous_reason_code": prior.get("ledger_reason") if prior else None,
            }
            if activity_id is None:
                outcomes.append(
                    {
                        **base,
                        "classification": "ambiguous",
                        "parser_status": None,
                        "reason_code": "unresolved_activity_id",
                        "row_count": 0,
                        "identity_evidence": {},
                    }
                )
                continue
            identity_record = _activity_identity(conn, activity_id)
            if identity_record is None:
                outcomes.append(
                    {
                        **base,
                        "classification": "ambiguous",
                        "parser_status": None,
                        "reason_code": "missing_activity_identity",
                        "row_count": 0,
                        "identity_evidence": {},
                    }
                )
                continue
            target, start_time_local = identity_record
            result = parse_activity_archive(archive_path, target)
            classification = {
                ArchiveParseStatus.PARSED: "matched",
                ArchiveParseStatus.RECOGNIZED_NO_RECORDS: "matched_recordless",
            }.get(result.status, result.status.value)
            timestamps = [str(row[1]) for row in result.rows]
            sequences = [int(row[0]) for row in result.rows]
            outcomes.append(
                {
                    **base,
                    "classification": classification,
                    "parser_status": result.status.value,
                    "archive_format": result.archive_format.value,
                    "reason_code": result.reason_code,
                    "row_count": len(result.rows),
                    "first_timestamp_utc": timestamps[0] if timestamps else None,
                    "last_timestamp_utc": timestamps[-1] if timestamps else None,
                    "sequence_is_contiguous_and_unique": sequences
                    == list(range(len(sequences))),
                    "timestamps_are_nondecreasing": _timestamps_are_nondecreasing(
                        timestamps
                    ),
                    "database_identity": {
                        "start_time_gmt": target.start_time_utc,
                        "start_time_local": start_time_local,
                        "duration_s": target.duration_s,
                        "activity_type": target.activity_type,
                    },
                    "identity_evidence": result.identity_evidence,
                }
            )

    input_changes = []
    for path, original in before.items():
        current = (path.stat().st_size, path.stat().st_mtime_ns, _sha256(path))
        if current != original:
            input_changes.append(path.name)
    db_after = (db_path.stat().st_size, db_path.stat().st_mtime_ns)
    counts = Counter(item["classification"] for item in outcomes)
    return {
        "phase": "3M.5A",
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace(
            "+00:00", "Z"
        ),
        "mode": "read_only_classification",
        "parser_algorithm_version": PARSER_ALGORITHM_VERSION,
        "inputs": {
            "database": str(db_path),
            "archive_directory": str(archive_dir),
            "previous_outcomes": (
                str(previous_outcomes_path.resolve())
                if previous_outcomes_path is not None
                else None
            ),
        },
        "safety": {
            "database_open_flags": "mode=ro&immutable=1",
            "reconciliation_invoked": False,
            "ingestion_invoked": False,
            "database_size_and_mtime_unchanged": db_before == db_after,
            "archive_input_changes": input_changes,
        },
        "archive_count": len(outcomes),
        "classification_counts": dict(sorted(counts.items())),
        "matched_trackpoint_count": sum(
            int(item["row_count"])
            for item in outcomes
            if item["classification"] == "matched"
        ),
        "outcomes": outcomes,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--archives", type=Path, required=True)
    parser.add_argument("--previous-outcomes", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    report = classify(args.db, args.archives, args.previous_outcomes)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report["classification_counts"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
