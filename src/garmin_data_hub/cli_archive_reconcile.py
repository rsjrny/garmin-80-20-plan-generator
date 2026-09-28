"""Explicit maintenance command for historical archive reconciliation."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from garmin_data_hub.analytics.post_sync_refresh import refresh_exact_activity_metrics
from garmin_data_hub.db import queries
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.ingest.archive_parser import PARSER_ALGORITHM_VERSION
from garmin_data_hub.ingest.reconciliation import (
    BASELINE_SETTING_KEY,
    reconcile_historical_archives,
    reconciliation_baseline_is_complete,
)
from garmin_data_hub.paths import default_db_path, schema_sql_path


_BASELINE_STATE_FIELDS = (
    "algorithm_version",
    "candidate_archives",
    "warnings",
    "accounted_archives",
    "terminal_archives",
    "unresolved_archives",
)
_CANDIDATE_STATE_FIELD = "candidate_state_sha256"


def _desired_baseline_state(summary: dict[str, object]) -> dict[str, object]:
    """Build durable completed-sweep state, not current-invocation metadata."""
    return {
        "algorithm_version": PARSER_ALGORITHM_VERSION,
        "candidate_archives": int(summary.get("candidate_archives", 0) or 0),
        "warnings": int(summary.get("baseline_warnings", 0) or 0),
        "accounted_archives": int(summary.get("accounted_archives", 0) or 0),
        "terminal_archives": int(summary.get("terminal_archives", 0) or 0),
        "unresolved_archives": int(summary.get("unresolved_archives", 0) or 0),
        _CANDIDATE_STATE_FIELD: str(
            summary.get(_CANDIDATE_STATE_FIELD, "") or ""
        ),
    }


def _baseline_state_changed(
    existing: object,
    desired: dict[str, object],
    summary: dict[str, object],
) -> bool:
    """Return whether this completed sweep established new durable state."""
    if not isinstance(existing, dict):
        return True
    if any(existing.get(field) != desired[field] for field in _BASELINE_STATE_FIELDS):
        return True
    existing_candidate_state = existing.get(_CANDIDATE_STATE_FIELD)
    if existing_candidate_state is not None:
        if existing_candidate_state != desired[_CANDIDATE_STATE_FIELD]:
            return True
    else:
        # Markers created before candidate-state metadata existed can still be
        # recognized without rewriting the real 3M.6 no-work baseline: every
        # current candidate must have reached the unchanged terminal fast path.
        candidates = int(summary.get("candidate_archives", 0) or 0)
        unchanged = int(summary.get("unchanged_terminal", 0) or 0)
        if unchanged != candidates:
            return True
    # For a complete applied sweep, every attempt has reached and committed a
    # terminal ledger decision. This catches fingerprint/forced re-evaluation
    # even when its aggregate accounting and warning classification are equal.
    return int(summary.get("attempted_archives", 0) or 0) > 0


def _connect_read_only(db_path: Path) -> sqlite3.Connection:
    """Open a dry-run database without creating files or changing PRAGMAs."""
    resolved = Path(db_path).resolve(strict=True)
    conn = sqlite3.connect(f"{resolved.as_uri()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def run_reconciliation(
    db_path: Path,
    fit_dir: Path,
    *,
    apply: bool = False,
    force: bool = False,
) -> dict[str, object]:
    """Run a dry-run or applied local reconciliation without Garmin access."""
    archive_paths = sorted(
        fit_dir.glob("*.zip"), key=lambda path: str(path).casefold()
    )
    conn = connect_sqlite(db_path) if apply else _connect_read_only(db_path)
    schema_ready = False
    try:
        if apply:
            apply_schema(conn, schema_sql_path())
            schema_ready = True
        summary = reconcile_historical_archives(
            conn,
            fit_dir,
            archive_paths=archive_paths,
            algorithm_version=PARSER_ALGORITHM_VERSION,
            apply=apply,
            force=force,
        )
        if apply:
            metric_refresh = refresh_exact_activity_metrics(
                conn,
                [
                    int(activity_id)
                    for activity_id in summary.get("updated_activity_ids", [])
                ],
            )
            summary["metric_refresh"] = metric_refresh
            metric_errors = int(metric_refresh.get("errors", 0) or 0)
            if metric_errors:
                summary["errors"] = int(summary.get("errors", 0) or 0) + metric_errors
                summary["baseline_complete"] = False
        if apply and reconciliation_baseline_is_complete(summary):
            desired_baseline = _desired_baseline_state(summary)
            existing_baseline = queries.get_setting(
                conn, BASELINE_SETTING_KEY, None
            )
            if _baseline_state_changed(
                existing_baseline, desired_baseline, summary
            ):
                queries.set_setting(
                    conn,
                    BASELINE_SETTING_KEY,
                    {
                        "algorithm_version": PARSER_ALGORITHM_VERSION,
                        "completed_at_utc": datetime.now(timezone.utc).strftime(
                            "%Y-%m-%dT%H:%M:%S.%fZ"
                        ),
                        "candidate_archives": desired_baseline[
                            "candidate_archives"
                        ],
                        "warnings": desired_baseline["warnings"],
                        "accounted_archives": desired_baseline[
                            "accounted_archives"
                        ],
                        "terminal_archives": desired_baseline[
                            "terminal_archives"
                        ],
                        "unresolved_archives": desired_baseline[
                            "unresolved_archives"
                        ],
                        _CANDIDATE_STATE_FIELD: desired_baseline[
                            _CANDIDATE_STATE_FIELD
                        ],
                    },
                )
        elif apply:
            queries.delete_setting(conn, BASELINE_SETTING_KEY)
        return summary
    except Exception:
        if apply and schema_ready:
            if conn.in_transaction:
                conn.rollback()
            queries.delete_setting(conn, BASELINE_SETTING_KEY)
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="garmin-reconcile-trackpoints",
        description=(
            "Reconcile local historical FIT/GPX/TCX archives. "
            "This command never authenticates to or downloads from Garmin."
        ),
    )
    parser.add_argument("--db", type=Path, default=None)
    parser.add_argument("--fit-dir", type=Path, default=None)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write trackpoints and durable ledger rows (default is dry-run)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-evaluate unchanged terminal fingerprints",
    )
    args = parser.parse_args()

    db_path = args.db or default_db_path()
    fit_dir = args.fit_dir or db_path.parent / "fit"
    summary = run_reconciliation(
        db_path,
        fit_dir,
        apply=bool(args.apply),
        force=bool(args.force),
    )
    print(json.dumps(summary, sort_keys=True))
    raise SystemExit(2 if int(summary.get("errors", 0) or 0) else 0)


if __name__ == "__main__":
    main()
