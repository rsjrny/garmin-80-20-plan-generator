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
    try:
        if apply:
            apply_schema(conn, schema_sql_path())
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
            queries.set_setting(
                conn,
                BASELINE_SETTING_KEY,
                {
                    "algorithm_version": PARSER_ALGORITHM_VERSION,
                    "completed_at_utc": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    ),
                    "candidate_archives": int(
                        summary.get("candidate_archives", 0) or 0
                    ),
                    "warnings": int(summary.get("warnings", 0) or 0),
                    "accounted_archives": int(
                        summary.get("accounted_archives", 0) or 0
                    ),
                    "terminal_archives": int(
                        summary.get("terminal_archives", 0) or 0
                    ),
                    "unresolved_archives": int(
                        summary.get("unresolved_archives", 0) or 0
                    ),
                },
            )
        return summary
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
