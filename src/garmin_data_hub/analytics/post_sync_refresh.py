from __future__ import annotations

import logging
import sqlite3

from garmin_data_hub.analytics.athlete_profile import update_athlete_profile
from garmin_data_hub.db import queries

logger = logging.getLogger(__name__)


def refresh_post_sync_tables(
    conn: sqlite3.Connection,
    activity_ids: list[int] | None = None,
    start_ts_iso: str | None = None,
    excluded_activity_ids: list[int] | None = None,
) -> dict[str, int]:
    """Refresh app-owned derived tables after garmin-givemydata sync completes."""
    summary = {
        "target_activities": 0,
        "rows_upserted": 0,
        "zones_updated": 0,
        "errors": 0,
        "athlete_profile_errors": 0,
    }

    try:
        update_athlete_profile(conn, required=True)
    except Exception:
        # Keep refreshing metrics even if athlete-profile update fails.
        logger.warning(
            "Athlete profile refresh failed; continuing with activity metric refresh",
            exc_info=True,
        )
        summary["athlete_profile_errors"] = 1

    effective_lthr = queries.get_effective_lthr(conn)
    metrics_summary = queries.refresh_persisted_activity_metrics(
        conn,
        activity_ids=activity_ids,
        start_ts_iso=start_ts_iso,
        lthr=effective_lthr,
        excluded_activity_ids=excluded_activity_ids,
    )
    summary.update(metrics_summary)
    summary["errors"] = int(metrics_summary.get("errors", 0) or 0) + summary[
        "athlete_profile_errors"
    ]

    # Incremental syncs usually target only changed or trackpoint-backed activities.
    # Do a small top-off pass for any rows still flagged as needing derived metrics so
    # the sync page does not show a stale non-zero count after an otherwise successful run.
    if activity_ids or start_ts_iso is not None:
        excluded_ids = {
            int(activity_id) for activity_id in (excluded_activity_ids or [])
        }
        remaining_ids = [
            activity_id
            for activity_id in queries.list_activities_needing_metrics(conn)
            if activity_id not in excluded_ids
        ]
        if remaining_ids:
            topoff_summary = queries.refresh_persisted_activity_metrics(
                conn,
                activity_ids=remaining_ids,
                lthr=effective_lthr,
                excluded_activity_ids=excluded_ids,
            )
            for key in summary:
                summary[key] += int(topoff_summary.get(key, 0) or 0)

    return summary


def refresh_exact_activity_metrics(
    conn: sqlite3.Connection,
    activity_ids: list[int],
) -> dict[str, int]:
    """Refresh only the supplied activities, without date expansion or top-off."""
    exact_ids = sorted(
        {
            int(activity_id)
            for activity_id in activity_ids
            if activity_id is not None and int(activity_id) > 0
        }
    )
    if not exact_ids:
        return {
            "target_activities": 0,
            "rows_upserted": 0,
            "zones_updated": 0,
            "errors": 0,
        }
    return queries.refresh_persisted_activity_metrics(
        conn,
        activity_ids=exact_ids,
        lthr=queries.get_effective_lthr(conn),
    )
