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
) -> dict[str, int]:
    """Refresh app-owned derived tables after garmin-givemydata sync completes."""
    summary = {
        "target_activities": 0,
        "rows_upserted": 0,
        "zones_updated": 0,
        "errors": 0,
    }

    try:
        update_athlete_profile(conn)
    except (sqlite3.Error, TypeError, ValueError, ImportError):
        # Keep refreshing metrics even if athlete-profile update fails.
        logger.warning(
            "Athlete profile refresh failed; continuing with activity metric refresh",
            exc_info=True,
        )

    effective_lthr = queries.get_effective_lthr(conn)
    metrics_summary = queries.refresh_persisted_activity_metrics(
        conn,
        activity_ids=activity_ids,
        start_ts_iso=start_ts_iso,
        lthr=effective_lthr,
    )
    summary.update(metrics_summary)

    # Incremental syncs usually target only changed or trackpoint-backed activities.
    # Do a small top-off pass for any rows still flagged as needing derived metrics so
    # the sync page does not show a stale non-zero count after an otherwise successful run.
    if activity_ids is not None or start_ts_iso is not None:
        remaining_ids = queries.list_activities_needing_metrics(conn)
        if remaining_ids:
            topoff_summary = queries.refresh_persisted_activity_metrics(
                conn,
                activity_ids=remaining_ids,
                lthr=effective_lthr,
            )
            for key in summary:
                summary[key] += int(topoff_summary.get(key, 0) or 0)

    return summary
