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

    metrics_summary = queries.refresh_persisted_activity_metrics(
        conn,
        activity_ids=activity_ids,
        start_ts_iso=start_ts_iso,
        lthr=queries.get_effective_lthr(conn),
    )

    summary.update(metrics_summary)
    return summary
