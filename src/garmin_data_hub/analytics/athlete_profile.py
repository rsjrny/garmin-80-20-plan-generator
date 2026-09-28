from __future__ import annotations
import logging
import sqlite3
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def _calculate_lthr_from_efforts(
    conn: sqlite3.Connection, days_back: int = 90
) -> int | None:
    """
    Calculate LTHR by finding threshold efforts (high-intensity sustained activities).

    Strategy:
    1. Find activities with average_hr > 80% of recent HRmax (threshold efforts)
    2. Use 95% of the average HR from these efforts as LTHR estimate
    3. Fallback to 0.86 * HRmax if no threshold efforts found
    """
    cutoff_date = (datetime.utcnow() - timedelta(days=days_back)).isoformat()

    try:
        # Get recent HRmax from activity table
        hrmax_result = conn.execute(
            """
            SELECT MAX(max_hr) as hrmax
            FROM activity
            WHERE start_time_gmt >= ?
              AND max_hr IS NOT NULL AND max_hr > 0
        """,
            (cutoff_date,),
        ).fetchone()

        if not hrmax_result or not hrmax_result["hrmax"]:
            return None

        hrmax = hrmax_result["hrmax"]
        threshold_hr_min = hrmax * 0.80

        # Find high-intensity activities (avg_hr above 80% HRmax, duration > 10 min)
        threshold_efforts = conn.execute(
            """
                SELECT average_hr, elapsed_duration_seconds
                FROM activity
                WHERE start_time_gmt >= ?
                    AND average_hr IS NOT NULL
                    AND average_hr >= ?
                    AND elapsed_duration_seconds >= 600
                ORDER BY average_hr DESC
                LIMIT 20
        """,
            (cutoff_date, threshold_hr_min),
        ).fetchall()

        if threshold_efforts:
            total_hr = sum(e["average_hr"] for e in threshold_efforts)
            avg_threshold_hr = total_hr / len(threshold_efforts)

            lthr_calc = int(round(avg_threshold_hr * 0.95))

            lthr_min = int(hrmax * 0.80)
            lthr_max = int(hrmax * 0.95)
            lthr_calc = max(lthr_min, min(lthr_max, lthr_calc))

            return lthr_calc
        else:
            return int(round(hrmax * 0.86))

    except (sqlite3.Error, TypeError, ValueError):
        logger.warning(
            "Could not calculate LTHR from recent threshold efforts", exc_info=True
        )
        return None


def update_athlete_profile(
    conn: sqlite3.Connection,
    *,
    required: bool = False,
):
    """
    Calculates and updates athlete profile metrics like HRMax and LTHR.
    Uses recent activity data (last 90 days) for robust estimates.
    """

    # The required post-sync path uses one outer savepoint.  The canonical
    # threshold service performs a nested atomic value/provenance update and
    # leaves legitimate "no evidence" distinct from an operational failure.
    savepoint_name = f"required_athlete_profile_{id(conn):x}"
    savepoint_active = False
    try:
        from garmin_data_hub.db import queries as db_queries

        if required:
            conn.execute(f"SAVEPOINT {savepoint_name}")
            savepoint_active = True
        from garmin_data_hub.services import thresholds

        result = thresholds.refresh_thresholds(
            conn,
            activity_metric_provenance_version=(
                db_queries.ACTIVITY_METRICS_PROVENANCE_VERSION
            ),
            commit=not required,
        )
        available = {key: value for key, value in result.items() if value is not None}
        if available:
            message = (
                "Athlete thresholds resolved. "
                + ", ".join(f"{key}: {value}" for key, value in available.items())
            )
            logger.info(message)
            print(message)
        else:
            logger.info(
                "No threshold evidence or retained values are available"
            )
            print("No threshold evidence or retained values are available.")
        if savepoint_active:
            conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            savepoint_active = False
    except Exception:
        if savepoint_active:
            try:
                conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            except Exception:
                logger.exception("Failed to roll back required athlete-profile refresh")
        logger.exception("Failed to update athlete profile from synced activities")
        print("ERROR updating athlete_profile via db.queries. See logs for details.")
        if required:
            raise
