from __future__ import annotations
import logging
import sqlite3
from datetime import datetime, timedelta, timezone

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


def update_athlete_profile(conn: sqlite3.Connection):
    """
    Calculates and updates athlete profile metrics like HRMax and LTHR.
    Uses recent activity data (last 90 days) for robust estimates.
    """

    # Use central DB helpers to compute a robust HRmax and suggested LTHR
    cutoff_iso = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
    try:
        from garmin_data_hub.db import queries as db_queries

        hrmax_calc, lthr_calc = db_queries.get_hrmax_robust_and_lthr(
            conn, cutoff_iso, percentile=0.995
        )

        if hrmax_calc:
            db_queries.ensure_athlete_profile_table(conn)
            db_queries.set_calculated_metrics(conn, hrmax_calc, lthr_calc)
            message = f"Athlete profile updated. HRMax: {hrmax_calc} bpm, LTHR: {lthr_calc} bpm"
            logger.info(message)
            print(message)
        else:
            logger.info("No recent HR data available to update athlete profile")
            print("No recent HR data to update athlete profile.")
    except (ImportError, sqlite3.Error, TypeError, ValueError):
        logger.exception("Failed to update athlete profile from synced activities")
        print("ERROR updating athlete_profile via db.queries. See logs for details.")
