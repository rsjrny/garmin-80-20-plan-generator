from __future__ import annotations

import logging
import sqlite3
from datetime import date, timedelta
from pathlib import Path

from garmin_data_hub.db import queries as db_queries

logger = logging.getLogger(__name__)


def ensure_athlete_metrics_table(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        db_queries.ensure_athlete_profile_table(conn)
    finally:
        conn.close()


def get_athlete_metrics(db_path: Path) -> dict:
    ensure_athlete_metrics_table(db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        return db_queries.get_athlete_metrics(conn)
    finally:
        conn.close()


def set_calculated_metrics(db_path: Path, hrmax: int | None, lthr: int | None) -> None:
    ensure_athlete_metrics_table(db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        db_queries.set_calculated_metrics(conn, hrmax, lthr)
    finally:
        conn.close()


def set_override_metrics(db_path: Path, hrmax: int | None, lthr: int | None) -> None:
    ensure_athlete_metrics_table(db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        profile = db_queries.get_athlete_profile(conn) or {}
        db_queries.set_override_metrics(
            conn, hrmax, lthr, profile.get("ftp_override")
        )
    finally:
        conn.close()


def clear_override_metrics(db_path: Path) -> None:
    ensure_athlete_metrics_table(db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        profile = db_queries.get_athlete_profile(conn) or {}
        db_queries.set_override_metrics(
            conn, None, None, profile.get("ftp_override")
        )
    finally:
        conn.close()


def calculate_metrics_from_db_sources(
    db_path: Path, years_back: int = 5
) -> tuple[int | None, int | None, str | None]:
    """Calculate validated HRmax and Estimated LTHR from trackpoint evidence."""
    cutoff_date = date.today() - timedelta(days=years_back * 365)
    cutoff_iso = cutoff_date.isoformat()

    if not db_path.exists():
        return None, None, "No activities found in DB (run Import first)."

    conn = sqlite3.connect(str(db_path))
    try:
        from garmin_data_hub.services import thresholds

        calculation = thresholds.calculate_validated_hrmax(
            conn, cutoff_utc=cutoff_iso
        )
        if calculation is not None:
            hrmax_robust = int(calculation["calculated_value"])
            thresholds.persist_hrmax_and_estimated_lthr(conn, calculation)
            return hrmax_robust, int(round(hrmax_robust * 0.86)), None
        return None, None, "No activities found in DB (run Import first)."
    except sqlite3.Error:
        logger.exception("Failed to calculate athlete metrics from %s", db_path)
        return None, None, "Could not calculate athlete metrics from the database."
    finally:
        conn.close()
