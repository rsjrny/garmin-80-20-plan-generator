import json
import logging
import math
import sqlite3
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping
import pandas as pd

from garmin_data_hub.analytics.temporal_metrics import (
    MAX_CONTIGUOUS_GAP_SECONDS,
    POWER_PEAK_DURATIONS_SECONDS,
    TemporalMetricResult,
    TemporalSample,
    calculate_temporal_metrics,
)
from garmin_data_hub.db.activity_dates import activity_calendar_day_sql

logger = logging.getLogger(__name__)

GET_SETTING_SQL = "SELECT value FROM app_settings WHERE key = ?"
UPSERT_SETTING_SQL = "INSERT OR REPLACE INTO app_settings (key, value) VALUES (?, ?)"
ACTIVITY_METRICS_LAST_REFRESH_KEY = "activity_metrics_last_refresh_utc"
ACTIVITY_METRICS_LAST_REFRESH_SUMMARY_KEY = "activity_metrics_last_refresh_summary"
ACTIVITY_METRICS_PROVENANCE_VERSION = 2
PHASE_2D_TEMPORAL_METRIC_COLUMNS = (
    "aerobic_decoupling_pct",
    "pace_decoupling_pct",
    "hr_drift_pct",
    "peak_power_5s_w",
    "peak_power_30s_w",
    "peak_power_60s_w",
    "peak_power_300s_w",
    "peak_power_1200s_w",
)


def current_temporal_metric_projection_sql(
    table_alias: str = "activity_metrics",
    columns: Iterable[str] = PHASE_2D_TEMPORAL_METRIC_COLUMNS,
) -> str:
    """Project Phase-2D metrics only when their algorithm provenance is current.

    Stored stale values deliberately remain in SQLite after a failed or pending
    refresh.  Normal readers use this projection so version 1, legacy NULL, and
    unknown future versions are consistently exposed as unavailable.
    """
    if not table_alias.replace("_", "").isalnum():
        raise ValueError(f"Invalid SQL table alias: {table_alias!r}")
    requested_columns = tuple(columns)
    unknown_columns = set(requested_columns) - set(PHASE_2D_TEMPORAL_METRIC_COLUMNS)
    if unknown_columns:
        raise ValueError(
            f"Not a Phase-2D temporal metric: {sorted(unknown_columns)!r}"
        )
    return ",\n".join(
        f"CASE WHEN {table_alias}.refresh_provenance_version = "
        f"{ACTIVITY_METRICS_PROVENANCE_VERSION} THEN "
        f"{table_alias}.{column_name} END AS {column_name}"
        for column_name in requested_columns
    )

# Columns rebuilt by refresh_persisted_activity_metrics(). Schema-only/legacy
# columns are intentionally absent so a refresh cannot erase data it does not own.
REFRESH_OWNED_ACTIVITY_METRIC_COLUMNS = (
    "moving_time_s",
    "stopped_time_s",
    "avg_moving_speed_mps",
    "hr_max_est_bpm",
    "lthr_est_bpm",
    "trimp",
    "aerobic_decoupling_pct",
    "hr_drift_pct",
    "avg_hr_to_max_pct",
    "zone_1_s",
    "zone_2_s",
    "zone_3_s",
    "zone_4_s",
    "zone_5_s",
    "np_w",
    "if_val",
    "tss",
    "variability_index",
    "avg_power_w",
    "max_power_w",
    "peak_power_5s_w",
    "peak_power_30s_w",
    "peak_power_60s_w",
    "peak_power_300s_w",
    "peak_power_1200s_w",
    "power_zone_1_s",
    "power_zone_2_s",
    "power_zone_3_s",
    "power_zone_4_s",
    "power_zone_5_s",
    "power_zone_6_s",
    "power_zone_7_s",
    "efficiency_factor",
    "pace_decoupling_pct",
    "avg_cadence_spm",
    "avg_temperature_c",
    "min_temperature_c",
    "max_temperature_c",
    "total_ascent_m",
    "total_descent_m",
    "max_altitude_m",
    "min_altitude_m",
    "training_effect_aerobic",
    "training_effect_anaerobic",
)


def get_setting(conn, key: str, default: Any):
    """Return a JSON-deserialized setting value or default."""
    try:
        row = conn.execute(GET_SETTING_SQL, (key,)).fetchone()
        return json.loads(row[0]) if row and row[0] is not None else default
    except (sqlite3.Error, TypeError, ValueError, json.JSONDecodeError):
        logger.warning(
            "Failed to load app setting '%s'; using default", key, exc_info=True
        )
        return default


def set_setting(conn, key: str, value: Any) -> None:
    """Upsert and durably commit a JSON-serialised setting value."""
    serialized = json.dumps(value)
    try:
        conn.execute(UPSERT_SETTING_SQL, (key, serialized))
        conn.commit()
    except Exception:
        try:
            if conn.in_transaction:
                conn.rollback()
        except Exception:
            logger.exception("Failed to roll back app setting '%s'", key)
        logger.warning("Failed to persist app setting '%s'", key, exc_info=True)
        raise


def set_settings(conn, values: Mapping[str, Any]) -> None:
    """Persist a group of JSON-serialised settings in one owned transaction."""
    rows = [(key, json.dumps(value)) for key, value in values.items()]
    try:
        conn.execute("BEGIN")
        for row in rows:
            conn.execute(UPSERT_SETTING_SQL, row)
        conn.commit()
    except Exception:
        try:
            if conn.in_transaction:
                conn.rollback()
        except Exception:
            logger.exception("Failed to roll back grouped app settings")
        logger.warning("Failed to persist grouped app settings", exc_info=True)
        raise


GET_DISTINCT_SPORTS_SQL = """
SELECT DISTINCT activity_type AS sport
FROM activity
WHERE activity_type IS NOT NULL
  AND start_time_gmt >= ?
ORDER BY activity_type
"""


def get_athlete_profile(conn, *, raise_on_error: bool = False):
    """Return the single-row athlete_profile as a mapping (or None).

    Tries to include FTP/resting-HR columns if present; falls back gracefully if
    schema differs.
    """
    try:
        # Prefer a wide selection if newer profile columns exist.
        try:
            row = conn.execute(
                "SELECT hrmax_calc, lthr_calc, hrmax_override, lthr_override, ftp_calc, ftp_override, resting_hr, calc_updated_utc, override_updated_utc FROM athlete_profile WHERE profile_id = 1"
            ).fetchone()
            if row:
                return {
                    "hrmax_calc": row[0],
                    "lthr_calc": row[1],
                    "hrmax_override": row[2],
                    "lthr_override": row[3],
                    "ftp_calc": row[4],
                    "ftp_override": row[5],
                    "resting_hr": row[6],
                    "calc_updated_utc": row[7],
                    "override_updated_utc": row[8],
                }
        except Exception:
            # Fallback to minimal selection for older schemas.
            row = conn.execute(
                "SELECT hrmax_calc, lthr_calc, hrmax_override, lthr_override, calc_updated_utc, override_updated_utc FROM athlete_profile WHERE profile_id = 1"
            ).fetchone()
            if row:
                return {
                    "hrmax_calc": row[0],
                    "lthr_calc": row[1],
                    "hrmax_override": row[2],
                    "lthr_override": row[3],
                    "ftp_calc": None,
                    "ftp_override": None,
                    "resting_hr": None,
                    "calc_updated_utc": row[4],
                    "override_updated_utc": row[5],
                }
        return None
    except Exception:
        if raise_on_error:
            raise
        return None


def _positive_profile_metric(
    profile: dict[str, Any], override_key: str, calculated_key: str
) -> int | None:
    """Return the first positive override/calculated profile value."""
    for key in (override_key, calculated_key):
        value = profile.get(key)
        if value is None:
            continue
        try:
            normalized = int(value)
        except (TypeError, ValueError):
            continue
        if normalized > 0:
            return normalized
    return None


def get_effective_lthr(conn, *, commit: bool = True) -> int | None:
    """Return LTHR override/calculated value, with robust fallback from activities."""
    try:
        profile = get_athlete_profile(conn) or {}
        lthr = _positive_profile_metric(profile, "lthr_override", "lthr_calc")
        if lthr is not None:
            return lthr
    except Exception:
        pass

    # Fallback to robust estimate from recent max HR values.
    try:
        cutoff_iso = "1970-01-01T00:00:00Z"
        hrmax, lthr = get_hrmax_robust_and_lthr(conn, cutoff_iso, percentile=0.995)
        if hrmax and lthr:
            ensure_athlete_profile_table(conn, commit=commit)
            set_calculated_metrics(conn, int(hrmax), int(lthr), commit=commit)
            return int(lthr)
    except Exception:
        if not commit:
            raise
        pass
    return None


def _estimate_ftp_from_recent_power(
    conn,
    days_back: int = 180,
    *,
    raise_on_error: bool = False,
) -> int | None:
    """Estimate cycling FTP from recent power-enabled activities.

    Uses the strongest recent sustained power efforts (favoring cycling/ride sports)
    and applies the common ~95% of best 20+ minute effort heuristic.
    """
    try:
        activity_columns = (
            _get_table_columns(conn, "activity", raise_on_error=True)
            if raise_on_error
            else _get_table_columns(conn, "activity")
        )
        if not activity_columns:
            return None

        power_col = None
        if "norm_power" in activity_columns:
            power_col = "norm_power"
        elif "avg_power" in activity_columns:
            power_col = "avg_power"
        if power_col is None or "elapsed_duration_seconds" not in activity_columns:
            return None

        cutoff_iso = (
            datetime.now(timezone.utc) - pd.Timedelta(days=int(days_back))
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        sport_filter = """
            LOWER(COALESCE(activity_type, '')) LIKE '%cycl%'
            OR LOWER(COALESCE(activity_type, '')) LIKE '%bike%'
            OR LOWER(COALESCE(activity_type, '')) LIKE '%ride%'
        """

        def _load_candidates(prefer_cycling: bool) -> list[float]:
            filter_sql = (
                f"AND ({sport_filter})"
                if prefer_cycling and "activity_type" in activity_columns
                else ""
            )
            rows = conn.execute(
                f"""
                SELECT COALESCE(norm_power, avg_power) AS candidate_power
                FROM activity
                WHERE start_time_gmt >= ?
                  AND elapsed_duration_seconds >= ?
                  AND COALESCE(norm_power, avg_power) IS NOT NULL
                  AND COALESCE(norm_power, avg_power) > 0
                  {filter_sql}
                ORDER BY candidate_power DESC
                LIMIT 32
                """,
                (cutoff_iso, 20 * 60),
            ).fetchall()
            return [
                float(r[0]) for r in rows if r and r[0] is not None and float(r[0]) > 0
            ]

        candidates = _load_candidates(prefer_cycling=True)
        if not candidates:
            candidates = _load_candidates(prefer_cycling=False)
        if not candidates:
            return None

        candidates.sort(reverse=True)
        best_sustained_power = candidates[0]
        ftp_est = int(round(best_sustained_power * 0.95))
        return ftp_est if 80 <= ftp_est <= 500 else None
    except (sqlite3.Error, TypeError, ValueError):
        logger.warning("Failed to estimate FTP from recent power data", exc_info=True)
        if raise_on_error:
            raise
        return None


def get_effective_ftp(
    conn,
    *,
    commit: bool = True,
    raise_on_error: bool = False,
) -> int | None:
    """Return FTP override/calculated value, with fallback estimation from power data."""
    try:
        profile = get_athlete_profile(conn, raise_on_error=raise_on_error) or {}
        ftp = _positive_profile_metric(profile, "ftp_override", "ftp_calc")
        if ftp is not None:
            return ftp
    except Exception:
        if raise_on_error:
            raise
        pass

    try:
        ftp_est = (
            _estimate_ftp_from_recent_power(conn, raise_on_error=True)
            if raise_on_error
            else _estimate_ftp_from_recent_power(conn)
        )
        if ftp_est and int(ftp_est) > 0:
            ensure_athlete_profile_table(conn, commit=commit)
            set_calculated_ftp(conn, int(ftp_est), commit=commit)
            return int(ftp_est)
    except Exception:
        if raise_on_error or not commit:
            raise
        pass

    return None


def get_current_activity_metric_provenance(
    conn,
) -> tuple[int | None, int | None, int]:
    """Return the canonical threshold inputs expected on current metric rows."""
    profile = get_athlete_profile(conn) or {}
    return (
        _positive_profile_metric(profile, "lthr_override", "lthr_calc"),
        _positive_profile_metric(profile, "ftp_override", "ftp_calc"),
        int(profile.get("resting_hr") or 60),
    )


def get_activity_metric_threshold_management(conn) -> tuple[bool, bool]:
    """Return whether LTHR and FTP are explicitly managed by the profile."""
    profile = get_athlete_profile(conn) or {}
    profile_was_updated = bool(
        profile.get("calc_updated_utc") is not None
        or profile.get("override_updated_utc") is not None
    )
    return (
        bool(
            profile.get("lthr_override") is not None
            or profile.get("lthr_calc") is not None
            or profile_was_updated
        ),
        bool(
            profile.get("ftp_override") is not None
            or profile.get("ftp_calc") is not None
            or profile_was_updated
        ),
    )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get_table_columns(
    conn,
    table_name: str,
    *,
    raise_on_error: bool = False,
) -> set[str]:
    """Return the column names present on `table_name`, or an empty set."""
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {str(r[1]) for r in rows if len(r) > 1 and r[1]}
    except sqlite3.Error:
        if raise_on_error:
            raise
        return set()


def _refresh_scalar_activity_metrics(
    conn,
    candidate_ids: list[int],
    lthr: int | None,
    ftp: int | None,
    resting_hr: int,
) -> None:
    """Populate scalar metric columns from the upstream `activity` table."""
    if not candidate_ids:
        return

    activity_columns = _get_table_columns(conn, "activity")
    if not activity_columns:
        return

    desired_columns = [
        "activity_id",
        "elapsed_duration_seconds",
        "moving_duration_seconds",
        "average_speed",
        "average_hr",
        "max_hr",
        "training_stress_score",
        "avg_power",
        "max_power",
        "norm_power",
        "intensity_factor",
        "avg_cadence",
        "elevation_gain",
        "elevation_loss",
        "min_elevation",
        "max_elevation",
        "aerobic_training_effect",
        "anaerobic_training_effect",
        "min_temperature",
        "max_temperature",
    ]
    select_list = [
        col if col in activity_columns else f"NULL AS {col}" for col in desired_columns
    ]
    placeholders = ",".join("?" for _ in candidate_ids)
    rows = conn.execute(
        f"SELECT {', '.join(select_list)} FROM activity WHERE activity_id IN ({placeholders})",
        tuple(candidate_ids),
    ).fetchall()

    update_rows = []
    for row in rows:
        (
            activity_id,
            elapsed_duration_s,
            moving_duration_s,
            average_speed,
            average_hr,
            max_hr,
            training_stress_score,
            avg_power,
            max_power,
            norm_power,
            intensity_factor,
            avg_cadence,
            elevation_gain,
            elevation_loss,
            min_elevation,
            max_elevation,
            aerobic_training_effect,
            anaerobic_training_effect,
            min_temperature,
            max_temperature,
        ) = row

        moving_time_s = (
            moving_duration_s if moving_duration_s is not None else elapsed_duration_s
        )
        stopped_time_s = None
        if elapsed_duration_s is not None and moving_time_s is not None:
            stopped_time_s = max(float(elapsed_duration_s) - float(moving_time_s), 0.0)

        effective_max_hr = max_hr
        if (effective_max_hr is None or float(effective_max_hr) <= 0) and lthr:
            effective_max_hr = int(round(float(lthr) / 0.86))

        avg_hr_to_max_pct = None
        if average_hr and effective_max_hr and float(effective_max_hr) > 0:
            avg_hr_to_max_pct = round(
                (float(average_hr) * 100.0) / float(effective_max_hr), 2
            )

        if_val = intensity_factor
        if if_val is None and ftp and norm_power is not None:
            if_val = round(float(norm_power) / float(ftp), 2)
        elif (
            if_val is None
            and lthr
            and average_hr
            and lthr > resting_hr
        ):
            hr_if = (float(average_hr) - float(resting_hr)) / float(lthr - resting_hr)
            if_val = round(max(0.0, min(2.0, hr_if)), 2)

        tss = training_stress_score
        if (
            tss is None
            and lthr
            and average_hr
            and moving_time_s
            and lthr > resting_hr
        ):
            hr_if = (float(average_hr) - float(resting_hr)) / float(lthr - resting_hr)
            hr_if = max(0.0, min(2.0, hr_if))
            tss = round((float(moving_time_s) / 3600.0) * (hr_if**2) * 100.0, 1)

        trimp = None
        if (
            moving_time_s
            and average_hr
            and effective_max_hr
            and float(effective_max_hr) > resting_hr
        ):
            hrr = (float(average_hr) - float(resting_hr)) / float(
                float(effective_max_hr) - float(resting_hr)
            )
            hrr = max(0.0, min(1.0, hrr))
            trimp_val = (
                (float(moving_time_s) / 60.0) * hrr * 0.64 * math.exp(1.92 * hrr)
            )
            trimp = round(trimp_val, 1) if trimp_val > 0 else None

        variability_index = None
        if norm_power is not None and avg_power is not None and float(avg_power) > 0:
            variability_index = round(float(norm_power) / float(avg_power), 3)

        efficiency_factor = None
        if norm_power is not None and average_hr and float(average_hr) > 0:
            efficiency_factor = round(float(norm_power) / float(average_hr), 3)
        elif average_speed is not None and average_hr and float(average_hr) > 0:
            efficiency_factor = round(float(average_speed) / float(average_hr), 4)

        avg_temperature_c = None
        if min_temperature is not None and max_temperature is not None:
            avg_temperature_c = round(
                (float(min_temperature) + float(max_temperature)) / 2.0, 2
            )
        elif min_temperature is not None:
            avg_temperature_c = float(min_temperature)
        elif max_temperature is not None:
            avg_temperature_c = float(max_temperature)

        update_rows.append(
            (
                moving_time_s,
                stopped_time_s,
                average_speed,
                effective_max_hr,
                trimp,
                avg_hr_to_max_pct,
                norm_power,
                if_val,
                tss,
                variability_index,
                avg_power,
                max_power,
                efficiency_factor,
                avg_cadence,
                avg_temperature_c,
                min_temperature,
                max_temperature,
                elevation_gain,
                elevation_loss,
                max_elevation,
                min_elevation,
                aerobic_training_effect,
                anaerobic_training_effect,
                activity_id,
            )
        )

    if update_rows:
        conn.executemany(
            """
            UPDATE activity_metrics
            SET moving_time_s = COALESCE(?, moving_time_s),
                stopped_time_s = COALESCE(?, stopped_time_s),
                avg_moving_speed_mps = COALESCE(?, avg_moving_speed_mps),
                hr_max_est_bpm = COALESCE(?, hr_max_est_bpm),
                trimp = COALESCE(?, trimp),
                avg_hr_to_max_pct = COALESCE(?, avg_hr_to_max_pct),
                np_w = COALESCE(?, np_w),
                if_val = COALESCE(?, if_val),
                tss = COALESCE(?, tss),
                variability_index = COALESCE(?, variability_index),
                avg_power_w = COALESCE(?, avg_power_w),
                max_power_w = COALESCE(?, max_power_w),
                efficiency_factor = COALESCE(?, efficiency_factor),
                avg_cadence_spm = COALESCE(?, avg_cadence_spm),
                avg_temperature_c = COALESCE(?, avg_temperature_c),
                min_temperature_c = COALESCE(?, min_temperature_c),
                max_temperature_c = COALESCE(?, max_temperature_c),
                total_ascent_m = COALESCE(?, total_ascent_m),
                total_descent_m = COALESCE(?, total_descent_m),
                max_altitude_m = COALESCE(?, max_altitude_m),
                min_altitude_m = COALESCE(?, min_altitude_m),
                training_effect_aerobic = COALESCE(?, training_effect_aerobic),
                training_effect_anaerobic = COALESCE(?, training_effect_anaerobic)
            WHERE activity_id = ?
            """,
            update_rows,
        )


def calculate_activity_temporal_metrics(
    conn, activity_id: int
) -> TemporalMetricResult:
    """Load one activity and calculate its canonical elapsed-time metrics."""
    rows = conn.execute(
        """
        SELECT seq, timestamp_utc, speed_mps, heart_rate_bpm, power_w
        FROM activity_trackpoints
        WHERE activity_id = ?
        ORDER BY timestamp_utc, seq
        """,
        (activity_id,),
    ).fetchall()
    return calculate_temporal_metrics(
        TemporalSample(
            seq=row[0],
            timestamp_utc=row[1],
            speed_mps=row[2],
            heart_rate_bpm=row[3],
            power_w=row[4],
        )
        for row in rows
    )


def _refresh_trackpoint_derived_metrics(
    conn,
    candidate_ids: list[int],
    ftp: int | None = None,
) -> None:
    """Populate metrics that are best derived from `activity_trackpoints`."""
    if not candidate_ids:
        return

    placeholders = ",".join("?" for _ in candidate_ids)

    agg_rows = conn.execute(
        f"""
        SELECT
            activity_id,
            AVG(CASE WHEN cadence IS NOT NULL AND cadence >= 0 THEN cadence END) AS avg_cadence_spm,
            AVG(CASE WHEN power_w IS NOT NULL AND power_w >= 0 THEN power_w END) AS avg_power_w,
            MAX(CASE WHEN power_w IS NOT NULL AND power_w >= 0 THEN power_w END) AS max_power_w,
            AVG(temperature_c) AS avg_temperature_c,
            MIN(temperature_c) AS min_temperature_c,
            MAX(temperature_c) AS max_temperature_c,
            MIN(altitude_m) AS min_altitude_m,
            MAX(altitude_m) AS max_altitude_m
        FROM activity_trackpoints
        WHERE activity_id IN ({placeholders})
        GROUP BY activity_id
        """,
        tuple(candidate_ids),
    ).fetchall()

    if agg_rows:
        conn.executemany(
            """
            UPDATE activity_metrics
            SET avg_cadence_spm = COALESCE(?, avg_cadence_spm),
                avg_power_w = COALESCE(?, avg_power_w),
                max_power_w = COALESCE(?, max_power_w),
                avg_temperature_c = COALESCE(?, avg_temperature_c),
                min_temperature_c = COALESCE(?, min_temperature_c),
                max_temperature_c = COALESCE(?, max_temperature_c),
                min_altitude_m = COALESCE(?, min_altitude_m),
                max_altitude_m = COALESCE(?, max_altitude_m)
            WHERE activity_id = ?
            """,
            [
                (
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[0],
                )
                for row in agg_rows
            ],
        )

    ascent_rows = conn.execute(
        f"""
        WITH altitude_steps AS (
            SELECT
                activity_id,
                altitude_m - LAG(altitude_m) OVER (
                    PARTITION BY activity_id
                    ORDER BY seq
                ) AS delta_altitude
            FROM activity_trackpoints
            WHERE activity_id IN ({placeholders})
              AND altitude_m IS NOT NULL
        )
        SELECT
            activity_id,
            SUM(CASE WHEN delta_altitude > 0 THEN delta_altitude ELSE 0 END) AS total_ascent_m,
            SUM(CASE WHEN delta_altitude < 0 THEN -delta_altitude ELSE 0 END) AS total_descent_m
        FROM altitude_steps
        GROUP BY activity_id
        """,
        tuple(candidate_ids),
    ).fetchall()

    if ascent_rows:
        conn.executemany(
            """
            UPDATE activity_metrics
            SET total_ascent_m = COALESCE(?, total_ascent_m),
                total_descent_m = COALESCE(?, total_descent_m)
            WHERE activity_id = ?
            """,
            [(row[1], row[2], row[0]) for row in ascent_rows],
        )

    temporal_update_rows = []
    for activity_id in candidate_ids:
        metrics = calculate_activity_temporal_metrics(conn, activity_id)
        peaks = metrics.power_peaks_w
        temporal_update_rows.append(
            (
                metrics.aerobic_decoupling_pct,
                metrics.hr_drift_pct,
                metrics.pace_decoupling_pct,
                *(peaks[duration] for duration in POWER_PEAK_DURATIONS_SECONDS),
                activity_id,
            )
        )

    conn.executemany(
        """
        UPDATE activity_metrics
        SET aerobic_decoupling_pct = ?,
            hr_drift_pct = ?,
            pace_decoupling_pct = ?,
            peak_power_5s_w = ?,
            peak_power_30s_w = ?,
            peak_power_60s_w = ?,
            peak_power_300s_w = ?,
            peak_power_1200s_w = ?
        WHERE activity_id = ?
        """,
        temporal_update_rows,
    )

    if ftp and int(ftp) > 0:
        z1_upper = float(ftp) * 0.55
        z2_upper = float(ftp) * 0.75
        z3_upper = float(ftp) * 0.90
        z4_upper = float(ftp) * 1.05
        z5_upper = float(ftp) * 1.20
        z6_upper = float(ftp) * 1.50

        power_zone_rows = conn.execute(
            f"""
            SELECT
                x.activity_id,
                SUM(CASE WHEN x.power < ? THEN x.dt_s ELSE 0 END) AS power_zone_1_s,
                SUM(CASE WHEN x.power >= ? AND x.power < ? THEN x.dt_s ELSE 0 END) AS power_zone_2_s,
                SUM(CASE WHEN x.power >= ? AND x.power < ? THEN x.dt_s ELSE 0 END) AS power_zone_3_s,
                SUM(CASE WHEN x.power >= ? AND x.power < ? THEN x.dt_s ELSE 0 END) AS power_zone_4_s,
                SUM(CASE WHEN x.power >= ? AND x.power < ? THEN x.dt_s ELSE 0 END) AS power_zone_5_s,
                SUM(CASE WHEN x.power >= ? AND x.power < ? THEN x.dt_s ELSE 0 END) AS power_zone_6_s,
                SUM(CASE WHEN x.power >= ? THEN x.dt_s ELSE 0 END) AS power_zone_7_s
            FROM (
                SELECT
                    tp.activity_id,
                    tp.power_w AS power,
                    (julianday(tp_next.timestamp_utc) - julianday(tp.timestamp_utc)) * 86400.0 AS dt_s
                FROM activity_trackpoints tp
                JOIN activity_trackpoints tp_next
                  ON tp_next.activity_id = tp.activity_id
                 AND tp_next.seq = tp.seq + 1
                WHERE tp.activity_id IN ({placeholders})
                  AND tp.power_w IS NOT NULL
                  AND tp.power_w >= 0
            ) x
            WHERE x.dt_s > 0
              AND x.dt_s <= ?
            GROUP BY x.activity_id
            """,
            (
                z1_upper,
                z1_upper,
                z2_upper,
                z2_upper,
                z3_upper,
                z3_upper,
                z4_upper,
                z4_upper,
                z5_upper,
                z5_upper,
                z6_upper,
                z6_upper,
                *candidate_ids,
                MAX_CONTIGUOUS_GAP_SECONDS,
            ),
        ).fetchall()

        if power_zone_rows:
            conn.executemany(
                """
                UPDATE activity_metrics
                SET power_zone_1_s = COALESCE(?, power_zone_1_s),
                    power_zone_2_s = COALESCE(?, power_zone_2_s),
                    power_zone_3_s = COALESCE(?, power_zone_3_s),
                    power_zone_4_s = COALESCE(?, power_zone_4_s),
                    power_zone_5_s = COALESCE(?, power_zone_5_s),
                    power_zone_6_s = COALESCE(?, power_zone_6_s),
                    power_zone_7_s = COALESCE(?, power_zone_7_s)
                WHERE activity_id = ?
                """,
                [
                    (
                        row[1],
                        row[2],
                        row[3],
                        row[4],
                        row[5],
                        row[6],
                        row[7],
                        row[0],
                    )
                    for row in power_zone_rows
                ],
            )


def ensure_athlete_profile_table(conn, *, commit: bool = True) -> None:
    """Create `athlete_profile` if it does not exist and ensure a row exists."""
    caller_owns_transaction = bool(conn.in_transaction)
    try:
        conn.execute(
            """
        CREATE TABLE IF NOT EXISTS athlete_profile (
            profile_id INTEGER PRIMARY KEY DEFAULT 1,
            hrmax_calc INTEGER,
            lthr_calc INTEGER,
            ftp_calc INTEGER,
            calc_updated_utc TEXT,
            hrmax_override INTEGER,
            lthr_override INTEGER,
            ftp_override INTEGER,
            resting_hr INTEGER,
            override_updated_utc TEXT
        );
        """
        )
        existing_columns = _get_table_columns(conn, "athlete_profile")
        for column_name, column_sql in {
            "ftp_calc": "INTEGER",
            "ftp_override": "INTEGER",
            "resting_hr": "INTEGER",
        }.items():
            if column_name not in existing_columns:
                conn.execute(
                    f"ALTER TABLE athlete_profile ADD COLUMN {column_name} {column_sql}"
                )
        conn.execute("INSERT OR IGNORE INTO athlete_profile(profile_id) VALUES (1)")
        if commit and not caller_owns_transaction:
            conn.commit()
    except Exception:
        if not caller_owns_transaction and conn.in_transaction:
            conn.rollback()
        if caller_owns_transaction or not commit:
            raise
        return


def upsert_activity_metrics(
    conn,
    activity_id: int,
    moving_time_s,
    stopped_time_s,
    avg_moving_speed_mps,
    hr_max_est_bpm,
    lthr_est_bpm,
    trimp,
    aerobic_decoupling_pct,
    np_w,
    if_val,
    tss,
    zone_1_s,
    zone_2_s,
    zone_3_s,
    zone_4_s,
    zone_5_s,
) -> None:
    """Upsert the core derived metrics without wiping extended metric columns."""
    try:
        conn.execute(
            """
            INSERT INTO activity_metrics(
                activity_id, moving_time_s, stopped_time_s, avg_moving_speed_mps,
                hr_max_est_bpm, lthr_est_bpm, trimp, aerobic_decoupling_pct,
                np_w, if_val, tss, zone_1_s, zone_2_s, zone_3_s, zone_4_s, zone_5_s
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(activity_id) DO UPDATE SET
                moving_time_s = excluded.moving_time_s,
                stopped_time_s = excluded.stopped_time_s,
                avg_moving_speed_mps = excluded.avg_moving_speed_mps,
                hr_max_est_bpm = excluded.hr_max_est_bpm,
                lthr_est_bpm = excluded.lthr_est_bpm,
                trimp = excluded.trimp,
                aerobic_decoupling_pct = excluded.aerobic_decoupling_pct,
                np_w = excluded.np_w,
                if_val = excluded.if_val,
                tss = excluded.tss,
                zone_1_s = excluded.zone_1_s,
                zone_2_s = excluded.zone_2_s,
                zone_3_s = excluded.zone_3_s,
                zone_4_s = excluded.zone_4_s,
                zone_5_s = excluded.zone_5_s
            """,
            (
                activity_id,
                moving_time_s,
                stopped_time_s,
                avg_moving_speed_mps,
                hr_max_est_bpm,
                lthr_est_bpm,
                trimp,
                aerobic_decoupling_pct,
                np_w,
                if_val,
                tss,
                zone_1_s,
                zone_2_s,
                zone_3_s,
                zone_4_s,
                zone_5_s,
            ),
        )
    except Exception:
        return


# upsert_fit_file_by_hash removed — FIT pipeline replaced by garmin-givemydata


def get_athlete_metrics(conn) -> dict:
    try:
        profile = get_athlete_profile(conn) or {}
        hrmax_calc = profile.get("hrmax_calc")
        lthr_calc = profile.get("lthr_calc")
        ftp_calc = profile.get("ftp_calc")
        hrmax_override = profile.get("hrmax_override")
        lthr_override = profile.get("lthr_override")
        ftp_override = profile.get("ftp_override")
        return {
            "hrmax_calc": hrmax_calc,
            "lthr_calc": lthr_calc,
            "ftp_calc": ftp_calc,
            "hrmax_override": hrmax_override,
            "lthr_override": lthr_override,
            "ftp_override": ftp_override,
            "resting_hr": profile.get("resting_hr"),
            "hrmax_effective": _positive_profile_metric(
                profile, "hrmax_override", "hrmax_calc"
            ),
            "lthr_effective": _positive_profile_metric(
                profile, "lthr_override", "lthr_calc"
            ),
            "ftp_effective": _positive_profile_metric(
                profile, "ftp_override", "ftp_calc"
            ),
            "calc_updated_at": profile.get("calc_updated_utc"),
            "override_updated_at": profile.get("override_updated_utc"),
        }
    except Exception:
        return {
            "hrmax_calc": None,
            "lthr_calc": None,
            "ftp_calc": None,
            "hrmax_override": None,
            "lthr_override": None,
            "ftp_override": None,
            "resting_hr": None,
            "hrmax_effective": None,
            "lthr_effective": None,
            "ftp_effective": None,
            "calc_updated_at": None,
            "override_updated_at": None,
        }


def _update_athlete_profile_thresholds(
    conn,
    sql: str,
    params: tuple[Any, ...],
    *,
    commit: bool,
) -> None:
    """Run one profile mutation without committing or rolling back caller work."""
    caller_owns_transaction = bool(conn.in_transaction)
    savepoint_name = f"athlete_threshold_update_{id(params):x}"
    savepoint_active = False
    try:
        if caller_owns_transaction:
            conn.execute(f"SAVEPOINT {savepoint_name}")
            savepoint_active = True
        ensure_athlete_profile_table(conn, commit=False)
        conn.execute(sql, params)
        if savepoint_active:
            conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            savepoint_active = False
        elif commit:
            conn.commit()
    except Exception:
        try:
            if savepoint_active:
                conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            elif conn.in_transaction:
                conn.rollback()
        except Exception:
            logger.exception("Failed to roll back athlete threshold update")
        logger.warning("Failed to update athlete thresholds", exc_info=True)
        raise


def set_calculated_metrics(
    conn, hrmax: int | None, lthr: int | None, *, commit: bool = True
) -> None:
    _update_athlete_profile_thresholds(
        conn,
        "UPDATE athlete_profile SET hrmax_calc=?, lthr_calc=?, calc_updated_utc=? WHERE profile_id=1",
        (hrmax, lthr, _utc_now_iso()),
        commit=commit,
    )


def set_calculated_ftp(conn, ftp: int | None, *, commit: bool = True) -> None:
    """Persist a calculated FTP estimate when the newer athlete-profile columns exist."""
    _update_athlete_profile_thresholds(
        conn,
        "UPDATE athlete_profile SET ftp_calc=?, calc_updated_utc=? WHERE profile_id=1",
        (ftp, _utc_now_iso()),
        commit=commit,
    )


def set_override_metrics(
    conn,
    hrmax: int | None,
    lthr: int | None,
    ftp: int | None = None,
    *,
    commit: bool = True,
) -> None:
    _update_athlete_profile_thresholds(
        conn,
        "UPDATE athlete_profile SET hrmax_override=?, lthr_override=?, ftp_override=?, override_updated_utc=? WHERE profile_id=1",
        (hrmax, lthr, ftp, _utc_now_iso()),
        commit=commit,
    )


def clear_override_metrics(conn, *, commit: bool = True) -> None:
    _update_athlete_profile_thresholds(
        conn,
        "UPDATE athlete_profile SET hrmax_override=NULL, lthr_override=NULL, ftp_override=NULL, override_updated_utc=? WHERE profile_id=1",
        (_utc_now_iso(),),
        commit=commit,
    )


def delete_planned_workouts_in_range(conn, min_date: str, max_date: str) -> None:
    try:
        conn.execute(
            "DELETE FROM planned_workout WHERE scheduled_date >= ? AND scheduled_date <= ?",
            (min_date, max_date),
        )
        conn.commit()
    except Exception:
        return


def insert_planned_workout(
    conn,
    scheduled_date: str,
    workout_name: str,
    description: str,
    planned_distance_m,
    planned_duration_s,
    planned_tss,
    structure_json=None,
) -> None:
    try:
        conn.execute(
            """
            INSERT INTO planned_workout(
                scheduled_date, workout_name, description, 
                planned_distance_m, planned_duration_s, planned_tss, structure_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                scheduled_date,
                workout_name,
                description,
                planned_distance_m,
                planned_duration_s,
                planned_tss,
                structure_json,
            ),
        )
        conn.commit()
    except Exception:
        return


GET_PLANNED_MIN_MAX_SQL = (
    "SELECT MIN(scheduled_date), MAX(scheduled_date) FROM planned_workout"
)

DELETE_SETTING_SQL = "DELETE FROM app_settings WHERE key = ?"


def get_sports_list(conn, start_ts_iso: str) -> list[str]:
    try:
        rows = conn.execute(GET_DISTINCT_SPORTS_SQL, (start_ts_iso,)).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


def get_planned_workout_date_range(conn):
    try:
        row = conn.execute(GET_PLANNED_MIN_MAX_SQL).fetchone()
        return (row[0], row[1]) if row else (None, None)
    except Exception:
        return (None, None)


def get_hrmax_robust_and_lthr(
    conn,
    cutoff_iso: str,
    percentile: float = 0.995,
    *,
    raise_on_error: bool = False,
) -> tuple[int | None, int | None]:
    """Compute a robust HRMax (percentile) from recent activity max HRs and suggest LTHR."""
    try:
        rows = conn.execute(
            """
            SELECT max_hr
            FROM activity
            WHERE start_time_gmt >= ?
              AND max_hr IS NOT NULL
              AND max_hr > 0
            ORDER BY max_hr ASC
            """,
            (cutoff_iso,),
        ).fetchall()

        if not rows:
            return None, None

        hrs = [r[0] for r in rows if r[0] is not None]
        if not hrs:
            return None, None

        hrs.sort()
        idx = int(len(hrs) * float(percentile))
        if idx >= len(hrs):
            idx = len(hrs) - 1

        hrmax_robust = int(hrs[idx])
        lthr_suggested = int(round(hrmax_robust * 0.86))
        return hrmax_robust, lthr_suggested
    except Exception:
        if raise_on_error:
            raise
        return None, None


def get_max_session_max_hr(conn) -> int | None:
    """Return the maximum max_hr observed in activity rows, or None."""
    try:
        row = conn.execute(
            "SELECT MAX(max_hr) FROM activity WHERE max_hr IS NOT NULL AND max_hr > 0 AND max_hr < 220"
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def delete_setting(conn, key: str) -> None:
    try:
        conn.execute(DELETE_SETTING_SQL, (key,))
        conn.commit()
    except Exception:
        return


def load_activity_preferences(conn, default=None):
    """Load JSON preferences stored under `activity_preferences` key in `app_settings`."""
    if default is None:
        default = {"selected_activity": None}
    try:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key = 'activity_preferences'"
        )
        r = row.fetchone()
        if r and r[0]:
            return json.loads(r[0])
    except Exception:
        pass
    return default


def save_activity_preferences(conn, prefs) -> None:
    try:
        conn.execute(
            "INSERT OR REPLACE INTO app_settings (key, value) VALUES (?, ?)",
            ("activity_preferences", json.dumps(prefs)),
        )
        conn.commit()
    except Exception:
        return


def get_activity_stats(conn) -> dict:
    try:
        total_activities = (
            conn.execute("SELECT COUNT(*) FROM activity").fetchone()[0] or 0
        )
        total_distance_m = (
            conn.execute("SELECT SUM(distance_meters) FROM activity").fetchone()[0] or 0
        )
        total_duration_s = (
            conn.execute(
                "SELECT SUM(elapsed_duration_seconds) FROM activity"
            ).fetchone()[0]
            or 0
        )
        last_activity_iso = conn.execute(
            "SELECT MAX(start_time_gmt) FROM activity"
        ).fetchone()[0]
        return {
            "total_activities": total_activities,
            "total_distance_m": total_distance_m,
            "total_duration_s": total_duration_s,
            "last_activity_iso": last_activity_iso,
        }
    except Exception:
        return {
            "total_activities": 0,
            "total_distance_m": 0,
            "total_duration_s": 0,
            "last_activity_iso": None,
        }


def list_recent_activities(conn, limit: int = 200) -> list[dict[str, Any]]:
    """Return a list of recent activities as dictionaries for UI consumption."""
    try:
        rows = conn.execute(
            """
            SELECT
                activity_id,
                activity_type AS sport,
                start_time_gmt AS start_time_utc,
                distance_meters,
                elapsed_duration_seconds,
                average_speed,
                average_hr,
                max_hr,
                start_latitude,
                start_longitude
            FROM activity
            ORDER BY start_time_gmt DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        out = []
        for r in rows:
            start_iso = r["start_time_utc"] or ""
            if start_iso:
                try:
                    dt = datetime.fromisoformat(start_iso)
                    start_iso = dt.strftime("%Y-%m-%d %H:%M")
                except Exception:
                    pass
            dist = r["distance_meters"]
            elapsed = r["elapsed_duration_seconds"]
            out.append(
                {
                    "activity_id": r["activity_id"],
                    "sport": r["sport"],
                    "start_utc": start_iso,
                    "distance_km": (dist / 1000.0) if dist is not None else None,
                    "elapsed_min": (elapsed / 60.0) if elapsed is not None else None,
                    "speed_mps": r["average_speed"],
                    "avg_hr": r["average_hr"],
                    "max_hr": r["max_hr"],
                    "start_latitude": r["start_latitude"],
                    "start_longitude": r["start_longitude"],
                }
            )
        return out
    except Exception:
        return []


def get_activity_records(conn, activity_id: int) -> pd.DataFrame:
    """Return a pandas DataFrame of split records for the given activity_id.

    Uses activity_splits from garmin-givemydata as the data source.
    Columns: split_number, distance_meters, duration_seconds, speed_mps,
             heart_rate_bpm, max_hr, altitude_m (elevation_gain per split),
             cadence_spm.
    """
    try:
        query = """
            SELECT
                split_number,
                distance_meters,
                duration_seconds,
                average_speed AS speed_mps,
                average_hr    AS heart_rate_bpm,
                max_hr,
                elevation_gain AS altitude_m,
                avg_cadence    AS cadence_spm
            FROM activity_splits
            WHERE activity_id = ?
            ORDER BY split_number ASC
        """
        return pd.read_sql_query(query, conn, params=(activity_id,))
    except Exception:
        return pd.DataFrame()


def get_activity_trackpoints(conn, activity_id: int) -> pd.DataFrame:
    """Return a pandas DataFrame of GPS trackpoints for the given activity_id.

    Columns: lat_deg, lon_deg, altitude_m, distance_m, speed_mps,
             heart_rate_bpm, cadence_spm, power_w, temperature_c
    """
    try:
        query = """
            SELECT
                latitude       AS lat_deg,
                longitude      AS lon_deg,
                altitude_m,
                distance_m,
                speed_mps,
                heart_rate_bpm,
                cadence        AS cadence_spm,
                power_w,
                temperature_c
            FROM activity_trackpoints
            WHERE activity_id = ?
            ORDER BY seq ASC
        """
        return pd.read_sql_query(query, conn, params=(activity_id,))
    except Exception:
        return pd.DataFrame()


def get_activities_dataframe(
    conn,
    start_ts_iso: str,
    sports_list: tuple | None = None,
    lthr: int | None = None,
    use_temp_zone_metrics: bool = False,
) -> pd.DataFrame:
    """Return a DataFrame of activities with activity_metrics for plotting.

    Column aliases preserve the legacy names expected by UI pages.

    When `use_temp_zone_metrics` is True, zone columns are sourced from a
    connection-local temp table computed from `activity_trackpoints`, with
    fallback to persisted `activity_metrics` values.
    """
    try:
        current_lthr, current_ftp, current_resting_hr = (
            get_current_activity_metric_provenance(conn)
        )
        lthr_is_managed, ftp_is_managed = (
            get_activity_metric_threshold_management(conn)
        )
        effective_lthr = lthr
        if use_temp_zone_metrics and effective_lthr is None:
            effective_lthr = current_lthr

        if use_temp_zone_metrics:
            refresh_temp_activity_zone_metrics(
                conn,
                effective_lthr,
                start_ts_iso=start_ts_iso,
                sports_list=sports_list,
            )

        if use_temp_zone_metrics:
            zone_select_sql = """
                COALESCE(tzm.zone_1_s, CASE WHEN am.lthr_metrics_current THEN am.zone_1_s END, 0) AS zone_1_s,
                COALESCE(tzm.zone_2_s, CASE WHEN am.lthr_metrics_current THEN am.zone_2_s END, 0) AS zone_2_s,
                COALESCE(tzm.zone_3_s, CASE WHEN am.lthr_metrics_current THEN am.zone_3_s END, 0) AS zone_3_s,
                COALESCE(tzm.zone_4_s, CASE WHEN am.lthr_metrics_current THEN am.zone_4_s END, 0) AS zone_4_s,
                COALESCE(tzm.zone_5_s, CASE WHEN am.lthr_metrics_current THEN am.zone_5_s END, 0) AS zone_5_s
            """
            temp_join_sql = "LEFT JOIN temp_activity_zone_metrics tzm ON tzm.activity_id = a.activity_id"
        else:
            zone_select_sql = """
                CASE WHEN am.lthr_metrics_current THEN COALESCE(am.zone_1_s, 0) ELSE 0 END AS zone_1_s,
                CASE WHEN am.lthr_metrics_current THEN COALESCE(am.zone_2_s, 0) ELSE 0 END AS zone_2_s,
                CASE WHEN am.lthr_metrics_current THEN COALESCE(am.zone_3_s, 0) ELSE 0 END AS zone_3_s,
                CASE WHEN am.lthr_metrics_current THEN COALESCE(am.zone_4_s, 0) ELSE 0 END AS zone_4_s,
                CASE WHEN am.lthr_metrics_current THEN COALESCE(am.zone_5_s, 0) ELSE 0 END AS zone_5_s
            """
            temp_join_sql = ""

        activity_day = activity_calendar_day_sql(conn, table_alias="a")
        temporal_metric_select_sql = current_temporal_metric_projection_sql(
            "am",
            (
                "aerobic_decoupling_pct",
                "hr_drift_pct",
                "peak_power_5s_w",
                "peak_power_30s_w",
                "peak_power_60s_w",
                "peak_power_300s_w",
                "peak_power_1200s_w",
            ),
        )
        query = f"""
            SELECT
                a.start_time_gmt          AS start_time_utc,
                {activity_day}            AS activity_date,
                a.activity_type           AS sport,
                a.distance_meters         AS total_distance_m,
                a.elapsed_duration_seconds AS total_elapsed_s,
                a.elevation_gain          AS total_ascent_m,
                a.average_hr              AS avg_hr_bpm,
                a.max_hr                  AS max_hr_bpm,
                a.average_speed           AS avg_speed_mps,
                a.avg_cadence             AS avg_cadence_spm,
                a.avg_power               AS avg_power_w,
                a.norm_power              AS normalized_power_w,
                a.intensity_factor,
                a.training_stress_score,
                am.moving_time_s,
                CASE WHEN am.hr_load_metrics_current THEN am.trimp END AS trimp,
                COALESCE(
                    CASE WHEN am.hr_load_metrics_current THEN am.tss END,
                    a.training_stress_score
                ) AS tss,
                {temporal_metric_select_sql},
                am.efficiency_factor,
                am.variability_index,
                CASE WHEN am.ftp_metrics_current THEN COALESCE(am.power_zone_1_s, 0) ELSE 0 END AS power_zone_1_s,
                CASE WHEN am.ftp_metrics_current THEN COALESCE(am.power_zone_2_s, 0) ELSE 0 END AS power_zone_2_s,
                CASE WHEN am.ftp_metrics_current THEN COALESCE(am.power_zone_3_s, 0) ELSE 0 END AS power_zone_3_s,
                CASE WHEN am.ftp_metrics_current THEN COALESCE(am.power_zone_4_s, 0) ELSE 0 END AS power_zone_4_s,
                CASE WHEN am.ftp_metrics_current THEN COALESCE(am.power_zone_5_s, 0) ELSE 0 END AS power_zone_5_s,
                CASE WHEN am.ftp_metrics_current THEN COALESCE(am.power_zone_6_s, 0) ELSE 0 END AS power_zone_6_s,
                CASE WHEN am.ftp_metrics_current THEN COALESCE(am.power_zone_7_s, 0) ELSE 0 END AS power_zone_7_s,
                {zone_select_sql}
            FROM activity a
            LEFT JOIN (
                SELECT
                    activity_metrics.*,
                    (
                        (
                            refresh_provenance_version = ?
                            AND (? = 0 OR threshold_lthr_bpm IS ?)
                        )
                        OR (
                            refresh_provenance_version IS NULL
                            AND (? = 0 OR lthr_est_bpm IS ?)
                        )
                    ) AS lthr_metrics_current,
                    (
                        (
                            refresh_provenance_version = ?
                            AND (? = 0 OR threshold_lthr_bpm IS ?)
                            AND threshold_resting_hr_bpm IS ?
                        )
                        OR (
                            refresh_provenance_version IS NULL
                            AND ? = 60
                            AND (? = 0 OR lthr_est_bpm IS ?)
                        )
                    ) AS hr_load_metrics_current,
                    (
                        (
                            refresh_provenance_version = ?
                            AND (? = 0 OR threshold_ftp_w IS ?)
                        )
                        OR (
                            refresh_provenance_version IS NULL
                            AND ? = 0
                        )
                    ) AS ftp_metrics_current
                FROM activity_metrics
            ) am ON am.activity_id = a.activity_id
            {temp_join_sql}
            WHERE {activity_day} >= date(?)
        """

        params = [
            ACTIVITY_METRICS_PROVENANCE_VERSION,
            int(lthr_is_managed),
            current_lthr,
            int(lthr_is_managed),
            current_lthr,
            ACTIVITY_METRICS_PROVENANCE_VERSION,
            int(lthr_is_managed),
            current_lthr,
            current_resting_hr,
            current_resting_hr,
            int(lthr_is_managed),
            current_lthr,
            ACTIVITY_METRICS_PROVENANCE_VERSION,
            int(ftp_is_managed),
            current_ftp,
            int(ftp_is_managed),
            start_ts_iso,
        ]
        if sports_list:
            placeholders = ",".join("?" for _ in sports_list)
            query += f" AND a.activity_type IN ({placeholders})"
            params.extend(sports_list)

        query += " ORDER BY a.start_time_gmt ASC"

        return pd.read_sql_query(query, conn, params=params)
    except Exception:
        return pd.DataFrame()


def refresh_temp_activity_zone_metrics(
    conn,
    lthr: int | None,
    start_ts_iso: str = "1970-01-01T00:00:00Z",
    sports_list: tuple | None = None,
) -> None:
    """Build a temporary per-activity HR zone summary from trackpoints.

    The temp table exists only for the current DB connection and avoids relying
    on persisted `activity_metrics` zone columns after schema/source changes.
    """
    try:
        sports_key = ""
        if sports_list:
            sports_key = "|".join(sorted(str(s) for s in sports_list))

        lthr_key = "" if lthr is None else str(int(lthr))

        # Reuse temp table if the inputs are unchanged for this connection.
        try:
            meta_rows = conn.execute(
                "SELECT key, value FROM temp_activity_zone_metrics_meta"
            ).fetchall()
            meta = {r[0]: r[1] for r in meta_rows}
            if (
                meta.get("lthr") == lthr_key
                and meta.get("start_ts_iso") == start_ts_iso
                and meta.get("sports_key") == sports_key
            ):
                return
        except Exception:
            pass

        conn.execute("DROP TABLE IF EXISTS temp_activity_zone_metrics")
        conn.execute(
            """
            CREATE TEMP TABLE temp_activity_zone_metrics (
                activity_id INTEGER PRIMARY KEY,
                zone_1_s REAL DEFAULT 0,
                zone_2_s REAL DEFAULT 0,
                zone_3_s REAL DEFAULT 0,
                zone_4_s REAL DEFAULT 0,
                zone_5_s REAL DEFAULT 0
            )
            """
        )

        if not lthr or lthr <= 0:
            conn.execute("DROP TABLE IF EXISTS temp_activity_zone_metrics_meta")
            conn.execute(
                "CREATE TEMP TABLE temp_activity_zone_metrics_meta (key TEXT PRIMARY KEY, value TEXT)"
            )
            conn.executemany(
                "INSERT OR REPLACE INTO temp_activity_zone_metrics_meta(key, value) VALUES (?, ?)",
                [
                    ("lthr", lthr_key),
                    ("start_ts_iso", start_ts_iso),
                    ("sports_key", sports_key),
                ],
            )
            return

        z1_upper = float(lthr) * 0.50
        z2_upper = float(lthr) * 0.70
        z3_upper = float(lthr) * 0.85
        z4_upper = float(lthr) * 1.00

        activity_day = activity_calendar_day_sql(conn, table_alias="a")
        filter_sql = f"AND {activity_day} >= date(?)"
        params = []
        if sports_list:
            placeholders = ",".join("?" for _ in sports_list)
            filter_sql += f" AND a.activity_type IN ({placeholders})"
            params.extend(sports_list)

        conn.execute(
            """
            INSERT INTO temp_activity_zone_metrics(
                activity_id,
                zone_1_s,
                zone_2_s,
                zone_3_s,
                zone_4_s,
                zone_5_s
            )
            SELECT
                x.activity_id,
                SUM(CASE WHEN x.hr < ? THEN x.dt_s ELSE 0 END) AS zone_1_s,
                SUM(CASE WHEN x.hr >= ? AND x.hr < ? THEN x.dt_s ELSE 0 END) AS zone_2_s,
                SUM(CASE WHEN x.hr >= ? AND x.hr < ? THEN x.dt_s ELSE 0 END) AS zone_3_s,
                SUM(CASE WHEN x.hr >= ? AND x.hr < ? THEN x.dt_s ELSE 0 END) AS zone_4_s,
                SUM(CASE WHEN x.hr >= ? THEN x.dt_s ELSE 0 END) AS zone_5_s
            FROM (
                SELECT
                    tp.activity_id,
                    tp.heart_rate_bpm AS hr,
                    (julianday(tp_next.timestamp_utc) - julianday(tp.timestamp_utc)) * 86400.0 AS dt_s
                FROM activity_trackpoints tp
                JOIN activity a ON a.activity_id = tp.activity_id
                JOIN activity_trackpoints tp_next
                  ON tp_next.activity_id = tp.activity_id
                 AND tp_next.seq = tp.seq + 1
                WHERE tp.heart_rate_bpm IS NOT NULL
                  AND tp.heart_rate_bpm >= 35
                  AND tp.heart_rate_bpm <= 220
                  """
            + filter_sql
            + """
            ) x
            WHERE x.dt_s > 0
              AND x.dt_s <= ?
            GROUP BY x.activity_id
            """,
            (
                z1_upper,
                z1_upper,
                z2_upper,
                z2_upper,
                z3_upper,
                z3_upper,
                z4_upper,
                z4_upper,
                start_ts_iso,
                *params,
                MAX_CONTIGUOUS_GAP_SECONDS,
            ),
        )

        conn.execute("DROP TABLE IF EXISTS temp_activity_zone_metrics_meta")
        conn.execute(
            "CREATE TEMP TABLE temp_activity_zone_metrics_meta (key TEXT PRIMARY KEY, value TEXT)"
        )
        conn.executemany(
            "INSERT OR REPLACE INTO temp_activity_zone_metrics_meta(key, value) VALUES (?, ?)",
            [
                ("lthr", lthr_key),
                ("start_ts_iso", start_ts_iso),
                ("sports_key", sports_key),
            ],
        )
    except Exception:
        return


def get_activities_dataframe_for_compliance(
    conn, start_ts_iso: str, lthr: int | None, sports_list: tuple | None = None
) -> pd.DataFrame:
    """Return activity rows for Compliance page using persisted zone metrics.

    Zone seconds are sourced from persisted `activity_metrics` columns.
    """
    return get_activities_dataframe(
        conn,
        start_ts_iso,
        sports_list=sports_list,
        lthr=lthr,
        use_temp_zone_metrics=False,
    )


def refresh_persisted_activity_metrics(
    conn,
    activity_ids: Iterable[int] | None = None,
    start_ts_iso: str | None = None,
    lthr: int | None = None,
) -> dict[str, int]:
    """Refresh persisted `activity_metrics` fields after sync.

    Updates are based on `activity` + `activity_trackpoints` so pages can read
    precomputed values without connection-local temp tables.
    """
    summary = {
        "target_activities": 0,
        "rows_upserted": 0,
        "zones_updated": 0,
        "errors": 0,
    }
    caller_owns_transaction = bool(conn.in_transaction)
    savepoint_name = f"refresh_persisted_activity_metrics_{id(summary):x}"
    refresh_boundary_active = False

    def _discard_refresh_changes() -> None:
        nonlocal refresh_boundary_active
        if not refresh_boundary_active:
            return
        if caller_owns_transaction:
            try:
                conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            finally:
                conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        else:
            conn.rollback()
        refresh_boundary_active = False

    def _persist_top_level_error_summary() -> None:
        if caller_owns_transaction or conn.in_transaction:
            return
        try:
            conn.execute(
                UPSERT_SETTING_SQL,
                (
                    ACTIVITY_METRICS_LAST_REFRESH_SUMMARY_KEY,
                    json.dumps(summary),
                ),
            )
            conn.commit()
        except Exception:
            logger.warning(
                "Failed to persist activity metrics refresh error summary",
                exc_info=True,
            )
            if conn.in_transaction:
                try:
                    conn.rollback()
                except Exception:
                    logger.exception(
                        "Failed to clean up activity metrics error-summary transaction"
                    )

    try:
        candidate_ids: list[int] = []
        if activity_ids is not None:
            candidate_ids = [
                int(aid) for aid in activity_ids if aid is not None and int(aid) > 0
            ]
        candidate_ids = list(dict.fromkeys(candidate_ids))

        if start_ts_iso:
            rows = conn.execute(
                "SELECT activity_id FROM activity WHERE start_time_gmt >= ?",
                (start_ts_iso,),
            ).fetchall()
            candidate_id_set = set(candidate_ids)
            for row in rows:
                if not row or row[0] is None:
                    continue
                activity_id = int(row[0])
                if activity_id not in candidate_id_set:
                    candidate_ids.append(activity_id)
                    candidate_id_set.add(activity_id)
        elif not candidate_ids:
            # An empty explicit list retains the historical all-activity fallback.
            rows = conn.execute("SELECT activity_id FROM activity").fetchall()
            candidate_ids = [int(r[0]) for r in rows if r and r[0] is not None]

        if not candidate_ids:
            return summary

        summary["target_activities"] = len(candidate_ids)

        if caller_owns_transaction:
            conn.execute(f"SAVEPOINT {savepoint_name}")
        else:
            conn.execute("BEGIN")
        refresh_boundary_active = True

        profile = get_athlete_profile(conn) or {}
        effective_lthr = (
            int(lthr)
            if lthr and int(lthr) > 0
            else get_effective_lthr(conn, commit=False)
        )
        effective_ftp = _positive_profile_metric(
            profile, "ftp_override", "ftp_calc"
        ) or get_effective_ftp(conn, commit=False)
        resting_hr = int(profile.get("resting_hr") or 60)

        # Ensure target rows exist.
        conn.executemany(
            "INSERT OR IGNORE INTO activity_metrics(activity_id) VALUES (?)",
            [(aid,) for aid in candidate_ids],
        )

        # A refresh is a current-state rebuild for the fields it owns. Clearing
        # them first prevents an unavailable source from inheriting an obsolete
        # value; the enclosing transaction/savepoint restores everything on error.
        placeholders = ",".join("?" for _ in candidate_ids)
        clear_assignments = ", ".join(
            f"{column_name} = NULL"
            for column_name in REFRESH_OWNED_ACTIVITY_METRIC_COLUMNS
        )
        conn.execute(
            f"UPDATE activity_metrics SET {clear_assignments} "
            f"WHERE activity_id IN ({placeholders})",
            tuple(candidate_ids),
        )

        _refresh_scalar_activity_metrics(
            conn,
            candidate_ids,
            effective_lthr,
            int(effective_ftp) if effective_ftp else None,
            resting_hr,
        )
        _refresh_trackpoint_derived_metrics(
            conn,
            candidate_ids,
            int(effective_ftp) if effective_ftp else None,
        )

        if effective_lthr:
            z1_upper = float(effective_lthr) * 0.50
            z2_upper = float(effective_lthr) * 0.70
            z3_upper = float(effective_lthr) * 0.85
            z4_upper = float(effective_lthr) * 1.00

            rows = conn.execute(
                f"""
                SELECT
                    x.activity_id,
                    SUM(CASE WHEN x.hr < ? THEN x.dt_s ELSE 0 END) AS zone_1_s,
                    SUM(CASE WHEN x.hr >= ? AND x.hr < ? THEN x.dt_s ELSE 0 END) AS zone_2_s,
                    SUM(CASE WHEN x.hr >= ? AND x.hr < ? THEN x.dt_s ELSE 0 END) AS zone_3_s,
                    SUM(CASE WHEN x.hr >= ? AND x.hr < ? THEN x.dt_s ELSE 0 END) AS zone_4_s,
                    SUM(CASE WHEN x.hr >= ? THEN x.dt_s ELSE 0 END) AS zone_5_s
                FROM (
                    SELECT
                        tp.activity_id,
                        tp.heart_rate_bpm AS hr,
                        (julianday(tp_next.timestamp_utc) - julianday(tp.timestamp_utc)) * 86400.0 AS dt_s
                    FROM activity_trackpoints tp
                    JOIN activity_trackpoints tp_next
                      ON tp_next.activity_id = tp.activity_id
                     AND tp_next.seq = tp.seq + 1
                    WHERE tp.heart_rate_bpm IS NOT NULL
                      AND tp.heart_rate_bpm >= 35
                      AND tp.heart_rate_bpm <= 220
                      AND tp.activity_id IN ({placeholders})
                ) x
                WHERE x.dt_s > 0
                  AND x.dt_s <= ?
                GROUP BY x.activity_id
                """,
                (
                    z1_upper,
                    z1_upper,
                    z2_upper,
                    z2_upper,
                    z3_upper,
                    z3_upper,
                    z4_upper,
                    z4_upper,
                    *candidate_ids,
                    MAX_CONTIGUOUS_GAP_SECONDS,
                ),
            ).fetchall()

            zone_by_activity = {
                int(r[0]): (
                    float(r[1] or 0),
                    float(r[2] or 0),
                    float(r[3] or 0),
                    float(r[4] or 0),
                    float(r[5] or 0),
                )
                for r in rows
            }

            update_rows = []
            for aid in candidate_ids:
                z = zone_by_activity.get(aid, (None, None, None, None, None))
                update_rows.append((*z, effective_lthr, aid))

            conn.executemany(
                """
                UPDATE activity_metrics
                SET zone_1_s = ?,
                    zone_2_s = ?,
                    zone_3_s = ?,
                    zone_4_s = ?,
                    zone_5_s = ?,
                    lthr_est_bpm = ?
                WHERE activity_id = ?
                """,
                update_rows,
            )
            summary["zones_updated"] = len(update_rows)

        conn.execute(
            f"""
            UPDATE activity_metrics
            SET refresh_provenance_version = ?,
                threshold_lthr_bpm = ?,
                threshold_ftp_w = ?,
                threshold_resting_hr_bpm = ?
            WHERE activity_id IN ({placeholders})
            """,
            (
                ACTIVITY_METRICS_PROVENANCE_VERSION,
                effective_lthr,
                effective_ftp,
                resting_hr,
                *candidate_ids,
            ),
        )

        summary["rows_upserted"] = len(candidate_ids)
        conn.execute(
            UPSERT_SETTING_SQL,
            (ACTIVITY_METRICS_LAST_REFRESH_KEY, json.dumps(_utc_now_iso())),
        )
        conn.execute(
            UPSERT_SETTING_SQL,
            (ACTIVITY_METRICS_LAST_REFRESH_SUMMARY_KEY, json.dumps(summary)),
        )
        if caller_owns_transaction:
            conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            refresh_boundary_active = False
        else:
            try:
                conn.commit()
            except Exception:
                # A wrapper can raise after the underlying COMMIT succeeds.
                # If SQLite no longer has a transaction, the refresh is durable
                # and must not be reported as rolled back.
                if not conn.in_transaction:
                    refresh_boundary_active = False
                    logger.warning(
                        "Activity metric refresh commit completed but finalization "
                        "reported an exception",
                        exc_info=True,
                    )
                    return summary
                raise
            refresh_boundary_active = False
        return summary
    except Exception:
        summary["errors"] += 1
        summary["rows_upserted"] = 0
        summary["zones_updated"] = 0
        logger.exception("Failed to refresh persisted activity metrics")
        try:
            _discard_refresh_changes()
        except Exception:
            logger.exception("Failed to roll back activity metric refresh boundary")
        _persist_top_level_error_summary()
        return summary


def get_activity_metrics_diagnostics(conn) -> dict[str, Any]:
    """Return high-level diagnostics for persisted activity metrics health."""
    try:
        total_activities_row = conn.execute("SELECT COUNT(*) FROM activity").fetchone()
        total_metrics_row = conn.execute(
            "SELECT COUNT(*) FROM activity_metrics"
        ).fetchone()
        missing_rows = list_activities_needing_metrics(conn)

        return {
            "total_activities": (
                int(total_activities_row[0] or 0) if total_activities_row else 0
            ),
            "total_metrics_rows": (
                int(total_metrics_row[0] or 0) if total_metrics_row else 0
            ),
            "missing_metrics_count": int(len(missing_rows)),
            "last_refresh_utc": get_setting(
                conn, ACTIVITY_METRICS_LAST_REFRESH_KEY, None
            ),
            "last_refresh_summary": get_setting(
                conn, ACTIVITY_METRICS_LAST_REFRESH_SUMMARY_KEY, {}
            ),
        }
    except sqlite3.Error:
        logger.exception("Failed to collect activity metrics diagnostics")
        return {
            "total_activities": 0,
            "total_metrics_rows": 0,
            "missing_metrics_count": 0,
            "last_refresh_utc": None,
            "last_refresh_summary": {},
        }


# FIT-pipeline bulk-insert functions removed — replaced by garmin-givemydata


def list_activities_needing_metrics(conn) -> list[int]:
    """Return activity_ids that still appear to need a derived-metrics refresh.

    The intent is to flag activities with genuinely incomplete refreshable zone data,
    not activities where optional load fields like `trimp` or `tss` are legitimately
    unavailable.
    """
    try:
        current_lthr, current_ftp, current_resting_hr = (
            get_current_activity_metric_provenance(conn)
        )
        lthr_is_managed, ftp_is_managed = (
            get_activity_metric_threshold_management(conn)
        )

        activity_columns = _get_table_columns(conn, "activity")
        legacy_power_source_checks = [
            """EXISTS (
                    SELECT 1
                    FROM activity_trackpoints power_tp
                    WHERE power_tp.activity_id = a.activity_id
                      AND power_tp.power_w IS NOT NULL
                )"""
        ]
        for source_column in ("norm_power", "avg_power", "max_power"):
            if source_column in activity_columns:
                legacy_power_source_checks.append(
                    f"a.{source_column} IS NOT NULL"
                )
        legacy_power_source_sql = " OR ".join(legacy_power_source_checks)
        rows = conn.execute(
            f"""
            SELECT a.activity_id
            FROM activity a
            LEFT JOIN activity_metrics am ON a.activity_id = am.activity_id
            WHERE am.activity_id IS NULL
               OR (
                    am.refresh_provenance_version IS NOT NULL
                    AND am.refresh_provenance_version <> ?
               )
               OR (
                    am.refresh_provenance_version = ?
                    AND (
                        (? = 1 AND am.threshold_lthr_bpm IS NOT ?)
                        OR (? = 1 AND am.threshold_ftp_w IS NOT ?)
                        OR am.threshold_resting_hr_bpm IS NOT ?
                    )
               )
               OR (
                    ? = 1
                    AND am.refresh_provenance_version IS NULL
                    AND (
                        am.lthr_est_bpm IS NOT ?
                        OR (
                            ? IS NULL
                            AND (
                                am.zone_1_s IS NOT NULL
                                OR am.zone_2_s IS NOT NULL
                                OR am.zone_3_s IS NOT NULL
                                OR am.zone_4_s IS NOT NULL
                                OR am.zone_5_s IS NOT NULL
                            )
                        )
                    )
               )
               OR (
                    ? = 1
                    AND am.refresh_provenance_version IS NULL
                    AND (
                        (
                            ? IS NOT NULL
                            AND ({legacy_power_source_sql})
                        )
                        OR (
                            ? IS NULL
                            AND (
                                am.power_zone_1_s IS NOT NULL
                                OR am.power_zone_2_s IS NOT NULL
                                OR am.power_zone_3_s IS NOT NULL
                                OR am.power_zone_4_s IS NOT NULL
                                OR am.power_zone_5_s IS NOT NULL
                                OR am.power_zone_6_s IS NOT NULL
                                OR am.power_zone_7_s IS NOT NULL
                                OR (
                                    am.if_val IS NOT NULL
                                    AND ({legacy_power_source_sql})
                                )
                            )
                        )
                    )
               )
               OR (
                    am.refresh_provenance_version IS NULL
                    AND ? <> 60
                    AND (
                        am.trimp IS NOT NULL
                        OR am.if_val IS NOT NULL
                        OR am.tss IS NOT NULL
                    )
               )
               OR (
                    EXISTS (
                        SELECT 1
                        FROM activity_trackpoints tp
                        WHERE tp.activity_id = a.activity_id
                          AND tp.heart_rate_bpm IS NOT NULL
                          AND tp.heart_rate_bpm >= 35
                          AND tp.heart_rate_bpm <= 220
                    )
                    AND (
                        am.lthr_est_bpm IS NULL
                        OR am.zone_1_s IS NULL
                        OR am.zone_2_s IS NULL
                        OR am.zone_3_s IS NULL
                        OR am.zone_4_s IS NULL
                        OR am.zone_5_s IS NULL
                        OR (
                            COALESCE(am.zone_1_s, 0) = 0
                            AND COALESCE(am.zone_2_s, 0) = 0
                            AND COALESCE(am.zone_3_s, 0) = 0
                            AND COALESCE(am.zone_4_s, 0) = 0
                            AND COALESCE(am.zone_5_s, 0) = 0
                        )
                    )
               )
            """,
            (
                ACTIVITY_METRICS_PROVENANCE_VERSION,
                ACTIVITY_METRICS_PROVENANCE_VERSION,
                int(lthr_is_managed),
                current_lthr,
                int(ftp_is_managed),
                current_ftp,
                current_resting_hr,
                int(lthr_is_managed),
                current_lthr,
                current_lthr,
                int(ftp_is_managed),
                current_ftp,
                current_ftp,
                current_resting_hr,
            ),
        ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


def list_all_activity_ids(conn) -> list[int]:
    """Return all activity_ids in the activity table as a list of ints."""
    try:
        rows = conn.execute(
            "SELECT activity_id FROM activity ORDER BY activity_id"
        ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


def get_activity_metrics(conn, activity_id: int):
    """Return the `activity_metrics` row for `activity_id` or None."""
    try:
        current_lthr, _, current_resting_hr = (
            get_current_activity_metric_provenance(conn)
        )
        lthr_is_managed, _ = get_activity_metric_threshold_management(conn)
        row = conn.execute(
            """
            SELECT
                CASE WHEN lthr_metrics_current THEN zone_1_s END AS zone_1_s,
                CASE WHEN hr_load_metrics_current THEN trimp END AS trimp,
                CASE WHEN hr_load_metrics_current THEN tss END AS tss
            FROM (
                SELECT
                    activity_metrics.*,
                    (
                        (
                            refresh_provenance_version = ?
                            AND (? = 0 OR threshold_lthr_bpm IS ?)
                        )
                        OR (
                            refresh_provenance_version IS NULL
                            AND (? = 0 OR lthr_est_bpm IS ?)
                        )
                    ) AS lthr_metrics_current,
                    (
                        (
                            refresh_provenance_version = ?
                            AND (? = 0 OR threshold_lthr_bpm IS ?)
                            AND threshold_resting_hr_bpm IS ?
                        )
                        OR (
                            refresh_provenance_version IS NULL
                            AND ? = 60
                            AND (? = 0 OR lthr_est_bpm IS ?)
                        )
                    ) AS hr_load_metrics_current
                FROM activity_metrics
            )
            WHERE activity_id = ?
            """,
            (
                ACTIVITY_METRICS_PROVENANCE_VERSION,
                int(lthr_is_managed),
                current_lthr,
                int(lthr_is_managed),
                current_lthr,
                ACTIVITY_METRICS_PROVENANCE_VERSION,
                int(lthr_is_managed),
                current_lthr,
                current_resting_hr,
                current_resting_hr,
                int(lthr_is_managed),
                current_lthr,
                activity_id,
            ),
        ).fetchone()
        return row
    except Exception:
        return None


# FIT file lookup functions removed — FIT pipeline replaced by garmin-givemydata


def get_problems(conn, limit: int = 200) -> list[dict]:
    """Return rows from a legacy `problems` table if present; otherwise empty list."""
    try:
        rows = conn.execute("SELECT * FROM problems LIMIT ?", (int(limit),)).fetchall()
        out = []
        for r in rows:
            # sqlite3.Row supports mapping access
            try:
                out.append(dict(r))
            except Exception:
                # Fallback to tuple-based mapping
                out.append({i: r[i] for i in range(len(r))})
        return out
    except Exception:
        return []


# insert_activity, insert_session removed — FIT pipeline replaced by garmin-givemydata
