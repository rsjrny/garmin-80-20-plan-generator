import json
import logging
import math
import sqlite3
from datetime import datetime, timezone
from typing import Any, Iterable
import pandas as pd

logger = logging.getLogger(__name__)

GET_SETTING_SQL = "SELECT value FROM app_settings WHERE key = ?"
UPSERT_SETTING_SQL = "INSERT OR REPLACE INTO app_settings (key, value) VALUES (?, ?)"
ACTIVITY_METRICS_LAST_REFRESH_KEY = "activity_metrics_last_refresh_utc"
ACTIVITY_METRICS_LAST_REFRESH_SUMMARY_KEY = "activity_metrics_last_refresh_summary"


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
    """Upsert a JSON-serialised setting value."""
    try:
        conn.execute(UPSERT_SETTING_SQL, (key, json.dumps(value)))
        conn.commit()
    except (sqlite3.Error, TypeError, ValueError):
        logger.warning("Failed to persist app setting '%s'", key, exc_info=True)
        return


GET_DISTINCT_SPORTS_SQL = """
SELECT DISTINCT activity_type AS sport
FROM activity
WHERE activity_type IS NOT NULL
  AND start_time_gmt >= ?
ORDER BY activity_type
"""


def get_athlete_profile(conn):
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
        return None


def get_effective_lthr(conn) -> int | None:
    """Return LTHR override/calculated value, with robust fallback from activities."""
    try:
        profile = get_athlete_profile(conn)
        if profile:
            lthr = profile.get("lthr_override") or profile.get("lthr_calc")
            if lthr and int(lthr) > 0:
                return int(lthr)
    except Exception:
        pass

    # Fallback to robust estimate from recent max HR values.
    try:
        cutoff_iso = "1970-01-01T00:00:00Z"
        hrmax, lthr = get_hrmax_robust_and_lthr(conn, cutoff_iso, percentile=0.995)
        if hrmax and lthr:
            ensure_athlete_profile_table(conn)
            set_calculated_metrics(conn, int(hrmax), int(lthr))
            return int(lthr)
    except Exception:
        pass
    return None


def _estimate_ftp_from_recent_power(conn, days_back: int = 180) -> int | None:
    """Estimate cycling FTP from recent power-enabled activities.

    Uses the strongest recent sustained power efforts (favoring cycling/ride sports)
    and applies the common ~95% of best 20+ minute effort heuristic.
    """
    try:
        activity_columns = _get_table_columns(conn, "activity")
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
        return None


def get_effective_ftp(conn) -> int | None:
    """Return FTP override/calculated value, with fallback estimation from power data."""
    try:
        profile = get_athlete_profile(conn)
        if profile:
            ftp = profile.get("ftp_override") or profile.get("ftp_calc")
            if ftp and int(ftp) > 0:
                return int(ftp)
    except Exception:
        pass

    try:
        ftp_est = _estimate_ftp_from_recent_power(conn)
        if ftp_est and int(ftp_est) > 0:
            ensure_athlete_profile_table(conn)
            set_calculated_ftp(conn, int(ftp_est))
            return int(ftp_est)
    except Exception:
        pass

    return None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get_table_columns(conn, table_name: str) -> set[str]:
    """Return the column names present on `table_name`, or an empty set."""
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {str(r[1]) for r in rows if len(r) > 1 and r[1]}
    except sqlite3.Error:
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
        if (if_val is None or float(if_val) <= 0) and ftp and norm_power:
            if_val = round(float(norm_power) / float(ftp), 2)
        elif (
            (if_val is None or float(if_val) <= 0)
            and lthr
            and average_hr
            and lthr > resting_hr
        ):
            hr_if = (float(average_hr) - float(resting_hr)) / float(lthr - resting_hr)
            if_val = round(max(0.0, min(2.0, hr_if)), 2)

        tss = training_stress_score
        if (
            (tss is None or float(tss) <= 0)
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
        if norm_power and avg_power and float(avg_power) > 0:
            variability_index = round(float(norm_power) / float(avg_power), 3)

        efficiency_factor = None
        if norm_power and average_hr and float(average_hr) > 0:
            efficiency_factor = round(float(norm_power) / float(average_hr), 3)
        elif average_speed and average_hr and float(average_hr) > 0:
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


def _refresh_trackpoint_derived_metrics(
    conn,
    candidate_ids: list[int],
    ftp: int | None = None,
) -> None:
    """Populate metrics that are best derived from `activity_trackpoint`."""
    if not candidate_ids:
        return

    placeholders = ",".join("?" for _ in candidate_ids)

    agg_rows = conn.execute(
        f"""
        SELECT
            activity_id,
            AVG(CASE WHEN cadence IS NOT NULL AND cadence > 0 THEN cadence END) AS avg_cadence_spm,
            AVG(CASE WHEN power_w IS NOT NULL AND power_w > 0 THEN power_w END) AS avg_power_w,
            MAX(CASE WHEN power_w IS NOT NULL AND power_w > 0 THEN power_w END) AS max_power_w,
            AVG(temperature_c) AS avg_temperature_c,
            MIN(temperature_c) AS min_temperature_c,
            MAX(temperature_c) AS max_temperature_c,
            MIN(altitude_m) AS min_altitude_m,
            MAX(altitude_m) AS max_altitude_m
        FROM activity_trackpoint
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
            FROM activity_trackpoint
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

    decoupling_rows = conn.execute(
        f"""
        WITH filtered AS (
            SELECT
                activity_id,
                heart_rate_bpm,
                CASE WHEN power_w IS NOT NULL AND power_w > 0 THEN power_w END AS power_w,
                CASE WHEN speed_mps IS NOT NULL AND speed_mps > 0 THEN speed_mps END AS speed_mps,
                ROW_NUMBER() OVER (PARTITION BY activity_id ORDER BY seq) AS rn,
                COUNT(*) OVER (PARTITION BY activity_id) AS total_points
            FROM activity_trackpoint
            WHERE activity_id IN ({placeholders})
              AND heart_rate_bpm IS NOT NULL
              AND heart_rate_bpm >= 35
              AND heart_rate_bpm <= 220
        ),
        halves AS (
            SELECT
                activity_id,
                AVG(CASE WHEN rn <= total_points / 2 THEN heart_rate_bpm END) AS hr_first,
                AVG(CASE WHEN rn > total_points / 2 THEN heart_rate_bpm END) AS hr_second,
                AVG(CASE WHEN rn <= total_points / 2 THEN COALESCE(power_w, speed_mps) END) AS output_first,
                AVG(CASE WHEN rn > total_points / 2 THEN COALESCE(power_w, speed_mps) END) AS output_second,
                AVG(CASE WHEN rn <= total_points / 2 THEN speed_mps END) AS speed_first,
                AVG(CASE WHEN rn > total_points / 2 THEN speed_mps END) AS speed_second
            FROM filtered
            GROUP BY activity_id
        )
        SELECT
            activity_id,
            CASE
                WHEN hr_first > 0 AND hr_second > 0 AND output_first > 0 AND output_second > 0
                THEN ROUND((((output_first / hr_first) - (output_second / hr_second)) / (output_first / hr_first)) * 100.0, 2)
            END AS aerobic_decoupling_pct,
            CASE
                WHEN hr_first > 0 AND hr_second > 0
                THEN ROUND(((hr_second - hr_first) / hr_first) * 100.0, 2)
            END AS hr_drift_pct,
            CASE
                WHEN hr_first > 0 AND hr_second > 0 AND speed_first > 0 AND speed_second > 0
                THEN ROUND((((speed_first / hr_first) - (speed_second / hr_second)) / (speed_first / hr_first)) * 100.0, 2)
            END AS pace_decoupling_pct
        FROM halves
        WHERE hr_first IS NOT NULL AND hr_second IS NOT NULL
        """,
        tuple(candidate_ids),
    ).fetchall()

    if decoupling_rows:
        conn.executemany(
            """
            UPDATE activity_metrics
            SET aerobic_decoupling_pct = COALESCE(?, aerobic_decoupling_pct),
                hr_drift_pct = COALESCE(?, hr_drift_pct),
                pace_decoupling_pct = COALESCE(?, pace_decoupling_pct)
            WHERE activity_id = ?
            """,
            [(row[1], row[2], row[3], row[0]) for row in decoupling_rows],
        )

    power_peak_rows = conn.execute(
        f"""
        WITH power_samples AS (
            SELECT activity_id, seq, CAST(power_w AS REAL) AS power_w
            FROM activity_trackpoint
            WHERE activity_id IN ({placeholders})
              AND power_w IS NOT NULL
              AND power_w > 0
        ),
        rolling AS (
            SELECT
                activity_id,
                AVG(power_w) OVER (PARTITION BY activity_id ORDER BY seq ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS peak_power_5s_w,
                AVG(power_w) OVER (PARTITION BY activity_id ORDER BY seq ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS peak_power_30s_w,
                AVG(power_w) OVER (PARTITION BY activity_id ORDER BY seq ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS peak_power_60s_w,
                AVG(power_w) OVER (PARTITION BY activity_id ORDER BY seq ROWS BETWEEN 299 PRECEDING AND CURRENT ROW) AS peak_power_300s_w,
                AVG(power_w) OVER (PARTITION BY activity_id ORDER BY seq ROWS BETWEEN 1199 PRECEDING AND CURRENT ROW) AS peak_power_1200s_w
            FROM power_samples
        )
        SELECT
            activity_id,
            MAX(peak_power_5s_w),
            MAX(peak_power_30s_w),
            MAX(peak_power_60s_w),
            MAX(peak_power_300s_w),
            MAX(peak_power_1200s_w)
        FROM rolling
        GROUP BY activity_id
        """,
        tuple(candidate_ids),
    ).fetchall()

    if power_peak_rows:
        conn.executemany(
            """
            UPDATE activity_metrics
            SET peak_power_5s_w = COALESCE(?, peak_power_5s_w),
                peak_power_30s_w = COALESCE(?, peak_power_30s_w),
                peak_power_60s_w = COALESCE(?, peak_power_60s_w),
                peak_power_300s_w = COALESCE(?, peak_power_300s_w),
                peak_power_1200s_w = COALESCE(?, peak_power_1200s_w)
            WHERE activity_id = ?
            """,
            [
                (row[1], row[2], row[3], row[4], row[5], row[0])
                for row in power_peak_rows
            ],
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
                FROM activity_trackpoint tp
                JOIN activity_trackpoint tp_next
                  ON tp_next.activity_id = tp.activity_id
                 AND tp_next.seq = tp.seq + 1
                WHERE tp.activity_id IN ({placeholders})
                  AND tp.power_w IS NOT NULL
                  AND tp.power_w > 0
            ) x
            WHERE x.dt_s > 0
              AND x.dt_s <= 30
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


def ensure_athlete_profile_table(conn) -> None:
    """Create `athlete_profile` if it does not exist and ensure a row exists."""
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
        conn.commit()
    except Exception:
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
            "hrmax_effective": (
                hrmax_override if hrmax_override is not None else hrmax_calc
            ),
            "lthr_effective": lthr_override if lthr_override is not None else lthr_calc,
            "ftp_effective": ftp_override if ftp_override is not None else ftp_calc,
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


def set_calculated_metrics(conn, hrmax: int | None, lthr: int | None) -> None:
    try:
        conn.execute(
            "UPDATE athlete_profile SET hrmax_calc=?, lthr_calc=?, calc_updated_utc=? WHERE profile_id=1",
            (hrmax, lthr, _utc_now_iso()),
        )
        conn.commit()
    except Exception:
        return


def set_calculated_ftp(conn, ftp: int | None) -> None:
    """Persist a calculated FTP estimate when the newer athlete-profile columns exist."""
    try:
        ensure_athlete_profile_table(conn)
        conn.execute(
            "UPDATE athlete_profile SET ftp_calc=?, calc_updated_utc=? WHERE profile_id=1",
            (ftp, _utc_now_iso()),
        )
        conn.commit()
    except Exception:
        return


def set_override_metrics(
    conn, hrmax: int | None, lthr: int | None, ftp: int | None = None
) -> None:
    try:
        ensure_athlete_profile_table(conn)
        columns = _get_table_columns(conn, "athlete_profile")
        if "ftp_override" in columns:
            conn.execute(
                "UPDATE athlete_profile SET hrmax_override=?, lthr_override=?, ftp_override=?, override_updated_utc=? WHERE profile_id=1",
                (hrmax, lthr, ftp, _utc_now_iso()),
            )
        else:
            conn.execute(
                "UPDATE athlete_profile SET hrmax_override=?, lthr_override=?, override_updated_utc=? WHERE profile_id=1",
                (hrmax, lthr, _utc_now_iso()),
            )
        conn.commit()
    except Exception:
        return


def clear_override_metrics(conn) -> None:
    try:
        ensure_athlete_profile_table(conn)
        columns = _get_table_columns(conn, "athlete_profile")
        if "ftp_override" in columns:
            conn.execute(
                "UPDATE athlete_profile SET hrmax_override=NULL, lthr_override=NULL, ftp_override=NULL, override_updated_utc=? WHERE profile_id=1",
                (_utc_now_iso(),),
            )
        else:
            conn.execute(
                "UPDATE athlete_profile SET hrmax_override=NULL, lthr_override=NULL, override_updated_utc=? WHERE profile_id=1",
                (_utc_now_iso(),),
            )
        conn.commit()
    except Exception:
        return


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
) -> None:
    try:
        conn.execute(
            """
            INSERT INTO planned_workout(
                scheduled_date, workout_name, description, 
                planned_distance_m, planned_duration_s, planned_tss
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                scheduled_date,
                workout_name,
                description,
                planned_distance_m,
                planned_duration_s,
                planned_tss,
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
    conn, cutoff_iso: str, percentile: float = 0.995
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
            FROM activity_trackpoint
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
    connection-local temp table computed from `activity_trackpoint`, with
    fallback to persisted `activity_metrics` values.
    """
    try:
        effective_lthr = lthr
        if use_temp_zone_metrics and effective_lthr is None:
            profile = get_athlete_profile(conn)
            if profile:
                effective_lthr = profile.get("lthr_override") or profile.get(
                    "lthr_calc"
                )

        if use_temp_zone_metrics:
            refresh_temp_activity_zone_metrics(
                conn,
                effective_lthr,
                start_ts_iso=start_ts_iso,
                sports_list=sports_list,
            )

        if use_temp_zone_metrics:
            zone_select_sql = """
                COALESCE(tzm.zone_1_s, am.zone_1_s, 0) AS zone_1_s,
                COALESCE(tzm.zone_2_s, am.zone_2_s, 0) AS zone_2_s,
                COALESCE(tzm.zone_3_s, am.zone_3_s, 0) AS zone_3_s,
                COALESCE(tzm.zone_4_s, am.zone_4_s, 0) AS zone_4_s,
                COALESCE(tzm.zone_5_s, am.zone_5_s, 0) AS zone_5_s
            """
            temp_join_sql = "LEFT JOIN temp_activity_zone_metrics tzm ON tzm.activity_id = a.activity_id"
        else:
            zone_select_sql = """
                COALESCE(am.zone_1_s, 0) AS zone_1_s,
                COALESCE(am.zone_2_s, 0) AS zone_2_s,
                COALESCE(am.zone_3_s, 0) AS zone_3_s,
                COALESCE(am.zone_4_s, 0) AS zone_4_s,
                COALESCE(am.zone_5_s, 0) AS zone_5_s
            """
            temp_join_sql = ""

        query = f"""
            SELECT
                a.start_time_gmt          AS start_time_utc,
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
                am.trimp,
                COALESCE(am.tss, a.training_stress_score) AS tss,
                am.aerobic_decoupling_pct,
                am.hr_drift_pct,
                am.efficiency_factor,
                am.variability_index,
                am.peak_power_5s_w,
                am.peak_power_30s_w,
                am.peak_power_60s_w,
                am.peak_power_300s_w,
                am.peak_power_1200s_w,
                COALESCE(am.power_zone_1_s, 0) AS power_zone_1_s,
                COALESCE(am.power_zone_2_s, 0) AS power_zone_2_s,
                COALESCE(am.power_zone_3_s, 0) AS power_zone_3_s,
                COALESCE(am.power_zone_4_s, 0) AS power_zone_4_s,
                COALESCE(am.power_zone_5_s, 0) AS power_zone_5_s,
                COALESCE(am.power_zone_6_s, 0) AS power_zone_6_s,
                COALESCE(am.power_zone_7_s, 0) AS power_zone_7_s,
                {zone_select_sql}
            FROM activity a
            LEFT JOIN activity_metrics am ON am.activity_id = a.activity_id
            {temp_join_sql}
            WHERE a.start_time_gmt >= ?
        """

        params = [start_ts_iso]
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

        filter_sql = "AND a.start_time_gmt >= ?"
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
                FROM activity_trackpoint tp
                JOIN activity a ON a.activity_id = tp.activity_id
                JOIN activity_trackpoint tp_next
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
              AND x.dt_s <= 30
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

    Updates are based on `activity` + `activity_trackpoint` so pages can read
    precomputed values without connection-local temp tables.
    """
    summary = {
        "target_activities": 0,
        "rows_upserted": 0,
        "zones_updated": 0,
        "errors": 0,
    }

    try:
        candidate_ids: list[int] = []
        if activity_ids is not None:
            candidate_ids = [
                int(aid) for aid in activity_ids if aid is not None and int(aid) > 0
            ]

        # Treat an empty explicit list the same as "no specific IDs supplied" so
        # post-sync refresh does not silently no-op when the upstream sync did not
        # report changed activity IDs.
        if not candidate_ids:
            if start_ts_iso:
                rows = conn.execute(
                    "SELECT activity_id FROM activity WHERE start_time_gmt >= ?",
                    (start_ts_iso,),
                ).fetchall()
                candidate_ids = [int(r[0]) for r in rows if r and r[0] is not None]
            else:
                rows = conn.execute("SELECT activity_id FROM activity").fetchall()
                candidate_ids = [int(r[0]) for r in rows if r and r[0] is not None]

        if not candidate_ids:
            return summary

        summary["target_activities"] = len(candidate_ids)

        # Ensure target rows exist.
        conn.executemany(
            "INSERT OR IGNORE INTO activity_metrics(activity_id) VALUES (?)",
            [(aid,) for aid in candidate_ids],
        )

        profile = get_athlete_profile(conn) or {}
        effective_lthr = (
            int(lthr) if lthr and int(lthr) > 0 else get_effective_lthr(conn)
        )
        effective_ftp = (
            int(profile.get("ftp_override") or profile.get("ftp_calc"))
            if (profile.get("ftp_override") or profile.get("ftp_calc"))
            else get_effective_ftp(conn)
        )
        resting_hr = int(profile.get("resting_hr") or 60)

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

        placeholders = ",".join("?" for _ in candidate_ids)
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
                    FROM activity_trackpoint tp
                    JOIN activity_trackpoint tp_next
                      ON tp_next.activity_id = tp.activity_id
                     AND tp_next.seq = tp.seq + 1
                    WHERE tp.heart_rate_bpm IS NOT NULL
                      AND tp.heart_rate_bpm >= 35
                      AND tp.heart_rate_bpm <= 220
                      AND tp.activity_id IN ({placeholders})
                ) x
                WHERE x.dt_s > 0
                  AND x.dt_s <= 30
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
                z = zone_by_activity.get(aid, (0.0, 0.0, 0.0, 0.0, 0.0))
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

        summary["rows_upserted"] = len(candidate_ids)
        set_setting(conn, ACTIVITY_METRICS_LAST_REFRESH_KEY, _utc_now_iso())
        set_setting(conn, ACTIVITY_METRICS_LAST_REFRESH_SUMMARY_KEY, summary)
        conn.commit()
        return summary
    except (sqlite3.Error, TypeError, ValueError):
        summary["errors"] += 1
        logger.exception("Failed to refresh persisted activity metrics")
        try:
            set_setting(conn, ACTIVITY_METRICS_LAST_REFRESH_SUMMARY_KEY, summary)
        except (sqlite3.Error, TypeError, ValueError):
            logger.warning(
                "Failed to persist activity metrics refresh error summary",
                exc_info=True,
            )
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
        rows = conn.execute(
            """
            SELECT a.activity_id
            FROM activity a
            LEFT JOIN activity_metrics am ON a.activity_id = am.activity_id
            WHERE am.activity_id IS NULL
               OR (
                    EXISTS (
                        SELECT 1
                        FROM activity_trackpoint tp
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
            """
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
        row = conn.execute(
            "SELECT zone_1_s, trimp, tss FROM activity_metrics WHERE activity_id = ?",
            (activity_id,),
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
