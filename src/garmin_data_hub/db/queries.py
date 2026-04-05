import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Iterable
import pandas as pd

GET_SETTING_SQL = "SELECT value FROM app_settings WHERE key = ?"
UPSERT_SETTING_SQL = "INSERT OR REPLACE INTO app_settings (key, value) VALUES (?, ?)"
ACTIVITY_METRICS_LAST_REFRESH_KEY = "activity_metrics_last_refresh_utc"
ACTIVITY_METRICS_LAST_REFRESH_SUMMARY_KEY = "activity_metrics_last_refresh_summary"


def get_setting(conn, key: str, default: Any):
    """Return a JSON-deserialized setting value or default."""
    try:
        row = conn.execute(GET_SETTING_SQL, (key,)).fetchone()
        return json.loads(row[0]) if row and row[0] is not None else default
    except Exception:
        return default


def set_setting(conn, key: str, value: Any) -> None:
    """Upsert a JSON-serialised setting value."""
    try:
        conn.execute(UPSERT_SETTING_SQL, (key, json.dumps(value)))
        conn.commit()
    except Exception:
        # swallow DB errors to preserve existing behaviour
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

    Tries to include FTP columns if present; falls back gracefully if schema differs.
    """
    try:
        # Prefer a wide selection if FTP columns exist
        try:
            row = conn.execute(
                "SELECT hrmax_calc, lthr_calc, hrmax_override, lthr_override, ftp_calc, ftp_override, calc_updated_utc, override_updated_utc FROM athlete_profile WHERE profile_id = 1"
            ).fetchone()
            if row:
                return {
                    "hrmax_calc": row[0],
                    "lthr_calc": row[1],
                    "hrmax_override": row[2],
                    "lthr_override": row[3],
                    "ftp_calc": row[4],
                    "ftp_override": row[5],
                    "calc_updated_utc": row[6],
                    "override_updated_utc": row[7],
                }
        except Exception:
            # Fallback to minimal selection
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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_athlete_profile_table(conn) -> None:
    """Create `athlete_profile` if it does not exist and ensure a row exists."""
    try:
        conn.execute(
            """
        CREATE TABLE IF NOT EXISTS athlete_profile (
            profile_id INTEGER PRIMARY KEY DEFAULT 1,
            hrmax_calc INTEGER,
            lthr_calc INTEGER,
            calc_updated_utc TEXT,
            hrmax_override INTEGER,
            lthr_override INTEGER,
            override_updated_utc TEXT
        );
        """
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
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO activity_metrics(
                activity_id, moving_time_s, stopped_time_s, avg_moving_speed_mps,
                hr_max_est_bpm, lthr_est_bpm, trimp, aerobic_decoupling_pct,
                np_w, if_val, tss, zone_1_s, zone_2_s, zone_3_s, zone_4_s, zone_5_s
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
        row = conn.execute(
            "SELECT hrmax_calc, lthr_calc, hrmax_override, lthr_override, calc_updated_utc, override_updated_utc FROM athlete_profile WHERE profile_id=1"
        ).fetchone()
        if not row:
            return {
                "hrmax_calc": None,
                "lthr_calc": None,
                "hrmax_override": None,
                "lthr_override": None,
                "hrmax_effective": None,
                "lthr_effective": None,
                "calc_updated_at": None,
                "override_updated_at": None,
            }
        (
            hrmax_calc,
            lthr_calc,
            hrmax_override,
            lthr_override,
            calc_updated_at,
            override_updated_at,
        ) = row
        hrmax_effective = hrmax_override if hrmax_override is not None else hrmax_calc
        lthr_effective = lthr_override if lthr_override is not None else lthr_calc
        return {
            "hrmax_calc": hrmax_calc,
            "lthr_calc": lthr_calc,
            "hrmax_override": hrmax_override,
            "lthr_override": lthr_override,
            "hrmax_effective": hrmax_effective,
            "lthr_effective": lthr_effective,
            "calc_updated_at": calc_updated_at,
            "override_updated_at": override_updated_at,
        }
    except Exception:
        return {
            "hrmax_calc": None,
            "lthr_calc": None,
            "hrmax_override": None,
            "lthr_override": None,
            "hrmax_effective": None,
            "lthr_effective": None,
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


def set_override_metrics(conn, hrmax: int | None, lthr: int | None) -> None:
    try:
        conn.execute(
            "UPDATE athlete_profile SET hrmax_override=?, lthr_override=?, override_updated_utc=? WHERE profile_id=1",
            (hrmax, lthr, _utc_now_iso()),
        )
        conn.commit()
    except Exception:
        return


def clear_override_metrics(conn) -> None:
    try:
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
            temp_join_sql = (
                "LEFT JOIN temp_activity_zone_metrics tzm ON tzm.activity_id = a.activity_id"
            )
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
        elif start_ts_iso:
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

        # Keep scalar fields in sync from activity table.
        placeholders = ",".join("?" for _ in candidate_ids)
        conn.execute(
            f"""
            UPDATE activity_metrics
            SET
                moving_time_s = COALESCE(
                    (SELECT a.elapsed_duration_seconds FROM activity a WHERE a.activity_id = activity_metrics.activity_id),
                    moving_time_s
                ),
                avg_moving_speed_mps = COALESCE(
                    (SELECT a.average_speed FROM activity a WHERE a.activity_id = activity_metrics.activity_id),
                    avg_moving_speed_mps
                ),
                hr_max_est_bpm = COALESCE(
                    (SELECT a.max_hr FROM activity a WHERE a.activity_id = activity_metrics.activity_id),
                    hr_max_est_bpm
                ),
                tss = COALESCE(
                    (SELECT a.training_stress_score FROM activity a WHERE a.activity_id = activity_metrics.activity_id),
                    tss
                )
            WHERE activity_id IN ({placeholders})
            """,
            tuple(candidate_ids),
        )

        effective_lthr = int(lthr) if lthr and int(lthr) > 0 else get_effective_lthr(conn)
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
    except Exception:
        summary["errors"] += 1
        try:
            set_setting(conn, ACTIVITY_METRICS_LAST_REFRESH_SUMMARY_KEY, summary)
        except Exception:
            pass
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
            "total_activities": int(total_activities_row[0] or 0)
            if total_activities_row
            else 0,
            "total_metrics_rows": int(total_metrics_row[0] or 0)
            if total_metrics_row
            else 0,
            "missing_metrics_count": int(len(missing_rows)),
            "last_refresh_utc": get_setting(conn, ACTIVITY_METRICS_LAST_REFRESH_KEY, None),
            "last_refresh_summary": get_setting(
                conn, ACTIVITY_METRICS_LAST_REFRESH_SUMMARY_KEY, {}
            ),
        }
    except Exception:
        return {
            "total_activities": 0,
            "total_metrics_rows": 0,
            "missing_metrics_count": 0,
            "last_refresh_utc": None,
            "last_refresh_summary": {},
        }


# FIT-pipeline bulk-insert functions removed — replaced by garmin-givemydata


def list_activities_needing_metrics(conn) -> list[int]:
    """Return activity_ids that appear to be missing activity_metrics or key fields."""
    try:
        rows = conn.execute(
            """
            SELECT a.activity_id 
            FROM activity a
            LEFT JOIN activity_metrics am ON a.activity_id = am.activity_id
            WHERE am.activity_id IS NULL 
               OR am.zone_1_s IS NULL 
               OR am.trimp IS NULL
               OR am.tss IS NULL
               OR (am.zone_1_s = 0 AND am.zone_2_s = 0 AND am.zone_3_s = 0 AND am.zone_4_s = 0 AND am.zone_5_s = 0)
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
