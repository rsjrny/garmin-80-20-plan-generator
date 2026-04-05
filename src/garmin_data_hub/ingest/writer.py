from __future__ import annotations
import sqlite3
from pathlib import Path
import math
from datetime import datetime
from .fingerprint import sha256_file, stat_signature

from .parsers import parse_activity_file
from garmin_data_hub.db import queries as db_queries

# Export for import in CLI and other modules
__all__ = [
    "recalculate_missing_metrics",
    "calculate_hr_zones_from_records",
    "calculate_trimp",
    "calculate_aerobic_decoupling",
    "calculate_hr_tss",
    "calculate_power_tss",
    "calculate_activity_metrics",
    "upsert_fit_file",
    "insert_raw_messages",
    "insert_canonical_data",
    "ingest_one_file",
]

PIPELINE_VERSION = "v2"


def _get_row_value(row, key, idx):
    """Safely get value from row supporting both dict-like and tuple access."""
    if row is None:
        return None
    try:
        return row[key]
    except (KeyError, TypeError, IndexError):
        try:
            return row[idx] if idx < len(row) else None
        except (TypeError, IndexError):
            return None


def recalculate_missing_metrics(
    conn: sqlite3.Connection, force_all: bool = False
) -> int:
    """
    Recalculate metrics for activities that are missing HR zone data.
    Called after bulk import when LTHR may now be available.

    Args:
        conn: Database connection
        force_all: If True, recalculate ALL activities, not just those missing data

    Returns: number of activities updated
    """
    print(f"  recalculate_missing_metrics called with force_all={force_all}")

    # First, ensure we have an LTHR (either from profile or calculated)
    lthr = None
    ftp = None

    # Try to get from athlete profile (handle missing columns gracefully)
    try:
        profile = db_queries.get_athlete_profile(conn)
        if profile:
            lthr = profile.get("lthr_override") or profile.get("lthr_calc")
            ftp = profile.get("ftp_override") or profile.get("ftp_calc")
            print(f"  From profile: lthr={lthr}, ftp={ftp}")
    except Exception as e:
        print(f"  Warning: Could not read athlete_profile: {e}")

    # If still no LTHR, estimate from all activities' max HR
    if not lthr:
        print("  No LTHR in profile, estimating from max HR...")
        max_hr = db_queries.get_max_session_max_hr(conn)
        print(f"  Max HR from sessions: {max_hr}")

        if max_hr and max_hr > 100:
            lthr = int(max_hr * 0.86)
            print(f"  Estimated LTHR from max HR ({max_hr}): {lthr} bpm")

            # Ensure athlete_profile row exists and save LTHR via central helper
            try:
                db_queries.ensure_athlete_profile_table(conn)
                # set_calculated_metrics expects (hrmax, lthr)
                db_queries.set_calculated_metrics(conn, max_hr, lthr)
                print(f"  Saved LTHR={lthr} and HRmax={max_hr} to athlete_profile")
            except Exception as e:
                print(f"  Warning: Could not save to athlete_profile: {e}")

    if not lthr:
        print("  No LTHR available and no max HR data - cannot calculate HR zones")
        return 0

    print(f"  Using LTHR: {lthr} bpm" + (f", FTP: {ftp} W" if ftp else ""))

    # Find activities to recalculate (use central helpers)
    if force_all:
        rows = db_queries.list_all_activity_ids(conn)
        print(f"  Force recalculating ALL {len(rows)} activities")
    else:
        rows = db_queries.list_activities_needing_metrics(conn)
        print(f"  Found {len(rows)} activities needing metrics recalculation")

    if len(rows) == 0:
        print("  No activities to process")
        return 0

    updated = 0
    errors = 0
    for row in rows:
        # support both helper-returned int lists and legacy [(id,),...] shapes
        activity_id = row[0] if isinstance(row, (list, tuple)) else row
        if activity_id:
            try:
                calculate_activity_metrics(conn, activity_id, hr_zone_data=None)
                updated += 1
                if updated <= 3:
                    # Verify the first few
                    check = db_queries.get_activity_metrics(conn, activity_id)
                    print(
                        f"    Activity {activity_id}: zone_1={check[0] if check else 'NO ROW'}, trimp={check[1] if check else None}, tss={check[2] if check else None}"
                    )
                if updated % 100 == 0:
                    print(f"    Processed {updated}/{len(rows)} activities...")
                    conn.commit()
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"    Error on activity {activity_id}: {e}")
                    import traceback

                    traceback.print_exc()

    if updated > 0:
        conn.commit()
        print(f"  Successfully updated {updated} activities")

    if errors > 0:
        print(f"  Errors encountered: {errors}")

    return updated


def calculate_hr_zones_from_records(
    conn: sqlite3.Connection, activity_id: int, lthr: int
) -> dict | None:
    """Calculate HR zones from record data in the database."""
    if not lthr:
        return None

    # Get all HR records with timestamps for this activity
    rows = db_queries.get_record_rows_for_activity(conn, activity_id)

    if len(rows) < 2:
        return None

    # Estimate max HR from LTHR (LTHR ≈ 86% of MaxHR)
    max_hr = int(lthr / 0.86)

    # Zone boundaries (% of MaxHR)
    zone_thresholds = [
        (0, max_hr * 0.60),  # Z1: Recovery
        (max_hr * 0.60, max_hr * 0.70),  # Z2: Aerobic
        (max_hr * 0.70, max_hr * 0.80),  # Z3: Tempo
        (max_hr * 0.80, max_hr * 0.90),  # Z4: Threshold
        (max_hr * 0.90, 250),  # Z5: VO2max+
    ]

    zone_times = {f"zone_{i}_s": 0.0 for i in range(1, 6)}

    # HR anomaly bounds
    HR_MIN = 35
    HR_MAX = 220

    # Parse timestamps and calculate durations, filtering out HR anomalies
    for i in range(1, len(rows)):
        try:
            prev_ts_str = _get_row_value(rows[i - 1], "timestamp_utc", 0)
            curr_ts_str = _get_row_value(rows[i], "timestamp_utc", 0)
            hr = _get_row_value(rows[i], "heart_rate_bpm", 1)

            if not prev_ts_str or not curr_ts_str or not hr:
                continue

            # Filter out HR anomalies
            if hr < HR_MIN or hr > HR_MAX:
                continue

            prev_ts = datetime.fromisoformat(prev_ts_str)
            curr_ts = datetime.fromisoformat(curr_ts_str)

            duration = (curr_ts - prev_ts).total_seconds()

            # Skip if duration is unreasonable (> 30 seconds between samples)
            if duration <= 0 or duration > 30:
                continue

            for zone_idx, (lower, upper) in enumerate(zone_thresholds, start=1):
                if lower <= hr < upper:
                    zone_times[f"zone_{zone_idx}_s"] += duration
                    break
        except Exception:
            continue

    # Only return if we calculated some zone time
    total = sum(zone_times.values())
    if total <= 0:
        return None

    return zone_times


def calculate_trimp(
    conn: sqlite3.Connection, activity_id: int, lthr: int, resting_hr: int = 60
) -> float | None:
    """Calculate TRIMP (Training Impulse) using Banister's method."""
    if not lthr:
        return None

    max_hr = int(lthr / 0.86)

    rows = db_queries.get_record_rows_for_activity(conn, activity_id)

    if len(rows) < 2:
        return None

    trimp = 0.0

    HR_MIN = 35
    HR_MAX = 220
    for i in range(1, len(rows)):
        try:
            prev_ts_str = _get_row_value(rows[i - 1], "timestamp_utc", 0)
            curr_ts_str = _get_row_value(rows[i], "timestamp_utc", 0)
            hr = _get_row_value(rows[i], "heart_rate_bpm", 1)

            if not prev_ts_str or not curr_ts_str or not hr:
                continue

            # Filter out HR anomalies
            if hr < HR_MIN or hr > HR_MAX:
                continue

            prev_ts = datetime.fromisoformat(prev_ts_str)
            curr_ts = datetime.fromisoformat(curr_ts_str)

            duration_min = (curr_ts - prev_ts).total_seconds() / 60.0

            if duration_min <= 0 or duration_min > 0.5:
                continue

            if max_hr <= resting_hr:
                continue
            hrr = (hr - resting_hr) / (max_hr - resting_hr)
            hrr = max(0, min(1, hrr))

            trimp += duration_min * hrr * 0.64 * math.exp(1.92 * hrr)
        except Exception:
            continue

    return round(trimp, 1) if trimp > 0 else None


def calculate_aerobic_decoupling(
    conn: sqlite3.Connection, activity_id: int
) -> float | None:
    """Calculate aerobic decoupling."""
    rows = db_queries.get_record_rows_for_activity(conn, activity_id)

    if len(rows) < 20:
        return None

    mid = len(rows) // 2
    first_half = rows[:mid]
    second_half = rows[mid:]

    def calc_efficiency(data):
        hr_sum = 0
        output_sum = 0
        count = 0
        for row in data:
            # helper returns (timestamp_utc, heart_rate_bpm, speed_mps, power_w)
            hr = _get_row_value(row, "heart_rate_bpm", 1)
            speed = _get_row_value(row, "speed_mps", 2)
            power = _get_row_value(row, "power_w", 3)
            if hr and hr > 0:
                output = (
                    power
                    if power and power > 0
                    else (speed if speed and speed > 0 else None)
                )
                if output:
                    hr_sum += hr
                    output_sum += output
                    count += 1
        if count == 0 or hr_sum == 0:
            return None
        return output_sum / hr_sum

    eff1 = calc_efficiency(first_half)
    eff2 = calc_efficiency(second_half)

    if not eff1 or not eff2 or eff1 == 0:
        return None

    decoupling = ((eff1 - eff2) / eff1) * 100
    return round(decoupling, 2)


def calculate_hr_tss(
    conn: sqlite3.Connection, activity_id: int, lthr: int, resting_hr: int = 60
) -> float | None:
    """
    Calculate heart rate-based Training Stress Score (hrTSS).

    hrTSS uses the TRIMP concept scaled to be comparable to power-based TSS.
    Formula: hrTSS = (duration_hr × IF^2) × 100
    Where IF (Intensity Factor) = avg HR / LTHR (simplified)

    More accurate formula uses the integral approach similar to TRIMP.
    """
    if not lthr:
        return None

    max_hr = int(lthr / 0.86)

    # Get session duration
    session_row = db_queries.get_session_first_row(conn, activity_id)

    if not session_row:
        return None

    duration_s = session_row[0] if session_row else None
    avg_hr = session_row[1] if session_row else None

    if not duration_s or not avg_hr or duration_s <= 0:
        return None

    # Method 1: Simple hrTSS based on average HR
    # IF_hr = (avg_hr - resting_hr) / (lthr - resting_hr)
    # hrTSS = (duration_hr) × IF^2 × 100

    if lthr <= resting_hr:
        return None

    # Filter out HR anomalies for avg_hr
    HR_MIN = 35
    HR_MAX = 220
    if avg_hr < HR_MIN or avg_hr > HR_MAX:
        return None

    # Heart rate intensity factor
    hr_if = (avg_hr - resting_hr) / (lthr - resting_hr)
    hr_if = max(0, min(2.0, hr_if))  # Clamp to reasonable range

    duration_hr = duration_s / 3600.0

    # hrTSS formula
    hr_tss = duration_hr * (hr_if**2) * 100

    return round(hr_tss, 1) if hr_tss > 0 else None


def calculate_power_tss(
    normalized_power: float | None, duration_s: float | None, ftp: int | None
) -> float | None:
    """
    Calculate power-based Training Stress Score (TSS).

    TSS = (duration_s × NP × IF) / (FTP × 3600) × 100
    Where IF = NP / FTP

    Simplified: TSS = (duration_s × NP^2) / (FTP^2 × 36)
    """
    if not normalized_power or not duration_s or not ftp or ftp <= 0:
        return None

    if duration_s <= 0 or normalized_power <= 0:
        return None

    # Intensity Factor
    intensity_factor = normalized_power / ftp

    # TSS formula
    tss = (duration_s * normalized_power * intensity_factor) / (ftp * 3600) * 100

    return round(tss, 1) if tss > 0 else None


def calculate_activity_metrics(
    conn: sqlite3.Connection, activity_id: int, hr_zone_data: dict | None = None
):
    """Populates the activity_metrics table with calculated values."""

    row = db_queries.get_session_first_row(conn, activity_id)

    if not row:
        return

    total_timer_s = _get_row_value(row, "total_timer_s", 0)
    total_elapsed_s = _get_row_value(row, "total_elapsed_s", 1)
    avg_speed_mps = _get_row_value(row, "avg_speed_mps", 2)
    normalized_power_w = _get_row_value(row, "normalized_power_w", 3)
    intensity_factor = _get_row_value(row, "intensity_factor", 4)
    training_stress_score = _get_row_value(row, "training_stress_score", 5)
    avg_hr_bpm = _get_row_value(row, "avg_hr_bpm", 6)
    max_hr_bpm = _get_row_value(row, "max_hr_bpm", 7)

    # Get LTHR and FTP from athlete profile (handle missing columns gracefully)
    lthr = None
    ftp = None
    try:
        profile = db_queries.get_athlete_profile(conn)
        if profile:
            lthr = profile.get("lthr_override") or profile.get("lthr_calc")
            ftp = profile.get("ftp_override") or profile.get("ftp_calc")
    except Exception:
        pass

    # If no LTHR in profile, estimate from this activity's max HR
    if not lthr and max_hr_bpm:
        lthr = int(max_hr_bpm * 0.86)

    # Calculate HR zones from records if not provided by parser
    zone_data = hr_zone_data
    if not zone_data and lthr:
        zone_data = calculate_hr_zones_from_records(conn, activity_id, lthr)

    zone_1_s = zone_data.get("zone_1_s") if zone_data else None
    zone_2_s = zone_data.get("zone_2_s") if zone_data else None
    zone_3_s = zone_data.get("zone_3_s") if zone_data else None
    zone_4_s = zone_data.get("zone_4_s") if zone_data else None
    zone_5_s = zone_data.get("zone_5_s") if zone_data else None

    # Calculate TRIMP
    trimp = calculate_trimp(conn, activity_id, lthr) if lthr else None

    # Calculate TSS - prefer device-provided, then power-based, then HR-based
    tss = training_stress_score  # From device/session

    if not tss and normalized_power_w and ftp:
        # Power-based TSS
        tss = calculate_power_tss(normalized_power_w, total_timer_s, ftp)

    if not tss and lthr:
        # Fall back to HR-based TSS
        tss = calculate_hr_tss(conn, activity_id, lthr)

    # Calculate Intensity Factor if not provided
    if_val = intensity_factor
    if not if_val and normalized_power_w and ftp:
        if_val = round(normalized_power_w / ftp, 2)
    elif not if_val and lthr and avg_hr_bpm:
        # HR-based IF approximation
        resting_hr = 60
        if lthr > resting_hr:
            if_val = round((avg_hr_bpm - resting_hr) / (lthr - resting_hr), 2)

    # Calculate aerobic decoupling
    aero_decoupling = calculate_aerobic_decoupling(conn, activity_id)

    # Prefer central helper; fall back to direct SQL if helper unavailable
    try:
        db_queries.upsert_activity_metrics(
            conn,
            activity_id,
            total_timer_s,
            (total_elapsed_s or 0) - (total_timer_s or 0),
            avg_speed_mps,
            max_hr_bpm,
            lthr,
            trimp,
            aero_decoupling,
            normalized_power_w,
            if_val,
            tss,
            zone_1_s,
            zone_2_s,
            zone_3_s,
            zone_4_s,
            zone_5_s,
        )
    except Exception:
        try:
            db_queries.upsert_activity_metrics(
                conn,
                activity_id,
                total_timer_s,
                (total_elapsed_s or 0) - (total_timer_s or 0),
                avg_speed_mps,
                max_hr_bpm,
                lthr,
                trimp,
                aero_decoupling,
                normalized_power_w,
                if_val,
                tss,
                zone_1_s,
                zone_2_s,
                zone_3_s,
                zone_4_s,
                zone_5_s,
            )
        except Exception:
            # If helper fails for any reason, swallow to preserve previous behaviour
            pass


def upsert_fit_file(conn: sqlite3.Connection, path: Path) -> int:
    # Calculate hash
    sha = sha256_file(path)
    size, _ = stat_signature(path)
    # Delegate to central helper which handles lookup/insert by hash
    try:
        fid = db_queries.upsert_fit_file_by_hash(
            conn, str(path), sha, size, datetime.utcnow().isoformat()
        )
        return int(fid) if fid and fid != -1 else 0
    except Exception:
        return 0


def insert_raw_messages(conn: sqlite3.Connection, fit_file_id: int, parsed):
    # Bulk insert messages
    msg_rows = []
    for m in parsed.messages:
        msg_rows.append((fit_file_id, m.msg_name, m.msg_index, m.timestamp_utc))

    if not msg_rows:
        return

    # Insert messages via central helper
    try:
        db_queries.insert_fit_file_messages(conn, msg_rows)
    except Exception:
        # swallow to preserve previous behaviour
        pass

    # Map (msg_name, msg_index) -> message_id via helper
    try:
        msg_map = db_queries.get_fit_file_message_map(conn, fit_file_id)
    except Exception:
        msg_map = {}

    field_rows = []
    for m in parsed.messages:
        mid = msg_map.get((m.msg_name, m.msg_index))
        if not mid:
            continue

        for f in m.fields:
            field_rows.append(
                (
                    mid,
                    f["name"],
                    f["base_type"],
                    f["units"],
                    f["val_int"],
                    f["val_real"],
                    f["val_text"],
                    f["val_blob"],
                    None,  # value_json
                )
            )

    if field_rows:
        try:
            db_queries.insert_fit_file_fields(conn, field_rows)
        except Exception:
            pass


def insert_canonical_data(
    conn: sqlite3.Connection, fit_file_id: int, parsed
) -> int | None:
    # 1. Create Activity
    # We try to find the main session to get start/end times
    main_session = parsed.sessions[0] if parsed.sessions else {}

    # If no session, maybe we can infer from records?
    start_time = main_session.get("start_time")
    if not start_time and parsed.records:
        start_time = parsed.records[0].get("timestamp")

    if not start_time:
        # Can't create activity without start time?
        # Actually we can, but it's weird.
        return None

    start_time_iso = (
        start_time.isoformat() if isinstance(start_time, datetime) else None
    )

    # Try to find end time
    end_time = main_session.get("timestamp")
    if not end_time and parsed.records:
        end_time = parsed.records[-1].get("timestamp")
    end_time_iso = end_time.isoformat() if isinstance(end_time, datetime) else None

    # Insert Activity via helper
    activity_id = db_queries.insert_activity(
        conn,
        fit_file_id,
        start_time_iso,
        end_time_iso,
        str(main_session.get("sport")),
        str(main_session.get("sub_sport")),
        main_session.get("total_timer_time"),
        main_session.get("total_elapsed_time"),
        main_session.get("total_distance"),
        main_session.get("total_calories"),
        main_session.get("total_ascent"),
        main_session.get("total_descent"),
    )
    if not activity_id:
        return 0

    # 2. Insert Sessions
    for i, s in enumerate(parsed.sessions):
        # Calculate avg_speed_mps if missing
        avg_speed = s.get("enhanced_avg_speed", s.get("avg_speed"))
        if avg_speed is None:
            dist = s.get("total_distance")
            time = s.get("total_timer_time")
            if dist and time and time > 0:
                avg_speed = float(dist) / float(time)

        # Insert session via helper and get session_id
        session_id = db_queries.insert_session(
            conn,
            activity_id,
            i,
            (
                s.get("start_time").isoformat()
                if isinstance(s.get("start_time"), datetime)
                else None
            ),
            (
                s.get("timestamp").isoformat()
                if isinstance(s.get("timestamp"), datetime)
                else None
            ),
            str(s.get("sport")),
            str(s.get("sub_sport")),
            s.get("total_timer_time"),
            s.get("total_elapsed_time"),
            s.get("total_distance"),
            s.get("total_calories"),
            s.get("total_ascent"),
            s.get("total_descent"),
            avg_speed,
            s.get("enhanced_max_speed", s.get("max_speed")),
            s.get("avg_heart_rate"),
            s.get("max_heart_rate"),
            s.get("avg_cadence"),
            s.get("max_cadence"),
            s.get("avg_power"),
            s.get("max_power"),
            s.get("normalized_power"),
            s.get("intensity_factor"),
            s.get("training_stress_score"),
        )

        # Validate session was created
        if not session_id or session_id == 0:
            # Session insert failed, skip this session
            continue

        # 3. Insert Laps (we assume laps belong to the main session for now, or we'd need to match timestamps)
        # Simplified: just dump all laps into the first session if we only have one session
        if i == 0:
            lap_rows = []
            for j, l in enumerate(parsed.laps):
                lap_rows.append(
                    (
                        session_id,
                        j,
                        (
                            l.get("start_time").isoformat()
                            if isinstance(l.get("start_time"), datetime)
                            else None
                        ),
                        (
                            l.get("timestamp").isoformat()
                            if isinstance(l.get("timestamp"), datetime)
                            else None
                        ),
                        l.get("total_timer_time"),
                        l.get("total_elapsed_time"),
                        l.get("total_distance"),
                        l.get("total_ascent"),
                        l.get("total_descent"),
                        l.get("enhanced_avg_speed", l.get("avg_speed")),
                        l.get("enhanced_max_speed", l.get("max_speed")),
                        l.get("avg_heart_rate"),
                        l.get("max_heart_rate"),
                        l.get("avg_cadence"),
                        l.get("max_cadence"),
                        l.get("avg_power"),
                        l.get("max_power"),
                    )
                )
            if lap_rows:
                try:
                    db_queries.insert_laps(conn, lap_rows)
                except Exception:
                    pass

            # 4. Insert Records
            # Also simplified: dump all records into first session
            record_rows = []
            for r in parsed.records:
                ts = r.get("timestamp")
                if not ts:
                    continue

                record_rows.append(
                    (
                        session_id,
                        ts.isoformat(),
                        (
                            r.get("position_lat") * (180.0 / 2**31)
                            if r.get("position_lat") is not None
                            else None
                        ),
                        (
                            r.get("position_long") * (180.0 / 2**31)
                            if r.get("position_long") is not None
                            else None
                        ),
                        r.get("enhanced_altitude", r.get("altitude")),
                        r.get("distance"),
                        r.get("enhanced_speed", r.get("speed")),
                        r.get("heart_rate"),
                        r.get("cadence"),
                        r.get("power"),
                        r.get("temperature"),
                        r.get("vertical_oscillation"),
                        r.get("stance_time"),
                        r.get("step_length"),
                        r.get("vertical_ratio"),
                        r.get("stance_time_balance"),
                        r.get("left_right_balance"),
                    )
                )

            if record_rows:
                try:
                    db_queries.insert_records(conn, record_rows)
                    conn.commit()
                except Exception as e:
                    # Log the error but continue
                    print(
                        f"Warning: Failed to insert records for activity {activity_id}: {e}"
                    )
                    pass

    return activity_id


def ingest_one_file(conn: sqlite3.Connection, path: Path) -> tuple[int, bool, int]:
    # returns (activity_id, inserted, fit_file_id)

    # Get athlete's LTHR for zone calculation (may be None on first import)
    lthr = None
    try:
        metrics = db_queries.get_athlete_metrics(conn)
        if metrics:
            lthr = metrics.get("lthr_override") or metrics.get("lthr_calc")
    except Exception:
        pass

    # 1. Parse file with LTHR for HR zone calculation
    parsed = parse_activity_file(path, lthr=lthr)

    # 2. Insert Raw Data
    fit_file_id = upsert_fit_file(conn, path)

    # Check if we already processed this file into an activity?
    try:
        existing = db_queries.get_activity_id_for_fit_file(conn, fit_file_id)
        if existing:
            return int(existing), False, fit_file_id
    except Exception:
        # If helper fails, continue processing (preserve behaviour)
        existing = None

    insert_raw_messages(conn, fit_file_id, parsed)

    # 3. Insert Canonical Data (this inserts records we need for zone calculation)
    activity_id = insert_canonical_data(conn, fit_file_id, parsed)

    if activity_id:
        # 4. Calculate Metrics - zones will be calculated from records if not from parser
        calculate_activity_metrics(
            conn, activity_id, getattr(parsed, "hr_zone_data", None)
        )

        return activity_id, True, fit_file_id

    return 0, False, fit_file_id
