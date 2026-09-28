"""Canonical threshold calculation, provenance, resolution, and staleness.

The service deliberately owns only athlete thresholds.  Activity-metric
freshness remains the responsibility of the Phase 2B metrics pipeline.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
import math
import sqlite3
from statistics import median
from typing import Any, Mapping

from garmin_data_hub.analytics.temporal_metrics import (
    TemporalSample,
    build_temporal_intervals,
    calculate_power_peaks,
)


RUNNING_SPORTS = frozenset({"running", "trail_running", "indoor_running"})
HRMAX_ALGORITHM_VERSION = "validated_hrmax_v1"
ESTIMATED_LTHR_ALGORITHM_VERSION = "estimated_lthr_from_hrmax_v1"
RUNNING_FTP_ALGORITHM_VERSION = "running_ftp_20min_v1"
RESTING_HR_ALGORITHM_VERSION = "garmin_resting_hr_median7_v1"
LEGACY_UNKNOWN_ALGORITHM_VERSION = "legacy_unknown_v1"
HR_SUPPORT_SECONDS = 5.0
HR_SUPPORT_TOLERANCE_BPM = 2.0
POWER_EVIDENCE_SECONDS = 1200.0
_EPSILON = 1e-9

_CALCULATION_COLUMNS = (
    "threshold_calculation_id",
    "threshold_type",
    "calculated_value",
    "algorithm_version",
    "calculated_at_utc",
    "evidence_cutoff_utc",
    "evidence_at_utc",
    "source_kind",
    "source_activity_id",
    "source_activity_timestamp_utc",
    "source_sport",
    "evidence_value",
    "evidence_duration_s",
    "candidate_count",
    "aggregate_evidence_json",
    "parent_calculation_id",
)

_PROFILE_TARGETS = {
    "hrmax": ("hrmax_calc", "hrmax_calculation_id"),
    "estimated_lthr": ("lthr_calc", "lthr_calculation_id"),
    "running_ftp": ("ftp_calc", "ftp_calculation_id"),
    "resting_hr": ("resting_hr", "resting_hr_calculation_id"),
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def positive_int(value: Any) -> int | None:
    try:
        normalized = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return normalized if normalized > 0 else None


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith(("Z", "z")):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            try:
                parsed = datetime.combine(date.fromisoformat(text), datetime.min.time())
            except ValueError:
                return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def derive_threshold_status(
    *,
    threshold: str,
    calculated_value: Any,
    override_value: Any,
    evidence_at_utc: Any,
    as_of_utc: Any | None = None,
    stale_after_days: float,
) -> dict[str, Any]:
    """Resolve a threshold value and derive source age without persisting it."""
    calculated = positive_int(calculated_value)
    override = positive_int(override_value)
    effective = override if override is not None else calculated
    effective_source = (
        "manual_override"
        if override is not None
        else "calculated"
        if calculated is not None
        else "unavailable"
    )
    evidence_at = parse_datetime(evidence_at_utc)
    as_of = parse_datetime(as_of_utc) or datetime.now(timezone.utc)
    if calculated is None or evidence_at is None:
        age_status = "unknown"
        age_days = None
    else:
        age_seconds = max(0.0, (as_of - evidence_at).total_seconds())
        age_days = age_seconds / 86400.0
        age_status = "stale" if age_days > float(stale_after_days) else "current"
    return {
        "threshold": threshold,
        "calculated_value": calculated,
        "override_value": override,
        "effective_value": effective,
        "effective_source": effective_source,
        "evidence_at_utc": evidence_at_utc,
        "source_age_days": age_days,
        "source_age_status": age_status,
        "stale_after_days": float(stale_after_days),
    }


def calculate_validated_hrmax(
    conn: sqlite3.Connection,
    *,
    cutoff_utc: str,
    calculated_at_utc: str | None = None,
) -> dict[str, Any] | None:
    """Return the highest trackpoint-corroborated recent HRmax candidate."""
    candidates = conn.execute(
        """
        SELECT activity_id, start_time_gmt, activity_type, max_hr
        FROM activity
        WHERE start_time_gmt >= ?
          AND datetime(start_time_gmt) >= datetime(?)
          AND max_hr BETWEEN 100 AND 220
        ORDER BY max_hr DESC, datetime(start_time_gmt) DESC, activity_id DESC
        """,
        (str(cutoff_utc)[:10], cutoff_utc),
    ).fetchall()
    candidate_count = len(candidates)
    for activity_id, started_at, sport, raw_candidate in candidates:
        try:
            candidate = float(raw_candidate)
        except (TypeError, ValueError, OverflowError):
            continue
        if not math.isfinite(candidate) or not 100.0 <= candidate <= 220.0:
            continue
        rows = conn.execute(
            """
            SELECT timestamp_utc, heart_rate_bpm, seq
            FROM activity_trackpoints
            WHERE activity_id=? AND heart_rate_bpm IS NOT NULL
            ORDER BY timestamp_utc, seq
            """,
            (activity_id,),
        ).fetchall()
        samples = [
            TemporalSample(
                timestamp_utc=row[0],
                heart_rate_bpm=row[1],
                seq=row[2],
            )
            for row in rows
        ]
        supported_seconds = sum(
            interval.duration_s
            for interval in build_temporal_intervals(samples)
            if interval.heart_rate_bpm is not None
            and abs(interval.heart_rate_bpm - candidate)
            <= HR_SUPPORT_TOLERANCE_BPM + _EPSILON
        )
        if supported_seconds + _EPSILON < HR_SUPPORT_SECONDS:
            continue
        value = int(round(candidate))
        return {
            "threshold_type": "hrmax",
            "calculated_value": value,
            "algorithm_version": HRMAX_ALGORITHM_VERSION,
            "calculated_at_utc": calculated_at_utc or utc_now_iso(),
            "evidence_cutoff_utc": cutoff_utc,
            "evidence_at_utc": started_at,
            "source_kind": "validated_hrmax",
            "source_activity_id": int(activity_id),
            "source_activity_timestamp_utc": started_at,
            "source_sport": sport,
            "evidence_value": candidate,
            "evidence_duration_s": supported_seconds,
            "candidate_count": candidate_count,
        }
    return None


def calculate_running_ftp(
    conn: sqlite3.Connection,
    *,
    cutoff_utc: str,
    activity_metric_provenance_version: int,
    calculated_at_utc: str | None = None,
) -> dict[str, Any] | None:
    """Return the strongest canonical 1200-second running-power result."""
    candidates = conn.execute(
        """
        SELECT a.activity_id, a.start_time_gmt, a.activity_type,
               am.peak_power_1200s_w, am.refresh_provenance_version
        FROM activity AS a
        LEFT JOIN activity_metrics AS am ON am.activity_id=a.activity_id
        WHERE a.start_time_gmt >= ?
          AND datetime(a.start_time_gmt) >= datetime(?)
          AND LOWER(TRIM(COALESCE(a.activity_type, ''))) IN
              ('running', 'trail_running', 'indoor_running')
          AND (
              (am.refresh_provenance_version=? AND am.peak_power_1200s_w IS NOT NULL)
              OR EXISTS (
                  SELECT 1 FROM activity_trackpoints AS tp
                  WHERE tp.activity_id=a.activity_id AND tp.power_w IS NOT NULL
              )
          )
        ORDER BY datetime(a.start_time_gmt) DESC, a.activity_id DESC
        """,
        (
            str(cutoff_utc)[:10],
            cutoff_utc,
            int(activity_metric_provenance_version),
        ),
    ).fetchall()
    best: dict[str, Any] | None = None
    for activity_id, started_at, sport, persisted_peak, provenance_version in candidates:
        peak: float | None = None
        if (
            provenance_version == int(activity_metric_provenance_version)
            and persisted_peak is not None
        ):
            try:
                candidate_peak = float(persisted_peak)
                if math.isfinite(candidate_peak):
                    peak = candidate_peak
            except (TypeError, ValueError, OverflowError):
                peak = None
        if peak is None:
            rows = conn.execute(
                """
                SELECT timestamp_utc, power_w, seq
                FROM activity_trackpoints
                WHERE activity_id=?
                ORDER BY timestamp_utc, seq
                """,
                (activity_id,),
            ).fetchall()
            intervals = build_temporal_intervals(
                TemporalSample(timestamp_utc=row[0], power_w=row[1], seq=row[2])
                for row in rows
            )
            peak = calculate_power_peaks(intervals, (1200,))[1200]
        if peak is None or peak <= 0.0:
            continue
        ftp = int(round(peak * 0.95))
        if ftp <= 0:
            continue
        result = {
            "threshold_type": "running_ftp",
            "calculated_value": ftp,
            "algorithm_version": RUNNING_FTP_ALGORITHM_VERSION,
            "calculated_at_utc": calculated_at_utc or utc_now_iso(),
            "evidence_cutoff_utc": cutoff_utc,
            "evidence_at_utc": started_at,
            "source_kind": "running_ftp",
            "source_activity_id": int(activity_id),
            "source_activity_timestamp_utc": started_at,
            "source_sport": str(sport).lower() if sport is not None else None,
            "evidence_value": float(peak),
            "evidence_duration_s": POWER_EVIDENCE_SECONDS,
        }
        if best is None or float(result["evidence_value"]) > float(
            best["evidence_value"]
        ):
            best = result
    return best


def calculate_resting_hr(
    conn: sqlite3.Connection,
    *,
    calculated_at_utc: str | None = None,
) -> dict[str, Any] | None:
    """Return median Garmin RHR from the latest seven available dates."""
    by_date: dict[str, int] = {}
    for table_name in ("daily_summary", "sleep"):
        try:
            rows = conn.execute(
                f"""
                SELECT calendar_date, resting_heart_rate
                FROM {table_name}
                WHERE resting_heart_rate IS NOT NULL
                """
            ).fetchall()
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc).lower():
                continue
            raise
        for raw_day, raw_value in rows:
            try:
                day = date.fromisoformat(str(raw_day)[:10]).isoformat()
                value = int(raw_value)
            except (TypeError, ValueError, OverflowError):
                continue
            if not 1 <= value <= 220:
                continue
            if table_name == "daily_summary" or day not in by_date:
                by_date[day] = value
    observations = [
        {"date": day, "value_bpm": by_date[day]}
        for day in sorted(by_date, reverse=True)[:7]
    ]
    if not observations:
        return None
    value = int(round(median(item["value_bpm"] for item in observations)))
    latest_day = observations[0]["date"]
    return {
        "threshold_type": "resting_hr",
        "calculated_value": value,
        "algorithm_version": RESTING_HR_ALGORITHM_VERSION,
        "calculated_at_utc": calculated_at_utc or utc_now_iso(),
        "evidence_at_utc": latest_day,
        "source_kind": "garmin_daily_aggregate",
        "aggregate_evidence_json": json.dumps(
            observations, separators=(",", ":"), sort_keys=True
        ),
        "candidate_count": len(observations),
    }


def make_estimated_lthr(
    hrmax_calculation: Mapping[str, Any],
    *,
    parent_calculation_id: int,
) -> dict[str, Any]:
    hrmax = int(hrmax_calculation["calculated_value"])
    return {
        "threshold_type": "estimated_lthr",
        "calculated_value": int(round(hrmax * 0.86)),
        "algorithm_version": ESTIMATED_LTHR_ALGORITHM_VERSION,
        "calculated_at_utc": hrmax_calculation.get("calculated_at_utc"),
        "evidence_cutoff_utc": hrmax_calculation.get("evidence_cutoff_utc"),
        "evidence_at_utc": hrmax_calculation.get("evidence_at_utc"),
        "source_kind": "estimated_from_validated_hrmax",
        "evidence_value": hrmax,
        "parent_calculation_id": int(parent_calculation_id),
    }


def insert_calculation(
    conn: sqlite3.Connection, calculation: Mapping[str, Any]
) -> int:
    columns = _CALCULATION_COLUMNS[1:]
    cursor = conn.execute(
        f"""
        INSERT INTO threshold_calculation({', '.join(columns)})
        VALUES ({', '.join('?' for _ in columns)})
        """,
        tuple(calculation.get(column) for column in columns),
    )
    return int(cursor.lastrowid)


def point_profile_at_calculation(
    conn: sqlite3.Connection,
    *,
    threshold_type: str,
    value: int,
    calculation_id: int,
) -> None:
    value_column, reference_column = _PROFILE_TARGETS[threshold_type]
    conn.execute(
        f"""
        UPDATE athlete_profile
        SET {value_column}=?, {reference_column}=?
        WHERE profile_id=1
        """,
        (int(value), int(calculation_id)),
    )


def persist_hrmax_and_estimated_lthr(
    conn: sqlite3.Connection,
    hrmax_calculation: Mapping[str, Any],
    *,
    commit: bool = True,
) -> tuple[int, int]:
    """Atomically persist validated HRmax and its derived Estimated LTHR."""
    caller_owns_transaction = bool(conn.in_transaction)
    savepoint_name = f"persist_hrmax_lthr_{id(hrmax_calculation):x}"
    conn.execute(f"SAVEPOINT {savepoint_name}")
    try:
        hrmax_id = insert_calculation(conn, hrmax_calculation)
        point_profile_at_calculation(
            conn,
            threshold_type="hrmax",
            value=int(hrmax_calculation["calculated_value"]),
            calculation_id=hrmax_id,
        )
        lthr = make_estimated_lthr(
            hrmax_calculation, parent_calculation_id=hrmax_id
        )
        lthr_id = insert_calculation(conn, lthr)
        point_profile_at_calculation(
            conn,
            threshold_type="estimated_lthr",
            value=int(lthr["calculated_value"]),
            calculation_id=lthr_id,
        )
        conn.execute(
            "UPDATE athlete_profile SET calc_updated_utc=? WHERE profile_id=1",
            (hrmax_calculation.get("calculated_at_utc") or utc_now_iso(),),
        )
        conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        if commit and not caller_owns_transaction:
            conn.commit()
        return hrmax_id, lthr_id
    except Exception:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
        finally:
            conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        if not caller_owns_transaction and conn.in_transaction:
            conn.rollback()
        raise


def get_current_provenance(
    conn: sqlite3.Connection, threshold_type: str
) -> dict[str, Any] | None:
    _value_column, reference_column = _PROFILE_TARGETS[threshold_type]
    row = conn.execute(
        f"""
        SELECT {', '.join('tc.' + column for column in _CALCULATION_COLUMNS)}
        FROM athlete_profile AS ap
        JOIN threshold_calculation AS tc
          ON tc.threshold_calculation_id=ap.{reference_column}
        WHERE ap.profile_id=1
        """
    ).fetchone()
    if row is None:
        return None
    raw = dict(zip(_CALCULATION_COLUMNS, row))
    return provenance_for_presentation(raw)


def provenance_for_presentation(raw: Mapping[str, Any]) -> dict[str, Any]:
    threshold_type = raw.get("threshold_type")
    result = {
        "threshold_calculation_id": raw.get("threshold_calculation_id"),
        "threshold_type": threshold_type,
        "calculated_value": raw.get("calculated_value"),
        "algorithm_version": raw.get("algorithm_version"),
        "calculated_at_utc": raw.get("calculated_at_utc"),
        "evidence_cutoff_utc": raw.get("evidence_cutoff_utc"),
        "evidence_at_utc": raw.get("evidence_at_utc"),
        "source_kind": raw.get("source_kind"),
        "source_activity_id": raw.get("source_activity_id"),
        "source_activity_timestamp_utc": raw.get(
            "source_activity_timestamp_utc"
        ),
        "source_sport": raw.get("source_sport"),
        "evidence_duration_s": raw.get("evidence_duration_s"),
        "candidate_count": raw.get("candidate_count"),
        "parent_calculation_id": raw.get("parent_calculation_id"),
    }
    if threshold_type == "hrmax":
        result["evidence_hr_bpm"] = raw.get("evidence_value")
    elif threshold_type == "estimated_lthr":
        result["source_hrmax_bpm"] = raw.get("evidence_value")
        result["source_hrmax_provenance_id"] = raw.get("parent_calculation_id")
    elif threshold_type == "running_ftp":
        result["evidence_peak_power_w"] = raw.get("evidence_value")
    elif threshold_type == "resting_hr":
        aggregate = raw.get("aggregate_evidence_json")
        try:
            observations = json.loads(aggregate) if aggregate else []
        except (TypeError, json.JSONDecodeError):
            observations = []
        result.update(
            {
                "latest_evidence_date": (
                    str(raw.get("evidence_at_utc"))[:10]
                    if raw.get("evidence_at_utc")
                    else None
                ),
                "observations": observations,
                "observation_count": len(observations),
                "same_day_preference": (
                    "daily_summary"
                    if raw.get("source_kind") == "garmin_daily_aggregate"
                    else None
                ),
            }
        )
    return result


def unknown_provenance(
    threshold_type: str,
    value: int,
    *,
    calculated_at_utc: str | None,
) -> dict[str, Any]:
    """Describe an orphaned calculated value without inventing evidence."""
    raw: dict[str, Any] = {
        "threshold_calculation_id": None,
        "threshold_type": threshold_type,
        "calculated_value": value,
        "algorithm_version": LEGACY_UNKNOWN_ALGORITHM_VERSION,
        "calculated_at_utc": calculated_at_utc,
        "evidence_cutoff_utc": None,
        "evidence_at_utc": None,
        "source_kind": "unknown",
        "source_activity_id": None,
        "source_activity_timestamp_utc": None,
        "source_sport": None,
        "evidence_value": None,
        "evidence_duration_s": None,
        "candidate_count": None,
        "aggregate_evidence_json": None,
        "parent_calculation_id": None,
    }
    return provenance_for_presentation(raw)


def refresh_thresholds(
    conn: sqlite3.Connection,
    *,
    activity_metric_provenance_version: int,
    as_of_utc: datetime | None = None,
    commit: bool = True,
) -> dict[str, int | None]:
    """Calculate and atomically persist all available current threshold evidence."""
    as_of = as_of_utc or datetime.now(timezone.utc)
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)
    as_of = as_of.astimezone(timezone.utc)
    calculated_at = as_of.strftime("%Y-%m-%dT%H:%M:%SZ")
    hr_cutoff = (as_of - timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ")
    ftp_cutoff = (as_of - timedelta(days=180)).strftime("%Y-%m-%dT%H:%M:%SZ")

    hrmax = calculate_validated_hrmax(
        conn, cutoff_utc=hr_cutoff, calculated_at_utc=calculated_at
    )
    running_ftp = calculate_running_ftp(
        conn,
        cutoff_utc=ftp_cutoff,
        calculated_at_utc=calculated_at,
        activity_metric_provenance_version=activity_metric_provenance_version,
    )
    resting_hr = calculate_resting_hr(conn, calculated_at_utc=calculated_at)

    caller_owns_transaction = bool(conn.in_transaction)
    started_transaction = False
    if not caller_owns_transaction:
        conn.execute("BEGIN")
        started_transaction = True
    savepoint_name = f"threshold_refresh_{id(conn):x}"
    conn.execute(f"SAVEPOINT {savepoint_name}")
    savepoint_active = True
    try:
        changed = False
        if hrmax is not None and not _matches_current_evidence(conn, hrmax):
            persist_hrmax_and_estimated_lthr(
                conn, hrmax, commit=False
            )
            changed = True
        if running_ftp is not None and not _matches_current_evidence(
            conn, running_ftp
        ):
            ftp_id = insert_calculation(conn, running_ftp)
            point_profile_at_calculation(
                conn,
                threshold_type="running_ftp",
                value=int(running_ftp["calculated_value"]),
                calculation_id=ftp_id,
            )
            changed = True
        if resting_hr is not None and _resting_result_is_new(conn, resting_hr):
            resting_id = insert_calculation(conn, resting_hr)
            point_profile_at_calculation(
                conn,
                threshold_type="resting_hr",
                value=int(resting_hr["calculated_value"]),
                calculation_id=resting_id,
            )
            changed = True
        if changed:
            conn.execute(
                "UPDATE athlete_profile SET calc_updated_utc=? WHERE profile_id=1",
                (calculated_at,),
            )
        conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        savepoint_active = False
        if commit and started_transaction:
            conn.commit()
    except Exception:
        if savepoint_active:
            try:
                conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            finally:
                conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        if started_transaction and conn.in_transaction:
            conn.rollback()
        raise

    row = conn.execute(
        "SELECT hrmax_calc, lthr_calc, ftp_calc, resting_hr FROM athlete_profile WHERE profile_id=1"
    ).fetchone()
    return {
        "hrmax": positive_int(row[0]) if row else None,
        "estimated_lthr": positive_int(row[1]) if row else None,
        "running_ftp": positive_int(row[2]) if row else None,
        "resting_hr": positive_int(row[3]) if row else None,
    }


def _matches_current_evidence(
    conn: sqlite3.Connection, calculation: Mapping[str, Any]
) -> bool:
    current = get_current_provenance(conn, str(calculation["threshold_type"]))
    if current is None:
        return False
    evidence_key = (
        "evidence_hr_bpm"
        if calculation["threshold_type"] == "hrmax"
        else "evidence_peak_power_w"
    )
    return (
        current.get("source_kind") == calculation.get("source_kind")
        and current.get("source_activity_id")
        == calculation.get("source_activity_id")
        and current.get("source_activity_timestamp_utc")
        == calculation.get("source_activity_timestamp_utc")
        and _numbers_equal(current.get(evidence_key), calculation.get("evidence_value"))
        and positive_int(current.get("calculated_value"))
        == positive_int(calculation.get("calculated_value"))
    )


def _resting_result_is_new(
    conn: sqlite3.Connection, calculation: Mapping[str, Any]
) -> bool:
    current = get_current_provenance(conn, "resting_hr")
    if current is None:
        return True
    current_day = current.get("latest_evidence_date")
    incoming_day = str(calculation.get("evidence_at_utc") or "")[:10] or None
    if current_day and incoming_day and incoming_day < current_day:
        return False
    try:
        incoming_observations = json.loads(
            str(calculation.get("aggregate_evidence_json") or "[]")
        )
    except json.JSONDecodeError:
        incoming_observations = []
    return not (
        current_day == incoming_day
        and current.get("observations") == incoming_observations
        and positive_int(current.get("calculated_value"))
        == positive_int(calculation.get("calculated_value"))
    )


def _numbers_equal(left: Any, right: Any) -> bool:
    try:
        return abs(float(left) - float(right)) <= _EPSILON
    except (TypeError, ValueError, OverflowError):
        return left is None and right is None
