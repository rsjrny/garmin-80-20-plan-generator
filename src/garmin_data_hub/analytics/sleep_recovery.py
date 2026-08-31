"""Read-only sleep and recovery trend analysis for Garmin health data."""

from __future__ import annotations

import math
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from statistics import fmean, pstdev
from typing import Any, Iterable


MAX_ANALYSIS_DAYS = 730

_TABLE_FIELDS = {
    "sleep": (
        "sleep_time_seconds",
        "sleep_need_minutes",
        "deep_sleep_seconds",
        "light_sleep_seconds",
        "rem_sleep_seconds",
        "awake_sleep_seconds",
        "sleep_score_overall",
        "average_hr_sleep",
        "resting_heart_rate",
        "avg_sleep_stress",
        "body_battery_change",
        "average_spo2",
        "lowest_spo2",
        "average_respiration",
        "avg_skin_temp_deviation_c",
        "sleep_score_feedback",
        "sleep_score_insight",
    ),
    "daily_summary": (
        "resting_heart_rate",
        "average_stress_level",
        "body_battery_at_wake",
        "body_battery_highest",
        "body_battery_lowest",
        "body_battery_charged",
        "average_spo2",
        "lowest_spo2",
        "avg_waking_respiration",
    ),
    "hrv": (
        "last_night_avg",
        "last_night",
        "weekly_avg",
        "status",
        "feedback_phrase",
        "baseline_low",
        "baseline_upper",
    ),
    "training_readiness": (
        "score",
        "level",
        "feedback_short",
        "recovery_time",
        "hrv_factor_feedback",
        "sleep_history_factor_feedback",
        "stress_history_factor_feedback",
    ),
    "body_battery": (
        "at_wake",
        "charged",
        "highest",
        "lowest",
        "during_sleep",
    ),
}


def _parse_date(value: object, label: str) -> date:
    try:
        return date.fromisoformat(str(value or "").strip())
    except ValueError as exc:
        raise ValueError(f"{label} must be a valid date") from exc


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if not exists:
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _fetch_daily_table(
    conn: sqlite3.Connection,
    table: str,
    fields: tuple[str, ...],
    start_iso: str,
    end_iso: str,
) -> tuple[dict[str, dict[str, Any]], bool]:
    columns = _table_columns(conn, table)
    if "calendar_date" not in columns:
        return {}, False
    projections = [
        field if field in columns else f"NULL AS {field}"
        for field in fields
    ]
    cursor = conn.execute(
        f"""
        SELECT calendar_date, {', '.join(projections)}
        FROM {table}
        WHERE calendar_date BETWEEN ? AND ?
        ORDER BY calendar_date
        """,
        (start_iso, end_iso),
    )
    rows = {
        str(row["calendar_date"]): dict(row)
        for row in cursor.fetchall()
        if row["calendar_date"]
    }
    return rows, True


def _fetch_activity_load(
    conn: sqlite3.Connection,
    start_iso: str,
    end_iso: str,
) -> tuple[dict[str, dict[str, Any]], bool]:
    columns = _table_columns(conn, "activity")
    date_column = next(
        (name for name in ("start_time_local", "start_time_gmt") if name in columns),
        None,
    )
    if date_column is None:
        return {}, False
    load_parts = [
        name
        for name in ("training_load", "training_stress_score")
        if name in columns
    ]
    duration_column = next(
        (
            name
            for name in ("duration_seconds", "elapsed_duration_seconds")
            if name in columns
        ),
        None,
    )
    load_expression = (
        f"COALESCE({', '.join(load_parts)}, 0)" if load_parts else "0"
    )
    duration_expression = duration_column or "0"
    cursor = conn.execute(
        f"""
        SELECT DATE({date_column}) AS calendar_date,
               COUNT(*) AS activity_sessions,
               ROUND(SUM({load_expression}), 1) AS activity_load,
               ROUND(SUM(COALESCE({duration_expression}, 0)) / 3600.0, 2)
                   AS activity_hours
        FROM activity
        WHERE DATE({date_column}) BETWEEN ? AND ?
        GROUP BY DATE({date_column})
        ORDER BY DATE({date_column})
        """,
        (start_iso, end_iso),
    )
    return {
        str(row["calendar_date"]): dict(row)
        for row in cursor.fetchall()
        if row["calendar_date"]
    }, True


def _number(*values: object) -> float | None:
    for value in values:
        if value is None:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            return number
    return None


def _scaled(value: object, divisor: float, digits: int = 1) -> float | None:
    number = _number(value)
    return round(number / divisor, digits) if number is not None else None


def _rounded(*values: object, digits: int = 1) -> float | None:
    number = _number(*values)
    return round(number, digits) if number is not None else None


def _average(rows: Iterable[dict[str, Any]], key: str) -> float | None:
    values = [value for row in rows if (value := _number(row.get(key))) is not None]
    return round(fmean(values), 2) if values else None


def _period_average(
    rows: list[dict[str, Any]],
    key: str,
    start: date,
    end: date,
) -> tuple[float | None, int]:
    selected = [
        row
        for row in rows
        if start <= date.fromisoformat(str(row["date"])) <= end
        and _number(row.get(key)) is not None
    ]
    return _average(selected, key), len(selected)


def _correlation(pairs: list[tuple[float, float]]) -> float | None:
    if len(pairs) < 5:
        return None
    xs, ys = zip(*pairs)
    x_mean, y_mean = fmean(xs), fmean(ys)
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in pairs)
    x_spread = sum((x - x_mean) ** 2 for x in xs)
    y_spread = sum((y - y_mean) ** 2 for y in ys)
    denominator = math.sqrt(x_spread * y_spread)
    if denominator == 0:
        return None
    return round(numerator / denominator, 2)


def analyze_sleep_recovery(
    db_path: Path,
    start_date: object,
    end_date: object,
) -> dict[str, Any]:
    """Build a personal-baseline sleep and recovery analysis without writes."""

    start = _parse_date(start_date, "Start date")
    end = _parse_date(end_date, "End date")
    if start > end:
        raise ValueError("Start date must be on or before end date")
    requested_days = (end - start).days + 1
    if requested_days > MAX_ANALYSIS_DAYS:
        raise ValueError(
            f"Sleep and recovery analysis is limited to {MAX_ANALYSIS_DAYS} days"
        )

    uri = f"file:{Path(db_path).resolve().as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        sources: dict[str, dict[str, Any]] = {}
        tables: dict[str, dict[str, dict[str, Any]]] = {}
        for table, fields in _TABLE_FIELDS.items():
            table_rows, available = _fetch_daily_table(
                conn,
                table,
                fields,
                start.isoformat(),
                end.isoformat(),
            )
            tables[table] = table_rows
            sources[table] = {
                "source": table.replace("_", " ").title(),
                "available": available,
                "days": len(table_rows),
            }
        activity_rows, activity_available = _fetch_activity_load(
            conn,
            start.isoformat(),
            end.isoformat(),
        )
        sources["activity"] = {
            "source": "Activity load",
            "available": activity_available,
            "days": len(activity_rows),
        }
    finally:
        conn.close()

    all_dates = sorted(
        set(activity_rows).union(*(set(items) for items in tables.values()))
    )
    rows: list[dict[str, Any]] = []
    for iso_date in all_dates:
        sleep = tables["sleep"].get(iso_date, {})
        daily = tables["daily_summary"].get(iso_date, {})
        hrv = tables["hrv"].get(iso_date, {})
        readiness = tables["training_readiness"].get(iso_date, {})
        battery = tables["body_battery"].get(iso_date, {})
        activity = activity_rows.get(iso_date, {})
        sleep_seconds = _number(sleep.get("sleep_time_seconds"))
        deep_seconds = _number(sleep.get("deep_sleep_seconds"))
        rem_seconds = _number(sleep.get("rem_sleep_seconds"))
        rows.append(
            {
                "date": iso_date,
                "sleep_hours": _scaled(sleep_seconds, 3600, 2),
                "sleep_need_hours": _scaled(sleep.get("sleep_need_minutes"), 60, 2),
                "sleep_score": _rounded(
                    sleep.get("sleep_score_overall"), digits=0
                ),
                "deep_min": _scaled(deep_seconds, 60, 0),
                "light_min": _scaled(sleep.get("light_sleep_seconds"), 60, 0),
                "rem_min": _scaled(rem_seconds, 60, 0),
                "awake_min": _scaled(sleep.get("awake_sleep_seconds"), 60, 0),
                "deep_pct": (
                    round(deep_seconds * 100 / sleep_seconds, 1)
                    if deep_seconds is not None and sleep_seconds
                    else None
                ),
                "rem_pct": (
                    round(rem_seconds * 100 / sleep_seconds, 1)
                    if rem_seconds is not None and sleep_seconds
                    else None
                ),
                "sleeping_hr": _rounded(
                    sleep.get("average_hr_sleep"), digits=1
                ),
                "resting_hr": _rounded(
                    sleep.get("resting_heart_rate"),
                    daily.get("resting_heart_rate"),
                    digits=1,
                ),
                "sleep_stress": _rounded(
                    sleep.get("avg_sleep_stress"), digits=1
                ),
                "daily_stress": _rounded(
                    daily.get("average_stress_level"), digits=1
                ),
                "body_battery_wake": _rounded(
                    daily.get("body_battery_at_wake"),
                    battery.get("at_wake"),
                    digits=0,
                ),
                "body_battery_change": _rounded(
                    sleep.get("body_battery_change"),
                    digits=0,
                ),
                "hrv_nightly": _rounded(
                    hrv.get("last_night_avg"), hrv.get("last_night"),
                ),
                "hrv_weekly": _rounded(hrv.get("weekly_avg"), digits=1),
                "hrv_status": hrv.get("status"),
                "hrv_baseline_low": _rounded(hrv.get("baseline_low"), digits=1),
                "hrv_baseline_upper": _rounded(
                    hrv.get("baseline_upper"), digits=1
                ),
                "readiness_score": _rounded(readiness.get("score"), digits=0),
                "readiness_level": readiness.get("level"),
                "recovery_time_hours": _rounded(
                    readiness.get("recovery_time"), digits=1
                ),
                "average_spo2": _rounded(
                    sleep.get("average_spo2"), daily.get("average_spo2"),
                ),
                "lowest_spo2": _rounded(
                    sleep.get("lowest_spo2"), daily.get("lowest_spo2"),
                ),
                "sleep_respiration": _rounded(
                    sleep.get("average_respiration"), digits=1
                ),
                "skin_temp_delta_c": _rounded(
                    sleep.get("avg_skin_temp_deviation_c"), digits=2
                ),
                "activity_sessions": activity.get("activity_sessions"),
                "activity_load": _rounded(
                    activity.get("activity_load"), digits=1
                ),
                "activity_hours": _rounded(
                    activity.get("activity_hours"), digits=2
                ),
                "sleep_feedback": sleep.get("sleep_score_feedback"),
                "sleep_insight": sleep.get("sleep_score_insight"),
                "hrv_feedback": hrv.get("feedback_phrase"),
                "readiness_feedback": readiness.get("feedback_short"),
            }
        )

    sleep_rows = [row for row in rows if row["sleep_hours"] is not None]
    latest = rows[-1] if rows else {}
    sleep_values = [float(row["sleep_hours"]) for row in sleep_rows]
    summary = {
        "requested_days": requested_days,
        "sleep_nights": len(sleep_rows),
        "sleep_coverage_pct": round(len(sleep_rows) * 100 / requested_days, 1),
        "latest_date": latest.get("date"),
        "average_sleep_hours": _average(rows, "sleep_hours"),
        "average_sleep_score": _average(rows, "sleep_score"),
        "sleep_duration_sd_hours": (
            round(pstdev(sleep_values), 2) if len(sleep_values) >= 2 else None
        ),
        "average_deep_pct": _average(rows, "deep_pct"),
        "average_rem_pct": _average(rows, "rem_pct"),
        "average_readiness": _average(rows, "readiness_score"),
        "average_hrv_nightly": _average(rows, "hrv_nightly"),
        "average_resting_hr": _average(rows, "resting_hr"),
        "average_daily_stress": _average(rows, "daily_stress"),
        "average_body_battery_wake": _average(rows, "body_battery_wake"),
    }

    recent_start = end - timedelta(days=6)
    previous_start = end - timedelta(days=13)
    previous_end = end - timedelta(days=7)
    trend_specs = (
        ("Sleep duration", "sleep_hours", "h"),
        ("Sleep score", "sleep_score", "points"),
        ("Training readiness", "readiness_score", "points"),
        ("Nightly HRV", "hrv_nightly", "ms"),
        ("Resting heart rate", "resting_hr", "bpm"),
        ("Daily stress", "daily_stress", "points"),
        ("Body Battery at wake", "body_battery_wake", "points"),
    )
    trends = []
    for label, key, unit in trend_specs:
        recent, recent_count = _period_average(rows, key, recent_start, end)
        previous, previous_count = _period_average(
            rows, key, previous_start, previous_end
        )
        trends.append(
            {
                "metric": label,
                "recent_7d": recent,
                "previous_7d": previous,
                "change": (
                    round(recent - previous, 2)
                    if recent is not None and previous is not None
                    else None
                ),
                "unit": unit,
                "recent_days": recent_count,
                "previous_days": previous_count,
            }
        )

    pairs_by_date = {row["date"]: row for row in rows}
    relationship_specs = (
        (
            "Sleep duration vs. readiness",
            [
                (float(row["sleep_hours"]), float(row["readiness_score"]))
                for row in rows
                if row["sleep_hours"] is not None
                and row["readiness_score"] is not None
            ],
        ),
        (
            "Nightly HRV vs. readiness",
            [
                (float(row["hrv_nightly"]), float(row["readiness_score"]))
                for row in rows
                if row["hrv_nightly"] is not None
                and row["readiness_score"] is not None
            ],
        ),
        (
            "Prior-day load vs. sleep score",
            [
                (float(previous["activity_load"]), float(row["sleep_score"]))
                for row in rows
                for previous in (
                    pairs_by_date.get(
                        (date.fromisoformat(row["date"]) - timedelta(days=1)).isoformat(),
                        {},
                    ),
                )
                if previous.get("activity_load") is not None
                and row["sleep_score"] is not None
            ],
        ),
    )
    relationships = [
        {"relationship": label, "correlation": correlation, "paired_days": len(pairs)}
        for label, pairs in relationship_specs
        if (correlation := _correlation(pairs)) is not None
    ]

    insights = [
        {
            "title": "Data coverage",
            "text": (
                f"Sleep was available for {len(sleep_rows)} of {requested_days} "
                f"days ({summary['sleep_coverage_pct']}%). Missing nights are "
                "excluded from averages rather than treated as zero."
            ),
        }
    ]
    actual_need_pairs = [
        (float(row["sleep_hours"]), float(row["sleep_need_hours"]))
        for row in rows
        if row["sleep_hours"] is not None and row["sleep_need_hours"] is not None
    ]
    if actual_need_pairs:
        gap = round(fmean(actual - need for actual, need in actual_need_pairs), 2)
        direction = "above" if gap >= 0 else "below"
        insights.append(
            {
                "title": "Sleep need comparison",
                "text": (
                    f"Recorded sleep averaged {abs(gap):.2f} hours {direction} "
                    f"Garmin's sleep-need estimate across {len(actual_need_pairs)} nights."
                ),
            }
        )
    for trend in trends:
        if (
            trend["change"] is not None
            and trend["recent_days"] >= 3
            and trend["previous_days"] >= 3
        ):
            insights.append(
                {
                    "title": f"{trend['metric']} trend",
                    "text": (
                        f"The recent 7-day average was {trend['recent_7d']} "
                        f"{trend['unit']} ({trend['change']:+g} versus the prior "
                        "7-day average)."
                    ),
                }
            )
    if latest.get("hrv_status") or latest.get("hrv_weekly") is not None:
        baseline = ""
        if (
            latest.get("hrv_baseline_low") is not None
            and latest.get("hrv_baseline_upper") is not None
        ):
            baseline = (
                f"; personal baseline {latest['hrv_baseline_low']}-"
                f"{latest['hrv_baseline_upper']} ms"
            )
        insights.append(
            {
                "title": "Latest Garmin HRV context",
                "text": (
                    f"Status: {latest.get('hrv_status') or 'not reported'}; "
                    f"7-day average: {latest.get('hrv_weekly') or 'not reported'} ms"
                    f"{baseline}."
                ),
            }
        )

    return {
        "period": {"start": start.isoformat(), "end": end.isoformat()},
        "summary": summary,
        "rows": rows,
        "trends": trends,
        "relationships": relationships,
        "insights": insights,
        "sources": list(sources.values()),
        "latest": latest,
    }
