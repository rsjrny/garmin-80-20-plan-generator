"""Canonical calendar-day semantics for Garmin activities."""

from __future__ import annotations

import re
import sqlite3


_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _activity_columns(conn: sqlite3.Connection) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute("PRAGMA table_info(activity)")}
    except sqlite3.Error:
        return set()


def _qualified(column: str, table_alias: str | None) -> str:
    if table_alias is None:
        return column
    if not _SQL_IDENTIFIER.fullmatch(table_alias):
        raise ValueError(f"Invalid SQL table alias: {table_alias!r}")
    return f"{table_alias}.{column}"


def _timestamp_is_usable_sql(timestamp: str, recorded_day: str) -> str:
    """Validate an ISO-like timestamp without choosing its timezone-derived day."""

    return f"""
        length(trim({timestamp})) >= 10
        AND date({recorded_day}, '+0 days') = {recorded_day}
        AND (
            length(trim({timestamp})) = 10
            OR (
                substr(trim({timestamp}), 11, 1) IN ('T', ' ')
                AND datetime(trim({timestamp})) IS NOT NULL
            )
        )
    """


def activity_calendar_day_sql(
    conn: sqlite3.Connection,
    *,
    table_alias: str | None = None,
) -> str:
    """Return SQL for the calendar day on which an activity started.

    Garmin's stored ``start_time_local`` is authoritative when it is a usable
    ISO-like timestamp.  Its recorded date prefix is returned directly so an
    embedded offset is never converted through UTC or the computer's timezone.
    If the local value (or legacy column) is unavailable, a valid
    ``start_time_gmt`` is reduced to its deterministic UTC calendar date.
    Unusable values in both fields produce ``NULL``.

    The expression adapts to legacy/test activity tables that contain only one
    of the two timestamp columns; no schema migration is required.
    """

    columns = _activity_columns(conn)
    candidates: list[str] = []

    if "start_time_local" in columns:
        local_timestamp = _qualified("start_time_local", table_alias)
        recorded_local_day = f"substr(trim({local_timestamp}), 1, 10)"
        candidates.append(
            f"""
            CASE WHEN {_timestamp_is_usable_sql(local_timestamp, recorded_local_day)}
                 THEN {recorded_local_day}
            END
            """
        )

    if "start_time_gmt" in columns:
        gmt_timestamp = _qualified("start_time_gmt", table_alias)
        recorded_gmt_day = f"substr(trim({gmt_timestamp}), 1, 10)"
        candidates.append(
            f"""
            CASE WHEN {_timestamp_is_usable_sql(gmt_timestamp, recorded_gmt_day)}
                 THEN date(trim({gmt_timestamp}))
            END
            """
        )

    if not candidates:
        return "NULL"
    if len(candidates) == 1:
        return f"({candidates[0]})"
    return f"COALESCE({', '.join(candidates)})"
