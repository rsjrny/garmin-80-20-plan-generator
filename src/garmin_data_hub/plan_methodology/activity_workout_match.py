"""Persistence for explicit activity-to-immutable-revision-workout matches."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import sqlite3
from typing import Any


class MatchPersistenceError(RuntimeError):
    """Raised when a match violates identity, FK, or immutability rules."""


@dataclass(frozen=True, slots=True)
class ActivityWorkoutMatch:
    activity_workout_match_id: int
    revision_id: str
    workout_id: str
    activity_id: int
    status: str
    source: str
    confidence: str
    reviewer: str | None
    reason: str | None
    created_at_utc: str
    updated_at_utc: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


_STATUSES = {"CANDIDATE", "CONFIRMED", "REJECTED"}
_SOURCES = {"MANUAL", "RECONCILIATION", "IMPORTED"}
_CONFIDENCES = {"HIGH", "MEDIUM", "LOW", "UNKNOWN"}


def _row_value(row: sqlite3.Row | tuple[Any, ...]) -> ActivityWorkoutMatch:
    return ActivityWorkoutMatch(*tuple(row))


def create_match(
    conn: sqlite3.Connection,
    *,
    revision_id: str,
    workout_id: str,
    activity_id: int,
    status: str,
    source: str,
    confidence: str,
    reviewer: str | None = None,
    reason: str | None = None,
) -> ActivityWorkoutMatch:
    if not revision_id or not workout_id:
        raise MatchPersistenceError("revision_id and workout_id are required")
    if isinstance(activity_id, bool) or not isinstance(activity_id, int):
        raise MatchPersistenceError("activity_id must be an integer")
    if status not in _STATUSES or source not in _SOURCES or confidence not in _CONFIDENCES:
        raise MatchPersistenceError("invalid match status, source, or confidence")
    if status == "CONFIRMED" and (not reviewer or not reason):
        raise MatchPersistenceError("confirmed matches require reviewer and reason")

    from .workout_origin import origin_identity
    from .revision_repository import load_revision
    identity = origin_identity(conn, revision_id, workout_id)
    if identity != (revision_id, workout_id):
        load_revision(None, revision_id, _connection=conn)  # verify immutable lineage
        revision_id, workout_id = identity

    savepoint = "create_activity_workout_match"
    conn.execute(f"SAVEPOINT {savepoint}")
    try:
        cursor = conn.execute(
            """
            INSERT INTO activity_workout_match(
              revision_id, workout_id, activity_id, status, source,
              confidence, reviewer, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                revision_id,
                workout_id,
                activity_id,
                status,
                source,
                confidence,
                reviewer,
                reason,
            ),
        )
        row = conn.execute(
            """
            SELECT activity_workout_match_id, revision_id, workout_id,
                   activity_id, status, source, confidence, reviewer, reason,
                   created_at_utc, updated_at_utc
            FROM activity_workout_match
            WHERE activity_workout_match_id=?
            """,
            (cursor.lastrowid,),
        ).fetchone()
        conn.execute(f"RELEASE SAVEPOINT {savepoint}")
    except (sqlite3.IntegrityError, sqlite3.OperationalError) as exc:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        finally:
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        raise MatchPersistenceError(str(exc)) from exc
    if row is None:
        raise MatchPersistenceError("inserted match could not be reloaded")
    return _row_value(row)


def load_confirmed_match(
    conn: sqlite3.Connection,
    *,
    revision_id: str | None = None,
    workout_id: str | None = None,
    activity_id: int | None = None,
) -> ActivityWorkoutMatch | None:
    if activity_id is None and (revision_id is None or workout_id is None):
        raise MatchPersistenceError(
            "confirmed lookup requires activity_id or both revision_id and workout_id"
        )
    if revision_id is not None and workout_id is not None:
        from .workout_origin import origin_identity
        from .revision_repository import load_revision
        identity = origin_identity(conn, revision_id, workout_id)
        if identity != (revision_id, workout_id):
            load_revision(None, revision_id, _connection=conn)
            revision_id, workout_id = identity
    clauses = ["status='CONFIRMED'"]
    params: list[Any] = []
    if revision_id is not None:
        clauses.append("revision_id=?")
        params.append(revision_id)
    if workout_id is not None:
        clauses.append("workout_id=?")
        params.append(workout_id)
    if activity_id is not None:
        clauses.append("activity_id=?")
        params.append(activity_id)
    rows = conn.execute(
        """
        SELECT activity_workout_match_id, revision_id, workout_id,
               activity_id, status, source, confidence, reviewer, reason,
               created_at_utc, updated_at_utc
        FROM activity_workout_match
        WHERE """
        + " AND ".join(clauses)
        + " ORDER BY activity_workout_match_id",
        tuple(params),
    ).fetchall()
    if len(rows) > 1:
        raise MatchPersistenceError("confirmed match uniqueness is corrupted")
    return None if not rows else _row_value(rows[0])


def review_match(
    conn: sqlite3.Connection,
    *,
    activity_workout_match_id: int,
    status: str,
    reviewer: str,
    reason: str,
) -> ActivityWorkoutMatch:
    """Apply an explicit review decision; confirmed records cannot be changed."""
    if status not in {"CONFIRMED", "REJECTED"}:
        raise MatchPersistenceError("review status must be CONFIRMED or REJECTED")
    if not reviewer or not reason:
        raise MatchPersistenceError("reviewer and reason are required")
    savepoint = "review_activity_workout_match"
    conn.execute(f"SAVEPOINT {savepoint}")
    try:
        cursor = conn.execute(
            """UPDATE activity_workout_match
            SET status=?, reviewer=?, reason=?,
                updated_at_utc=strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE activity_workout_match_id=? AND status!='CONFIRMED'""",
            (status, reviewer, reason, activity_workout_match_id),
        )
        if cursor.rowcount != 1:
            raise MatchPersistenceError("match is missing or already confirmed")
        row = conn.execute(
            """SELECT activity_workout_match_id, revision_id, workout_id,
            activity_id, status, source, confidence, reviewer, reason,
            created_at_utc, updated_at_utc FROM activity_workout_match
            WHERE activity_workout_match_id=?""",
            (activity_workout_match_id,),
        ).fetchone()
        conn.execute(f"RELEASE SAVEPOINT {savepoint}")
    except (sqlite3.IntegrityError, sqlite3.OperationalError, MatchPersistenceError) as exc:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        finally:
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        if isinstance(exc, MatchPersistenceError):
            raise
        raise MatchPersistenceError(str(exc)) from exc
    return _row_value(row)
