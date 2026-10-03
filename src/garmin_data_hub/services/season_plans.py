"""Editable season intent and explicit plan linking; never generates workouts."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import sqlite3
from typing import Iterator
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.plan_methodology.canonical import canonical_json, canonical_value

INPUT_SCHEMA = "season-input.v1"
DISTANCES = {"5K": 5000, "10K": 10000, "10M": 16093, "HM": 21098,
             "20M": 32187, "MAR": 42195, "50K": 50000, "50M": 80467,
             "100K": 100000, "100M": 160934}
RECOVERY_DAYS = {"5K": 2, "10K": 3, "10M": 3, "HM": 7, "20M": 14,
                 "MAR": 14, "50K": 21, "50M": 21, "100K": 28, "100M": 28}
WEEKDAYS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
METHODS = {"eighty_twenty": "FITZGERALD_80_20_RUNNING_V1", "maffetone": "MAFFETONE_RUNNING_V1"}


class SeasonError(ValueError):
    """Invalid or conflicting season intent."""


class StaleSeasonError(SeasonError):
    """An editor is working with an older season input version."""


class SeasonOwnershipError(SeasonError):
    """A legacy writer cannot overwrite a season-owned schedule."""


@dataclass(frozen=True)
class SeasonInputs:
    age: int = 40
    training_method: str = "eighty_twenty"
    run_days_per_week: int = 5
    long_run_day: str = "Saturday"
    available_weekdays: tuple[int, ...] = (0, 1, 2, 3, 4, 5, 6)
    starting_duration_seconds: int | None = None
    hrmax: int | None = None
    lthr: int | None = None


@dataclass(frozen=True)
class SeasonDraft:
    name: str
    start_date: str
    end_date: str
    timezone: str
    inputs: SeasonInputs = SeasonInputs()


@dataclass(frozen=True)
class Season:
    season_id: str
    name: str
    start_date: str
    end_date: str
    timezone: str
    status: str
    inputs: SeasonInputs
    input_version: int
    plan_id: str | None
    current_revision_id: str | None


@dataclass(frozen=True)
class EventDraft:
    name: str
    event_date: str
    distance_code: str = "HM"
    priority: str = "A"
    event_type: str = "road"
    status: str = "planned"
    goal_intent: str = "COMPLETION"
    target_seconds: int | None = None
    target_speed_mps: str | None = None
    terrain: str = ""
    course_notes: str = ""
    taper_days: int | None = None
    recovery_days: int | None = None
    participation_seconds: int | None = None


@dataclass(frozen=True)
class SeasonEvent:
    event_id: str
    season_id: str
    draft: EventDraft
    distance_metres: int


def _stamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _text(value: str, field: str, maximum: int = 200, *, empty: bool = False) -> str:
    if not isinstance(value, str):
        raise SeasonError(f"{field} must be text.")
    value = value.strip()
    if (not value and not empty) or len(value) > maximum:
        raise SeasonError(f"{field} must contain {'0' if empty else '1'}–{maximum} characters.")
    return value


def _day(value: str, field: str) -> date:
    try:
        result = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise SeasonError(f"{field} must be a valid YYYY-MM-DD date.") from exc
    if result.isoformat() != value:
        raise SeasonError(f"{field} must use YYYY-MM-DD.")
    return result


def _integer(value: int, field: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise SeasonError(f"{field} must be an integer from {minimum} to {maximum}.")
    return value


def _validate_draft(draft: SeasonDraft) -> SeasonDraft:
    name = _text(draft.name, "Season name")
    start, end = _day(draft.start_date, "Season start"), _day(draft.end_date, "Season end")
    if not 0 <= (end - start).days <= 365:
        raise SeasonError("A season must cover 1–366 inclusive days.")
    try:
        ZoneInfo(draft.timezone)
    except (ZoneInfoNotFoundError, ValueError, TypeError) as exc:
        raise SeasonError("Choose a valid IANA timezone, such as America/New_York.") from exc
    i = draft.inputs
    if not isinstance(i, SeasonInputs):
        raise SeasonError("Season inputs must use the supported fields.")
    _integer(i.age, "Age", 10, 100)
    if i.training_method not in METHODS:
        raise SeasonError("Choose 80/20 or Maffetone running.")
    _integer(i.run_days_per_week, "Run days", 1, 7)
    if i.long_run_day not in WEEKDAYS:
        raise SeasonError("Choose a long-run weekday.")
    days = tuple(i.available_weekdays)
    for d in days:
        _integer(d, "Available weekday", 0, 6)
    if len(set(days)) != len(days) or len(days) < i.run_days_per_week:
        raise SeasonError("Availability must have distinct days and cover the configured run days.")
    if WEEKDAYS.index(i.long_run_day) not in days:
        raise SeasonError("The long-run day must be available.")
    for value, field, low, high in [(i.starting_duration_seconds, "Starting weekly duration", 60, 144000),
                                     (i.hrmax, "HRmax", 80, 250), (i.lthr, "LTHR", 80, 220)]:
        if value is not None:
            _integer(value, field, low, high)
    if i.hrmax is not None and i.lthr is not None and i.lthr >= i.hrmax:
        raise SeasonError("LTHR must be below HRmax.")
    values = asdict(i)
    values["available_weekdays"] = tuple(sorted(days))
    return SeasonDraft(name, start.isoformat(), end.isoformat(), draft.timezone, SeasonInputs(**values))


def _validate_event(draft: EventDraft, season: Season) -> EventDraft:
    values = asdict(draft)
    values["name"] = _text(draft.name, "Event name")
    day = _day(draft.event_date, "Event date")
    if not season.start_date <= day.isoformat() <= season.end_date:
        raise SeasonError("Event date must be within the season.")
    for value, options, field in [(draft.distance_code, DISTANCES, "distance"),
                                   (draft.priority, ("A", "B", "C"), "priority"),
                                   (draft.event_type, ("road", "trail"), "event type"),
                                   (draft.status, ("planned", "completed", "skipped", "cancelled"), "status"),
                                   (draft.goal_intent, ("COMPLETION", "PERFORMANCE"), "goal")]:
        if value not in options:
            raise SeasonError(f"Unsupported event {field}.")
    if draft.status == "completed" and day > datetime.now(ZoneInfo(season.timezone)).date():
        raise SeasonError("An event in the future cannot be marked completed.")
    if draft.target_seconds is not None:
        _integer(draft.target_seconds, "Target time in seconds", 1, 604800)
    if draft.target_speed_mps is not None:
        if not isinstance(draft.target_speed_mps, str) or len(draft.target_speed_mps) > 32 or "e" in draft.target_speed_mps.lower():
            raise SeasonError("Target speed must be a decimal string of at most 32 characters, without an exponent.")
        try:
            speed = Decimal(draft.target_speed_mps)
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise SeasonError("Target speed must be a positive decimal in m/s.") from exc
        if not isinstance(draft.target_speed_mps, str) or not speed.is_finite() or not 0 < speed <= 20:
            raise SeasonError("Target speed must be a positive decimal up to 20 m/s.")
        values["target_speed_mps"] = format(speed.normalize(), "f")
    if draft.target_seconds is not None and draft.target_speed_mps is not None:
        raise SeasonError("Choose a target time or speed, not both.")
    if draft.goal_intent == "COMPLETION" and (draft.target_seconds is not None or draft.target_speed_mps is not None):
        raise SeasonError("Select a performance goal to store a time or speed target.")
    if draft.participation_seconds is not None:
        _integer(draft.participation_seconds, "Planned participation seconds", 1, 604800)
        if draft.goal_intent != "COMPLETION":
            raise SeasonError("Participation duration is for completion goals; use a time or pace target for performance goals.")
    if draft.taper_days is not None:
        _integer(draft.taper_days, "Taper days", 0, 28)
    if draft.recovery_days is not None:
        _integer(draft.recovery_days, "Recovery days", RECOVERY_DAYS[draft.distance_code], 42)
    values["terrain"] = _text(draft.terrain, "Terrain", 500, empty=True)
    values["course_notes"] = _text(draft.course_notes, "Course notes", 2000, empty=True)
    return EventDraft(**values)


@contextmanager
def _connection(db_path: Path | str, *, write: bool = False) -> Iterator[sqlite3.Connection]:
    if not Path(db_path).is_file():
        raise FileNotFoundError(f"Database does not exist: {db_path}")
    conn = connect_sqlite(Path(db_path))
    try:
        if write:
            conn.execute("BEGIN IMMEDIATE")
        yield conn
        if write:
            conn.commit()
    except BaseException:
        if write:
            conn.rollback()
        raise
    finally:
        conn.close()


def _season_row(row: sqlite3.Row) -> Season:
    try:
        if row["input_schema_version"] != INPUT_SCHEMA:
            raise ValueError("unsupported version")
        inputs = json.loads(row["inputs_json"])
        inputs["available_weekdays"] = tuple(inputs["available_weekdays"])
        draft = _validate_draft(SeasonDraft(row["name"], row["start_date"], row["end_date"],
                                           row["timezone"], SeasonInputs(**inputs)))
    except (TypeError, ValueError, KeyError) as exc:
        raise SeasonError("Stored season inputs are invalid; repair the source before editing.") from exc
    return Season(row["season_id"], draft.name, draft.start_date, draft.end_date, draft.timezone,
                  row["status"], draft.inputs, row["input_version"], row["plan_id"], row["current_revision_id"])


_SEASON_SQL = """SELECT s.*, p.current_revision_id FROM season_plan s
                 LEFT JOIN training_plan p ON p.plan_id=s.plan_id"""


def list_seasons(db_path: Path | str) -> tuple[Season, ...]:
    with _connection(db_path) as conn:
        return tuple(_season_row(r) for r in conn.execute(_SEASON_SQL + " ORDER BY s.start_date,s.season_id"))


def _get(conn: sqlite3.Connection, season_id: str, expected_version: int | None = None) -> Season:
    row = conn.execute(_SEASON_SQL + " WHERE s.season_id=?", (season_id,)).fetchone()
    if row is None:
        raise SeasonError("Season no longer exists. Refresh the list.")
    season = _season_row(row)
    if expected_version is not None:
        _integer(expected_version, "Expected season version", 1, 2**63 - 1)
        if season.input_version != expected_version:
            raise StaleSeasonError("Season changed in another editor. Refresh before saving.")
        if season.status == "archived":
            raise SeasonError("Restore the archived season before editing.")
    return season


def get_season(db_path: Path | str, season_id: str) -> Season:
    with _connection(db_path) as conn:
        return _get(conn, season_id)


def _overlap(conn: sqlite3.Connection, draft: SeasonDraft, season_id: str = "") -> None:
    if conn.execute("""SELECT 1 FROM season_plan WHERE season_id != ?
        AND (status != 'archived' OR plan_id IS NOT NULL) AND start_date <= ? AND end_date >= ?""",
                    (season_id, draft.end_date, draft.start_date)).fetchone():
        raise SeasonError("Season dates overlap another season. Choose a separate date range.")


def create_season(db_path: Path | str, draft: SeasonDraft) -> Season:
    draft = _validate_draft(draft)
    season_id, now = uuid4().hex, _stamp()
    with _connection(db_path, write=True) as conn:
        _overlap(conn, draft)
        conn.execute("""INSERT INTO season_plan(season_id,name,start_date,end_date,timezone,
            input_schema_version,inputs_json,created_at_utc,updated_at_utc) VALUES(?,?,?,?,?,?,?,?,?)""",
                     (season_id, draft.name, draft.start_date, draft.end_date, draft.timezone,
                      INPUT_SCHEMA, canonical_json(asdict(draft.inputs)), now, now))
        return _get(conn, season_id)


def update_season(db_path: Path | str, season_id: str, draft: SeasonDraft, *, expected_version: int) -> Season:
    _integer(expected_version, "Expected season version", 1, 2**63 - 1)
    draft = _validate_draft(draft)
    with _connection(db_path, write=True) as conn:
        current = _get(conn, season_id, expected_version)
        _overlap(conn, draft, season_id)
        if conn.execute("SELECT 1 FROM season_event WHERE season_id=? AND (event_date < ? OR event_date > ?)",
                        (season_id, draft.start_date, draft.end_date)).fetchone():
            raise SeasonError("Season dates must include all saved events, including cancelled history.")
        if current.plan_id:
            if draft.timezone != current.timezone or draft.inputs.training_method != current.inputs.training_method:
                raise SeasonError("Timezone and methodology cannot change after linking a plan.")
            if conn.execute("""SELECT 1 FROM plan_revision_workout w JOIN plan_revision r USING(revision_id)
                WHERE r.plan_id=? AND (w.scheduled_date < ? OR w.scheduled_date > ?)""",
                            (current.plan_id, draft.start_date, draft.end_date)).fetchone():
                raise SeasonError("Season dates must retain linked plan history.")
        conn.execute("""UPDATE season_plan SET name=?,start_date=?,end_date=?,timezone=?,inputs_json=?,
            input_version=input_version+1,updated_at_utc=? WHERE season_id=?""",
                     (draft.name, draft.start_date, draft.end_date, draft.timezone,
                      canonical_json(asdict(draft.inputs)), _stamp(), season_id))
        return _get(conn, season_id)


def set_archived(db_path: Path | str, season_id: str, *, expected_version: int, archived: bool) -> Season:
    _integer(expected_version, "Expected season version", 1, 2**63 - 1)
    if not isinstance(archived, bool):
        raise SeasonError("Archived state must be true or false.")
    with _connection(db_path, write=True) as conn:
        season = _get(conn, season_id)
        if season.input_version != expected_version:
            raise StaleSeasonError("Season changed in another editor. Refresh before saving.")
        if not archived:
            _overlap(conn, SeasonDraft(season.name, season.start_date, season.end_date, season.timezone, season.inputs), season_id)
        status = "archived" if archived else "active" if season.plan_id else "draft"
        conn.execute("UPDATE season_plan SET status=?,input_version=input_version+1,updated_at_utc=? WHERE season_id=?",
                     (status, _stamp(), season_id))
        return _get(conn, season_id)


def _event_row(row: sqlite3.Row) -> SeasonEvent:
    values = {key: row[key] for key in EventDraft.__dataclass_fields__}
    return SeasonEvent(row["event_id"], row["season_id"], EventDraft(**values), row["distance_metres"])


def list_events(db_path: Path | str, season_id: str) -> tuple[SeasonEvent, ...]:
    with _connection(db_path) as conn:
        season = _get(conn, season_id)
        events = tuple(_event_row(r) for r in conn.execute(
            "SELECT * FROM season_event WHERE season_id=? ORDER BY event_date,event_id", (season_id,)))
        for event in events:
            if _validate_event(event.draft, season) != event.draft or event.distance_metres != DISTANCES[event.draft.distance_code]:
                raise SeasonError("Stored event data is inconsistent; repair it before editing.")
        return events


def _touch(conn: sqlite3.Connection, season_id: str) -> None:
    conn.execute("UPDATE season_plan SET input_version=input_version+1,updated_at_utc=? WHERE season_id=?", ( _stamp(), season_id))


def save_event(db_path: Path | str, season_id: str, draft: EventDraft, *, expected_version: int,
               event_id: str | None = None) -> SeasonEvent:
    _integer(expected_version, "Expected season version", 1, 2**63 - 1)
    with _connection(db_path, write=True) as conn:
        season = _get(conn, season_id, expected_version)
        draft = _validate_event(draft, season)
        if event_id:
            row = conn.execute("SELECT * FROM season_event WHERE event_id=? AND season_id=?", (event_id, season_id)).fetchone()
            if row is None:
                raise SeasonError("Event no longer exists in this season.")
            if row["status"] == "completed" and _event_row(row).draft != draft:
                raise SeasonError("Completed event history cannot be changed here.")
        identity = event_id or uuid4().hex
        if draft.status in {"planned", "completed"} and conn.execute(
                "SELECT 1 FROM season_event WHERE season_id=? AND event_date=? AND status IN ('planned','completed') AND event_id!=?",
                (season_id, draft.event_date, identity)).fetchone():
            raise SeasonError("Another active event is scheduled on that date. Move or cancel it first.")
        values = asdict(draft)
        values.update(sport="RUNNING", distance_metres=DISTANCES[draft.distance_code], updated_at_utc=_stamp())
        if event_id:
            assignments = ",".join(f"{key}=?" for key in values)
            conn.execute(f"UPDATE season_event SET {assignments} WHERE event_id=?", (*values.values(), identity))
        else:
            values.update(event_id=identity, season_id=season_id, created_at_utc=values["updated_at_utc"])
            conn.execute(f"INSERT INTO season_event({','.join(values)}) VALUES({','.join('?' for _ in values)})", tuple(values.values()))
        _touch(conn, season_id)
        return _event_row(conn.execute("SELECT * FROM season_event WHERE event_id=?", (identity,)).fetchone())


def remove_event(db_path: Path | str, season_id: str, event_id: str, *, expected_version: int) -> None:
    _integer(expected_version, "Expected season version", 1, 2**63 - 1)
    with _connection(db_path, write=True) as conn:
        season = _get(conn, season_id, expected_version)
        row = conn.execute("SELECT * FROM season_event WHERE event_id=? AND season_id=?", (event_id, season_id)).fetchone()
        if row is None:
            raise SeasonError("Event no longer exists in this season.")
        if row["status"] == "completed":
            raise SeasonError("Completed event history cannot be removed.")
        if season.plan_id:
            conn.execute("UPDATE season_event SET status='cancelled',updated_at_utc=? WHERE event_id=?", (_stamp(), event_id))
        else:
            conn.execute("DELETE FROM season_event WHERE event_id=?", (event_id,))
        _touch(conn, season_id)


def linkable_plans(db_path: Path | str) -> tuple[dict, ...]:
    with _connection(db_path) as conn:
        return tuple(dict(r) for r in conn.execute("""SELECT p.plan_id,p.current_revision_id,
            p.origin,r.content_sha256,r.methodology_id,MIN(w.scheduled_date) AS start_date,
            MAX(w.scheduled_date) AS end_date,COUNT(w.workout_id) AS workout_count
            FROM training_plan p JOIN plan_revision r ON r.revision_id=p.current_revision_id
            JOIN plan_revision_workout w ON w.revision_id=r.revision_id
            WHERE NOT EXISTS (SELECT 1 FROM season_plan s WHERE s.plan_id=p.plan_id)
            GROUP BY p.plan_id ORDER BY start_date,p.plan_id"""))


def link_plan(db_path: Path | str, season_id: str, plan_id: str, *, expected_version: int,
              expected_revision_id: str, expected_content_hash: str) -> Season:
    """Explicitly link a reviewed canonical plan, preserving every schedule row."""
    _integer(expected_version, "Expected season version", 1, 2**63 - 1)
    from garmin_data_hub.plan_methodology.revision_repository import load_revision, _projection_totals
    with _connection(db_path, write=True) as conn:
        season = _get(conn, season_id, expected_version)
        if season.plan_id:
            raise SeasonError("This season already has a linked plan.")
        row = conn.execute("""SELECT p.current_revision_id,r.content_sha256,r.methodology_id
            FROM training_plan p JOIN plan_revision r ON r.revision_id=p.current_revision_id
            WHERE p.plan_id=?""", (plan_id,)).fetchone()
        if row is None or row["current_revision_id"] != expected_revision_id or row["content_sha256"] != expected_content_hash:
            raise StaleSeasonError("The selected plan changed. Refresh and review it again.")
        if row["methodology_id"] != METHODS[season.inputs.training_method]:
            raise SeasonError("Season methodology must match the selected plan.")
        if conn.execute("SELECT 1 FROM season_plan WHERE plan_id=?", (plan_id,)).fetchone():
            raise SeasonError("That plan already belongs to another season.")
        revision = load_revision(db_path, expected_revision_id)
        if revision is None or not revision.candidate.workouts:
            raise SeasonError("The plan has no verified workout content.")
        if any(w.sport.value != "RUNNING" or not season.start_date <= w.scheduled_date.isoformat() <= season.end_date
               for w in revision.candidate.workouts):
            raise SeasonError("Season dates must contain every workout in the running plan.")
        if conn.execute("""SELECT 1 FROM plan_revision_workout w JOIN plan_revision r USING(revision_id)
            WHERE r.plan_id=? AND (w.scheduled_date < ? OR w.scheduled_date > ?)""",
                        (plan_id, season.start_date, season.end_date)).fetchone():
            raise SeasonError("Season dates must contain all prior plan history.")
        projections = {r["source_workout_id"]: r for r in conn.execute(
            "SELECT * FROM planned_workout WHERE source_plan_id=?", (plan_id,))}
        if set(projections) != {w.workout_id for w in revision.candidate.workouts}:
            raise SeasonError("Plan projection is incomplete; repair it before linking.")
        for workout in revision.candidate.workouts:
            projection = projections[workout.workout_id]
            distance, duration = _projection_totals(workout)
            try:
                structure = json.loads(projection["structure_json"])
                if (projection["source_revision_id"] != expected_revision_id
                        or projection["scheduled_date"] != workout.scheduled_date.isoformat()
                        or projection["workout_name"] != (workout.title or workout.family)
                        or projection["description"] != workout.description
                        or projection["planned_distance_m"] != distance
                        or projection["planned_duration_s"] != duration
                        or projection["planned_tss"] is not None
                        or structure["workout"] != canonical_value(workout)):
                    raise ValueError("projection mismatch")
            except (ValueError, TypeError, KeyError) as exc:
                raise SeasonError("Plan projection changed; repair it before linking.") from exc
        if conn.execute("""SELECT 1 FROM planned_workout WHERE scheduled_date BETWEEN ? AND ?
            AND ((source_plan_id IS NULL)+(source_revision_id IS NULL)+(source_workout_id IS NULL)) NOT IN (0,3)""",
                        (season.start_date, season.end_date)).fetchone():
            raise SeasonError("Schedule has partial source provenance; repair it before linking.")
        if conn.execute("""SELECT 1 FROM active_planned_workout WHERE scheduled_date BETWEEN ? AND ?
            AND (source_plan_id IS NULL OR source_plan_id != ?)""",
                        (season.start_date, season.end_date, plan_id)).fetchone():
            raise SeasonError("Another unmanaged schedule overlaps these dates. Keep it separate or explicitly convert/link it first.")
        _overlap(conn, SeasonDraft(season.name, season.start_date, season.end_date, season.timezone, season.inputs), season_id)
        conn.execute("UPDATE season_plan SET plan_id=?,status='active',input_version=input_version+1,updated_at_utc=? WHERE season_id=?",
                     (plan_id, _stamp(), season_id))
        return _get(conn, season_id)


def _has_seasons(conn: sqlite3.Connection) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='season_plan'").fetchone() is not None


def assert_legacy_write_allowed(conn: sqlite3.Connection, start: str, end: str) -> None:
    if _has_seasons(conn) and conn.execute("""SELECT 1 FROM season_plan
        WHERE plan_id IS NOT NULL AND start_date <= ? AND end_date >= ?""", (end, start)).fetchone():
        raise SeasonOwnershipError("These dates belong to a linked season. Use Seasons to manage its schedule.")


def assert_plan_write_allowed(conn: sqlite3.Connection, plan_id: str) -> None:
    if _has_seasons(conn) and conn.execute("SELECT 1 FROM season_plan WHERE plan_id=?", (plan_id,)).fetchone():
        raise SeasonOwnershipError("This plan belongs to a season. Use its reviewed season workflow; direct revision approval is unavailable.")
