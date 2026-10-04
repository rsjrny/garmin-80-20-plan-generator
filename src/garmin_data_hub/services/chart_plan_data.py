"""Read-only, verified active-plan facts for Charts; never infer workout matches."""
from __future__ import annotations

from pathlib import Path
import sqlite3

from garmin_data_hub.db.activity_dates import activity_calendar_day_sql
from garmin_data_hub.plan_methodology.activity_workout_match import load_confirmed_match
from garmin_data_hub.plan_methodology.domain import MeasureRole, Sport
from garmin_data_hub.plan_methodology.revision_repository import load_revision, CorruptRevisionError
from garmin_data_hub.plan_methodology.workout_origin import origin_identity


def _measure(workout, field, role):
    if workout.sport is Sport.REST:
        return 0, False
    leaves = workout.segments
    if not all(getattr(s, field) is not None and getattr(s, role) in
               {MeasureRole.AUTHORITATIVE, MeasureRole.ESTIMATED} for s in leaves):
        return None, False
    return sum(getattr(s, field) for s in leaves), any(
        getattr(s, role) is MeasureRole.ESTIMATED for s in leaves)


def active_chart_plan_data(db_path):
    """Snapshot canonical active-view members, original matches and manual completion.

    Load immutable revisions rather than trusting mutable projection totals. A
    legacy schedule is reported separately until explicitly converted by Plan.
    """
    conn = sqlite3.connect(Path(db_path).resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("BEGIN")
        rows = list(conn.execute("SELECT * FROM active_planned_workout ORDER BY scheduled_date, planned_workout_id"))
        revisions = {}
        members = {}
        for (rid,) in conn.execute("SELECT current_revision_id FROM training_plan WHERE current_revision_id IS NOT NULL"):
            revision = load_revision(None, rid, _connection=conn)
            if revision is None:
                raise CorruptRevisionError("Active schedule revision is missing")
            revisions[rid] = revision.candidate
            members[rid] = set()
        unmanaged = 0
        for row in rows:
            rid = row["source_revision_id"]
            if not rid:
                unmanaged += 1
                continue
            if rid not in revisions:
                raise CorruptRevisionError("Active schedule pointer differs from projection")
            wid = row["source_workout_id"]
            if wid in members[rid]:
                raise CorruptRevisionError("Duplicate active workout identity")
            workout = next((w for w in revisions[rid].workouts if w.workout_id == wid), None)
            if workout is None or row["source_plan_id"] != revisions[rid].plan_id or row["scheduled_date"] != workout.scheduled_date.isoformat():
                raise CorruptRevisionError("Active schedule date/identity differs from its canonical revision")
            members[rid].add(wid)
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        states = {(r["plan_id"], r["workout_id"]): bool(r["explicitly_completed"])
                  for r in conn.execute("SELECT plan_id,workout_id,explicitly_completed FROM season_workout_state")} if "season_workout_state" in tables else {}
        calendar = activity_calendar_day_sql(conn, table_alias="a")
        workouts, plans = [], []
        for rid, revision in revisions.items():
            if members[rid] != {w.workout_id for w in revision.workouts}:
                raise CorruptRevisionError("Active schedule membership differs from its canonical revision")
            if not revision.workouts:
                continue
            anchor = min(w.scheduled_date for w in revision.workouts)
            plans.append(dict(id=revision.plan_id, revision=rid, anchor=anchor.isoformat()))
            for workout in revision.workouts:
                duration, duration_estimated = _measure(workout, "duration_seconds", "duration_role")
                distance, distance_estimated = _measure(workout, "distance_metres", "distance_role")
                origin = origin_identity(conn, rid, workout.workout_id)
                match = load_confirmed_match(conn, revision_id=origin[0], workout_id=origin[1])
                activity = None
                if match:
                    activity = conn.execute(f"SELECT a.activity_id, a.activity_type AS sport, {calendar} AS date FROM activity a WHERE a.activity_id=?", (match.activity_id,)).fetchone()
                candidate_ids = [r[0] for r in conn.execute("SELECT activity_id FROM activity_workout_match WHERE revision_id=? AND workout_id=? AND status='CANDIDATE' ORDER BY activity_id", origin)]
                workouts.append(dict(plan_id=revision.plan_id, revision_id=rid,
                    workout_id=workout.workout_id, date=workout.scheduled_date.isoformat(),
                    title=workout.title or workout.family, sport=workout.sport.value,
                    rest=workout.sport is Sport.REST, duration_s=duration, distance_m=distance,
                    duration_estimated=duration_estimated, distance_estimated=distance_estimated,
                    load=None, explicitly_completed=states.get((revision.plan_id, workout.workout_id), False),
                    candidates=len(candidate_ids), candidate_activity_ids=candidate_ids,
                    match_reason=match.reason if match else None, activity_id=match.activity_id if match else None,
                    activity_date=activity["date"] if activity else None,
                    activity_sport=activity["sport"] if activity else None,
                    missing_activity=bool(match and not activity)))
        return dict(plans=plans, workouts=workouts, unmanaged=unmanaged)
    finally:
        conn.close()
