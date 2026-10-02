"""Versioned operational protection of immutable season occurrences."""
from __future__ import annotations

from datetime import date

from garmin_data_hub.plan_methodology.revision_repository import load_revision
from garmin_data_hub.plan_methodology.workout_origin import origin_identity
from garmin_data_hub.services import season_plans as intent


def _current(conn, season, workout_id):
    if not season.current_revision_id or season.status=="archived":
        raise intent.SeasonError("Restore a linked season before editing workout protection.")
    parent = load_revision(None,season.current_revision_id,_connection=conn).candidate
    workout = next((w for w in parent.workouts if w.workout_id==workout_id),None)
    if workout is None:
        raise intent.SeasonError("This workout is no longer active. Refresh the season.")
    return parent,workout


def _set_state(conn, season, parent, workout_id, *, expected_version, editor, reason,
               locked=None, explicitly_completed=None, manually_edited=None, manually_created=None):
    editor,reason = intent._text(editor,"Editor"),intent._text(reason,"Reason",1000)
    row = conn.execute("SELECT * FROM season_workout_state WHERE plan_id=? AND workout_id=?",(season.plan_id,workout_id)).fetchone()
    version = row["version"] if row else 0
    if type(expected_version) is not int or expected_version!=version:
        raise intent.StaleSeasonError("Workout protection changed. Refresh before editing.")
    values = {k:bool(row[k]) if row else False for k in ("locked","explicitly_completed","manually_edited","manually_created")}
    for k,v in (("locked",locked),("explicitly_completed",explicitly_completed),("manually_edited",manually_edited),("manually_created",manually_created)):
        if v is not None:
            if type(v) is not bool:
                raise intent.SeasonError("Protection fields must be true or false.")
            if k!="locked" and values[k] and not v:
                raise intent.SeasonError("Completion and manual provenance cannot be cleared.")
            values[k] = v
    rid,wid = origin_identity(conn,parent.revision_id,workout_id)
    conn.execute("""INSERT INTO season_workout_state(season_id,plan_id,workout_id,origin_revision_id,origin_workout_id,
      locked,explicitly_completed,manually_edited,manually_created,version,editor,reason,updated_at_utc)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(plan_id,workout_id) DO UPDATE SET
      locked=excluded.locked,explicitly_completed=excluded.explicitly_completed,manually_edited=excluded.manually_edited,
      manually_created=excluded.manually_created,version=excluded.version,editor=excluded.editor,reason=excluded.reason,updated_at_utc=excluded.updated_at_utc""",
      (season.season_id,season.plan_id,workout_id,rid,wid,int(values["locked"]),int(values["explicitly_completed"]),int(values["manually_edited"]),int(values["manually_created"]),version+1,editor,reason,intent._stamp()))


def set_workout_protection(db_path, season_id, workout_id, *, expected_version, editor, reason, locked=None, explicitly_completed=None):
    with intent._connection(db_path,write=True) as conn:
        season = intent._get(conn,season_id)
        parent,_ = _current(conn,season,workout_id)
        _set_state(conn,season,parent,workout_id,expected_version=expected_version,editor=editor,reason=reason,
                   locked=locked,explicitly_completed=explicitly_completed)


def list_workouts(db_path, season_id):
    from garmin_data_hub.services.season_generation import _local_today
    from garmin_data_hub.services.season_regeneration import protection
    with intent._connection(db_path) as conn:
        conn.execute("BEGIN")
        season = intent._get(conn,season_id)
        if not season.current_revision_id:
            return ()
        parent = load_revision(None,season.current_revision_id,_connection=conn).candidate
        today = _local_today(season.timezone)
        _,decisions = protection(conn,season,parent,today,(today,date.fromisoformat(season.end_date)),set())
        states = {r["workout_id"]:dict(r) for r in conn.execute("SELECT * FROM season_workout_state WHERE plan_id=?",(season.plan_id,))}
        return tuple(dict(d,state=states.get(d["workout_id"],{}),workout=next(w for w in parent.workouts if w.workout_id==d["workout_id"])) for d in decisions)
