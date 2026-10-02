"""Immutable carry-forward identity and original prescription interpretation."""
from __future__ import annotations

from .canonical import canonical_value, content_sha256


def prescribed_content(workout):
    value = canonical_value(workout)
    value.pop("ordinal", None)
    return value


def prescribed_sha256(workout):
    return content_sha256(prescribed_content(workout))


def origin_identity(conn, revision_id, workout_id):
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='plan_revision_workout_origin'").fetchone():
        return revision_id, workout_id
    row = conn.execute("SELECT origin_revision_id,origin_workout_id FROM plan_revision_workout_origin WHERE revision_id=? AND workout_id=?", (revision_id,workout_id)).fetchone()
    return (row[0],row[1]) if row else (revision_id,workout_id)


def verify_origins(conn, candidate):
    from .revision_repository import CorruptRevisionError, load_revision
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='plan_revision_workout_origin'").fetchone():
        if (candidate.change_summary or {}).get("origins"):
            raise CorruptRevisionError("workout origins are missing")
        return
    rows = [dict(r) for r in conn.execute("SELECT workout_id,origin_revision_id,origin_workout_id,prescribed_sha256 FROM plan_revision_workout_origin WHERE revision_id=? ORDER BY workout_id", (candidate.revision_id,))]
    expected = sorted([dict(r) for r in (candidate.change_summary or {}).get("origins", ())],key=lambda r:r["workout_id"])
    if rows != expected:
        raise CorruptRevisionError("relational workout origins differ from hashed revision lineage")
    ancestors = set()
    cursor = candidate.parent_revision_id
    while cursor:
        if cursor in ancestors:
            raise CorruptRevisionError("revision ancestry contains a cycle")
        ancestors.add(cursor)
        row = conn.execute("SELECT parent_revision_id,plan_id FROM plan_revision WHERE revision_id=?",(cursor,)).fetchone()
        if row is None or row[1] != candidate.plan_id:
            raise CorruptRevisionError("workout ancestry belongs to another plan")
        cursor = row[0]
    by_id = {w.workout_id:w for w in candidate.workouts}
    loaded = {}
    for row in rows:
        rid, wid = row["origin_revision_id"],row["origin_workout_id"]
        if rid not in ancestors or origin_identity(conn,rid,wid) != (rid,wid):
            raise CorruptRevisionError("workout origin must be an original ancestor")
        if rid not in loaded:
            loaded[rid] = load_revision(None,rid,_connection=conn)
        source = next((w for w in loaded[rid].candidate.workouts if w.workout_id==wid),None)
        copied = by_id.get(row["workout_id"])
        if source is None or copied is None or prescribed_content(source)!=prescribed_content(copied) or row["prescribed_sha256"]!=prescribed_sha256(copied):
            raise CorruptRevisionError("carried workout differs from its original prescription")


def insert_origins(conn, candidate):
    for row in (candidate.change_summary or {}).get("origins", ()):
        conn.execute("INSERT INTO plan_revision_workout_origin(revision_id,workout_id,origin_revision_id,origin_workout_id,prescribed_sha256) VALUES(?,?,?,?,?)",(candidate.revision_id,row["workout_id"],row["origin_revision_id"],row["origin_workout_id"],row["prescribed_sha256"]))
    verify_origins(conn,candidate)
