"""Read-only season previews and atomic, audited initial application.

Y2 intentionally rejects replacing any existing/adopted season schedule. Y3 owns
history protection, incremental replacement and immutable origin lineage.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

from garmin_data_hub.plan_methodology.canonical import canonical_json, content_sha256
from garmin_data_hub.plan_methodology.revision_repository import (
    _approve_revision_on_connection, _call_hook, load_revision,
)
from garmin_data_hub.services import season_plans as intent
from garmin_data_hub.services.plan_persistence import active_plan_sha256
from garmin_data_hub.services.season_schedule import (
    GENERATOR_VERSION, POLICY_VERSION, GenerationSettings,
    SeasonSchedule, generate_season_schedule,
)


class StalePreviewError(intent.SeasonError):
    """Inputs, schedule, runtime evidence or local date changed after review."""


@dataclass(frozen=True)
class SeasonPreview:
    preview_id: str
    season: intent.Season
    events: tuple[intent.SeasonEvent, ...]
    settings: GenerationSettings
    local_today: str
    input_sha256: str
    active_sha256: str
    runtime_sha256: str
    raw_schedule_sha256: str
    parent_sha256: str | None
    snapshot_json: str
    schedule: SeasonSchedule
    apply_blockers: tuple[str, ...]
    generator_version: str = GENERATOR_VERSION
    policy_version: str = POLICY_VERSION

    @property
    def can_apply(self):
        return not self.apply_blockers and not self.schedule.errors

    @property
    def identity(self):
        return content_sha256({"preview_id":self.preview_id,"snapshot":self.snapshot_json,
            "candidate_sha256":self.schedule.candidate.content_hash,"local_today":self.local_today,
            "active_sha256":self.active_sha256,"runtime_sha256":self.runtime_sha256,
            "raw_schedule_sha256":self.raw_schedule_sha256,"parent_sha256":self.parent_sha256,
            "generator_version":self.generator_version,"policy_version":self.policy_version})


@dataclass(frozen=True)
class SeasonApplyResult:
    status: str
    application_id: str
    plan_id: str
    revision_id: str
    workout_count: int


def _local_today(timezone):
    return datetime.now(ZoneInfo(timezone)).date()


def _row_hash(conn, query):
    rows = [dict(r) for r in conn.execute(query)]
    return hashlib.sha256(json.dumps(rows,sort_keys=True,separators=(",",":"),ensure_ascii=False,allow_nan=False).encode()).hexdigest()


def _state(conn):
    return (active_plan_sha256(conn),
            _row_hash(conn,"SELECT * FROM activity_workout_match ORDER BY activity_workout_match_id"),
            _row_hash(conn,"SELECT * FROM planned_workout ORDER BY planned_workout_id"))


def _events(conn,season):
    events = tuple(intent._event_row(r) for r in conn.execute(
        "SELECT * FROM season_event WHERE season_id=? ORDER BY event_date,event_id",(season.season_id,)))
    for e in events:
        if intent._validate_event(e.draft,season)!=e.draft or e.distance_metres!=intent.DISTANCES[e.draft.distance_code]:
            raise intent.SeasonError("Stored event intent is inconsistent.")
    return events


def _snapshot(season,events,settings):
    return canonical_json({"schema_version":"season-preview-input.v1", "season":asdict(season),
                           "events":[asdict(e) for e in events],"settings":asdict(settings)})


def _blockers(conn,season,today):
    blockers = []
    if season.status=="archived":
        blockers.append("Restore this archived season before generating an applicable preview.")
    if season.start_date<today.isoformat():
        blockers.append("Initial apply requires a season starting today or later. Keep historical schedules in place.")
    if season.plan_id or season.current_revision_id:
        blockers.append("A schedule is already linked. You can review a full preview; replacing existing season workouts is not available yet.")
    if conn.execute("SELECT 1 FROM active_planned_workout WHERE scheduled_date BETWEEN ? AND ?",(season.start_date,season.end_date)).fetchone():
        blockers.append("Existing workouts overlap this season. Explicitly review adoption/removal or choose separate dates before initial apply.")
    if conn.execute("SELECT 1 FROM planned_workout WHERE scheduled_date BETWEEN ? AND ? AND ((source_plan_id IS NULL)+(source_revision_id IS NULL)+(source_workout_id IS NULL)) NOT IN (0,3)",
                    (season.start_date,season.end_date)).fetchone():
        blockers.append("Partial workout provenance exists in this range; repair it before applying.")
    for w in conn.execute("SELECT season_id,start_date FROM season_plan WHERE season_id!=? AND plan_id IS NOT NULL AND start_date>? ORDER BY start_date",(season.season_id,season.end_date)):
        events = _events(conn,season)
        from garmin_data_hub.services.season_schedule import event_windows
        if any(window.recovery_end.isoformat()>=w["start_date"] for window in event_windows(season,events)):
            blockers.append("Recovery extends into a following owned season. Resolve the boundary conflict before applying.")
            break
    from garmin_data_hub.services.season_schedule import event_windows
    current_windows = event_windows(season,_events(conn,season))
    for window in current_windows:
        if window.recovery_end.isoformat()>season.end_date and conn.execute(
            "SELECT 1 FROM active_planned_workout WHERE scheduled_date>? AND scheduled_date<=?",
            (season.end_date,window.recovery_end.isoformat())).fetchone():
            blockers.append("Recovery extends into existing following workouts. Resolve the boundary before applying.")
            break
    previous = conn.execute("SELECT s.* FROM season_plan s WHERE s.season_id!=? AND s.plan_id IS NOT NULL AND s.end_date<? AND s.end_date>=?",
        (season.season_id,season.start_date,(datetime.fromisoformat(season.start_date).date()-timedelta(days=42)).isoformat()))
    for row in previous:
        prior = intent._get(conn,row["season_id"])
        for window in event_windows(prior,_events(conn,prior)):
            if window.event_date.isoformat()<season.start_date<=window.recovery_end.isoformat():
                blockers.append("A preceding season's event recovery reaches this season. Resolve the recovery boundary before initial apply.")
                break
    intent._overlap(conn,intent.SeasonDraft(season.name,season.start_date,season.end_date,season.timezone,season.inputs),season.season_id)
    return tuple(blockers)


def preview_season(db_path: Path | str, season_id: str, settings: GenerationSettings) -> SeasonPreview:
    """Freeze intent/runtime state and generate without any database mutation."""
    with intent._connection(db_path) as conn:
        conn.execute("BEGIN")
        season = intent._get(conn,season_id)
        events = _events(conn,season)
        today = _local_today(season.timezone)
        parent_revision = load_revision(db_path,season.current_revision_id) if season.current_revision_id else None
        if season.current_revision_id and parent_revision is None:
            raise intent.SeasonError("Linked revision is missing; repair the source before generating.")
        parent = parent_revision.candidate if parent_revision else None
        plan_id = season.plan_id or uuid4().hex
        snapshot = _snapshot(season,events,settings)
        state = _state(conn)
        blockers = _blockers(conn,season,today)
        schedule = generate_season_schedule(season,events,settings,today=today,
                                           plan_id=plan_id,revision_id=uuid4().hex,parent=parent)
        # A preceding unmanaged event has no trustworthy recovery metadata: reject
        # overlap via active rows, and never guess its distance from a title.
        return SeasonPreview(uuid4().hex,season,events,settings,today.isoformat(),
            hashlib.sha256(snapshot.encode()).hexdigest(),*state,
            parent.content_hash if parent else None,snapshot,schedule,blockers)


def _invalidate_nutrition(conn,start,end):
    row = conn.execute("SELECT value FROM app_settings WHERE key='last_generated_plan'").fetchone()
    if row is None:
        return
    try:
        data = json.loads(row["value"])
        if data in (None, ""):
            return
        double = isinstance(data,str)
        if double:
            data = json.loads(data)
        if not isinstance(data,dict) or not isinstance(data.get("day_plans",[]),list):
            raise ValueError("unknown cache shape")
        for day in data.get("day_plans",[]):
            if isinstance(day,dict) and start<=str(day.get("iso_date",""))<=end:
                day.pop("nutrition",None)
        serialized = json.dumps(data,ensure_ascii=False,sort_keys=True)
        if double:
            serialized = json.dumps(serialized)
        conn.execute("UPDATE app_settings SET value=? WHERE key='last_generated_plan'",(serialized,))
    except (ValueError,TypeError):
        raise intent.SeasonError("The saved nutrition cache is unreadable. Repair it before applying the season.")


def apply_season_preview(db_path: Path | str, preview: SeasonPreview, *, approved_by: str,
                         acknowledge_warnings: bool = False, failure_hook=None) -> SeasonApplyResult:
    """Recompute reviewed content under one write lock, then activate and audit once."""
    if not isinstance(preview,SeasonPreview) or not approved_by.strip():
        raise intent.SeasonError("Apply requires a reviewed season preview and reviewer.")
    with intent._connection(db_path,write=True) as conn:
        duplicate = conn.execute("SELECT * FROM season_revision_application WHERE season_id=? AND preview_id=?",
                                 (preview.season.season_id,preview.preview_id)).fetchone()
        if duplicate:
            review = json.loads(duplicate["review_json"])
            if duplicate["candidate_sha256"]!=preview.schedule.candidate.content_hash or review["preview_sha256"]!=preview.identity:
                raise StalePreviewError("Applied preview identity does not match this reviewed content.")
            return SeasonApplyResult("duplicate",duplicate["application_id"],duplicate["plan_id"],duplicate["resulting_revision_id"],len(preview.schedule.candidate.workouts))
        season = intent._get(conn,preview.season.season_id)
        events = _events(conn,season)
        today = _local_today(season.timezone)
        snapshot = _snapshot(season,events,preview.settings)
        if (preview.generator_version!=GENERATOR_VERSION or preview.policy_version!=POLICY_VERSION
                or today.isoformat()!=preview.local_today or snapshot!=preview.snapshot_json
                or hashlib.sha256(snapshot.encode()).hexdigest()!=preview.input_sha256
                or _state(conn)!=(preview.active_sha256,preview.runtime_sha256,preview.raw_schedule_sha256)):
            raise StalePreviewError("Season inputs, schedule, completion evidence, policy or local date changed. Generate a fresh preview.")
        blockers = _blockers(conn,season,today)
        if blockers:
            raise intent.SeasonError(" ".join(blockers))
        candidate = preview.schedule.candidate
        if candidate.parent_revision_id is not None or preview.parent_sha256 is not None:
            raise intent.SeasonError("Replacing an existing season revision is not available yet.")
        if conn.execute("SELECT 1 FROM training_plan WHERE plan_id=?",(candidate.plan_id,)).fetchone():
            raise StalePreviewError("The preview's new plan identity is already in use.")
        regenerated = generate_season_schedule(season,events,preview.settings,today=today,
            plan_id=candidate.plan_id,revision_id=candidate.revision_id)
        if regenerated.candidate.content_hash!=candidate.content_hash:
            raise StalePreviewError("Reviewed candidate differs from deterministic validated output. Generate a new preview.")
        if regenerated.errors:
            raise intent.SeasonError(" ".join(i.message for i in regenerated.errors))
        if regenerated.warnings and acknowledge_warnings is not True:
            raise intent.SeasonError("Review and acknowledge every preview warning before applying.")
        _call_hook(failure_hook,"after_recheck",conn)
        now = intent._stamp()
        # Reserve ownership before the private approval path. All changes roll back
        # together if graph, projection, cache, audit or activation fails.
        conn.execute("INSERT INTO training_plan(plan_id,current_revision_id,origin,created_at_utc) VALUES(?,NULL,'NATIVE',?)",(candidate.plan_id,now))
        conn.execute("UPDATE season_plan SET plan_id=?,status='active',input_version=input_version+1,updated_at_utc=? WHERE season_id=?",(candidate.plan_id,now,season.season_id))
        _call_hook(failure_hook,"after_ownership",conn)
        result = _approve_revision_on_connection(conn,candidate,expected_content_hash=candidate.content_hash,
            approved_by=approved_by,_season_initial_owner=season.season_id,failure_hook=failure_hook)
        _invalidate_nutrition(conn,season.start_date,season.end_date)
        _call_hook(failure_hook,"after_nutrition",conn)
        application_id = uuid4().hex
        review = {"schema_version":"season-application-review.v1","preview_sha256":preview.identity,
            "active_sha256_before":preview.active_sha256,"active_sha256_after":active_plan_sha256(conn),
            "runtime_sha256":preview.runtime_sha256,"raw_schedule_sha256":preview.raw_schedule_sha256,
            "previous_revision_sha256":None,"resulting_revision_sha256":candidate.content_hash,
            "diff":dict(candidate.change_summary),"event_delta":{"added":[asdict(e) for e in events]},
            "preserved":[],"overrides":[],"warnings":[asdict(i) for i in regenerated.warnings],
            "warnings_acknowledged":acknowledge_warnings,"rationale":"One full, deterministic initial season revision; event priority, recovery and continuous local-week load rules applied."}
        conn.execute("""INSERT INTO season_revision_application(application_id,preview_id,season_id,plan_id,
            resulting_revision_id,previous_revision_id,input_version,input_sha256,candidate_sha256,
            snapshot_json,review_json,generator_version,policy_version,local_today,timezone,range_start,
            range_end,mode,approved_by,applied_at_utc) VALUES(?,?,?,?,?,NULL,?,?,?,?,?,?,?,?,?,?,?,'INITIAL_FULL',?,?)""",
            (application_id,preview.preview_id,season.season_id,candidate.plan_id,result.revision_id,
             season.input_version,preview.input_sha256,candidate.content_hash,snapshot,canonical_json(review),
             GENERATOR_VERSION,POLICY_VERSION,preview.local_today,season.timezone,season.start_date,season.end_date,approved_by,now))
        _call_hook(failure_hook,"after_audit",conn)
        _call_hook(failure_hook,"before_commit",conn)
        return SeasonApplyResult("applied",application_id,candidate.plan_id,candidate.revision_id,len(candidate.workouts))


def list_applications(db_path: Path | str, season_id: str) -> tuple[dict,...]:
    with intent._connection(db_path) as conn:
        return tuple(dict(r) for r in conn.execute("SELECT * FROM season_revision_application WHERE season_id=? ORDER BY applied_at_utc,application_id",(season_id,)))
