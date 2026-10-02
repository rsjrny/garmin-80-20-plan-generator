"""Read-only range selection, protection decisions and complete revision merging."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import date, timedelta

from garmin_data_hub.plan_methodology.canonical import canonical_value
from garmin_data_hub.plan_methodology.domain import ApprovalState, RevisionReason, Sport
from garmin_data_hub.plan_methodology.segments import PlannedWorkout
from garmin_data_hub.plan_methodology.revision_repository import load_revision
from garmin_data_hub.plan_methodology.workout_origin import origin_identity, prescribed_content, prescribed_sha256
from garmin_data_hub.services import season_plans as intent
from garmin_data_hub.services.season_schedule import event_windows, monday, ScheduleIssue, generate_season_schedule


@dataclass(frozen=True)
class RegenerationRequest:
    mode: str = "AFFECTED"
    range_start: str | None = None
    range_end: str | None = None
    override_workout_ids: tuple[str, ...] = ()
    refresh_parameters: bool = False
    manual_workout: PlannedWorkout | None = None
    replaces_workout_id: str | None = None

    def __post_init__(self):
        object.__setattr__(self,"override_workout_ids",tuple(sorted(self.override_workout_ids)))
        if self.mode not in {"AFFECTED","FROM_TODAY","CUSTOM","MANUAL_EDIT"}:
            raise intent.SeasonError("Choose affected range, from today, or custom range.")
        if len(set(self.override_workout_ids))!=len(self.override_workout_ids) or any(not isinstance(i,str) or not i for i in self.override_workout_ids):
            raise intent.SeasonError("Replacement overrides must be distinct workout IDs.")
        if type(self.refresh_parameters) is not bool:
            raise intent.SeasonError("Parameter refresh must be explicit.")
        if (self.mode=="MANUAL_EDIT") != (self.manual_workout is not None):
            raise intent.SeasonError("Manual edit requires a reviewed canonical workout.")
        if self.mode!="MANUAL_EDIT" and self.replaces_workout_id is not None:
            raise intent.SeasonError("Only a manual edit accepts a replacement workout identity.")
        if self.mode!="CUSTOM" and (self.range_start is not None or self.range_end is not None):
            raise intent.SeasonError("Only custom mode accepts replacement dates.")


def previous_events(parent, season):
    result = []
    for value in parent.goal_snapshot.get("events", ()):
        value = canonical_value(value)
        result.append(intent.SeasonEvent(value["event_id"],value["season_id"],intent.EventDraft(**value["draft"]),value["distance_metres"]))
    return tuple(result)


def affected_range(season, events, parent, today, request, settings=None):
    old = previous_events(parent,season)
    before = {e.event_id:asdict(e) for e in old}
    after = {e.event_id:asdict(e) for e in events}
    delta = {"added":[after[k] for k in sorted(after.keys()-before.keys())],
             "removed":[before[k] for k in sorted(before.keys()-after.keys())],
             "changed":[{"before":before[k],"after":after[k]} for k in sorted(before.keys()&after.keys()) if before[k]!=after[k]]}
    changed = set(before)^set(after) | {k for k in before.keys()&after.keys() if before[k]!=after[k]}
    start, end = date.fromisoformat(season.start_date),date.fromisoformat(season.end_date)
    cutover = max(start,today)
    reasons = []
    windows = event_windows(season,old)+event_windows(season,events)
    old_inputs = canonical_value((parent.constraints or {}).get("inputs", {}))
    inputs_changed = old_inputs != canonical_value(asdict(season.inputs))
    bounds_changed = parent.goal_snapshot.get("start_date")!=season.start_date or parent.goal_snapshot.get("end_date")!=season.end_date
    previous_settings = canonical_value((parent.constraints or {}).get("settings", {}))
    history_changed = settings is not None and previous_settings.get("completed_weeks", [])!=canonical_value(settings.completed_weeks)
    if inputs_changed or bounds_changed or history_changed or request.refresh_parameters or not old_inputs:
        low, high = cutover,end
        reasons.append("Athlete inputs, availability, bounds or prescribing parameters require full future evaluation.")
    elif changed:
        touched = [w for w in windows if w.event.event_id in changed]
        if not touched:
            touched_dates = [date.fromisoformat(v["draft"]["event_date"]) for k in changed for v in (before.get(k),after.get(k)) if v]
            low,high = min(touched_dates),max(touched_dates)
        else:
            low,high = min(w.preparation_start for w in touched),max(w.recovery_end for w in touched)
        low,high = monday(low-timedelta(days=7)),monday(high+timedelta(days=7))+timedelta(days=6)
        reasons.append("Union of old and new event windows, seven transition days and complete local weeks.")
        while True:
            neighbors = [w for w in windows if w.preparation_start-timedelta(days=7)<=high and w.recovery_end+timedelta(days=7)>=low]
            new_low = min([low]+[monday(w.preparation_start-timedelta(days=7)) for w in neighbors])
            new_high = max([high]+[monday(w.recovery_end+timedelta(days=7))+timedelta(days=6) for w in neighbors])
            new_low,new_high = max(start,new_low),min(end,new_high)
            if (new_low,new_high)==(max(start,low),min(end,high)):
                low,high = new_low,new_high
                break
            low,high = new_low,new_high
        reasons.append("Intersecting neighbor influence windows expanded to a fixed point.")
    else:
        low,high = cutover,end
        reasons.append("No event delta: explicit regeneration evaluates the future schedule.")
    recommended = (max(cutover,low),min(end,high))
    if recommended[0]>recommended[1]:
        recommended = (cutover,end)
        reasons.append("Changes precede the cutover; future reconnection is evaluated.")
    if request.mode=="FROM_TODAY":
        selected = (cutover,end)
        reasons.append("Requested full schedule from the local cutover.")
    elif request.mode=="CUSTOM":
        selected = (intent._day(request.range_start,"Replacement start"),intent._day(request.range_end,"Replacement end"))
        if selected[0]<cutover or selected[1]>end or selected[0]>selected[1]:
            raise intent.SeasonError("Custom replacement must be ordered and within the future season.")
        reasons.append("Custom range is validated against the complete merged timeline.")
    else:
        selected = recommended
    if cutover>end:
        raise intent.SeasonError("This season has no future dates to regenerate.")
    reasons.append(f"History before {today} remains protected in {season.timezone}.")
    return selected,recommended,tuple(reasons),delta


def protection(conn, season, parent, today, selected, overrides):
    state = {r["workout_id"]:dict(r) for r in conn.execute("SELECT * FROM season_workout_state WHERE plan_id=?",(parent.plan_id,))}
    protected, decisions = [],[]
    eligible_overrides = set()
    for w in parent.workouts:
        rid,wid = origin_identity(conn,parent.revision_id,w.workout_id)
        evidence = conn.execute("SELECT status FROM activity_workout_match WHERE revision_id=? AND workout_id=? AND status!='REJECTED'",(rid,wid)).fetchall()
        flags = state.get(w.workout_id,{})
        reasons = []
        immutable = False
        if w.scheduled_date<today:
            reasons.append("history")
            immutable = True
        if flags.get("explicitly_completed") or any(r[0]=="CONFIRMED" for r in evidence):
            reasons.append("completed")
            immutable = True
        if any(r[0]=="CANDIDATE" for r in evidence):
            reasons.append("unresolved_match")
            immutable = True
        in_range = selected[0]<=w.scheduled_date<=selected[1]
        if not in_range:
            reasons.append("outside_range")
        manual = [k for k in ("locked","manually_edited","manually_created") if flags.get(k)]
        if manual:
            reasons.extend(manual)
            if in_range and not immutable:
                eligible_overrides.add(w.workout_id)
        if w.workout_id in overrides:
            if w.workout_id not in eligible_overrides:
                raise intent.SeasonError("Override IDs must identify future, incomplete locked/manual workouts inside this preview's range.")
            reasons = []
        if reasons:
            protected.append(w)
        decisions.append({"workout_id":w.workout_id,"date":w.scheduled_date.isoformat(),"title":w.title,
                          "reasons":reasons,"protection_version":flags.get("version",0),"override":w.workout_id in overrides,"origin_revision_id":rid,"origin_workout_id":wid})
    if set(overrides)-eligible_overrides:
        raise intent.SeasonError("An override refers to a missing or ineligible workout.")
    return tuple(protected),decisions


def compose(conn, season, events, settings, today, parent, revision_id, request):
    if request.mode=="MANUAL_EDIT":
        return compose_manual(conn,season,events,settings,today,parent,revision_id,request)
    selected,recommended,reasons,delta = affected_range(season,events,parent,today,request,settings)
    fixed,decisions = protection(conn,season,parent,today,selected,set(request.override_workout_ids))
    schedule = generate_season_schedule(season,events,settings,today=today,plan_id=parent.plan_id,
        revision_id=revision_id,parent=parent,fixed_workouts=fixed,replacement_range=selected,refresh_parameters=request.refresh_parameters)
    extra_issues = []
    for w in fixed:
        if w.scheduled_date>=today and w.sport is Sport.RUNNING and not w.event_flag and any(s.duration_seconds is None for s in w.segments):
            extra_issues.append(ScheduleIssue("PROTECTED_LOAD_UNKNOWN","error",
                "Preserved running duration is unknown. Resolve the source prescription before generating replacement load.",w.scheduled_date.isoformat()))
        if w.event_flag and not any(e.event_id==(w.metadata or {}).get("event_id") and e.draft.status=="planned" and e.draft.event_date==w.scheduled_date.isoformat() for e in events):
            extra_issues.append(ScheduleIssue("PRESERVED_EVENT_INTENT","warning",
                "A protected event session differs from current event intent and is retained. Replacing a future locked/manual event requires its explicit override.",w.scheduled_date.isoformat()))
    for d in decisions:
        if "unresolved_match" in d["reasons"]:
            extra_issues.append(ScheduleIssue("UNRESOLVED_MATCH_PROTECTED","warning",
                "An unreviewed activity match protects this occurrence. Resolve or reject its evidence and make a new preview before replacing it.",d["date"]))
    schedule = replace(schedule,issues=schedule.issues+tuple(extra_issues))
    fixed_ids = {w.workout_id for w in fixed}
    old_by_date = defaultdict(list)
    for w in parent.workouts:
        if w.workout_id not in fixed_ids:
            old_by_date[w.scheduled_date].append(w)
    rows,added,removed,changed,carried = [],[],[],[],[]
    origins = []
    decision_map = {d["workout_id"]:d for d in decisions}
    for w in schedule.candidate.workouts:
        old = next((o for o in old_by_date[w.scheduled_date] if prescribed_content(replace(w,workout_id=o.workout_id))==prescribed_content(o)
                    and canonical_value(parent.parameter_snapshot)==canonical_value(schedule.candidate.parameter_snapshot)),None)
        if old:
            w = old
            decision_map[old.workout_id]["reasons"] = ["unchanged"]
            old_by_date[old.scheduled_date].remove(old)
        if w.workout_id in fixed_ids or old:
            rid,wid = origin_identity(conn,parent.revision_id,w.workout_id)
            origins.append({"workout_id":w.workout_id,"origin_revision_id":rid,"origin_workout_id":wid,"prescribed_sha256":prescribed_sha256(w)})
            carried.append(w.workout_id)
        else:
            added.append(w.workout_id)
            replaces = old_by_date[w.scheduled_date]
            if replaces:
                prior = replaces.pop(0)
                removed.append(prior.workout_id)
                changed.append({"old_workout_id":prior.workout_id,"new_workout_id":w.workout_id,"date":w.scheduled_date.isoformat()})
        rows.append(replace(w,ordinal=len(rows)))
    removed.extend(w.workout_id for values in old_by_date.values() for w in values)
    if any(i.severity=="error" for i in schedule.issues):
        extra = ScheduleIssue("RECONNECTION_REVIEW","warning",
            f"The merged timeline is infeasible. Review the recommended range {recommended[0]} through {recommended[1]}, or full-from-today, and resolve the listed protections/conflicts.")
        schedule = replace(schedule,issues=schedule.issues+(extra,))
    summary = {"schema_version":"season-regeneration-diff.v1","mode":request.mode,
        "range_start":selected[0].isoformat(),"range_end":selected[1].isoformat(),
        "recommended_start":recommended[0].isoformat(),"recommended_end":recommended[1].isoformat(),
        "range_reasons":reasons,"event_delta":delta,"added_workout_ids":added,"removed_workout_ids":removed,
        "changed":changed,"removed":[{"workout_id":w.workout_id,"date":w.scheduled_date.isoformat(),"title":w.title} for w in parent.workouts if w.workout_id in removed],
        "preserved_workout_ids":carried,"preserved":[decision_map[i] for i in carried],
        "overrides":list(request.override_workout_ids),"origins":sorted(origins,key=lambda r:r["workout_id"]),
        "ordinals":[{"workout_id":w.workout_id,"old":next(o.ordinal for o in parent.workouts if o.workout_id==w.workout_id),"new":w.ordinal} for w in rows if w.workout_id in carried]}
    validation = dict(schedule.candidate.validation_summary)
    validation.update(issues=[asdict(i) for i in schedule.issues],accepted=not schedule.errors)
    candidate = replace(schedule.candidate,workouts=tuple(rows),change_summary=summary,validation_summary=validation,
        approval_state=ApprovalState.CANDIDATE if schedule.errors else ApprovalState.VALIDATED)
    return replace(schedule,candidate=candidate)


def compose_manual(conn, season, events, settings, today, parent, revision_id, request):
    w = request.manual_workout
    if not isinstance(w,PlannedWorkout) or not w.workout_id:
        raise intent.SeasonError("A manual workout must have a canonical prescription and new identity.")
    if not max(today,date.fromisoformat(season.start_date))<=w.scheduled_date<=date.fromisoformat(season.end_date):
        raise intent.SeasonError("Manual prescription edits are limited to future season dates.")
    if conn.execute("SELECT 1 FROM plan_revision_workout WHERE workout_id=?",(w.workout_id,)).fetchone():
        raise intent.SeasonError("A changed/manual occurrence requires a new workout ID.")
    old_events = previous_events(parent,season)
    if canonical_value(old_events)!=canonical_value(events):
        raise intent.SeasonError("Apply pending event changes before editing individual prescriptions.")
    selected = (w.scheduled_date,w.scheduled_date)
    _,decisions = protection(conn,season,parent,today,(date.fromisoformat(season.start_date),date.fromisoformat(season.end_date)),set())
    source = next((o for o in parent.workouts if o.workout_id==request.replaces_workout_id),None)
    if request.replaces_workout_id and source is None:
        raise intent.SeasonError("The edited workout is no longer active.")
    if source:
        d = next(d for d in decisions if d["workout_id"]==source.workout_id)
        if any(r in d["reasons"] for r in ("history","completed","unresolved_match")):
            raise intent.SeasonError("History, completion and unresolved evidence prevent prescription editing.")
        if any(r in d["reasons"] for r in ("locked","manually_edited","manually_created")) and source.workout_id not in request.override_workout_ids:
            raise intent.SeasonError("Explicitly authorize replacement of this locked/manual occurrence.")
    if set(request.override_workout_ids)-({source.workout_id} if source else set()):
        raise intent.SeasonError("Manual overrides must identify only the replaced future occurrence.")
    fixed = tuple(o for o in parent.workouts if o is not source)
    parameters = parent.parameter_snapshot
    if source:
        rid,_ = origin_identity(conn,parent.revision_id,source.workout_id)
        parameters = load_revision(None,rid,_connection=conn).candidate.parameter_snapshot
        selected = (min(w.scheduled_date,source.scheduled_date),max(w.scheduled_date,source.scheduled_date))
    scheduling_parent = replace(parent,parameter_snapshot=parameters)
    schedule = generate_season_schedule(season,events,settings,today=today,plan_id=parent.plan_id,revision_id=revision_id,
        parent=scheduling_parent,fixed_workouts=tuple(sorted(fixed+(w,),key=lambda o:(o.scheduled_date,o.ordinal or 0,o.workout_id))),
        replacement_range=(w.scheduled_date,w.scheduled_date))
    # A moved workout must not fill its former date with newly generated training.
    ids = {o.workout_id for o in fixed}|{w.workout_id}
    rows = tuple(replace(o,ordinal=i) for i,o in enumerate(o for o in schedule.candidate.workouts if o.workout_id in ids))
    origins=[]
    for o in fixed:
        rid,wid = origin_identity(conn,parent.revision_id,o.workout_id)
        origins.append({"workout_id":o.workout_id,"origin_revision_id":rid,"origin_workout_id":wid,"prescribed_sha256":prescribed_sha256(o)})
    summary={"schema_version":"season-regeneration-diff.v1","mode":"MANUAL_EDIT",
        "range_start":selected[0].isoformat(),"range_end":selected[1].isoformat(),
        "recommended_start":selected[0].isoformat(),"recommended_end":selected[1].isoformat(),
        "range_reasons":["Reviewed manual prescription; all other occurrences retain their origin."],
        "event_delta":{},"added_workout_ids":[w.workout_id],"removed_workout_ids":[source.workout_id] if source else [],
        "changed":[{"old_workout_id":source.workout_id,"new_workout_id":w.workout_id,"date":w.scheduled_date.isoformat()}] if source else [],
        "removed":[{"workout_id":source.workout_id,"date":source.scheduled_date.isoformat(),"title":source.title}] if source else [],
        "preserved_workout_ids":[o.workout_id for o in fixed],
        "preserved":[dict(d,reasons=d["reasons"] or ["outside_manual_edit"]) for d in decisions if d["workout_id"] in {o.workout_id for o in fixed}],
        "overrides":list(request.override_workout_ids),"origins":sorted(origins,key=lambda r:r["workout_id"]),
        "manual_state":{"workout_id":w.workout_id,"manually_edited":bool(source),"manually_created":source is None}}
    return replace(schedule,candidate=replace(schedule.candidate,workouts=rows,parameter_snapshot=parameters,
        change_summary=summary,reason=RevisionReason.PRESCRIPTION_EDIT))
