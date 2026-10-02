"""Pure deterministic season orchestration and canonical workout composition.

Product scheduling rules are versioned, reproducible and separate from persistence.
No single-event calendars are concatenated, and no unknown duration/TSS is invented.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import date, timedelta
from decimal import Decimal

from garmin_data_hub.exports.forever.training_rules import (
    build_weekly_schedule, cutback_week_frequency, get_intensity_cap,
)
from garmin_data_hub.plan_methodology.canonical import canonical_value, content_sha256
from garmin_data_hub.plan_methodology.domain import (
    ApprovalState, Confidence, FitzgeraldTarget, LoadMode, MaffetoneTarget,
    MeasureRole, MethodologyId, Metric, RevisionReason, SegmentKind, Sport,
)
from garmin_data_hub.plan_methodology.fitzgerald_parameters import HR_FACTORS
from garmin_data_hub.plan_methodology.maffetone_parameters import derive
from garmin_data_hub.plan_methodology.prescriptions import IntensityPrescription, MetricCeiling, MetricRange
from garmin_data_hub.plan_methodology.registry import STATIC_DEFINITIONS, validate_methodology_candidate
from garmin_data_hub.plan_methodology.revisions import PlanRevisionCandidate
from garmin_data_hub.plan_methodology.segments import PlannedWorkout, WorkoutSegment
from garmin_data_hub.services import season_plans as intent

GENERATOR_VERSION = "season-generator.v1"
POLICY_VERSION = "season-windows-load.v1"
PREPARATION = {"5K": 42, "10K": 56, "10M": 56, "HM": 84, "20M": 112,
               "MAR": 112, "50K": 140, "50M": 140, "100K": 168, "100M": 168}
TAPER = {"5K": 7, "10K": 7, "10M": 7, "HM": 10, "20M": 14,
         "MAR": 14, "50K": 14, "50M": 14, "100K": 21, "100M": 21}
DAY = timedelta(days=1)


@dataclass(frozen=True)
class CompletedWeek:
    """Explicit Monday-week coverage evidence; missing days must not imply zero."""
    monday: str
    running_duration_seconds: int | None
    complete: bool
    evidence_ref: str


@dataclass(frozen=True)
class GenerationSettings:
    lthr_confirmed: bool = False
    maf_adjustment: int | None = None
    maf_confirmed: bool = False
    completed_weeks: tuple[CompletedWeek, ...] = ()

    def __post_init__(self):
        object.__setattr__(self, "completed_weeks", tuple(self.completed_weeks))
        if type(self.lthr_confirmed) is not bool or type(self.maf_confirmed) is not bool:
            raise intent.SeasonError("Parameter confirmation must be true or false.")
        if self.maf_adjustment is not None and (type(self.maf_adjustment) is not int or self.maf_adjustment not in (-10, -5, 0, 5)):
            raise intent.SeasonError("Choose an explicit MAF adjustment: -10, -5, 0 or +5.")


@dataclass(frozen=True)
class ScheduleIssue:
    code: str
    severity: str
    message: str
    date: str | None = None
    event_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class EventWindow:
    event: intent.SeasonEvent
    preparation_days: int
    taper_days: int
    recovery_days: int
    preparation_start: date
    taper_start: date
    event_date: date
    recovery_end: date


@dataclass(frozen=True)
class DayControl:
    day: date
    phase: str
    event_id: str | None
    load_fraction: Decimal
    influencing_event_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class WeekLoad:
    monday: str
    partial: bool
    envelope_seconds: int
    training_seconds: int
    event_duration_seconds: int | None
    event_distance_metres: int
    hard_days: int
    cutback: bool
    running_distance_metres: int | None = None


@dataclass(frozen=True)
class SeasonSchedule:
    candidate: PlanRevisionCandidate
    controls: tuple[DayControl, ...]
    windows: tuple[EventWindow, ...]
    weeks: tuple[WeekLoad, ...]
    issues: tuple[ScheduleIssue, ...]
    baseline_seconds: int | None

    @property
    def errors(self):
        return tuple(i for i in self.issues if i.severity == "error")

    @property
    def warnings(self):
        return tuple(i for i in self.issues if i.severity == "warning")


def monday(day: date) -> date:
    return day - timedelta(days=day.weekday())


def _days(start: date, end: date):
    while start <= end:
        yield start
        start += DAY


def event_windows(season: intent.Season, events: tuple[intent.SeasonEvent, ...]) -> tuple[EventWindow, ...]:
    windows = []
    seen = set()
    for e in sorted(events, key=lambda e: (e.draft.event_date, e.event_id)):
        if e.season_id != season.season_id or e.event_id in seen:
            raise intent.SeasonError("Event identity must be unique and belong to this season.")
        seen.add(e.event_id)
        draft = intent._validate_event(e.draft, season)
        if draft != e.draft or e.distance_metres != intent.DISTANCES[draft.distance_code]:
            raise intent.SeasonError("Stored event distance or input is inconsistent.")
        if draft.status not in {"planned", "completed"}:
            continue
        p = PREPARATION[draft.distance_code]
        t = TAPER[draft.distance_code]
        if draft.priority == "B":
            p, t = (p + 1) // 2, min(t, 3)
        elif draft.priority == "C":
            p, t = 0, 0
        t = draft.taper_days if draft.taper_days is not None else t
        # C events cannot obtain separate preparation/taper through an override.
        if draft.priority == "C":
            t = 0
        r = draft.recovery_days if draft.recovery_days is not None else intent.RECOVERY_DAYS[draft.distance_code]
        d = date.fromisoformat(draft.event_date)
        windows.append(EventWindow(e, p, t, r, d - timedelta(days=t+p), d - timedelta(days=t), d, d + timedelta(days=r)))
    return tuple(windows)


def _timeline(season, windows):
    issues = []
    planned = [w for w in windows if w.event.draft.status == "planned"]
    start, end = date.fromisoformat(season.start_date), date.fromisoformat(season.end_date)
    for w in planned:
        if w.preparation_start < start and w.preparation_days:
            issues.append(ScheduleIssue("INSUFFICIENT_PREPARATION", "warning",
                f"{w.event.draft.name}: preparation begins {w.preparation_start}; the season starts later. Missed training is not compressed.",
                w.event.draft.event_date, (w.event.event_id,)))
        if w.recovery_end > end:
            issues.append(ScheduleIssue("RECOVERY_OVERFLOW", "warning",
                f"{w.event.draft.name}: recovery continues through {w.recovery_end}, beyond this season.", end.isoformat(), (w.event.event_id,)))
    for i, earlier in enumerate(windows):
        for later in windows[i+1:]:
            a, b = earlier.event, later.event
            if later.event_date == earlier.event_date:
                issues.append(ScheduleIssue("SAME_DAY_EVENTS", "error", "Move or cancel one of the active events on the same date.", later.event_date.isoformat(), (a.event_id,b.event_id)))
            if b.draft.status != "planned":
                continue
            if earlier.event_date < later.event_date <= earlier.recovery_end:
                issues.append(ScheduleIssue("EVENT_IN_RECOVERY", "error",
                    f"{b.draft.name} falls inside recovery from {a.draft.name}. Move, skip or cancel it.", b.draft.event_date, (a.event_id,b.event_id)))
            if a.draft.priority == b.draft.priority == "A" and a.draft.status == "planned":
                if max(earlier.event_date + DAY, later.taper_start) <= min(earlier.recovery_end, later.event_date - DAY):
                    issues.append(ScheduleIssue("A_PEAKS_OVERLAP", "error",
                        f"{a.draft.name} recovery overlaps {b.draft.name} taper. Separate peaks are infeasible; downgrade, move or cancel one.", b.draft.event_date, (a.event_id,b.event_id)))
                elif later.preparation_start <= earlier.recovery_end:
                    issues.append(ScheduleIssue("SHARED_A_BUILD", "warning",
                        f"{a.draft.name} and {b.draft.name} share preparation. The earlier A event controls until its recovery ends.", b.draft.event_date, (a.event_id,b.event_id)))
    controls = []
    for d in _days(start, end):
        recovery = [w for w in windows if w.event_date < d <= w.recovery_end]
        tapers = [w for w in planned if w.taper_start <= d < w.event_date]
        preparation = [w for w in planned if w.preparation_start <= d < w.taper_start]
        races = [w for w in planned if d == w.event_date]
        active = recovery + tapers + preparation + races
        controlling = None
        phase, fraction = "MAINTENANCE", Decimal("0.8")
        if recovery:
            controlling = max(recovery, key=lambda w: (w.recovery_end, w.event_date, w.event.event_id))
            fraction = min(Decimal(0) if (d-w.event_date).days <= min(3,w.recovery_days) else Decimal("0.3") for w in recovery)
            if tapers:
                fraction = min(fraction, *(Decimal("0.25") if (w.event_date-d).days <= 7 else Decimal("0.5") for w in tapers))
            phase = "RECOVERY"
        elif tapers:
            controlling = min(tapers, key=lambda w: (w.event.draft.priority, w.event_date, w.event.event_id))
            fraction = min(Decimal("0.25") if (w.event_date-d).days <= 7 else Decimal("0.5") for w in tapers)
            phase = "TAPER"
        elif preparation:
            controlling = min(preparation, key=lambda w: (w.event.draft.priority, w.event_date, w.event.event_id))
            elapsed = (d-controlling.preparation_start).days
            p = controlling.preparation_days
            phase = "BASE" if elapsed < p//3 else "BUILD" if elapsed < p*2//3 else "EVENT_SPECIFIC"
            fraction = Decimal(1)
        if races:
            # Retain the constrained budget for conflict checking; an event reserves this day's session.
            race = min(races, key=lambda w: (w.event.draft.priority,w.event.event_id))
            controlling = race
            phase = "EVENT"
        controls.append(DayControl(d,phase,controlling.event.event_id if controlling else None,fraction,
                                   tuple(sorted({w.event.event_id for w in active}))))
    return tuple(controls), issues


def _baseline(season, settings, today, issues):
    usable = []
    seen = set()
    for w in settings.completed_weeks:
        if not isinstance(w, CompletedWeek):
            raise intent.SeasonError("Completed load evidence must use covered local weeks.")
        d = intent._day(w.monday, "History Monday")
        if d.weekday() or w.monday in seen or type(w.complete) is not bool or not w.evidence_ref.strip():
            raise intent.SeasonError("History requires unique Monday weeks and explicit coverage/provenance.")
        seen.add(w.monday)
        if w.running_duration_seconds is not None:
            intent._integer(w.running_duration_seconds, "Completed weekly duration", 1, 144000)
        if w.complete and w.running_duration_seconds is not None and today-timedelta(days=56) <= d and d+timedelta(days=6) < today:
            usable.append(w)
    usable.sort(key=lambda w:w.monday)
    if len(usable) >= 4:
        return sum(w.running_duration_seconds for w in usable[-4:]) // 4
    explicit = season.inputs.starting_duration_seconds
    if explicit is None:
        issues.append(ScheduleIssue("STARTING_LOAD_REQUIRED", "error",
            "Fewer than four covered completed weeks are available. Set an explicit starting weekly duration before generating."))
        return None
    issues.append(ScheduleIssue("READINESS_UNVERIFIED", "warning",
        f"Starting load uses the explicit {explicit//60}-minute weekly setting; {len(usable)} covered completed weeks are available. Progression/readiness is unverified."))
    return explicit


def _parameters(season, settings, today):
    method = MethodologyId(intent.METHODS[season.inputs.training_method])
    if method is MethodologyId.FITZGERALD_80_20_RUNNING_V1:
        if season.inputs.lthr is None or not settings.lthr_confirmed:
            raise intent.SeasonError("80/20 generation requires LTHR and explicit confirmation that it is a measured running threshold.")
        return {"schema_version":"season-f80-parameters.v1", "context":"STEADY_AEROBIC", "pace":{},
                "lthr":{"value":str(season.inputs.lthr), "evidence_class":"MEASURED", "quality":"VALID",
                        "source_record":"USER_CONFIRMED_RUNNING_LTHR", "confirmed_as_of":today.isoformat()}}
    if settings.maf_adjustment is None or not settings.maf_confirmed:
        raise intent.SeasonError("Maffetone generation requires an explicitly selected and confirmed adjustment.")
    p = derive(completed_age=season.inputs.age, calculation_date=today.isoformat(),
               selected_adjustment=settings.maf_adjustment, confirmed=True)
    if p.get("blocked"):
        raise intent.SeasonError("This age requires a separately reviewed manual MAF exception; ordinary yearly generation is unavailable.")
    return {"schema_version":"maf-parameters.v1", "formula_version":"MAF-180-RANGE-1.0.0",
            "calculation_date":today.isoformat(), "completed_age":season.inputs.age,
            "age_provenance":"USER_AGE_AS_OF_PREVIEW", "selected_adjustment":settings.maf_adjustment,
            "confirmed":True, "ceiling_bpm":p["ceiling_bpm"], "lower_bpm":p["lower_bpm"],
            "provenance":"USER_SELECTED", "state":"AEROBIC_BASE"}


def _rx(method, parameters, target="ZONE_2"):
    ref = content_sha256(parameters)
    if method is MethodologyId.FITZGERALD_80_20_RUNNING_V1:
        lthr = Decimal(parameters["lthr"]["value"])
        lo, hi = HR_FACTORS[target]
        return IntensityPrescription(method, FitzgeraldTarget("F80."+target),
            MetricRange(Metric.HEART_RATE,"bpm",lthr*lo,lthr*hi if hi else None,True,False),
            "F80-SEVEN-ZONE-1.0.0",Confidence.REDUCED,"VALID_HR",parameter_snapshot_ref=ref,
            confidence_reason="Confirmed LTHR; pace threshold unavailable.")
    return IntensityPrescription(method,MaffetoneTarget.MAF_AEROBIC_RANGE,
        MetricRange(Metric.HEART_RATE,"bpm",Decimal(parameters["lower_bpm"]),Decimal(parameters["ceiling_bpm"]),True,True),
        "MAF-180-RANGE-1.0.0",Confidence.HIGH,"VALID_HR",
        ceiling=MetricCeiling(Decimal(parameters["ceiling_bpm"]),True),parameter_snapshot_ref=ref)


def _duration_leaf(kind, seconds, prescription=None):
    return WorkoutSegment(kind,LoadMode.DURATION,seconds,duration_role=MeasureRole.AUTHORITATIVE,
                          purpose="CONTROLLED_TRAINING",prescription=prescription)


def _running_segments(seconds, method, parameters, quality):
    warm = min(720 if method is MethodologyId.MAFFETONE_RUNNING_V1 else 300, max(1, seconds//4))
    easy = _rx(method,parameters)
    if quality:
        hard = max(1, seconds//5)
        return (_duration_leaf(SegmentKind.WARMUP,warm,easy),
                _duration_leaf(SegmentKind.WORK,seconds-2*warm-hard,easy),
                _duration_leaf(SegmentKind.WORK,hard,_rx(method,parameters,"ZONE_3")),
                _duration_leaf(SegmentKind.COOLDOWN,warm,easy))
    return (_duration_leaf(SegmentKind.WARMUP,warm,easy),
            _duration_leaf(SegmentKind.WORK,seconds-2*warm,easy),
            _duration_leaf(SegmentKind.COOLDOWN,warm,easy))


def _weekday_order(inputs):
    template = build_weekly_schedule(inputs.run_days_per_week, inputs.long_run_day)
    long = intent.WEEKDAYS.index(inputs.long_run_day)
    return sorted(inputs.available_weekdays, key=lambda d:(d!=long,template[intent.WEEKDAYS[d]]=="rest",d))


def generate_season_schedule(season: intent.Season, events: tuple[intent.SeasonEvent, ...],
                             settings: GenerationSettings, *, today: date,
                             plan_id: str, revision_id: str,
                             parent: PlanRevisionCandidate | None = None) -> SeasonSchedule:
    """Generate one complete preview, including blocking findings and exact weekly load."""
    intent._validate_draft(intent.SeasonDraft(season.name,season.start_date,season.end_date,season.timezone,season.inputs))
    if type(today) is not date:
        raise intent.SeasonError("Generation requires a frozen local calendar date.")
    windows = event_windows(season,events)
    controls, issues = _timeline(season,windows)
    baseline = _baseline(season,settings,today,issues)
    parameters = canonical_value(parent.parameter_snapshot) if parent else _parameters(season,settings,today)
    method = MethodologyId(intent.METHODS[season.inputs.training_method])
    manifest = canonical_value(parent.manifest) if parent else canonical_value(next(d.manifest for d in STATIC_DEFINITIONS if d.methodology_id == method))
    if manifest["methodology_id"] != method.value:
        raise intent.SeasonError("Generation cannot switch the linked plan's methodology.")
    # Existing parameters stay fixed; unsupported adopted snapshots block composition.
    try:
        _rx(method,parameters)
    except (KeyError,ValueError,TypeError) as exc:
        raise intent.SeasonError("Linked parameters are unsupported by deterministic yearly generation; retain the existing schedule.") from exc
    issues.append(ScheduleIssue("DISTANCE_AND_TSS_UNKNOWN", "warning",
        "Training distance and TSS are unknown. Duration progression is checked; distance growth and TSS caps cannot be verified from missing data."))
    planned = {w.event_date:w for w in windows if w.event.draft.status=="planned"}
    groups = defaultdict(list)
    for c in controls:
        groups[monday(c.day)].append(c)
    rows, weeks = [], []
    envelope = baseline or 0
    last_normal = baseline or 0
    return_pending = False
    full_normal_seen = False
    cut_frequency = cutback_week_frequency(season.inputs.age)
    start, end = controls[0].day, controls[-1].day
    order = _weekday_order(season.inputs)
    long_dow = intent.WEEKDAYS.index(season.inputs.long_run_day)
    for index, (week, days) in enumerate(sorted(groups.items())):
        partial = days[0].day != week or days[-1].day != week+timedelta(days=6)
        disrupted = any(c.phase in {"RECOVERY","TAPER","EVENT"} for c in days)
        normal = not disrupted and all(c.phase in {"BASE","BUILD","EVENT_SPECIFIC"} for c in days)
        cutback = (index+1)%cut_frequency == 0 and not disrupted
        if any(c.phase=="RECOVERY" for c in days):
            return_pending = True
            envelope = min(last_normal,baseline or 0)
        if normal and not partial and not cutback:
            if return_pending:
                envelope = min(last_normal,baseline or 0)
                return_pending = False
            elif full_normal_seen:
                envelope = min(144000, envelope*105//100)
            full_normal_seen = True
        event_days = [c.day for c in days if c.day in planned]
        available_events = [d for d in event_days if d.weekday() in season.inputs.available_weekdays]
        for d in event_days:
            if d not in available_events:
                issues.append(ScheduleIssue("EVENT_UNAVAILABLE", "error", "Event date is unavailable. Update availability or move the event.", d.isoformat(), (planned[d].event.event_id,)))
        chosen = set(available_events)
        # Events consume the configured run-day slots; they never add a workout day.
        for dow in order:
            if len(chosen) >= season.inputs.run_days_per_week:
                break
            d = week + timedelta(days=dow)
            if start<=d<=end:
                chosen.add(d)
        nominal = set(order[:season.inputs.run_days_per_week])
        denominator = sum(2 if dow==long_dow else 1 for dow in nominal)
        # Use a full-week denominator even on a partial week; do not compress its load.
        budgets = {c.day:int(Decimal(envelope)*c.load_fraction*Decimal("0.75" if cutback else "1")*
                     Decimal(2 if c.day.weekday()==long_dow else 1)/Decimal(denominator)) for c in days}
        event_count = len(event_days)
        hard_cap = get_intensity_cap(season.inputs.age) if method is MethodologyId.FITZGERALD_80_20_RUNNING_V1 else 1
        quality_day = None
        if normal and not cutback and not event_count and hard_cap and method is MethodologyId.FITZGERALD_80_20_RUNNING_V1:
            quality_day = next((c.day for c in days if c.day in chosen and c.day.weekday()!=long_dow and budgets[c.day]>=600 and c.phase in {"BUILD","EVENT_SPECIFIC"}),None)
        strength_added = False
        week_rows = []
        for c in days:
            d = c.day
            meta = {"schema_version":"season-workout.v1", "season_id":season.season_id,
                    "event_id":c.event_id,"influencing_event_ids":c.influencing_event_ids,
                    "generator_version":GENERATOR_VERSION,"policy_version":POLICY_VERSION,
                    "cutback":cutback,"description_provenance":"ORIGINAL"}
            quality, long = False, False
            if d in planned:
                w = planned[d]
                e = w.event
                family,sport,title = "EVENT_DAY",Sport.RUNNING,e.draft.name
                duration = e.draft.target_seconds
                if e.draft.target_speed_mps is not None:
                    duration = max(1,int(Decimal(e.distance_metres)/Decimal(e.draft.target_speed_mps)))
                segments = (WorkoutSegment(SegmentKind.EVENT,LoadMode.DISTANCE,duration,e.distance_metres,
                    MeasureRole.ESTIMATED if duration else None,MeasureRole.AUTHORITATIVE,
                    purpose="EVENT_PARTICIPATION",prescription=_rx(method,parameters,"ZONE_3" if e.draft.goal_intent=="PERFORMANCE" and method is MethodologyId.FITZGERALD_80_20_RUNNING_V1 else "ZONE_2")),)
                meta.update(event_id=e.event_id,event_priority=e.draft.priority,goal_intent=e.draft.goal_intent,
                            target_seconds=e.draft.target_seconds,target_speed_mps=e.draft.target_speed_mps,
                            event_type=e.draft.event_type,terrain=e.draft.terrain,course_notes=e.draft.course_notes)
                others = [x for x in windows if x.event.event_id!=e.event_id]
                a_taper = [x for x in others if x.event.draft.status=="planned" and x.event.draft.priority=="A" and x.taper_start<=d<x.event_date]
                if e.draft.priority in {"B","C"} and a_taper:
                    # Completion at easy native intensity is the only supported taper participation.
                    if e.draft.goal_intent!="COMPLETION" or e.distance_metres>5000 or duration is None or duration>budgets[d]:
                        issues.append(ScheduleIssue("EVENT_EXCEEDS_A_TAPER", "error",
                            f"{title} cannot fit the A taper easy-session budget ({budgets[d]//60} minutes). Move, skip or cancel it.",d.isoformat(),tuple([e.event_id]+[x.event.event_id for x in a_taper])))
                    else:
                        issues.append(ScheduleIssue("EASY_EVENT_IN_A_TAPER", "warning",f"{title} replaces an easy taper session and must stay at the prescribed easy intensity.",d.isoformat(),(e.event_id,)))
                issues.append(ScheduleIssue("EVENT_LOAD_ESTIMATED" if duration else "EVENT_DURATION_UNKNOWN", "warning",
                    f"{title}: event distance is known; duration is {'a target estimate' if duration else 'unknown'}. TSS is unknown. Native HR targets do not guarantee the finish-time goal.",d.isoformat(),(e.event_id,)))
                description = "Event replaces the day's training session. Follow native HR targets; target time/pace is retained as intent."
            elif d in chosen and budgets[d]>=3 and c.load_fraction>0:
                sport = Sport.RUNNING
                quality = d==quality_day
                long = d.weekday()==long_dow and c.phase not in {"TAPER","RECOVERY"}
                family = "MODERATE_DEVELOPMENT" if quality else "LONG_AEROBIC" if long else "RECOVERY" if c.phase=="RECOVERY" else "AEROBIC"
                title = "Controlled tempo" if quality else "Long aerobic run" if long else "Recovery run" if c.phase=="RECOVERY" else "Easy aerobic run"
                segments = _running_segments(budgets[d],method,parameters,quality)
                description = "Original season prescription from continuous weekly load and event phase. Keep warmup, work and cooldown within their native HR targets."
            elif d.weekday() in season.inputs.available_weekdays and c.phase in {"BASE","BUILD","EVENT_SPECIFIC","MAINTENANCE"} and not strength_added and d not in chosen:
                family,sport,title = "STRENGTH",Sport.STRENGTH,"Strength maintenance"
                segments = (_duration_leaf(SegmentKind.WORK,1200),)
                description = "Controlled strength maintenance; no endurance targets."
                strength_added = True
            elif d.weekday() in season.inputs.available_weekdays and c.phase=="RECOVERY" and c.load_fraction>0 and d not in chosen:
                family,sport,title = "MOBILITY",Sport.MOBILITY,"Gentle mobility"
                segments = (_duration_leaf(SegmentKind.WORK,600),)
                description = "Gentle mobility during recovery; no endurance targets."
            else:
                family,sport,title = "REST",Sport.REST,"Rest"
                segments = (WorkoutSegment(SegmentKind.NON_TRAINING,LoadMode.OPEN,purpose="REST"),)
                description = "Rest day; no training duration or endurance targets."
            workout_id = content_sha256({"revision_id":revision_id,"day":d.isoformat(),"family":family})[:32]
            workout = PlannedWorkout(d,sport,family,"SEASON_"+c.phase,description,segments,
                event_flag=d in planned,workout_id=workout_id,ordinal=len(rows),title=title,phase=c.phase,
                quality_flag=quality,long_run_flag=long,metadata=meta)
            rows.append(workout)
            week_rows.append(workout)
        runs = [w for w in week_rows if w.sport is Sport.RUNNING and not w.event_flag]
        seconds = sum(s.duration_seconds or 0 for w in runs for s in w.segments)
        races = [w for w in week_rows if w.event_flag]
        event_durations = [s.duration_seconds for w in races for s in w.segments]
        weeks.append(WeekLoad(week.isoformat(),partial,envelope,seconds,
            None if any(s is None for s in event_durations) else sum(event_durations),
            sum(s.distance_metres or 0 for w in races for s in w.segments),
            sum(w.event_flag or w.quality_flag for w in week_rows),cutback))
        if normal and not partial and not cutback:
            last_normal = seconds
        if partial:
            issues.append(ScheduleIssue("PARTIAL_WEEK", "warning", "Partial local Monday-Sunday week; load is not compared as a full week.",week.isoformat()))
    snapshot = {"schema_version":"season-goal.v1","sport":"RUNNING","season_id":season.season_id,
                "name":season.name,"start_date":season.start_date,"end_date":season.end_date,
                "timezone":season.timezone,"events":[asdict(e) for e in sorted(events,key=lambda e:(e.draft.event_date,e.event_id))],
                "effective_windows":[canonical_value(w) for w in windows]}
    candidate = PlanRevisionCandidate(revision_id,plan_id,parent.revision_id if parent else None,
        RevisionReason.REGENERATION if parent else RevisionReason.INITIAL_GENERATION,
        manifest,snapshot,{"age":season.inputs.age},parameters,tuple(rows),
        provenance={"generator":GENERATOR_VERSION,"policy":POLICY_VERSION,"local_today":today.isoformat()},
        constraints={"schema_version":"season-constraints.v1","input_version":season.input_version,
                     "inputs":asdict(season.inputs),"settings":asdict(settings),"baseline_seconds":baseline,
                     "week_loads":[asdict(w) for w in weeks]},
        change_summary={"mode":"FULL_PREVIEW" if parent else "INITIAL_FULL",
                        "added_workout_ids":[w.workout_id for w in rows],"preserved_workout_ids":[]})
    issues.extend(validate_season_workload(season,events,candidate,weeks,baseline))
    method_findings = validate_methodology_candidate(methodology_id=method.value,candidate=candidate)
    for f in method_findings:
        if f.severity.value in {"ERROR","WARNING"}:
            issues.append(ScheduleIssue(f.rule_id,f.severity.value.lower(),f.message,
                next((w.scheduled_date.isoformat() for w in rows if w.workout_id==f.workout_id),None)))
    summary = {"schema_version":"season-validation.v1","issues":[asdict(i) for i in issues],
               "methodology_findings":[f.to_dict() for f in method_findings],
               "accepted":not any(i.severity=="error" for i in issues)}
    candidate = replace(candidate,validation_summary=summary,
                        approval_state=ApprovalState.VALIDATED if summary["accepted"] else ApprovalState.CANDIDATE)
    return SeasonSchedule(candidate,controls,windows,tuple(weeks),tuple(issues),baseline)


def validate_season_workload(season, events, candidate, weeks, baseline):
    """Validate combined local weeks; unknown event duration never becomes zero load."""
    issues = []
    by_day, by_week = defaultdict(list),defaultdict(list)
    expected_events = {e.event_id:e for e in events if e.draft.status=="planned"}
    found = defaultdict(list)
    for w in candidate.workouts:
        d = w.scheduled_date
        if not season.start_date<=d.isoformat()<=season.end_date:
            issues.append(ScheduleIssue("OUTSIDE_SEASON","error","Workout date falls outside the season.",d.isoformat()))
        by_day[d].append(w)
        by_week[monday(d)].append(w)
        if w.event_flag:
            found[(w.metadata or {}).get("event_id")].append(w)
    for eid,e in expected_events.items():
        matches = found[eid]
        if len(matches)!=1 or matches[0].scheduled_date.isoformat()!=e.draft.event_date or sum(s.distance_metres or 0 for s in matches[0].segments)!=e.distance_metres:
            issues.append(ScheduleIssue("EVENT_NOT_REPRESENTED","error","Each planned event requires exactly one correctly dated and distanced event session.",e.draft.event_date,(eid,)))
    if set(found)-set(expected_events):
        issues.append(ScheduleIssue("UNEXPECTED_EVENT","error","The schedule contains an event absent from planned intent."))
    hard_dates = set()
    for d,rows in sorted(by_day.items()):
        active = [w for w in rows if w.sport is not Sport.REST]
        if len(active)>3:
            issues.append(ScheduleIssue("DAILY_SESSION_CAP","error","More than three active sessions are scheduled in a day.",d.isoformat()))
        if active and any(w.sport is Sport.REST for w in rows):
            issues.append(ScheduleIssue("REST_CONFLICT","error","Rest reserves its date and cannot share active sessions.",d.isoformat()))
        hard = [w for w in rows if w.event_flag or w.quality_flag]
        if len(hard)>1:
            issues.append(ScheduleIssue("DAILY_HARD_CAP","error","Only one hard/event session is permitted per day.",d.isoformat()))
        if hard:
            hard_dates.add(d)
        if active and d.weekday() not in season.inputs.available_weekdays:
            issues.append(ScheduleIssue("UNAVAILABLE_DAY","error","Training falls on an unavailable day.",d.isoformat()))
    for d in hard_dates:
        if d-DAY in hard_dates:
            issues.append(ScheduleIssue("CONSECUTIVE_HARD_DAYS","error","Hard/event days must not be consecutive.",d.isoformat()))
    last_normal = None
    duration_envelope = baseline
    return_pending = False
    last_distance = None
    for load in weeks:
        rows = by_week[date.fromisoformat(load.monday)]
        runs = [w for w in rows if w.sport is Sport.RUNNING]
        if len({w.scheduled_date for w in runs})>season.inputs.run_days_per_week:
            issues.append(ScheduleIssue("RUN_DAY_CAP","error","Combined events and training exceed configured weekly run days.",load.monday))
        hard_cap = get_intensity_cap(season.inputs.age) if season.inputs.training_method=="eighty_twenty" else 1
        if sum(w.event_flag or w.quality_flag for w in runs)>hard_cap:
            issues.append(ScheduleIssue("WEEKLY_HARD_CAP","error",f"Combined event and quality load exceeds the {hard_cap}-day weekly cap.",load.monday))
        if sum(w.sport is Sport.STRENGTH for w in rows)>3:
            issues.append(ScheduleIssue("STRENGTH_CAP","error","More than three strength sessions in a local week.",load.monday))
        nonrace = [w for w in rows if not w.event_flag and w.sport is not Sport.REST]
        if sum(s.duration_seconds or 0 for w in nonrace for s in w.segments)>144000:
            issues.append(ScheduleIssue("WEEKLY_DURATION_CAP","error","Non-event training exceeds 40 hours in a local week.",load.monday))
        # TSS stays unknown in canonical generation, with no invented estimates.
        if any(w.phase=="RECOVERY" for w in rows):
            return_pending = True
        training_runs = [w for w in runs if not w.event_flag]
        known_duration = all(s.duration_seconds is not None for w in training_runs for s in w.segments)
        actual_duration = sum(s.duration_seconds or 0 for w in training_runs for s in w.segments)
        known_distance = bool(training_runs) and all(s.distance_metres is not None for w in training_runs for s in w.segments)
        actual_distance = sum(s.distance_metres or 0 for w in training_runs for s in w.segments) if known_distance else None
        normal = all(w.phase in {"BASE","BUILD","EVENT_SPECIFIC"} for w in rows)
        if normal and not load.partial and not load.cutback:
            if return_pending:
                limit = min(last_normal or baseline or 0,baseline or 0)
                return_pending = False
            else:
                limit = int(Decimal(duration_envelope or 0)*Decimal("1.1"))
            if not known_duration:
                issues.append(ScheduleIssue("DURATION_UNKNOWN", "warning", "Duration progression cannot be checked with missing running duration.",load.monday))
            if known_duration and actual_duration>limit:
                issues.append(ScheduleIssue("DURATION_GROWTH","error","Build duration exceeds the 10%/post-recovery return envelope.",load.monday))
            if actual_distance is not None and last_distance is not None and actual_distance>last_distance*110//100:
                issues.append(ScheduleIssue("DISTANCE_GROWTH", "error", "Build distance exceeds 10% growth from the last complete normal week.",load.monday))
            if known_duration:
                duration_envelope = actual_duration
                last_normal = actual_duration
            last_distance = actual_distance
    return issues
