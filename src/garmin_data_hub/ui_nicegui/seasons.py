"""Season calendar and planning-intent editor."""
from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from garmin_data_hub.services import season_plans as plans
from garmin_data_hub.services import season_generation as generation
from garmin_data_hub.services.season_schedule import GenerationSettings
from garmin_data_hub.services.baseline_plan_builder import BASELINE_DISTANCE_OPTIONS
from garmin_data_hub.ui_nicegui.layout import page_heading, render_shell


def _integer(value, label: str, *, optional: bool = False) -> int | None:
    if optional and value in (None, ""):
        return None
    try:
        integer = int(value)
        if isinstance(value, bool) or Decimal(str(value)) != integer:
            raise ValueError()
        return integer
    except (ValueError, TypeError, ArithmeticError) as exc:
        raise plans.SeasonError(f"{label} must be a whole number.") from exc


def _seconds(value: str, *, pace: bool = False) -> int | None:
    if not value.strip():
        return None
    parts = value.strip().split(":")
    expected = 2 if pace else 3
    if len(parts) != expected or any(not p.isdigit() for p in parts):
        raise plans.SeasonError("Use MM:SS for pace or HH:MM:SS for finish time.")
    numbers = [int(p) for p in parts]
    if any(n >= 60 for n in numbers[1:]):
        raise plans.SeasonError("Minutes and seconds after ':' must be below 60.")
    result = sum(n * 60**i for i, n in enumerate(reversed(numbers)))
    if result <= 0:
        raise plans.SeasonError("Target time or pace must be positive.")
    return result


def register_seasons_page(db_path: Path, *, sandboxed: bool) -> None:
    from nicegui import ui

    @ui.page("/seasons")
    def seasons_page(season_id: str = "") -> None:
        render_shell("Seasons", db_path, sandboxed=sandboxed)
        ui.add_css(".season-form { grid-template-columns: repeat(2,minmax(0,1fr)); }"
                   "@media(max-width:600px) { .season-form { grid-template-columns:minmax(0,1fr); } }")
        selected = {"id": season_id}

        def show_error(error: Exception, label=None) -> None:
            message = str(error) if isinstance(error, (plans.SeasonError, ValueError)) else "Database unavailable. Refresh and try again."
            if label is not None:
                label.text = message
            ui.notify(message, type="negative")

        def season_editor(season: plans.Season | None = None) -> None:
            today = datetime.now(ZoneInfo("America/New_York")).date()
            original = plans.SeasonDraft(season.name, season.start_date, season.end_date, season.timezone, season.inputs) if season else plans.SeasonDraft(
                "", today.isoformat(), (today + timedelta(days=365)).isoformat(), "America/New_York")
            with ui.dialog() as dialog, ui.card().classes("w-full max-w-2xl max-h-[90vh] overflow-y-auto"):
                ui.label("Edit season" if season else "New season").classes("text-xl font-semibold")
                with ui.grid().classes("season-form w-full gap-3"):
                    name = ui.input("Season name", value=original.name).classes("w-full")
                    tz = ui.input("Timezone", value=original.timezone).classes("w-full").props('hint="IANA name, e.g. America/New_York"')
                    start = ui.input("Season start", value=original.start_date).props("type=date").classes("w-full")
                    end = ui.input("Season end", value=original.end_date).props("type=date").classes("w-full")
                    age = ui.number("Age", value=original.inputs.age, min=10, max=100, step=1).classes("w-full")
                    method = ui.select({"eighty_twenty": "80/20 running", "maffetone": "Maffetone running"}, value=original.inputs.training_method, label="Training method").classes("w-full")
                    days = ui.number("Run days per week", value=original.inputs.run_days_per_week, min=1, max=7, step=1).classes("w-full")
                    long_day = ui.select(list(plans.WEEKDAYS), value=original.inputs.long_run_day, label="Long-run day").classes("w-full")
                    available = ui.select(dict(enumerate(plans.WEEKDAYS)), multiple=True, value=list(original.inputs.available_weekdays), label="Available days").props("use-chips").classes("w-full")
                    duration = ui.number("Starting weekly minutes (optional)", value=None if original.inputs.starting_duration_seconds is None else original.inputs.starting_duration_seconds / 60, min=1, max=2400).classes("w-full")
                    hrmax = ui.number("HRmax (optional)", value=original.inputs.hrmax, min=80, max=250, step=1).classes("w-full")
                    lthr = ui.number("LTHR (optional)", value=original.inputs.lthr, min=80, max=220, step=1).classes("w-full")
                if season and season.plan_id:
                    tz.disable()
                    method.disable()
                error = ui.label("").classes("text-red-800 whitespace-normal break-words")

                def save() -> None:
                    try:
                        seconds = None if duration.value in (None, "") else _integer(Decimal(str(duration.value)) * 60, "Weekly duration")
                        inputs = plans.SeasonInputs(_integer(age.value, "Age"), method.value,
                                                    _integer(days.value, "Run days"), long_day.value,
                                                    tuple(available.value or ()), seconds,
                                                    _integer(hrmax.value, "HRmax", optional=True),
                                                    _integer(lthr.value, "LTHR", optional=True))
                        draft = plans.SeasonDraft(name.value, start.value, end.value, tz.value, inputs)
                        result = plans.update_season(db_path, season.season_id, draft, expected_version=season.input_version) if season else plans.create_season(db_path, draft)
                    except (plans.SeasonError, sqlite3.Error, OSError, ValueError) as exc:
                        show_error(exc, error)
                        return
                    selected["id"] = result.season_id
                    dialog.close()
                    if season:
                        content.refresh()
                    else:
                        ui.navigate.to(f"/seasons?season_id={result.season_id}")
                    ui.notify("Season saved.", type="positive")

                with ui.row().classes("w-full justify-end"):
                    ui.button("Close season editor", on_click=dialog.close).props("flat")
                    ui.button("Save season", on_click=save)
            dialog.open()

        def event_editor(season: plans.Season, event: plans.SeasonEvent | None = None) -> None:
            today = datetime.now(ZoneInfo(season.timezone)).date().isoformat()
            draft = event.draft if event else plans.EventDraft("", min(max(today, season.start_date), season.end_date))
            pace_seconds = None if draft.target_speed_mps is None else round(Decimal(1000) / Decimal(draft.target_speed_mps))
            original_pace = "" if pace_seconds is None else f"{pace_seconds // 60:02}:{pace_seconds % 60:02}"
            target = draft.target_seconds
            original_time = "" if target is None else f"{target // 3600:02}:{target // 60 % 60:02}:{target % 60:02}"
            with ui.dialog() as dialog, ui.card().classes("w-full max-w-2xl max-h-[90vh] overflow-y-auto"):
                ui.label("Edit event" if event else "Add event").classes("text-xl font-semibold")
                with ui.grid().classes("season-form w-full gap-3"):
                    name = ui.input("Event name", value=draft.name).classes("w-full")
                    day = ui.input("Event date", value=draft.event_date).props("type=date").classes("w-full")
                    distance = ui.select(BASELINE_DISTANCE_OPTIONS, value=draft.distance_code, label="Event distance").classes("w-full")
                    kind = ui.select({"road": "Road", "trail": "Trail"}, value=draft.event_type, label="Event type").classes("w-full")
                    priority = ui.select({"A": "A · Primary goal", "B": "B · Supporting event", "C": "C · Training event"}, value=draft.priority, label="Event priority").classes("w-full")
                    status = ui.select({v: v.title() for v in ("planned", "completed", "skipped", "cancelled")}, value=draft.status, label="Event status").classes("w-full")
                    goal = ui.select({"COMPLETION": "Completion", "PERFORMANCE": "Performance"}, value=draft.goal_intent, label="Event goal").classes("w-full")
                    finish = ui.input("Target finish time (HH:MM:SS)", value=original_time).classes("w-full")
                    pace = ui.input("Target pace (MM:SS per km)", value=original_pace).classes("w-full")
                    terrain = ui.input("Terrain", value=draft.terrain).classes("w-full")
                    taper = ui.number("Taper days (optional)", value=draft.taper_days, min=0, max=28, step=1).classes("w-full")
                    recovery = ui.number("Recovery days (optional)", value=draft.recovery_days, min=plans.RECOVERY_DAYS[draft.distance_code], max=42, step=1).classes("w-full")
                notes = ui.textarea("Course notes", value=draft.course_notes).classes("w-full")
                ui.label("Use one target: finish time or pace. Recovery cannot be shorter than the distance default.").classes("text-sm text-slate-600")
                error = ui.label("").classes("text-red-800 whitespace-normal break-words")

                def save() -> None:
                    try:
                        speed = draft.target_speed_mps if pace.value == original_pace else None
                        if pace.value and pace.value != original_pace:
                            speed = format(Decimal(1000) / _seconds(pace.value, pace=True), "f")
                        updated = plans.EventDraft(name.value, day.value, distance.value, priority.value,
                                                   kind.value, status.value, goal.value, _seconds(finish.value),
                                                   speed, terrain.value, notes.value,
                                                   _integer(taper.value, "Taper days", optional=True),
                                                   _integer(recovery.value, "Recovery days", optional=True))
                        plans.save_event(db_path, season.season_id, updated, expected_version=season.input_version,
                                         event_id=None if event is None else event.event_id)
                    except (plans.SeasonError, sqlite3.Error, OSError, ValueError) as exc:
                        show_error(exc, error)
                        return
                    dialog.close()
                    content.refresh()
                    ui.notify("Event saved.", type="positive")

                with ui.row().classes("w-full justify-end"):
                    ui.button("Close event editor", on_click=dialog.close).props("flat")
                    ui.button("Save event", on_click=save)
            dialog.open()

        def remove_event(season: plans.Season, event: plans.SeasonEvent) -> None:
            with ui.dialog() as dialog, ui.card().classes("w-full max-w-lg"):
                ui.label(f"Remove {event.draft.name}?").classes("text-xl font-semibold break-words")
                ui.label("This event will be cancelled; the linked schedule is retained." if season.plan_id else "This draft event will be deleted.")
                error = ui.label("").classes("text-red-800 break-words")

                def remove() -> None:
                    try:
                        plans.remove_event(db_path, season.season_id, event.event_id, expected_version=season.input_version)
                    except (plans.SeasonError, sqlite3.Error, OSError) as exc:
                        show_error(exc, error)
                        return
                    dialog.close()
                    content.refresh()
                with ui.row():
                    ui.button("Keep event", on_click=dialog.close).props("flat")
                    ui.button("Confirm removal", on_click=remove, color="negative")
            dialog.open()

        def archive(season: plans.Season) -> None:
            try:
                plans.set_archived(db_path, season.season_id, expected_version=season.input_version, archived=season.status != "archived")
            except (plans.SeasonError, sqlite3.Error, OSError) as exc:
                show_error(exc)
                return
            content.refresh()

        def generation_editor(season: plans.Season) -> None:
            with ui.dialog() as dialog, ui.card().classes("w-full max-w-xl max-h-[90vh] overflow-y-auto"):
                ui.label("Generate season preview").classes("text-xl font-semibold")
                ui.label("Review one continuous schedule using every saved event. Generation does not change your calendar.").classes("break-words")
                if season.current_revision_id:
                    ui.label("This preview uses the linked plan’s saved intensity parameters. Replacing its workouts is not available yet.").classes("break-words")
                    confirmation = None
                    adjustment = None
                elif season.inputs.training_method == "eighty_twenty":
                    ui.label(f"Saved running LTHR: {season.inputs.lthr or 'not set'} bpm. Set it in Edit season if needed.")
                    confirmation = ui.checkbox("I confirm this LTHR is a measured running threshold.").props('aria-label="I confirm this LTHR is a measured running threshold."')
                    adjustment = None
                else:
                    adjustment = ui.select({-10:"-10 bpm",-5:"-5 bpm",0:"0 bpm",5:"+5 bpm"}, label="MAF adjustment").classes("w-full")
                    confirmation = ui.checkbox("I confirm the selected MAF adjustment.").props('aria-label="I confirm the selected MAF adjustment."')
                ui.label("Starting duration: " + (f"{season.inputs.starting_duration_seconds / 60:g} minutes/week" if season.inputs.starting_duration_seconds else "not set; add an explicit value in Edit season.")).classes("break-words")
                error = ui.label("").classes("text-red-800 break-words")

                def generate() -> None:
                    try:
                        settings = GenerationSettings(lthr_confirmed=confirmation.value if confirmation is not None and adjustment is None else False,
                            maf_adjustment=adjustment.value if adjustment is not None else None,
                            maf_confirmed=confirmation.value if confirmation is not None and adjustment is not None else False)
                        preview = generation.preview_season(db_path,season.season_id,settings)
                    except (plans.SeasonError,sqlite3.Error,OSError,ValueError) as exc:
                        show_error(exc,error)
                        return
                    dialog.close()
                    review_preview(preview)
                with ui.row().classes("w-full justify-end"):
                    ui.button("Close generation settings",on_click=dialog.close).props("flat")
                    ui.button("Generate preview",on_click=generate)
            dialog.open()

        def review_preview(preview: generation.SeasonPreview) -> None:
            schedule = preview.schedule
            with ui.dialog() as dialog, ui.card().classes("w-full max-w-5xl max-h-[90vh] overflow-y-auto min-w-0"):
                ui.label("Season schedule preview").classes("text-xl font-semibold")
                ui.label(f"{preview.season.start_date} → {preview.season.end_date} · {len(schedule.candidate.workouts)} sessions · {len(preview.events)} saved events").classes("break-words")
                ui.label("Full generation preview" if preview.season.plan_id else "Initial schedule: all sessions are additions; no workouts are removed or preserved.").classes("text-slate-600 break-words")
                for message in preview.apply_blockers:
                    ui.label(message).classes("text-amber-900 break-words")
                ui.label("Conflicts and warnings").classes("font-semibold")
                with ui.expansion(f"Review {len(schedule.errors)} conflicts and {len(schedule.warnings)} warnings",value=True).classes("w-full"):
                    for issue in schedule.errors + schedule.warnings:
                        ui.label((f"{issue.date} · " if issue.date else "") + issue.message).classes("break-words text-red-800" if issue.severity=="error" else "break-words text-amber-900")
                phases = []
                for c in schedule.controls:
                    key = (c.phase,c.event_id)
                    if phases and phases[-1]["key"]==key:
                        phases[-1]["end"] = c.day.isoformat()
                    else:
                        phases.append({"key":key,"start":c.day.isoformat(),"end":c.day.isoformat(),"phase":c.phase,
                                       "event":next((e.draft.name for e in preview.events if e.event_id==c.event_id),"Maintenance")})
                with ui.expansion("Training phases",value=True).classes("w-full"):
                    for phase in phases:
                        ui.label(f'{phase["start"]} → {phase["end"]} · {phase["phase"].replace("_"," ").title()} · {phase["event"]}').classes("break-words text-sm")
                week_rows = [{"week":w.monday,"coverage":"Partial" if w.partial else "Full",
                    "minutes":round(w.training_seconds/60,1),"event_minutes":None if w.event_duration_seconds is None else round(w.event_duration_seconds/60,1),
                    "event_km":round(w.event_distance_metres/1000,2),"hard":w.hard_days,"cutback":"Yes" if w.cutback else ""} for w in schedule.weeks]
                ui.label("Local Monday–Sunday workload").classes("font-semibold")
                ui.label("Training minutes exclude event load, shown separately. Unknown event minutes and TSS remain unknown.").classes("text-sm break-words")
                ui.table(columns=[{"name":k,"label":v,"field":k} for k,v in [("week","Week starting"),("coverage","Coverage"),("minutes","Training min"),("event_minutes","Event min (estimated)"),("event_km","Event km"),("hard","Hard days"),("cutback","Cutback")]],
                         rows=week_rows,row_key="week",pagination=8).classes("w-full")
                ui.label("Proposed sessions").classes("font-semibold")
                ui.table(columns=[{"name":k,"label":v,"field":k} for k,v in [("date","Date"),("title","Session"),("sport","Sport"),("phase","Phase"),("minutes","Minutes"),("targets","Native targets")]],
                    rows=[{"id":w.workout_id,"date":w.scheduled_date.isoformat(),"title":w.title,"sport":w.sport.value,"phase":w.phase,
                           "minutes":round(sum(s.duration_seconds for s in w.segments)/60,1) if all(s.duration_seconds is not None for s in w.segments) else None,
                           "targets":" · ".join(sorted({s.prescription.native_target.value for s in w.segments if s.prescription}))} for w in schedule.candidate.workouts],
                    row_key="id",pagination=10).classes("w-full")
                acknowledgement = ui.checkbox("I reviewed the schedule and acknowledge all warnings.").props('aria-label="I reviewed the schedule and acknowledge all warnings."').classes("w-full")
                error = ui.label("").classes("text-red-800 break-words")

                def apply() -> None:
                    try:
                        if not acknowledgement.value:
                            raise plans.SeasonError("Review and acknowledge the schedule warnings before applying.")
                        result = generation.apply_season_preview(db_path,preview,approved_by="LOCAL_USER",
                                                                 acknowledge_warnings=acknowledgement.value)
                    except (plans.SeasonError,sqlite3.Error,OSError,ValueError) as exc:
                        show_error(exc,error)
                        return
                    dialog.close()
                    content.refresh()
                    ui.notify("Season schedule applied." if result.status=="applied" else "This preview was already applied.",type="positive")
                with ui.row().classes("w-full justify-end"):
                    ui.button("Close preview",on_click=dialog.close).props("flat")
                    button = ui.button("Apply reviewed season",on_click=apply)
                    if not preview.can_apply:
                        button.disable()
            dialog.open()

        @ui.refreshable
        def content() -> None:
            try:
                seasons = plans.list_seasons(db_path)
                season = next((s for s in seasons if s.season_id == selected["id"]), None)
                if not selected["id"] and seasons:
                    season = seasons[0]
                    selected["id"] = season.season_id
                events = plans.list_events(db_path, season.season_id) if season else ()
            except (plans.SeasonError, sqlite3.Error, OSError) as exc:
                show_error(exc)
                ui.label("Season data could not be loaded.")
                ui.button("Refresh seasons", on_click=content.refresh)
                return
            with ui.row().classes("w-full items-center gap-3"):
                ui.button("New season", icon="add", on_click=lambda: season_editor())
                ui.button("Refresh seasons", icon="refresh", on_click=content.refresh).props("outline")
                if seasons:
                    ui.select({s.season_id: f"{s.name} · {s.status}" for s in seasons}, value=None if season is None else season.season_id,
                              label="Season", on_change=lambda e: ui.navigate.to(f"/seasons?season_id={e.value}")).classes("w-full max-w-lg")
            if season is None:
                ui.label("Season not found. Choose one from the list." if selected["id"] else "Create a season to organize your events and training availability.")
                return
            with ui.card().classes("gdh-card w-full min-w-0"):
                ui.label(season.name).classes("text-2xl font-semibold break-words")
                ui.label(f"{season.start_date} → {season.end_date} · {season.timezone}").classes("text-slate-600 break-words")
                ui.label(f"{season.inputs.run_days_per_week} run days/week · Long run {season.inputs.long_run_day} · {'80/20' if season.inputs.training_method == 'eighty_twenty' else 'Maffetone'}").classes("break-words")
                ui.label("Available: " + ", ".join(plans.WEEKDAYS[d] for d in season.inputs.available_weekdays)).classes("break-words")
                ui.label(f"Planning version {season.input_version}" + (f" · Linked revision {season.current_revision_id}" if season.plan_id else " · No linked schedule")).classes("text-sm text-slate-500 break-all")
                if season.plan_id:
                    ui.label("Event changes are saved as planning intent. The linked schedule stays in place until regeneration is available.").classes("text-amber-900 break-words")
                with ui.row().classes("gap-2"):
                    if season.status != "archived":
                        ui.button("Edit season", on_click=lambda: season_editor(season)).props("outline")
                    if season.status != "archived":
                        ui.button("Preview yearly schedule", icon="preview", on_click=lambda: generation_editor(season))
                    ui.button("Restore season" if season.status == "archived" else "Archive season", on_click=lambda: archive(season)).props("flat")
            with ui.row().classes("w-full items-center justify-between"):
                ui.label("Event calendar").classes("text-xl font-semibold")
                if season.status != "archived":
                    ui.button("Add event", icon="add", on_click=lambda: event_editor(season))
            ui.label("A: primary peak goal · B: supporting preparation · C: training event").classes("text-sm text-slate-600")
            if not events:
                ui.label("No events yet.")
            for event in events:
                draft = event.draft
                with ui.card().classes("gdh-card w-full min-w-0"):
                    with ui.row().classes("w-full items-start justify-between gap-3 flex-col sm:flex-row"):
                        with ui.column().classes("gap-1 min-w-0 w-full sm:flex-1"):
                            with ui.row().classes("items-center"):
                                ui.badge(draft.priority, color={"A": "blue", "B": "teal", "C": "grey"}[draft.priority])
                                ui.label(draft.name).classes("font-semibold break-words min-w-0 flex-1")
                            ui.label(f"{draft.event_date} · {BASELINE_DISTANCE_OPTIONS[draft.distance_code]} · {draft.event_type.title()} · {draft.status.title()}").classes("break-words")
                            ui.label(draft.goal_intent.title()).classes("text-sm text-slate-600")
                            if draft.target_seconds is not None:
                                t = draft.target_seconds
                                ui.label(f"Target finish: {t // 3600:02}:{t // 60 % 60:02}:{t % 60:02}")
                            if draft.target_speed_mps is not None:
                                p = round(Decimal(1000) / Decimal(draft.target_speed_mps))
                                ui.label(f"Target pace: {p // 60:02}:{p % 60:02} /km")
                            if draft.terrain or draft.course_notes:
                                ui.label(" · ".join(filter(None, (draft.terrain, draft.course_notes)))).classes("text-sm whitespace-pre-wrap break-words")
                            if draft.taper_days is not None or draft.recovery_days is not None:
                                ui.label(f"Taper: {draft.taper_days if draft.taper_days is not None else 'default'} days · Recovery: {draft.recovery_days if draft.recovery_days is not None else 'default'} days").classes("text-sm")
                        if season.status != "archived" and draft.status != "completed":
                            with ui.row().classes("gap-1 shrink-0"):
                                ui.button("Edit", on_click=lambda e=event: event_editor(season, e)).props(f'outline aria-label="Edit event {event.event_id}"').tooltip(f"Edit {draft.name}")
                                ui.button("Remove", on_click=lambda e=event: remove_event(season, e)).props(f'flat aria-label="Remove event {event.event_id}"').tooltip(f"Remove {draft.name}")
            applications = generation.list_applications(db_path,season.season_id)
            if applications:
                with ui.expansion("Applied season revisions", icon="history").classes("gdh-card w-full"):
                    for application in applications:
                        ui.label(f'{application["applied_at_utc"]} · {application["range_start"]} → {application["range_end"]} · {application["mode"]}').classes("break-words")
                        ui.label(f'Revision {application["resulting_revision_id"]} · Reviewed by {application["approved_by"]}').classes("text-sm break-all")
                        review = json.loads(application["review_json"])
                        ui.label(review["rationale"]).classes("text-sm break-words")
                        with ui.expansion("Acknowledged warnings").classes("w-full"):
                            for warning in review["warnings"]:
                                ui.label(warning["message"]).classes("text-sm text-amber-900 break-words")
            if season.status != "archived" and not season.plan_id:
                with ui.expansion("Link an existing plan", icon="link").classes("gdh-card w-full"):
                    ui.label("Link a reviewed structured plan without changing its workouts. Older single-event schedules stay available on Plan; they must be explicitly converted before linking.").classes("break-words")
                    candidates = plans.linkable_plans(db_path)
                    if not candidates:
                        ui.label("No unlinked structured plans are available.")
                    else:
                        options = {p["plan_id"]: f'{p["plan_id"]} · {p["start_date"]}–{p["end_date"]} · {p["workout_count"]} session' + ('s' if p["workout_count"] != 1 else '') for p in candidates}
                        picker = ui.select(options, label="Existing plan").classes("w-full")
                        acknowledgement = ui.checkbox("I reviewed this plan and want the season to own its date range.").props(
                            'aria-label="I reviewed this plan and want the season to own its date range."')

                        def link() -> None:
                            try:
                                p = next((p for p in candidates if p["plan_id"] == picker.value), None)
                                if not acknowledgement.value or p is None:
                                    raise plans.SeasonError("Choose a plan and acknowledge linking its schedule.")
                                plans.link_plan(db_path, season.season_id, p["plan_id"], expected_version=season.input_version,
                                                expected_revision_id=p["current_revision_id"], expected_content_hash=p["content_sha256"])
                            except (plans.SeasonError, sqlite3.Error, OSError, ValueError) as exc:
                                show_error(exc)
                                return
                            content.refresh()
                            ui.notify("Plan linked. Workouts retained.", type="positive")
                        ui.button("Link reviewed plan", on_click=link)

        with ui.column().classes("gdh-page min-w-0"):
            page_heading("Seasons", "Organize a year of running events, priorities and availability.")
            ui.label("Preview a continuous yearly schedule, then apply it to an empty future season.").classes("text-slate-600")
            ui.link("Manage single-event plan", "/plan")
            content()
