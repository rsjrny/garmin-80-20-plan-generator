"""Framework-neutral state and safety boundary for the NiceGUI Codex workspace."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Mapping

from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db import queries as db_queries
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services.ai_plan_import import (
    ImportedTrainingPlan,
    parse_chatgpt_plan,
)
from garmin_data_hub.services.athlete_metrics_service import get_athlete_metrics
from garmin_data_hub.services.coaching_packet import build_coaching_packet
from garmin_data_hub.services.codex_plan_generator import (
    CodexPlanGenerationError,
    find_codex_cli,
    generate_plan_with_codex,
)
from garmin_data_hub.services.plan_persistence import (
    PlanImportSaveResult,
    load_chatgpt_exchange_settings,
    load_plan_settings,
    save_imported_plan,
    save_plan_setting,
)
from garmin_data_hub.services.training_policy import (
    DEFAULT_TRAINING_METHOD,
    TrainingPolicyReport,
    evaluate_training_policy,
    normalize_training_method,
)


PREFERENCE_TO_SETTING = {
    "injuries_or_limitations": "chatgpt_exchange_injuries",
    "scheduling_notes": "chatgpt_exchange_schedule",
    "strength_equipment": "chatgpt_exchange_equipment",
    "strength_experience": "chatgpt_exchange_strength_experience",
    "dietary_preferences": "chatgpt_exchange_diet",
    "allergies_or_intolerances": "chatgpt_exchange_allergies",
    "gi_considerations": "chatgpt_exchange_gi",
}
PROMPT_SETTING_KEY = "chatgpt_exchange_prompt_template"
REQUEST_ID_TOKEN = "{{CURRENT_REQUEST_ID}}"
PLAN_HASH_TOKEN = "{{CURRENT_ACTIVE_PLAN_SHA256}}"


def _as_date(value: object, fallback: date) -> date:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return fallback


def _as_int(value: object, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


@dataclass(frozen=True)
class WorkspaceContext:
    db_path: Path
    plan_start: date
    event_date: date
    age: int
    distance: str
    training_method: str
    run_days_per_week: int
    long_run_day: str
    sodium_mg_per_hour: int | None
    hrmax: int | None
    lthr: int | None
    preferences: dict[str, str]
    lookback_weeks: int
    sandboxed: bool = True

    @property
    def horizon_days(self) -> int:
        return (self.event_date - self.plan_start).days + 1


@dataclass(frozen=True)
class ProposalReview:
    packet: dict[str, Any]
    plan: ImportedTrainingPlan
    policy: TrainingPolicyReport
    errors: tuple[str, ...]
    warnings: tuple[str, ...]
    changes: tuple[dict[str, str], ...]

    @property
    def can_apply(self) -> bool:
        return not self.errors


@dataclass(frozen=True)
class GenerationSnapshot:
    state: str
    elapsed_seconds: float
    response_json: str | None
    error: str | None


@dataclass
class GenerationJob:
    """Thread-backed Codex task whose child process can be cancelled safely."""

    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _cancel_event: threading.Event = field(default_factory=threading.Event, init=False)
    _thread: threading.Thread | None = field(default=None, init=False)
    _state: str = field(default="idle", init=False)
    _started: float | None = field(default=None, init=False)
    _elapsed: float = field(default=0.0, init=False)
    _response_json: str | None = field(default=None, init=False)
    _error: str | None = field(default=None, init=False)

    def start(
        self,
        packet: Mapping[str, Any],
        *,
        prompt: str,
        executable: str | None = None,
    ) -> None:
        with self._lock:
            if self._state in {"running", "cancelling"}:
                raise RuntimeError("Codex generation is already running")
            self._cancel_event = threading.Event()
            self._state = "running"
            self._started = time.monotonic()
            self._elapsed = 0.0
            self._response_json = None
            self._error = None

        def update_elapsed(value: float) -> None:
            with self._lock:
                self._elapsed = value

        def run() -> None:
            try:
                result = generate_plan_with_codex(
                    packet,
                    prompt=prompt,
                    executable=executable,
                    cancel_event=self._cancel_event,
                    progress_callback=update_elapsed,
                )
            except CodexPlanGenerationError as exc:
                with self._lock:
                    self._state = (
                        "cancelled" if self._cancel_event.is_set() else "failed"
                    )
                    self._error = str(exc)
            except Exception as exc:  # pragma: no cover - defensive thread boundary
                with self._lock:
                    self._state = "failed"
                    self._error = f"Unexpected Codex failure: {exc}"
            else:
                with self._lock:
                    self._state = "completed"
                    self._response_json = result.response_json
            finally:
                with self._lock:
                    if self._started is not None:
                        self._elapsed = time.monotonic() - self._started

        self._thread = threading.Thread(
            target=run,
            name="garmin-codex-plan-generation",
            daemon=True,
        )
        self._thread.start()

    def cancel(self) -> bool:
        with self._lock:
            if self._state != "running":
                return False
            self._state = "cancelling"
            self._cancel_event.set()
            return True

    def snapshot(self) -> GenerationSnapshot:
        with self._lock:
            elapsed = self._elapsed
            if self._started is not None and self._state in {"running", "cancelling"}:
                elapsed = time.monotonic() - self._started
            return GenerationSnapshot(
                state=self._state,
                elapsed_seconds=elapsed,
                response_json=self._response_json,
                error=self._error,
            )


def create_database_snapshot(source: Path, target: Path) -> Path:
    """Create a transactionally consistent SQLite snapshot for UI prototyping."""
    source = Path(source).resolve()
    target = Path(target).resolve()
    if source == target:
        raise ValueError("prototype database target must differ from the live database")
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.exists():
        source_uri = f"file:{source.as_posix()}?mode=ro"
        with sqlite3.connect(source_uri, uri=True) as source_conn:
            with sqlite3.connect(target) as target_conn:
                source_conn.backup(target_conn)
    else:
        with sqlite3.connect(target):
            pass
    conn = connect_sqlite(target)
    try:
        apply_schema(conn, schema_sql_path())
    finally:
        conn.close()
    return target


def load_workspace_context(db_path: Path, *, sandboxed: bool = True) -> WorkspaceContext:
    settings = load_plan_settings(db_path)
    exchange = load_chatgpt_exchange_settings(db_path)
    metrics = get_athlete_metrics(db_path)
    today = date.today()
    start = max(today, _as_date(settings.get("plan_start_date"), today))
    event = _as_date(settings.get("plan_event_date"), today)
    preferences = {
        preference: str(exchange.get(setting, "") or "")
        for preference, setting in PREFERENCE_TO_SETTING.items()
    }
    lookback = _as_int(exchange.get("chatgpt_exchange_lookback_weeks"), 12)
    if lookback not in {4, 8, 12, 16, 24, 52}:
        lookback = 12
    sodium = _as_int(settings.get("plan_sodium"), 0)
    try:
        training_method = normalize_training_method(settings.get("plan_training_method"))
    except ValueError:
        training_method = DEFAULT_TRAINING_METHOD
    return WorkspaceContext(
        db_path=Path(db_path),
        plan_start=start,
        event_date=event,
        age=_as_int(settings.get("plan_age"), 50),
        distance=str(settings.get("plan_distance") or "50K"),
        training_method=training_method,
        run_days_per_week=_as_int(settings.get("plan_run_days"), 5),
        long_run_day=str(settings.get("plan_long_run_day") or "Saturday"),
        sodium_mg_per_hour=sodium if sodium > 0 else None,
        hrmax=(
            int(metrics["hrmax_effective"])
            if metrics.get("hrmax_effective") is not None
            else None
        ),
        lthr=(
            int(metrics["lthr_effective"])
            if metrics.get("lthr_effective") is not None
            else None
        ),
        preferences=preferences,
        lookback_weeks=lookback,
        sandboxed=sandboxed,
    )


def validate_workspace_context(context: WorkspaceContext) -> tuple[str, ...]:
    errors: list[str] = []
    if not 10 <= context.age <= 100:
        errors.append("Athlete age must be between 10 and 100.")
    if not 1 <= context.run_days_per_week <= 7:
        errors.append("Run days per week must be between 1 and 7.")
    if not context.distance.strip():
        errors.append("Race distance cannot be blank.")
    try:
        normalize_training_method(context.training_method)
    except ValueError as exc:
        errors.append(str(exc))
    if context.sodium_mg_per_hour is not None:
        if not 0 <= context.sodium_mg_per_hour <= 3_000:
            errors.append("Sodium must be between 0 and 3,000 mg/hour.")
    if context.hrmax is not None and not 80 <= context.hrmax <= 250:
        errors.append("HRmax must be between 80 and 250 bpm.")
    if context.lthr is not None and not 60 <= context.lthr <= 220:
        errors.append("LTHR must be between 60 and 220 bpm.")
    if context.event_date < context.plan_start:
        errors.append("The event date must be on or after the plan start date.")
    if context.horizon_days > 366:
        errors.append("The Codex prototype supports at most 366 plan days.")
    if context.hrmax is not None and context.lthr is not None:
        if context.lthr >= context.hrmax:
            errors.append("LTHR must be lower than HRmax.")
    return tuple(errors)


def build_workspace_packet(context: WorkspaceContext) -> dict[str, Any]:
    errors = validate_workspace_context(context)
    if errors:
        raise ValueError(" ".join(errors))
    return build_coaching_packet(
        context.db_path,
        as_of=date.today(),
        plan_start=context.plan_start,
        lookback_days=context.lookback_weeks * 7,
        recent_activity_limit=40,
        plan_horizon_days=context.horizon_days,
        preferences=context.preferences,
        plan_context={
            "age": context.age,
            "run_days_per_week": context.run_days_per_week,
            "distance": context.distance,
            "training_method": context.training_method,
            "long_run_day": context.long_run_day,
            "sodium_mg_per_hour": context.sodium_mg_per_hour,
        },
    )


def persist_workspace_preferences(
    context: WorkspaceContext,
    preferences: Mapping[str, str],
    *,
    lookback_weeks: int,
) -> WorkspaceContext:
    values = {
        setting: str(preferences.get(preference, ""))
        for preference, setting in PREFERENCE_TO_SETTING.items()
    }
    values["chatgpt_exchange_lookback_weeks"] = int(lookback_weeks)
    conn = connect_sqlite(context.db_path)
    try:
        db_queries.set_settings(conn, values)
    finally:
        conn.close()
    return load_workspace_context(context.db_path, sandboxed=context.sandboxed)


def prompt_as_template(prompt: str, request_id: str, plan_hash: str) -> str:
    template = str(prompt)
    if request_id:
        template = template.replace(request_id, REQUEST_ID_TOKEN)
    if plan_hash:
        template = template.replace(plan_hash, PLAN_HASH_TOKEN)
    return template


def prompt_for_packet(template: str, packet: Mapping[str, Any]) -> str:
    return str(template).replace(REQUEST_ID_TOKEN, str(packet["request_id"])).replace(
        PLAN_HASH_TOKEN, str(packet["active_plan_sha256"])
    )


def load_workspace_prompt(db_path: Path, packet: Mapping[str, Any]) -> str:
    conn = connect_sqlite(db_path)
    try:
        template = db_queries.get_setting(conn, PROMPT_SETTING_KEY, "")
    finally:
        conn.close()
    if not isinstance(template, str) or not template.strip():
        return str(packet["chatgpt"]["copyable_prompt"])
    return prompt_for_packet(template, packet)


def save_workspace_prompt(
    db_path: Path, prompt: str, packet: Mapping[str, Any]
) -> None:
    template = prompt_as_template(
        str(prompt), str(packet["request_id"]), str(packet["active_plan_sha256"])
    )
    save_plan_setting(db_path, PROMPT_SETTING_KEY, template)


def _locked_context_errors(
    plan: ImportedTrainingPlan, packet: Mapping[str, Any]
) -> list[str]:
    athlete = packet["context"]["athlete"]
    event = packet["context"]["event"]
    current_plan = packet["context"]["current_plan"]
    expected = (
        (plan.inputs.event.start_date, current_plan["window_start"], "plan start date"),
        (plan.inputs.event.event_date, current_plan["window_end"], "event date"),
        (plan.inputs.event.distance, event["distance"], "event distance"),
        (
            plan.inputs.event.run_days_per_week,
            event["run_days_per_week"],
            "run-days setting",
        ),
        (plan.inputs.athlete.age, athlete["age"], "athlete age"),
        (plan.inputs.athlete.hrmax, athlete["hrmax_bpm"], "HRmax"),
        (plan.inputs.athlete.lthr, athlete["lthr_bpm"], "LTHR"),
        (
            plan.inputs.athlete.sodium_mg_per_hr_hot,
            athlete["sodium_mg_per_hour"],
            "sodium setting",
        ),
        (plan.primary_sport, athlete["primary_sport"], "primary sport"),
        (plan.event_sport, event["sport"], "event sport"),
    )
    return [
        f"The response changed {label}: expected {wanted!r}, got {actual!r}."
        for actual, wanted, label in expected
        if actual != wanted
    ]


def _existing_by_date(
    db_path: Path, start_date: str, end_date: str
) -> dict[str, list[dict[str, Any]]]:
    conn = connect_sqlite(db_path)
    try:
        rows = conn.execute(
            """
            SELECT scheduled_date, workout_name, planned_duration_s,
                   planned_distance_m, planned_tss, description, structure_json
            FROM planned_workout
            WHERE scheduled_date BETWEEN ? AND ?
            ORDER BY scheduled_date, planned_workout_id
            """,
            (start_date, end_date),
        ).fetchall()
    finally:
        conn.close()
    grouped: dict[str, list[dict[str, Any]]] = {}
    for raw in rows:
        row = dict(raw)
        structured: dict[str, Any] = {}
        try:
            structure = json.loads(row.get("structure_json") or "{}")
            if isinstance(structure, dict) and isinstance(structure.get("workout"), dict):
                structured = structure["workout"]
        except json.JSONDecodeError:
            pass
        grouped.setdefault(str(row["scheduled_date"]), []).append(
            {
                "sport": structured.get("sport"),
                "phase": structured.get("phase"),
                "workout": structured.get("workout") or row.get("workout_name"),
                "intensity": structured.get("intensity"),
                "duration_minutes": structured.get("duration_minutes")
                if "duration_minutes" in structured
                else (
                    round(float(row["planned_duration_s"]) / 60, 6)
                    if row.get("planned_duration_s") is not None
                    else None
                ),
                "distance_km": structured.get("distance_km")
                if "distance_km" in structured
                else (
                    round(float(row["planned_distance_m"]) / 1000, 6)
                    if row.get("planned_distance_m") is not None
                    else None
                ),
                "tss": structured.get("tss", row.get("planned_tss")),
                "flags": structured.get("flags") or [],
                "notes": structured.get("notes", row.get("description") or ""),
            }
        )
    return grouped


def _summary(
    session: Mapping[str, Any], *, detail_fields: tuple[str, ...] = ()
) -> str:
    details = [str(session.get("workout") or "(unnamed workout)")]
    if session.get("duration_minutes") is not None:
        details.append(f"{session['duration_minutes']:g} min")
    if session.get("distance_km") is not None:
        details.append(f"{session['distance_km']:g} km")
    if session.get("intensity"):
        details.append(str(session["intensity"]))
    if "phase" in detail_fields and session.get("phase"):
        details.append(f"Phase: {session['phase']}")
    if "tss" in detail_fields and session.get("tss") is not None:
        details.append(f"TSS: {session['tss']:g}")
    if "flags" in detail_fields:
        flags = session.get("flags") or []
        details.append(f"Flags: {', '.join(flags) if flags else '(none)'}")
    if "notes" in detail_fields:
        note = str(session.get("notes") or "").strip()
        details.append(f"Notes: {note or '(blank)'}")
    return " | ".join(details)


def _comparison_value(value: Any) -> Any:
    if isinstance(value, (int, float)):
        return round(float(value), 6)
    if isinstance(value, str):
        return " ".join(value.split())
    if isinstance(value, list):
        return tuple(sorted(_comparison_value(item) for item in value))
    return value


def _plan_changes(db_path: Path, plan: ImportedTrainingPlan) -> tuple[dict[str, str], ...]:
    start = plan.day_plans[0].iso_date
    end = plan.day_plans[-1].iso_date
    old_by_date = _existing_by_date(db_path, start, end)
    new_by_date: dict[str, list[dict[str, Any]]] = {}
    for workout in plan.workouts:
        normalized = workout.to_dict()
        normalized.pop("date", None)
        new_by_date.setdefault(workout.iso_date, []).append(normalized)

    changes: list[dict[str, str]] = []
    fields = (
        "sport",
        "phase",
        "workout",
        "intensity",
        "duration_minutes",
        "distance_km",
        "tss",
        "flags",
        "notes",
    )
    for iso_date in sorted(set(old_by_date) | set(new_by_date)):
        old = old_by_date.get(iso_date, [])
        new = new_by_date.get(iso_date, [])
        old_values = sorted(
            json.dumps(item, sort_keys=True, default=str) for item in old
        )
        new_values = sorted(
            json.dumps(item, sort_keys=True, default=str) for item in new
        )
        if old_values == new_values:
            continue
        changed_field_keys = tuple(
            field
            for field in fields
            if sorted(_comparison_value(item.get(field)) for item in old)
            != sorted(_comparison_value(item.get(field)) for item in new)
        )
        changed_fields = [
            field.replace("_", " ").title() for field in changed_field_keys
        ]
        current_summary = " ; ".join(
            _summary(item, detail_fields=changed_field_keys) for item in old
        )
        proposed_summary = " ; ".join(
            _summary(item, detail_fields=changed_field_keys) for item in new
        )
        changes.append(
            {
                "Date": iso_date,
                "Change": "Changed" if old and new else "Added" if new else "Removed",
                "Changed fields": ", ".join(changed_fields) or "Sessions",
                "Current": " ; ".join(_summary(item) for item in old) or "—",
                "Proposed": " ; ".join(_summary(item) for item in new)
                or "Rest / no scheduled row",
            }
        )
        changes[-1]["Current"] = current_summary or "(none)"
        changes[-1]["Proposed"] = proposed_summary or "Rest / no scheduled row"
    return tuple(changes)


def review_proposal(
    context: WorkspaceContext,
    packet: dict[str, Any],
    response_json: str,
) -> ProposalReview:
    plan = parse_chatgpt_plan(
        response_json,
        expected_request_id=packet["request_id"],
        expected_active_plan_sha256=packet["active_plan_sha256"],
        minimum_strength_sessions_per_week=1,
        training_method=context.training_method,
    )
    errors = _locked_context_errors(plan, packet)
    policy = evaluate_training_policy(
        plan.workouts,
        start=context.plan_start,
        event_date=context.event_date,
        age=context.age,
        run_days_per_week=context.run_days_per_week,
        preferred_long_session_day=context.long_run_day,
        minimum_strength_sessions_per_week=1,
        training_method=context.training_method,
    )
    errors.extend(f"Local policy: {issue.message}" for issue in policy.errors)
    warnings = list(plan.warnings)
    warnings.extend(f"Local policy: {issue.message}" for issue in policy.warnings)
    return ProposalReview(
        packet=packet,
        plan=plan,
        policy=policy,
        errors=tuple(errors),
        warnings=tuple(warnings),
        changes=_plan_changes(context.db_path, plan),
    )


def save_review_to_database(
    context: WorkspaceContext, review: ProposalReview
) -> PlanImportSaveResult:
    """Apply an explicitly reviewed proposal to the controller's database."""
    if not review.can_apply:
        raise ValueError("proposal has validation errors and cannot be applied")
    return save_imported_plan(context.db_path, review.plan)


def codex_executable() -> str | None:
    return find_codex_cli()
