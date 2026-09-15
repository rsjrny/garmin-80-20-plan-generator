"""Strict, API-free import of ChatGPT-generated training plans.

The importer accepts only the documented versioned JSON contract.  It does not
call a model, touch SQLite, or trust model-provided summary rows.  Instead it
validates the response, applies deterministic safety limits, and adapts the
accepted data to the existing plan dataclasses.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import json
import math
from pathlib import Path
import re
from typing import Any

from garmin_data_hub.exports.forever.calendar_builder import DayPlan
from garmin_data_hub.exports.forever.models import (
    AnalysisSummary,
    AthleteProfile,
    EventProfile,
    Inputs,
)
from garmin_data_hub.services.training_policy import evaluate_training_policy


CHATGPT_PLAN_CONTRACT = "garmin-data-hub.chatgpt-plan"
CHATGPT_PLAN_VERSION = 1
MAX_PAYLOAD_BYTES = 1_000_000
MAX_PLAN_DAYS = 366
MAX_WORKOUTS_PER_DAY = 3
MAX_WORKOUTS = MAX_PLAN_DAYS * MAX_WORKOUTS_PER_DAY

SPORTS = frozenset(
    {
        "run",
        "cycle",
        "swim",
        "strength",
        "mobility",
        "hike",
        "cross_training",
        "rest",
        "other",
    }
)
INTENSITIES = frozenset({"rest", "recovery", "easy", "moderate", "hard", "race"})
PHASES = frozenset(
    {"Base", "Build", "Peak", "Taper", "Race", "Recovery", "Maintenance"}
)
FLAGS = frozenset({"CUTBACK", "TAPER", "RACE", "RECOVERY"})
NUTRITION_DAY_TYPES = frozenset(
    {"rest", "easy", "moderate", "hard", "long", "race"}
)

_TOP_REQUIRED = frozenset(
    {
        "contract",
        "version",
        "request_id",
        "active_plan_sha256",
        "athlete",
        "event",
        "analysis",
        "workouts",
    }
)
_TOP_OPTIONAL = frozenset(
    {
        "nutrition_guidance",
        "nutrition_targets",
        "strength_guidance",
        "rationale",
        "warnings",
    }
)
_ATHLETE_KEYS = frozenset(
    {
        "name",
        "age",
        "primary_sport",
        "hrmax_bpm",
        "lthr_bpm",
        "sodium_mg_per_hour",
        "notes",
    }
)
_EVENT_KEYS = frozenset(
    {"name", "sport", "distance", "start_date", "event_date", "run_days_per_week"}
)
_ANALYSIS_KEYS = frozenset(
    {
        "hrmax_observed_bpm",
        "hrmax_robust_bpm",
        "lthr_suggested_bpm",
        "active_weeks",
        "avg_weekly_hours",
        "avg_weekly_miles",
        "z2_fraction",
        "notes",
    }
)
_WORKOUT_KEYS = frozenset(
    {
        "date",
        "sport",
        "phase",
        "workout",
        "notes",
        "flags",
        "intensity",
        "duration_minutes",
        "distance_km",
        "tss",
    }
)
_NUTRITION_TARGET_KEYS = frozenset(
    {
        "date",
        "day_type",
        "carbohydrate_g_per_kg_min",
        "carbohydrate_g_per_kg_max",
        "protein_g_per_kg_min",
        "protein_g_per_kg_max",
        "fat_g_per_kg_min",
        "fat_g_per_kg_max",
        "during_training_carbohydrate_g_per_hour_min",
        "during_training_carbohydrate_g_per_hour_max",
        "notes",
    }
)
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_ACTIVE_CONTENT_RE = re.compile(
    r"<\s*(?:script|iframe|object|embed|style|link|meta)\b|javascript\s*:",
    re.IGNORECASE,
)


class PlanImportError(ValueError):
    """Base error for rejected ChatGPT plan responses."""


class PlanContractError(PlanImportError):
    """The response does not conform to the versioned JSON contract."""


class PlanSafetyError(PlanImportError):
    """The response is well-formed but violates a deterministic safety rule."""


class StalePlanResponseError(PlanContractError):
    """The response does not echo the expected request or active-plan hash."""


@dataclass(frozen=True)
class ImportedWorkout:
    """One explicitly supplied workout with exact numeric planning values."""

    iso_date: str
    sport: str
    phase: str
    workout: str
    notes: str
    flags: tuple[str, ...]
    intensity: str
    duration_minutes: float | None
    distance_km: float | None
    tss: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.iso_date,
            "sport": self.sport,
            "phase": self.phase,
            "workout": self.workout,
            "notes": self.notes,
            "flags": list(self.flags),
            "intensity": self.intensity,
            "duration_minutes": self.duration_minutes,
            "distance_km": self.distance_km,
            "tss": self.tss,
        }


@dataclass(frozen=True)
class ImportedNutritionTarget:
    """One food-agnostic, date-linked macro suggestion."""

    iso_date: str
    day_type: str
    carbohydrate_g_per_kg_min: float
    carbohydrate_g_per_kg_max: float
    protein_g_per_kg_min: float
    protein_g_per_kg_max: float
    fat_g_per_kg_min: float
    fat_g_per_kg_max: float
    during_training_carbohydrate_g_per_hour_min: float | None
    during_training_carbohydrate_g_per_hour_max: float | None
    notes: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.iso_date,
            "day_type": self.day_type,
            "carbohydrate_g_per_kg_min": self.carbohydrate_g_per_kg_min,
            "carbohydrate_g_per_kg_max": self.carbohydrate_g_per_kg_max,
            "protein_g_per_kg_min": self.protein_g_per_kg_min,
            "protein_g_per_kg_max": self.protein_g_per_kg_max,
            "fat_g_per_kg_min": self.fat_g_per_kg_min,
            "fat_g_per_kg_max": self.fat_g_per_kg_max,
            "during_training_carbohydrate_g_per_hour_min": self.during_training_carbohydrate_g_per_hour_min,
            "during_training_carbohydrate_g_per_hour_max": self.during_training_carbohydrate_g_per_hour_max,
            "notes": self.notes,
        }


@dataclass(frozen=True)
class ImportedTrainingPlan:
    """Validated response plus adapters for current and future persistence."""

    contract: str
    version: int
    request_id: str
    active_plan_sha256: str
    primary_sport: str
    event_sport: str
    inputs: Inputs
    analysis: AnalysisSummary
    workouts: tuple[ImportedWorkout, ...]
    nutrition_targets: tuple[ImportedNutritionTarget, ...]
    day_plans: tuple[DayPlan, ...]
    weekly_rows: tuple[dict[str, Any], ...]
    nutrition_guidance: tuple[str, ...]
    strength_guidance: tuple[str, ...]
    rationale: str
    warnings: tuple[str, ...]

    def as_persistence_args(
        self,
    ) -> tuple[Inputs, AnalysisSummary, list[DayPlan], list[dict[str, Any]]]:
        """Return arguments accepted by ``save_generated_plan``."""

        return (
            self.inputs,
            self.analysis,
            list(self.day_plans),
            [dict(row) for row in self.weekly_rows],
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the normalized, audit-safe v1 contract as JSON-compatible data."""

        return {
            "contract": self.contract,
            "version": self.version,
            "request_id": self.request_id,
            "active_plan_sha256": self.active_plan_sha256,
            "athlete": {
                "name": self.inputs.athlete.athlete_name,
                "age": self.inputs.athlete.age,
                "primary_sport": self.primary_sport,
                "hrmax_bpm": self.inputs.athlete.hrmax,
                "lthr_bpm": self.inputs.athlete.lthr,
                "sodium_mg_per_hour": self.inputs.athlete.sodium_mg_per_hr_hot,
                "notes": self.inputs.athlete.notes,
            },
            "event": {
                "name": self.inputs.event.event_name,
                "sport": self.event_sport,
                "distance": self.inputs.event.distance,
                "start_date": self.inputs.event.start_date,
                "event_date": self.inputs.event.event_date,
                "run_days_per_week": self.inputs.event.run_days_per_week,
            },
            "analysis": {
                "hrmax_observed_bpm": self.analysis.hrmax_observed,
                "hrmax_robust_bpm": self.analysis.hrmax_robust,
                "lthr_suggested_bpm": self.analysis.lthr_suggested,
                "active_weeks": self.analysis.active_weeks,
                "avg_weekly_hours": self.analysis.avg_weekly_hours,
                "avg_weekly_miles": self.analysis.avg_weekly_miles,
                "z2_fraction": self.analysis.z2_fraction,
                "notes": self.analysis.notes,
            },
            "workouts": [workout.to_dict() for workout in self.workouts],
            "nutrition_targets": [
                target.to_dict() for target in self.nutrition_targets
            ],
            "nutrition_guidance": list(self.nutrition_guidance),
            "strength_guidance": list(self.strength_guidance),
            "rationale": self.rationale,
            "warnings": list(self.warnings),
        }

    @property
    def canonical_payload(self) -> str:
        """Return deterministic normalized JSON for hashing and audit storage."""

        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )


def parse_chatgpt_plan(
    payload: bytes | str | dict[str, Any],
    *,
    expected_request_id: str | None = None,
    expected_active_plan_sha256: str | None = None,
    minimum_strength_sessions_per_week: int = 0,
    training_method: object = "eighty_twenty",
) -> ImportedTrainingPlan:
    """Parse, strictly validate, and normalize a ChatGPT plan response.

    Expected echo values are optional for low-level parsing, but callers that
    issued a request should provide both so stale replies cannot be accepted.
    """

    document = _load_document(payload)
    if type(minimum_strength_sessions_per_week) is not int or not 0 <= minimum_strength_sessions_per_week <= 3:
        raise ValueError("minimum_strength_sessions_per_week must be between 0 and 3")
    _require_keys(document, "$", _TOP_REQUIRED, _TOP_OPTIONAL)

    contract = _text(document["contract"], "$.contract", max_length=80)
    if contract != CHATGPT_PLAN_CONTRACT:
        raise PlanContractError(
            f"$.contract must be {CHATGPT_PLAN_CONTRACT!r}; got {contract!r}."
        )
    version = _integer(document["version"], "$.version", minimum=1, maximum=1)
    if version != CHATGPT_PLAN_VERSION:
        raise PlanContractError(
            f"$.version must be {CHATGPT_PLAN_VERSION}; got {version}."
        )

    request_id = _sha256(document["request_id"], "$.request_id")
    active_plan_sha256 = _sha256(
        document["active_plan_sha256"], "$.active_plan_sha256"
    )
    _check_echo(request_id, expected_request_id, "request_id")
    _check_echo(
        active_plan_sha256,
        expected_active_plan_sha256,
        "active_plan_sha256",
    )

    athlete_data = _object(document["athlete"], "$.athlete", _ATHLETE_KEYS)
    event_data = _object(document["event"], "$.event", _EVENT_KEYS)
    analysis_data = _object(document["analysis"], "$.analysis", _ANALYSIS_KEYS)

    athlete, primary_sport = _parse_athlete(athlete_data)
    event, event_sport, start, event_date = _parse_event(event_data)
    analysis = _parse_analysis(analysis_data)
    workouts = _parse_workouts(
        document["workouts"],
        start=start,
        event_date=event_date,
        event_sport=event_sport,
    )
    nutrition_targets = _parse_nutrition_targets(
        document.get("nutrition_targets", []),
        start=start,
        event_date=event_date,
    )
    _validate_schedule(
        workouts,
        athlete.age,
        event.run_days_per_week,
        start,
        event_date,
        minimum_strength_sessions_per_week,
        training_method,
    )

    nutrition_guidance = _guidance(
        document.get("nutrition_guidance", []), "$.nutrition_guidance"
    )
    strength_guidance = _guidance(
        document.get("strength_guidance", []), "$.strength_guidance"
    )
    rationale = _text(
        document.get("rationale", ""),
        "$.rationale",
        allow_empty=True,
        max_length=4_000,
    )
    warnings = _guidance(document.get("warnings", []), "$.warnings")

    day_plans = _build_day_plans(
        workouts,
        nutrition_targets,
        start,
        event_date,
    )
    weekly_rows = _build_weekly_rows(workouts, day_plans, start)
    inputs = Inputs(
        athlete=athlete,
        event=event,
        garmin_files=[],
        output_dir=Path.cwd(),
    )

    return ImportedTrainingPlan(
        contract=contract,
        version=version,
        request_id=request_id,
        active_plan_sha256=active_plan_sha256,
        primary_sport=primary_sport,
        event_sport=event_sport,
        inputs=inputs,
        analysis=analysis,
        workouts=workouts,
        nutrition_targets=nutrition_targets,
        day_plans=day_plans,
        weekly_rows=weekly_rows,
        nutrition_guidance=nutrition_guidance,
        strength_guidance=strength_guidance,
        rationale=rationale,
        warnings=warnings,
    )


def _load_document(payload: bytes | str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(payload, bytes):
        if len(payload) > MAX_PAYLOAD_BYTES:
            raise PlanContractError("JSON payload exceeds the 1,000,000 byte limit.")
        try:
            payload = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PlanContractError("JSON bytes must be valid UTF-8.") from exc

    if isinstance(payload, str):
        if len(payload.encode("utf-8")) > MAX_PAYLOAD_BYTES:
            raise PlanContractError("JSON payload exceeds the 1,000,000 byte limit.")
        try:
            loaded = json.loads(
                payload,
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=lambda value: _reject_json_constant(value),
            )
        except PlanContractError:
            raise
        except json.JSONDecodeError as exc:
            raise PlanContractError(
                f"Response is not valid JSON: {exc.msg} at line {exc.lineno}."
            ) from exc
    elif type(payload) is dict:
        loaded = payload
    else:
        raise PlanContractError("Payload must be UTF-8 bytes, a JSON string, or a dict.")

    if type(loaded) is not dict:
        raise PlanContractError("$ must be a JSON object.")
    return loaded


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise PlanContractError(f"Duplicate JSON key {key!r} is not allowed.")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise PlanContractError(f"Non-finite JSON number {value!r} is not allowed.")


def _require_keys(
    value: Any,
    path: str,
    required: frozenset[str],
    optional: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    if type(value) is not dict:
        raise PlanContractError(f"{path} must be an object.")
    if any(type(key) is not str for key in value):
        raise PlanContractError(f"{path} may contain only string keys.")
    keys = set(value)
    missing = sorted(required - keys)
    unknown = sorted(keys - required - optional)
    if missing:
        raise PlanContractError(f"{path} is missing required keys: {', '.join(missing)}.")
    if unknown:
        raise PlanContractError(f"{path} contains unknown keys: {', '.join(unknown)}.")
    return value


def _object(value: Any, path: str, keys: frozenset[str]) -> dict[str, Any]:
    return _require_keys(value, path, keys)


def _text(
    value: Any,
    path: str,
    *,
    allow_empty: bool = False,
    max_length: int = 2_000,
) -> str:
    if type(value) is not str:
        raise PlanContractError(f"{path} must be a string.")
    normalized = value.strip()
    if not normalized and not allow_empty:
        raise PlanContractError(f"{path} must not be empty.")
    if len(normalized) > max_length:
        raise PlanContractError(f"{path} exceeds the {max_length} character limit.")
    if _CONTROL_RE.search(normalized):
        raise PlanSafetyError(f"{path} contains unsafe control characters.")
    if _ACTIVE_CONTENT_RE.search(normalized):
        raise PlanSafetyError(f"{path} contains active HTML or a script URL.")
    if normalized.startswith(("=", "+", "@")):
        raise PlanSafetyError(f"{path} begins with a spreadsheet formula marker.")
    return normalized


def _integer(value: Any, path: str, *, minimum: int, maximum: int) -> int:
    if type(value) is not int:
        raise PlanContractError(f"{path} must be an integer.")
    if not minimum <= value <= maximum:
        raise PlanContractError(f"{path} must be between {minimum} and {maximum}.")
    return value


def _optional_integer(
    value: Any, path: str, *, minimum: int, maximum: int
) -> int | None:
    if value is None:
        return None
    return _integer(value, path, minimum=minimum, maximum=maximum)


def _optional_number(
    value: Any, path: str, *, minimum: float, maximum: float
) -> float | None:
    if value is None:
        return None
    if type(value) not in (int, float) or not math.isfinite(value):
        raise PlanContractError(f"{path} must be a finite number or null.")
    number = float(value)
    if not minimum <= number <= maximum:
        raise PlanContractError(f"{path} must be between {minimum:g} and {maximum:g}.")
    return number


def _sha256(value: Any, path: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise PlanContractError(f"{path} must be a 64-character lowercase SHA-256 hex string.")
    return value


def _check_echo(actual: str, expected: str | None, field: str) -> None:
    if expected is None:
        return
    expected_hash = _sha256(expected, f"expected_{field}")
    if actual != expected_hash:
        raise StalePlanResponseError(
            f"$.{field} does not match the request context; the response is stale."
        )


def _enum(value: Any, path: str, allowed: frozenset[str]) -> str:
    text = _text(value, path, max_length=40)
    if text not in allowed:
        raise PlanContractError(
            f"{path} must be one of: {', '.join(sorted(allowed))}."
        )
    return text


def _iso_date(value: Any, path: str) -> date:
    if type(value) is not str or _DATE_RE.fullmatch(value) is None:
        raise PlanContractError(f"{path} must use ISO date format YYYY-MM-DD.")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise PlanContractError(f"{path} is not a valid calendar date.") from exc


def _parse_athlete(data: dict[str, Any]) -> tuple[AthleteProfile, str]:
    name = _text(data["name"], "$.athlete.name", max_length=120)
    age = _integer(data["age"], "$.athlete.age", minimum=10, maximum=100)
    primary_sport = _enum(data["primary_sport"], "$.athlete.primary_sport", SPORTS)
    if primary_sport == "rest":
        raise PlanContractError("$.athlete.primary_sport cannot be 'rest'.")
    hrmax = _optional_integer(
        data["hrmax_bpm"], "$.athlete.hrmax_bpm", minimum=80, maximum=250
    )
    lthr = _optional_integer(
        data["lthr_bpm"], "$.athlete.lthr_bpm", minimum=60, maximum=220
    )
    if hrmax is not None and lthr is not None and lthr >= hrmax:
        raise PlanSafetyError("$.athlete.lthr_bpm must be lower than hrmax_bpm.")
    sodium = _optional_integer(
        data["sodium_mg_per_hour"],
        "$.athlete.sodium_mg_per_hour",
        minimum=0,
        maximum=3_000,
    )
    notes = _text(
        data["notes"], "$.athlete.notes", allow_empty=True, max_length=2_000
    )
    return (
        AthleteProfile(
            athlete_name=name,
            age=age,
            sport=primary_sport,
            hrmax=hrmax,
            lthr=lthr,
            sodium_mg_per_hr_hot=sodium,
            notes=notes,
        ),
        primary_sport,
    )


def _parse_event(
    data: dict[str, Any],
) -> tuple[EventProfile, str, date, date]:
    name = _text(data["name"], "$.event.name", max_length=160)
    sport = _enum(data["sport"], "$.event.sport", SPORTS)
    if sport == "rest":
        raise PlanContractError("$.event.sport cannot be 'rest'.")
    distance = _text(data["distance"], "$.event.distance", max_length=40)
    start = _iso_date(data["start_date"], "$.event.start_date")
    event_date = _iso_date(data["event_date"], "$.event.event_date")
    if event_date < start:
        raise PlanContractError("$.event.event_date must not precede start_date.")
    if (event_date - start).days + 1 > MAX_PLAN_DAYS:
        raise PlanContractError(f"Plan span may not exceed {MAX_PLAN_DAYS} days.")
    run_days = _integer(
        data["run_days_per_week"],
        "$.event.run_days_per_week",
        minimum=1,
        maximum=7,
    )
    return (
        EventProfile(
            event_name=name,
            distance=distance,
            event_date=event_date.isoformat(),
            start_date=start.isoformat(),
            run_days_per_week=run_days,
        ),
        sport,
        start,
        event_date,
    )


def _parse_analysis(data: dict[str, Any]) -> AnalysisSummary:
    return AnalysisSummary(
        hrmax_observed=_optional_integer(
            data["hrmax_observed_bpm"],
            "$.analysis.hrmax_observed_bpm",
            minimum=60,
            maximum=250,
        ),
        hrmax_robust=_optional_integer(
            data["hrmax_robust_bpm"],
            "$.analysis.hrmax_robust_bpm",
            minimum=60,
            maximum=250,
        ),
        lthr_suggested=_optional_integer(
            data["lthr_suggested_bpm"],
            "$.analysis.lthr_suggested_bpm",
            minimum=50,
            maximum=220,
        ),
        active_weeks=_optional_integer(
            data["active_weeks"],
            "$.analysis.active_weeks",
            minimum=0,
            maximum=5_200,
        ),
        avg_weekly_hours=_optional_number(
            data["avg_weekly_hours"],
            "$.analysis.avg_weekly_hours",
            minimum=0,
            maximum=168,
        ),
        avg_weekly_miles=_optional_number(
            data["avg_weekly_miles"],
            "$.analysis.avg_weekly_miles",
            minimum=0,
            maximum=1_000,
        ),
        z2_fraction=_optional_number(
            data["z2_fraction"],
            "$.analysis.z2_fraction",
            minimum=0,
            maximum=1,
        ),
        notes=_text(data["notes"], "$.analysis.notes", allow_empty=True, max_length=4_000),
        raw={"source": "chatgpt_plan_import", "contract_version": CHATGPT_PLAN_VERSION},
    )


def _required_number(
    value: Any, path: str, *, minimum: float, maximum: float
) -> float:
    number = _optional_number(value, path, minimum=minimum, maximum=maximum)
    if number is None:
        raise PlanContractError(f"{path} must be a finite number.")
    return number


def _parse_nutrition_targets(
    value: Any,
    *,
    start: date,
    event_date: date,
) -> tuple[ImportedNutritionTarget, ...]:
    """Validate optional v1 date-linked macro targets for every plan day."""
    if type(value) is not list:
        raise PlanContractError("$.nutrition_targets must be an array.")
    if not value:
        return ()
    plan_days = (event_date - start).days + 1
    if len(value) != plan_days:
        raise PlanContractError(
            "$.nutrition_targets must contain exactly one entry for every plan date."
        )

    targets: list[ImportedNutritionTarget] = []
    seen: set[date] = set()
    for index, raw in enumerate(value):
        path = f"$.nutrition_targets[{index}]"
        data = _object(raw, path, _NUTRITION_TARGET_KEYS)
        target_date = _iso_date(data["date"], f"{path}.date")
        if not start <= target_date <= event_date:
            raise PlanContractError(f"{path}.date is outside the plan window.")
        if target_date in seen:
            raise PlanContractError(f"{path}.date duplicates another nutrition date.")
        seen.add(target_date)

        carbohydrate_min = _required_number(
            data["carbohydrate_g_per_kg_min"],
            f"{path}.carbohydrate_g_per_kg_min",
            minimum=0,
            maximum=15,
        )
        carbohydrate_max = _required_number(
            data["carbohydrate_g_per_kg_max"],
            f"{path}.carbohydrate_g_per_kg_max",
            minimum=0,
            maximum=15,
        )
        protein_min = _required_number(
            data["protein_g_per_kg_min"],
            f"{path}.protein_g_per_kg_min",
            minimum=0,
            maximum=4,
        )
        protein_max = _required_number(
            data["protein_g_per_kg_max"],
            f"{path}.protein_g_per_kg_max",
            minimum=0,
            maximum=4,
        )
        fat_min = _required_number(
            data["fat_g_per_kg_min"],
            f"{path}.fat_g_per_kg_min",
            minimum=0,
            maximum=4,
        )
        fat_max = _required_number(
            data["fat_g_per_kg_max"],
            f"{path}.fat_g_per_kg_max",
            minimum=0,
            maximum=4,
        )
        during_min = _optional_number(
            data["during_training_carbohydrate_g_per_hour_min"],
            f"{path}.during_training_carbohydrate_g_per_hour_min",
            minimum=0,
            maximum=150,
        )
        during_max = _optional_number(
            data["during_training_carbohydrate_g_per_hour_max"],
            f"{path}.during_training_carbohydrate_g_per_hour_max",
            minimum=0,
            maximum=150,
        )
        for minimum_value, maximum_value, label in (
            (carbohydrate_min, carbohydrate_max, "carbohydrate_g_per_kg"),
            (protein_min, protein_max, "protein_g_per_kg"),
            (fat_min, fat_max, "fat_g_per_kg"),
        ):
            if minimum_value > maximum_value:
                raise PlanContractError(f"{path}.{label}_min must not exceed max.")
        if (during_min is None) != (during_max is None):
            raise PlanContractError(
                f"{path} during-training carbohydrate min and max must both be null or numbers."
            )
        if during_min is not None and during_max is not None and during_min > during_max:
            raise PlanContractError(
                f"{path}.during_training_carbohydrate_g_per_hour_min must not exceed max."
            )

        targets.append(
            ImportedNutritionTarget(
                iso_date=target_date.isoformat(),
                day_type=_enum(
                    data["day_type"], f"{path}.day_type", NUTRITION_DAY_TYPES
                ),
                carbohydrate_g_per_kg_min=carbohydrate_min,
                carbohydrate_g_per_kg_max=carbohydrate_max,
                protein_g_per_kg_min=protein_min,
                protein_g_per_kg_max=protein_max,
                fat_g_per_kg_min=fat_min,
                fat_g_per_kg_max=fat_max,
                during_training_carbohydrate_g_per_hour_min=during_min,
                during_training_carbohydrate_g_per_hour_max=during_max,
                notes=_text(
                    data["notes"], f"{path}.notes", allow_empty=True, max_length=500
                ),
            )
        )

    return tuple(sorted(targets, key=lambda target: target.iso_date))


def _parse_workouts(
    value: Any,
    *,
    start: date,
    event_date: date,
    event_sport: str,
) -> tuple[ImportedWorkout, ...]:
    if type(value) is not list or not value:
        raise PlanContractError("$.workouts must be a non-empty array.")
    if len(value) > MAX_WORKOUTS:
        raise PlanContractError(f"$.workouts may contain at most {MAX_WORKOUTS} entries.")

    workouts: list[ImportedWorkout] = []
    workouts_by_date: dict[date, list[ImportedWorkout]] = {}
    for index, item in enumerate(value):
        path = f"$.workouts[{index}]"
        data = _object(item, path, _WORKOUT_KEYS)
        workout_date = _iso_date(data["date"], f"{path}.date")
        if not start <= workout_date <= event_date:
            raise PlanContractError(f"{path}.date falls outside the event plan range.")
        sport = _enum(data["sport"], f"{path}.sport", SPORTS)
        phase = _enum(data["phase"], f"{path}.phase", PHASES)
        workout_name = _text(data["workout"], f"{path}.workout", max_length=160)
        notes = _text(
            data["notes"], f"{path}.notes", allow_empty=True, max_length=2_000
        )
        flags = _flags(data["flags"], f"{path}.flags")
        intensity = _enum(data["intensity"], f"{path}.intensity", INTENSITIES)
        duration = _optional_number(
            data["duration_minutes"],
            f"{path}.duration_minutes",
            minimum=0,
            maximum=4_320,
        )
        distance = _optional_number(
            data["distance_km"],
            f"{path}.distance_km",
            minimum=0,
            maximum=500,
        )
        tss = _optional_number(
            data["tss"], f"{path}.tss", minimum=0, maximum=1_000
        )

        workout = ImportedWorkout(
            iso_date=workout_date.isoformat(),
            sport=sport,
            phase=phase,
            workout=workout_name,
            notes=notes,
            flags=flags,
            intensity=intensity,
            duration_minutes=duration,
            distance_km=distance,
            tss=tss,
        )
        _validate_workout(workout, path, workout_date, event_date, event_sport)
        date_workouts = workouts_by_date.setdefault(workout_date, [])
        if workout in date_workouts:
            raise PlanContractError(
                f"{path} duplicates another session on {workout_date.isoformat()}."
            )
        date_workouts.append(workout)
        if len(date_workouts) > MAX_WORKOUTS_PER_DAY:
            raise PlanSafetyError(
                f"{path}.date has more than {MAX_WORKOUTS_PER_DAY} sessions."
            )
        workouts.append(workout)

    for workout_date, date_workouts in workouts_by_date.items():
        rest_workouts = [
            workout for workout in date_workouts if workout.intensity == "rest"
        ]
        if rest_workouts and len(date_workouts) != 1:
            raise PlanSafetyError(
                f"Workouts on {workout_date.isoformat()} cannot mix rest with active sessions."
            )
        hard_workouts = [
            workout
            for workout in date_workouts
            if workout.intensity in {"hard", "race"}
        ]
        if len(hard_workouts) > 1:
            raise PlanSafetyError(
                f"Workouts on {workout_date.isoformat()} contain more than one hard/race session."
            )

    event_workouts = workouts_by_date.get(event_date, [])
    if len(event_workouts) != 1 or event_workouts[0].intensity != "race":
        raise PlanSafetyError(
            "$.workouts must contain exactly one race workout on event_date."
        )
    return tuple(
        sorted(
            workouts,
            key=lambda workout: (
                workout.iso_date,
                json.dumps(
                    workout.to_dict(),
                    ensure_ascii=False,
                    allow_nan=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ),
        )
    )


def _flags(value: Any, path: str) -> tuple[str, ...]:
    if type(value) is not list or len(value) > 8:
        raise PlanContractError(f"{path} must be an array with at most 8 entries.")
    result: list[str] = []
    for index, item in enumerate(value):
        flag = _enum(item, f"{path}[{index}]", FLAGS)
        if flag in result:
            raise PlanContractError(f"{path} contains duplicate flag {flag!r}.")
        result.append(flag)
    return tuple(sorted(result))


def _validate_workout(
    workout: ImportedWorkout,
    path: str,
    workout_date: date,
    event_date: date,
    event_sport: str,
) -> None:
    metrics = (workout.duration_minutes, workout.distance_km, workout.tss)
    if workout.intensity == "rest" or workout.sport == "rest":
        if workout.intensity != "rest" or workout.sport != "rest":
            raise PlanSafetyError(f"{path}.sport and intensity must both be 'rest'.")
        if any(value not in (None, 0.0) for value in metrics):
            raise PlanSafetyError(f"{path} rest workouts cannot carry positive metrics.")
        if workout_date == event_date:
            raise PlanSafetyError(f"{path} cannot make event_date a rest day.")
        return

    if workout.duration_minutes in (None, 0.0) and workout.distance_km in (None, 0.0):
        raise PlanSafetyError(f"{path} must provide positive duration_minutes or distance_km.")
    if workout.intensity != "race" and (workout.duration_minutes or 0) > 720:
        raise PlanSafetyError(f"{path}.duration_minutes exceeds the 12-hour training cap.")
    if workout.sport in {"strength", "mobility"}:
        if workout.duration_minutes in (None, 0.0):
            raise PlanSafetyError(f"{path} strength/mobility work requires a duration.")
        if workout.distance_km not in (None, 0.0):
            raise PlanSafetyError(f"{path} strength/mobility work cannot carry distance_km.")
    if workout.sport == "swim" and (workout.distance_km or 0) > 50:
        raise PlanSafetyError(f"{path}.distance_km exceeds the swim cap.")
    if workout.sport in {"run", "hike"} and (workout.distance_km or 0) > 250:
        raise PlanSafetyError(f"{path}.distance_km exceeds the run/hike cap.")

    if workout.intensity == "race":
        if workout_date != event_date:
            raise PlanSafetyError(f"{path} race intensity is allowed only on event_date.")
        if event_sport != "other" and workout.sport != event_sport:
            raise PlanSafetyError(f"{path}.sport must match $.event.sport on race day.")
    elif workout_date == event_date:
        raise PlanSafetyError(f"{path}.intensity must be 'race' on event_date.")


def _validate_schedule(
    workouts: tuple[ImportedWorkout, ...],
    age: int,
    run_days_per_week: int,
    start: date,
    event_date: date,
    minimum_strength_sessions_per_week: int,
    training_method: object,
) -> None:
    report = evaluate_training_policy(
        workouts,
        start=start,
        event_date=event_date,
        age=age,
        run_days_per_week=run_days_per_week,
        minimum_strength_sessions_per_week=minimum_strength_sessions_per_week,
        training_method=training_method,
    )
    if report.errors:
        raise PlanSafetyError(report.errors[0].message)


def _guidance(value: Any, path: str) -> tuple[str, ...]:
    if type(value) is not list or len(value) > 50:
        raise PlanContractError(f"{path} must be an array with at most 50 entries.")
    return tuple(
        _text(item, f"{path}[{index}]", max_length=1_000)
        for index, item in enumerate(value)
    )


def _build_day_plans(
    workouts: tuple[ImportedWorkout, ...],
    nutrition_targets: tuple[ImportedNutritionTarget, ...],
    start: date,
    event_date: date,
) -> tuple[DayPlan, ...]:
    by_date: dict[date, list[ImportedWorkout]] = {}
    for workout in workouts:
        by_date.setdefault(date.fromisoformat(workout.iso_date), []).append(workout)
    nutrition_by_date = {
        date.fromisoformat(target.iso_date): target.to_dict()
        for target in nutrition_targets
    }
    total_weeks = max(1, (event_date - start).days // 7)
    result: list[DayPlan] = []
    current = start
    while current <= event_date:
        week = ((current - start).days // 7) + 1
        date_workouts = by_date.get(current, [])
        if not date_workouts:
            phase = _phase_for_date(current, event_date, total_weeks)
            flags = "TAPER" if phase == "Taper" else ""
            result.append(
                DayPlan(
                    iso_date=current.isoformat(),
                    day=current.strftime("%A"),
                    week=week,
                    phase=phase,
                    flags=flags,
                    workout="Rest Day",
                    notes="Complete rest.",
                    sport="rest",
                    intensity="rest",
                    session_count=0,
                    nutrition=nutrition_by_date.get(current),
                )
            )
        else:
            dominant = max(date_workouts, key=_session_priority)
            flags = sorted(
                {flag for workout in date_workouts for flag in workout.flags}
            )
            result.append(
                DayPlan(
                    iso_date=current.isoformat(),
                    day=current.strftime("%A"),
                    week=week,
                    phase=dominant.phase,
                    flags=", ".join(flags),
                    workout=" + ".join(workout.workout for workout in date_workouts),
                    notes=_aggregate_persistence_notes(date_workouts),
                    sport=dominant.sport,
                    intensity=dominant.intensity,
                    session_count=len(date_workouts),
                    nutrition=nutrition_by_date.get(current),
                )
            )
        current += timedelta(days=1)
    return tuple(result)


_INTENSITY_PRIORITY = {
    "rest": 0,
    "recovery": 1,
    "easy": 2,
    "moderate": 3,
    "hard": 4,
    "race": 5,
}


def _session_priority(workout: ImportedWorkout) -> int:
    return _INTENSITY_PRIORITY[workout.intensity]


def _aggregate_persistence_notes(workouts: list[ImportedWorkout]) -> str:
    if len(workouts) == 1:
        return _persistence_notes(workouts[0])
    return " | ".join(
        f"{workout.workout}: {_persistence_notes(workout)}".rstrip(": ")
        for workout in workouts
    )


def _phase_for_date(current: date, event_date: date, total_weeks: int) -> str:
    weeks_from_event = (event_date - current).days // 7
    if weeks_from_event <= 2:
        return "Taper"
    if weeks_from_event <= 8:
        return "Peak"
    if weeks_from_event <= total_weeks * 0.66:
        return "Build"
    return "Base"


def _persistence_notes(workout: ImportedWorkout) -> str:
    metrics: list[str] = []
    if workout.duration_minutes is not None:
        metrics.append(f"{workout.duration_minutes:g} min")
    if workout.distance_km is not None:
        metrics.append(f"{workout.distance_km:g} km")
    if workout.tss is not None:
        metrics.append(f"TSS {workout.tss:g}")
    prefix = "; ".join(metrics)
    if prefix and workout.notes:
        return f"{prefix}. {workout.notes}"
    return prefix or workout.notes


def _build_weekly_rows(
    workouts: tuple[ImportedWorkout, ...],
    day_plans: tuple[DayPlan, ...],
    start: date,
) -> tuple[dict[str, Any], ...]:
    workouts_by_date: dict[str, list[ImportedWorkout]] = {}
    for workout in workouts:
        workouts_by_date.setdefault(workout.iso_date, []).append(workout)
    grouped: dict[int, list[DayPlan]] = {}
    for day_plan in day_plans:
        grouped.setdefault(day_plan.week, []).append(day_plan)

    rows: list[dict[str, Any]] = []
    for week in sorted(grouped):
        plans = grouped[week]
        explicit = [
            workout
            for plan in plans
            for workout in workouts_by_date.get(plan.iso_date, [])
        ]
        phases = [workout.phase for workout in explicit] or [plans[0].phase]
        flags = sorted({flag for workout in explicit for flag in workout.flags})
        run_workouts = [
            w for w in explicit if w.sport == "run" and w.intensity != "rest"
        ]
        saturday_long = [
            w
            for w in run_workouts
            if date.fromisoformat(w.iso_date).weekday() == 5
            and "long" in w.workout.lower()
        ]
        sunday_back_to_back = [
            w
            for w in run_workouts
            if date.fromisoformat(w.iso_date).weekday() == 6
            and "back-to-back" in w.workout.lower()
        ]
        rows.append(
            {
                "Week#": week,
                "Phase": phases[-1],
                "Flags": ", ".join(flags),
                "Run Days": len({w.iso_date for w in run_workouts}),
                "Strength Days": len(
                    {w.iso_date for w in explicit if w.sport == "strength"}
                ),
                "Quality Sessions": len(
                    [w for w in explicit if w.intensity in {"hard", "race"}]
                ),
                "Run Hours (est)": round(
                    sum(w.duration_minutes or 0 for w in run_workouts) / 60, 2
                ),
                "Long Run (Sat) hrs": round(
                    max((w.duration_minutes or 0 for w in saturday_long), default=0) / 60,
                    2,
                ),
                "Back-to-Back (Sun) hrs": round(
                    max(
                        (w.duration_minutes or 0 for w in sunday_back_to_back),
                        default=0,
                    )
                    / 60,
                    2,
                ),
            }
        )
    return tuple(rows)
