"""Generate, validate, and persist the deterministic offline training plan."""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from garmin_data_hub.exports.forever.excel_writer import write_master_workbook
from garmin_data_hub.exports.master_export import generate_plan_data
from garmin_data_hub.services.plan_persistence import save_generated_plan
from garmin_data_hub.services.training_policy import (
    DEFAULT_TRAINING_METHOD,
    TrainingPolicyReport,
    evaluate_training_policy,
    normalize_policy_sessions,
    normalize_training_method,
)


BASELINE_DISTANCE_OPTIONS = {
    "5K": "5K",
    "10K": "10K",
    "10M": "10 Miler",
    "HM": "Half Marathon",
    "20M": "20 Miler",
    "MAR": "Marathon",
    "50K": "50K",
    "50M": "50 Mile",
    "100K": "100K",
    "100M": "100 Mile",
}
MAX_BASELINE_HORIZON_DAYS = 730

_DISTANCE_ALIASES = {
    "5k": "5K",
    "10k": "10K",
    "10m": "10M",
    "10 mile": "10M",
    "10 miler": "10M",
    "hm": "HM",
    "half marathon": "HM",
    "20m": "20M",
    "20 mile": "20M",
    "20 miler": "20M",
    "mar": "MAR",
    "marathon": "MAR",
    "50k": "50K",
    "50m": "50M",
    "50 mile": "50M",
    "100k": "100K",
    "100m": "100M",
    "100 mile": "100M",
}


class BaselinePlanBuildError(RuntimeError):
    """A deterministic baseline could not be generated or saved safely."""


class BaselinePolicyError(BaselinePlanBuildError):
    """The generated baseline violated a mandatory local training policy."""

    def __init__(self, report: TrainingPolicyReport) -> None:
        self.report = report
        messages = "\n".join(f"- {issue.message}" for issue in report.errors)
        super().__init__(
            "The offline baseline failed local training-policy validation:\n"
            + messages
        )


class BaselineWorkbookError(BaselinePlanBuildError):
    """The requested workbook could not be prepared before changing the plan."""


@dataclass(frozen=True)
class BaselinePlanRequest:
    athlete_name: str
    age: int
    lthr: int | None
    hrmax: int | None
    sodium_mg_per_hour: int | None
    event_name: str
    distance: str
    start_date: str
    event_date: str
    run_days_per_week: int
    long_run_day: str
    training_method: str = DEFAULT_TRAINING_METHOD


@dataclass(frozen=True)
class BaselinePlanBuildResult:
    start_date: str
    event_date: str
    plan_day_count: int
    schedule_row_count: int
    active_session_count: int
    warnings: tuple[str, ...]
    workbook_path: Path | None = None
    workbook_error: str | None = None


def normalize_baseline_distance(value: object) -> str:
    """Return the rule engine's canonical distance code for a UI label or code."""

    normalized = " ".join(str(value or "").strip().casefold().split())
    try:
        return _DISTANCE_ALIASES[normalized]
    except KeyError as exc:
        allowed = ", ".join(BASELINE_DISTANCE_OPTIONS.values())
        raise ValueError(f"Unsupported race distance. Choose one of: {allowed}") from exc


def _validated_request(
    request: BaselinePlanRequest,
) -> tuple[BaselinePlanRequest, date, date]:
    athlete_name = str(request.athlete_name or "").strip() or "Runner"
    distance = normalize_baseline_distance(request.distance)
    training_method = normalize_training_method(request.training_method)
    event_name = str(request.event_name or "").strip() or f"{distance} Training Plan"
    long_run_day = str(request.long_run_day or "").strip()
    if long_run_day not in {
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    }:
        raise ValueError("Long-session day must be a weekday")
    if not 10 <= int(request.age) <= 100:
        raise ValueError("Age must be between 10 and 100")
    if not 1 <= int(request.run_days_per_week) <= 7:
        raise ValueError("Run days must be between 1 and 7")
    sodium = (
        int(request.sodium_mg_per_hour)
        if request.sodium_mg_per_hour is not None
        else None
    )
    if sodium is not None and not 0 <= sodium <= 3000:
        raise ValueError("Sodium must be between 0 and 3,000 mg/hour")
    hrmax = int(request.hrmax) if request.hrmax is not None else None
    lthr = int(request.lthr) if request.lthr is not None else None
    if hrmax is not None and not 80 <= hrmax <= 250:
        raise ValueError("HRmax must be between 80 and 250 bpm")
    if lthr is not None and not 80 <= lthr <= 220:
        raise ValueError("LTHR must be between 80 and 220 bpm")
    if hrmax is not None and lthr is not None and lthr >= hrmax:
        raise ValueError("LTHR must be below HRmax")
    start = date.fromisoformat(str(request.start_date))
    event = date.fromisoformat(str(request.event_date))
    if event < start:
        raise ValueError("Event date must be on or after plan start")
    if (event - start).days > MAX_BASELINE_HORIZON_DAYS:
        raise ValueError(
            f"Offline baseline duration cannot exceed "
            f"{MAX_BASELINE_HORIZON_DAYS} days"
        )
    return (
        BaselinePlanRequest(
            athlete_name=athlete_name,
            age=int(request.age),
            lthr=lthr,
            hrmax=hrmax,
            sodium_mg_per_hour=sodium,
            event_name=event_name,
            distance=distance,
            start_date=start.isoformat(),
            event_date=event.isoformat(),
            run_days_per_week=int(request.run_days_per_week),
            long_run_day=long_run_day,
            training_method=training_method,
        ),
        start,
        event,
    )


def _workbook_target(value: Path | str) -> Path:
    target = Path(value).expanduser().resolve()
    if target.suffix.casefold() != ".xlsx":
        raise ValueError("Workbook filename must end with .xlsx")
    if not target.name or target.name in {".", ".."}:
        raise ValueError("Workbook filename is invalid")
    return target


def _prepare_workbook(
    target: Path,
    inputs: object,
    analysis: object,
    day_plans: list[object],
    weekly_rows: list[dict[str, object]],
) -> Path:
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".garmin-data-hub-plan-",
            suffix=".xlsx",
            dir=target.parent,
        )
        os.close(descriptor)
        temporary_path = Path(temporary_name)
        write_master_workbook(
            out_path=temporary_path,
            inputs=inputs,
            analysis=analysis,
            day_plans=day_plans,
            weekly_rows=weekly_rows,
            narrative=None,
        )
        return temporary_path
    except Exception as exc:
        temporary_path = locals().get("temporary_path")
        if isinstance(temporary_path, Path):
            temporary_path.unlink(missing_ok=True)
        raise BaselineWorkbookError(
            f"Could not create the baseline workbook: {exc}"
        ) from exc


def build_and_save_baseline(
    db_path: Path | str,
    request: BaselinePlanRequest,
    *,
    workbook_path: Path | str | None = None,
    expected_active_plan_sha256: str | None = None,
) -> BaselinePlanBuildResult:
    """Generate once, enforce policy, atomically save, and optionally export."""

    validated, start, event = _validated_request(request)
    target = _workbook_target(workbook_path) if workbook_path is not None else None
    inputs, analysis, day_plans, weekly_rows = generate_plan_data(
        athlete_name=validated.athlete_name,
        age=validated.age,
        lthr=validated.lthr,
        hrmax=validated.hrmax,
        sodium_mg_per_hr_hot=validated.sodium_mg_per_hour,
        event_name=validated.event_name,
        distance=validated.distance,
        start_date_iso=validated.start_date,
        event_date_iso=validated.event_date,
        run_days_per_week=validated.run_days_per_week,
        long_run_day=validated.long_run_day,
        training_method=validated.training_method,
        garmin_files=[],
        out_dir=target.parent if target is not None else None,
    )
    policy = evaluate_training_policy(
        day_plans,
        start=start,
        event_date=event,
        age=validated.age,
        run_days_per_week=validated.run_days_per_week,
        preferred_long_session_day=validated.long_run_day,
        minimum_strength_sessions_per_week=1,
        training_method=validated.training_method,
    )
    if policy.errors:
        raise BaselinePolicyError(policy)

    sessions = normalize_policy_sessions(day_plans)
    plan_day_count = len(day_plans)
    schedule_row_count = sum(bool(day.workout) for day in day_plans)
    active_session_count = sum(
        session.sport != "rest" and session.intensity != "rest"
        for session in sessions
    )
    warnings = tuple(issue.message for issue in policy.warnings)

    temporary_workbook = (
        _prepare_workbook(target, inputs, analysis, day_plans, weekly_rows)
        if target is not None
        else None
    )
    try:
        save_generated_plan(
            Path(db_path),
            inputs,
            analysis,
            list(day_plans),
            list(weekly_rows),
            expected_active_plan_sha256=expected_active_plan_sha256,
        )
    except Exception:
        if temporary_workbook is not None:
            temporary_workbook.unlink(missing_ok=True)
        raise

    saved_workbook: Path | None = None
    workbook_error: str | None = None
    if temporary_workbook is not None and target is not None:
        try:
            os.replace(temporary_workbook, target)
        except OSError as exc:
            temporary_workbook.unlink(missing_ok=True)
            workbook_error = (
                "The baseline was saved, but the workbook could not replace "
                f"{target}: {exc}"
            )
        else:
            saved_workbook = target

    return BaselinePlanBuildResult(
        start_date=start.isoformat(),
        event_date=event.isoformat(),
        plan_day_count=plan_day_count,
        schedule_row_count=schedule_row_count,
        active_session_count=active_session_count,
        warnings=warnings,
        workbook_path=saved_workbook,
        workbook_error=workbook_error,
    )
