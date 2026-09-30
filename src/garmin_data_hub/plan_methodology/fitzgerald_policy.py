"""Deterministic Fitzgerald V1 prescription-policy behavior."""

from __future__ import annotations

from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from dataclasses import dataclass
import re
from typing import Any, Mapping, Sequence

from .domain import DomainError, FitzgeraldCategory, MethodologyId, canonical_decimal, exact_decimal
from .fitzgerald_parameters import select_primary_metric
from .validation import FindingSeverity, ValidationFinding, ValidationLayer
from .workout_vocabulary import RUNNING_WORKOUT_FAMILIES


TARGET_CATEGORIES = {
    "ZONE_1": FitzgeraldCategory.LOW,
    "ZONE_2": FitzgeraldCategory.LOW,
    "ZONE_X": FitzgeraldCategory.MODERATE,
    "ZONE_3": FitzgeraldCategory.MODERATE,
    "ZONE_Y": FitzgeraldCategory.HIGH,
    "ZONE_4": FitzgeraldCategory.HIGH,
    "ZONE_5": FitzgeraldCategory.HIGH,
}

OWNER = MethodologyId.FITZGERALD_80_20_RUNNING_V1.value
_WINDOW_ORDER = ("WEEK", "ROLLING_FOUR_COMPLETED_WEEKS", "PHASE", "REVISION")
_WEEK_RE = re.compile(r"^(\d{4})-W(\d{2})$")


def _short_target(value: str) -> str:
    if "." not in value:
        return value
    namespace, target = value.split(".", 1)
    return target if namespace == "F80" else ""


def validate_sport_scope(*, sports: Sequence[str]) -> dict[str, Any]:
    result = {sport: "APPLICABLE" if sport == "RUNNING" else "NOT_APPLICABLE" for sport in sports}
    result["severity"] = "ERROR"
    return result


def classify_native_targets(*, native_targets: Sequence[str]) -> dict[str, Any]:
    categories = [
        TARGET_CATEGORIES.get(_short_target(target), FitzgeraldCategory.UNACCOUNTED).value
        for target in native_targets
    ]
    return {"categories": categories, "severity": "ERROR"}


def account_segments(
    *, segments: Sequence[Mapping[str, Any]], **_: Any
) -> dict[str, Any]:
    totals = {
        FitzgeraldCategory.LOW: 0,
        FitzgeraldCategory.MODERATE: 0,
        FitzgeraldCategory.HIGH: 0,
    }
    unaccounted = 0
    unaccounted_segments = 0
    for segment in segments:
        duration = segment.get("duration_seconds")
        category = TARGET_CATEGORIES.get(_short_target(str(segment.get("native_target", ""))))
        valid_duration = isinstance(duration, int) and not isinstance(duration, bool) and duration > 0
        open_load = segment.get("load_mode") == "OPEN"
        if not valid_duration or category is None or open_load:
            unaccounted_segments += 1
            if valid_duration:
                unaccounted += duration
            continue
        totals[category] += duration
    accounted = sum(totals.values())
    return {
        "low_seconds": totals[FitzgeraldCategory.LOW],
        "moderate_seconds": totals[FitzgeraldCategory.MODERATE],
        "high_seconds": totals[FitzgeraldCategory.HIGH],
        "accounted_seconds": accounted,
        "unaccounted_seconds": unaccounted,
        "unaccounted_segments": unaccounted_segments,
    }


def _seconds(value: Any, field_name: str) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float):
        raise DomainError(f"{field_name} must be exact non-negative seconds")
    try:
        result = Decimal(value)
    except Exception as exc:
        raise DomainError(f"{field_name} must be exact non-negative seconds") from exc
    if not result.is_finite() or result < 0:
        raise DomainError(f"{field_name} must be exact non-negative seconds")
    return result


def _percent(numerator: Decimal, denominator: Decimal) -> str:
    if denominator == 0:
        return "0"
    value = (Decimal(numerator) * Decimal(100) / Decimal(denominator)).quantize(
        Decimal("0.1"), rounding=ROUND_HALF_UP
    )
    return canonical_decimal(value)


def interpret_distribution(
    *,
    low_seconds: int,
    moderate_seconds: int,
    high_seconds: int,
    configured_tolerance: Mapping[str, Any] | None = None,
    **_: Any,
) -> dict[str, Any]:
    """Report exact distribution without manufacturing an 80/20 pass band."""

    low = _seconds(low_seconds, "low_seconds")
    moderate = _seconds(moderate_seconds, "moderate_seconds")
    high = _seconds(high_seconds, "high_seconds")
    total = low + moderate + high
    result = {
        "low_percent": _percent(low, total),
        "hard_percent": _percent(moderate + high, total),
        "low_seconds": low,
        "hard_seconds": moderate + high,
        "status": "UNRATED",
        "severity": "INFORMATIONAL",
        "accounted_seconds": total,
        "tolerance_applied": False,
    }
    if configured_tolerance is not None:
        result["tolerance_reason"] = "NO_FROZEN_V1_NUMERIC_PASS_BAND"
    return result


def aggregate_runtime_results(
    *,
    activity_results: Sequence[Mapping[str, Any]],
    include: Sequence[str] = (
        "WEEK",
        "ROLLING_FOUR_COMPLETED_WEEKS",
        "PHASE",
        "REVISION",
    ),
    completed_weeks: Sequence[str] = (),
) -> dict[str, Any]:
    """Aggregate exact native result durations into canonical ISO-week windows."""
    grouped: dict[str, dict[str, Any]] = {}
    phase_totals: dict[str, dict[str, Any]] = {}
    unassigned_phase_results = 0
    completed = set(completed_weeks)
    for result in activity_results:
        raw_date = result.get("local_activity_date")
        try:
            day = date.fromisoformat(str(raw_date))
        except ValueError as exc:
            raise DomainError("runtime results require an ISO local_activity_date") from exc
        iso_year, iso_week, _ = day.isocalendar()
        label = f"{iso_year:04d}-W{iso_week:02d}"
        bucket = grouped.setdefault(
            label,
            {
                "week": label,
                "low_seconds": Decimal(0),
                "moderate_seconds": Decimal(0),
                "high_seconds": Decimal(0),
                "complete": label in completed,
                "_qualities": [],
                "_metric_supported_seconds": Decimal(0),
                "_unsupported_seconds": Decimal(0),
                "_unsupported_unknown": False,
            },
        )
        category = result.get("category_seconds")
        if not isinstance(category, Mapping):
            raise DomainError("Fitzgerald runtime result lacks category_seconds")
        bucket["low_seconds"] += _seconds(category.get("LOW", 0), "LOW")
        bucket["moderate_seconds"] += _seconds(category.get("MODERATE", 0), "MODERATE")
        bucket["high_seconds"] += _seconds(category.get("HIGH", 0), "HIGH")
        quality = result.get("quality")
        if quality not in {"VALID", "PARTIAL", "INSUFFICIENT", "UNAVAILABLE"}:
            raise DomainError("Fitzgerald runtime result lacks a valid evidence quality")
        coverage = result.get("coverage")
        if not isinstance(coverage, Mapping):
            raise DomainError("Fitzgerald runtime result lacks metric coverage")
        supported = _seconds(
            coverage.get("metric_supported_seconds", 0),
            "metric_supported_seconds",
        )
        raw_unsupported = coverage.get("unsupported_seconds")
        bucket["_qualities"].append(quality)
        bucket["_metric_supported_seconds"] += supported
        if raw_unsupported is None:
            bucket["_unsupported_unknown"] = True
        else:
            bucket["_unsupported_seconds"] += _seconds(
                raw_unsupported, "unsupported_seconds"
            )
        phase = result.get("phase")
        if isinstance(phase, str) and phase.strip():
            phase_bucket = phase_totals.setdefault(
                phase,
                {
                    "low_seconds": Decimal(0),
                    "moderate_seconds": Decimal(0),
                    "high_seconds": Decimal(0),
                    "_qualities": [],
                    "_metric_supported_seconds": Decimal(0),
                    "_unsupported_seconds": Decimal(0),
                    "_unsupported_unknown": False,
                },
            )
            phase_bucket["low_seconds"] += _seconds(category.get("LOW", 0), "LOW")
            phase_bucket["moderate_seconds"] += _seconds(
                category.get("MODERATE", 0), "MODERATE"
            )
            phase_bucket["high_seconds"] += _seconds(
                category.get("HIGH", 0), "HIGH"
            )
            phase_bucket["_qualities"].append(quality)
            phase_bucket["_metric_supported_seconds"] += supported
            if raw_unsupported is None:
                phase_bucket["_unsupported_unknown"] = True
            else:
                phase_bucket["_unsupported_seconds"] += _seconds(
                    raw_unsupported, "unsupported_seconds"
                )
        else:
            unassigned_phase_results += 1
    aggregate = distribution_windows(
        local_calendar_weeks=list(grouped.values()),
        include=include,
    )
    for week, bucket in grouped.items():
        aggregate[week].update(_runtime_evidence_summary((bucket,)))
    context = aggregate.setdefault("context", {})
    completed_buckets = [
        grouped[week]
        for week in sorted(grouped)
        if grouped[week].get("complete") is True
    ][-4:]
    if "ROLLING_FOUR_COMPLETED_WEEKS" in include:
        context["ROLLING_FOUR_COMPLETED_WEEKS"].update(
            _runtime_evidence_summary(completed_buckets)
        )
    if "REVISION" in include:
        context["REVISION"].update(
            _runtime_evidence_summary(tuple(grouped[week] for week in sorted(grouped)))
        )
    if "PHASE" in include:
        context["PHASE"] = {
            "coverage": (
                "FULL_WINDOW"
                if phase_totals and unassigned_phase_results == 0
                else "PARTIAL_WINDOW"
            ),
            "phases": {
                phase: {
                    key: value
                    for key, value in phase_totals[phase].items()
                    if not key.startswith("_")
                }
                | _runtime_evidence_summary((phase_totals[phase],))
                for phase in sorted(phase_totals)
            },
            "unassigned_activity_count": unassigned_phase_results,
            "status": "UNRATED",
        }
    return aggregate


def _runtime_evidence_summary(
    buckets: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    qualities = [
        str(quality)
        for bucket in buckets
        for quality in bucket.get("_qualities", ())
    ]
    if not qualities or all(item == "UNAVAILABLE" for item in qualities):
        quality = "UNAVAILABLE"
    elif all(item == "VALID" for item in qualities):
        quality = "VALID"
    elif not any(item in {"VALID", "PARTIAL"} for item in qualities):
        quality = "INSUFFICIENT"
    else:
        quality = "PARTIAL"
    supported = sum(
        (
            _seconds(bucket.get("_metric_supported_seconds", 0), "metric_supported_seconds")
            for bucket in buckets
        ),
        Decimal(0),
    )
    unsupported_unknown = any(
        bucket.get("_unsupported_unknown") is True for bucket in buckets
    )
    unsupported = None if unsupported_unknown else sum(
        (
            _seconds(bucket.get("_unsupported_seconds", 0), "unsupported_seconds")
            for bucket in buckets
        ),
        Decimal(0),
    )
    return {
        "evidence_quality": quality,
        "evidence_status": "UNRATED" if quality == "VALID" else "UNKNOWN",
        "metric_supported_seconds": supported,
        "unsupported_seconds": unsupported,
    }


def distribution_windows(
    *, local_calendar_weeks: Sequence[Mapping[str, Any]], include: Sequence[str], **_: Any
) -> dict[str, Any]:
    requested = tuple(include)
    if len(requested) != len(set(requested)) or any(item not in _WINDOW_ORDER for item in requested):
        raise DomainError("distribution windows must be unique frozen window identifiers")
    windows = [item for item in _WINDOW_ORDER if item in requested]
    result: dict[str, Any] = {
        "primary_window": "WEEK",
        "windows": windows,
        "severity": "INFORMATIONAL",
    }
    seen: set[str] = set()
    ordered_weeks = sorted(local_calendar_weeks, key=lambda week: str(week.get("week", "")))
    for item in ordered_weeks:
        week = item.get("week")
        match = _WEEK_RE.fullmatch(week) if isinstance(week, str) else None
        if match is None or week in seen:
            raise DomainError("calendar weeks require unique canonical labels")
        try:
            date.fromisocalendar(int(match.group(1)), int(match.group(2)), 1)
        except ValueError as exc:
            raise DomainError("calendar weeks require valid ISO week labels") from exc
        seen.add(week)
        low = _seconds(item.get("low_seconds", 0), "low_seconds")
        moderate = _seconds(item.get("moderate_seconds", 0), "moderate_seconds")
        high = _seconds(item.get("high_seconds", 0), "high_seconds")
        result[week] = {
            "coverage": "FULL_WINDOW" if item.get("complete") is True else "PARTIAL_WINDOW",
            "low_seconds": low,
            "moderate_seconds": moderate,
            "high_seconds": high,
            "compared_as_full_week": item.get("complete") is True,
        }
    context: dict[str, Any] = {}
    if "ROLLING_FOUR_COMPLETED_WEEKS" in windows:
        completed = [item for item in ordered_weeks if item.get("complete") is True][-4:]
        context["ROLLING_FOUR_COMPLETED_WEEKS"] = {
            "weeks": [item["week"] for item in completed],
            "coverage": "FULL_WINDOW" if len(completed) == 4 else "INSUFFICIENT_COMPLETED_WEEKS",
            "low_seconds": sum(_seconds(item.get("low_seconds", 0), "low_seconds") for item in completed),
            "moderate_seconds": sum(
                _seconds(item.get("moderate_seconds", 0), "moderate_seconds")
                for item in completed
            ),
            "high_seconds": sum(_seconds(item.get("high_seconds", 0), "high_seconds") for item in completed),
            "status": "UNRATED",
        }
    if "REVISION" in windows:
        context["REVISION"] = {
            "coverage": "FULL_WINDOW"
            if ordered_weeks and all(item.get("complete") is True for item in ordered_weeks)
            else "PARTIAL_WINDOW",
            "low_seconds": sum(_seconds(item.get("low_seconds", 0), "low_seconds") for item in ordered_weeks),
            "moderate_seconds": sum(
                _seconds(item.get("moderate_seconds", 0), "moderate_seconds")
                for item in ordered_weeks
            ),
            "high_seconds": sum(_seconds(item.get("high_seconds", 0), "high_seconds") for item in ordered_weeks),
            "status": "UNRATED",
        }
    if context:
        result["context"] = context
    return result


def validate_gap_zone_purpose(
    *,
    native_target: str,
    purpose: str | None,
    exception: bool = False,
    exception_reason: str | None = None,
    approver: str | None = None,
    **_: Any,
) -> dict[str, Any]:
    target = _short_target(native_target)
    category = TARGET_CATEGORIES.get(target)
    if category is None:
        return {
            "accepted": False,
            "category": FitzgeraldCategory.UNACCOUNTED.value,
            "reason": "FOREIGN_OR_UNKNOWN_NATIVE_TARGET",
            "severity": "ERROR",
        }
    if target not in {"ZONE_X", "ZONE_Y"}:
        return {"accepted": True, "category": category.value, "severity": "WARNING"}
    if not isinstance(purpose, str) or not purpose.strip():
        return {
            "accepted": False,
            "category": category.value,
            "reason": "GAP_ZONE_PURPOSE_REQUIRED",
            "severity": "WARNING",
        }
    if not isinstance(exception, bool):
        return {
            "accepted": False,
            "category": category.value,
            "reason": "STRUCTURED_EXCEPTION_FLAG_REQUIRED",
            "severity": "WARNING",
        }
    if exception:
        if not isinstance(exception_reason, str) or not exception_reason.strip() or not isinstance(
            approver, str
        ) or not approver.strip():
            return {
                "accepted": False,
                "category": category.value,
                "reason": "EXCEPTION_PROVENANCE_REQUIRED",
                "severity": "WARNING",
            }
    return {"accepted": True, "category": category.value, "severity": "WARNING"}


def validate_workout_vocabulary(
    *,
    family: str,
    description_provenance: str,
    segments: Sequence[Mapping[str, Any]],
    **_: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if family not in RUNNING_WORKOUT_FAMILIES:
        reasons.append("UNKNOWN_RUNNING_WORKOUT_FAMILY")
    if description_provenance != "ORIGINAL":
        reasons.append("ORIGINAL_DESCRIPTION_PROVENANCE_REQUIRED")
    if not segments:
        reasons.append("STRUCTURED_SEGMENTS_REQUIRED")
    for segment in segments:
        if not isinstance(segment.get("purpose"), str) or not segment["purpose"].strip():
            reasons.append("SEGMENT_PURPOSE_REQUIRED")
        if _short_target(str(segment.get("native_target", ""))) not in TARGET_CATEGORIES:
            reasons.append("FITZGERALD_NATIVE_PRESCRIPTION_REQUIRED")
    result: dict[str, Any] = {
        "valid": not reasons,
        "official_catalog_claim": False,
        "severity": "ERROR",
    }
    if reasons:
        result["reasons"] = sorted(set(reasons))
    return result


def _exact_optional(value: Any, field_name: str) -> Decimal | None:
    return None if value is None else exact_decimal(value, field_name)


def apply_goal_intent(
    *, completion: Mapping[str, Any], performance: Mapping[str, Any], **_: Any
) -> dict[str, Any]:
    """Verify intent overlays leave Fitzgerald identity and mathematics unchanged."""

    completion_method = completion.get("methodology_id", OWNER)
    performance_method = performance.get("methodology_id", OWNER)
    zone_inputs = ("threshold_speed_mps", "lthr_bpm")
    zone_math_equal = all(
        _exact_optional(completion.get(name), name)
        == _exact_optional(performance.get(name), name)
        for name in zone_inputs
    )
    accounting_equal = completion.get("category_mapping", TARGET_CATEGORIES) == performance.get(
        "category_mapping", TARGET_CATEGORIES
    )
    unchanged = completion_method == performance_method == OWNER
    return {
        "methodology_id_unchanged": unchanged,
        "zone_math_equal": zone_math_equal,
        "accounting_equal": accounting_equal,
        "accepted": unchanged and zone_math_equal and accounting_equal,
        "severity": "ERROR",
    }


def validate(candidate: Any) -> tuple[ValidationFinding, ...]:
    """Validate a frozen candidate through the MethodologyPolicy boundary."""

    parameter_snapshot = getattr(candidate, "parameter_snapshot", {})
    manifest = getattr(candidate, "manifest", {})
    workouts = tuple(getattr(candidate, "workouts", ()))
    findings: list[ValidationFinding] = []
    if isinstance(manifest, Mapping) and manifest.get("methodology_id") not in {None, OWNER}:
        findings.append(
            ValidationFinding(
                "F80-V1-001",
                ValidationLayer.METHODOLOGY,
                OWNER,
                FindingSeverity.ERROR,
                "Candidate manifest does not match the selected Fitzgerald methodology.",
                evidence={"manifest_methodology_id": manifest.get("methodology_id")},
            )
        )
    if isinstance(parameter_snapshot, Mapping):
        foreign_keys = sorted(
            set(parameter_snapshot)
            & {"selected_adjustment", "ceiling_bpm", "lower_bpm", "state", "higher_intensity_state"}
        )
        if foreign_keys:
            findings.append(
                ValidationFinding(
                    "F80-V1-012",
                    ValidationLayer.METHODOLOGY,
                    OWNER,
                    FindingSeverity.ERROR,
                    "Fitzgerald candidates cannot consume Maffetone parameter or authorization state.",
                    evidence={"foreign_fields": foreign_keys},
                )
            )
        readiness = select_primary_metric(
            pace=parameter_snapshot.get("pace", {}),
            lthr=parameter_snapshot.get("lthr", {}),
            context=str(parameter_snapshot.get("context", "STEADY_AEROBIC")),
        )
        if readiness["blocked"]:
            findings.append(
                ValidationFinding(
                    "F80-V1-006",
                    ValidationLayer.METHODOLOGY,
                    OWNER,
                    FindingSeverity.ERROR,
                    "Named Fitzgerald numeric generation requires usable threshold pace or LTHR evidence.",
                    evidence={"reason": readiness["reason"], "invented_target": False},
                )
            )
    by_week: dict[str, list[Mapping[str, Any]]] = {}
    for workout in workouts:
        workout_id = getattr(workout, "workout_id", None)
        workout_metadata = getattr(workout, "metadata", None)
        exception_map = (
            workout_metadata.get("fitzgerald_gap_zone_exceptions", {})
            if isinstance(workout_metadata, Mapping)
            else {}
        )
        vocabulary = validate_workout_vocabulary(
            family=getattr(workout, "family", ""),
            description_provenance=(
                workout_metadata.get("description_provenance", "ORIGINAL")
                if isinstance(workout_metadata, Mapping)
                else "ORIGINAL"
            ),
            segments=[
                {
                    "purpose": getattr(segment, "purpose", None),
                    "native_target": getattr(getattr(segment, "prescription", None), "native_target", ""),
                }
                for segment in getattr(workout, "segments", ())
            ],
        )
        if not vocabulary["valid"]:
            findings.append(
                ValidationFinding(
                    "F80-V1-012",
                    ValidationLayer.METHODOLOGY,
                    OWNER,
                    FindingSeverity.ERROR,
                    "Workout must use original product vocabulary and Fitzgerald-native structured prescriptions.",
                    workout_id=workout_id,
                    evidence={"reasons": vocabulary.get("reasons", [])},
                )
            )
        for index, segment in enumerate(getattr(workout, "segments", ()), start=1):
            prescription = getattr(segment, "prescription", None)
            native_target = getattr(prescription, "native_target", "")
            short_target = _short_target(str(native_target))
            if short_target not in {"ZONE_X", "ZONE_Y"}:
                continue
            segment_id = f"segment-{index}"
            exception_value = exception_map.get(segment_id, {}) if isinstance(exception_map, Mapping) else {}
            gap_result = validate_gap_zone_purpose(
                native_target=str(native_target),
                purpose=getattr(segment, "purpose", None),
                exception=bool(exception_value),
                exception_reason=exception_value.get("reason")
                if isinstance(exception_value, Mapping)
                else None,
                approver=exception_value.get("approver")
                if isinstance(exception_value, Mapping)
                else None,
            )
            findings.append(
                ValidationFinding(
                    "F80-V1-011",
                    ValidationLayer.METHODOLOGY,
                    OWNER,
                    FindingSeverity.WARNING,
                    "Fitzgerald gap-zone use requires typed purpose and explicit exception provenance where applicable.",
                    workout_id=workout_id,
                    segment_id=segment_id,
                    evidence={
                        "native_target": short_target,
                        "accepted": gap_result["accepted"],
                        "reason": gap_result.get("reason"),
                        "category": gap_result["category"],
                    },
                )
            )
        week = getattr(workout, "scheduled_date", None)
        week_key = week.isocalendar()[:2] if week is not None else None
        if week_key is not None:
            label = f"{week_key[0]:04d}-W{week_key[1]:02d}"
            by_week.setdefault(label, []).extend(
                {
                    "duration_seconds": getattr(segment, "duration_seconds", None),
                    "load_mode": getattr(
                        getattr(segment, "load_mode", None),
                        "value",
                        getattr(segment, "load_mode", None),
                    ),
                    "native_target": getattr(getattr(segment, "prescription", None), "native_target", ""),
                }
                for segment in getattr(workout, "segments", ())
            )
    for week in sorted(by_week):
        accounting = account_segments(segments=by_week[week])
        distribution = interpret_distribution(
            low_seconds=accounting["low_seconds"],
            moderate_seconds=accounting["moderate_seconds"],
            high_seconds=accounting["high_seconds"],
        )
        findings.append(
            ValidationFinding(
                "F80-V1-009",
                ValidationLayer.METHODOLOGY,
                OWNER,
                FindingSeverity.INFORMATIONAL,
                "Exact weekly Fitzgerald distribution is reported without a V1 pass/fail band.",
                evidence={"week": week, **distribution, **accounting},
            )
        )
    return tuple(sorted(findings, key=lambda item: (item.rule_id, item.workout_id or "", item.segment_id or "")))


@dataclass(frozen=True, slots=True)
class FitzgeraldMethodologyPolicy:
    methodology_id: MethodologyId = MethodologyId.FITZGERALD_80_20_RUNNING_V1

    def validate(self, value: Any) -> tuple[ValidationFinding, ...]:
        return validate(value)


POLICY = FitzgeraldMethodologyPolicy()
