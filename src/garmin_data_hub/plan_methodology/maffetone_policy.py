"""Deterministic Maffetone V1 prescription-policy behavior."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .domain import DomainError, MethodologyId
from .maffetone_parameters import derive, validate_adjustment
from .validation import FindingSeverity, ValidationFinding, ValidationLayer


OWNER = MethodologyId.MAFFETONE_RUNNING_V1.value
_GUIDELINE_MIN_SECONDS = 12 * 60
_GUIDELINE_MAX_SECONDS = 15 * 60


def validate_sport_scope(*, sports: Sequence[str]) -> dict[str, Any]:
    result = {sport: "APPLICABLE" if sport == "RUNNING" else "NOT_APPLICABLE" for sport in sports}
    result["severity"] = "ERROR"
    return result


def validate_aerobic_base(
    *,
    state: str,
    ceiling_bpm: int,
    segments: Sequence[Mapping[str, Any]],
    event_exception: Mapping[str, Any] | None = None,
    **_: Any,
) -> dict[str, Any]:
    if not isinstance(ceiling_bpm, int) or isinstance(ceiling_bpm, bool) or ceiling_bpm <= 0:
        raise DomainError("ceiling_bpm must be a positive integer")
    if state not in {"AEROBIC_BASE", "HIGHER_INTENSITY_PERMITTED"}:
        raise DomainError(f"unknown Maffetone state: {state!r}")
    if event_exception and event_exception.get("acknowledged") is True:
        event_only = bool(segments) and all(segment.get("kind") == "EVENT" for segment in segments)
        if event_only and event_exception.get("context") == "EVENT_DAY":
            return {
                "accepted": True,
                "aerobic_training_conformance": "NOT_APPLICABLE",
                "ceiling_changed": False,
                "range_changed": False,
                "state_changed": False,
                "training_authorization_changed": False,
            }
    if state == "AEROBIC_BASE":
        for segment in segments:
            upper = segment.get("upper_bpm")
            if upper is not None and upper > ceiling_bpm:
                return {
                    "accepted": False,
                    "reason": "ABOVE_CEILING_PRESCRIPTION",
                    "severity": "ERROR",
                }
    return {"accepted": True, "severity": "ERROR"}


def _duration_for(segments: Sequence[Mapping[str, Any]], kind: str) -> int:
    total = 0
    for segment in segments:
        if segment.get("kind") != kind:
            continue
        duration = segment.get("duration_seconds")
        if not isinstance(duration, int) or isinstance(duration, bool) or duration <= 0:
            raise DomainError(f"{kind} duration must be a positive integer")
        total += duration
    return total


def validate_warmup_cooldown(
    *,
    session_seconds: int,
    segments: Sequence[Mapping[str, Any]],
    exception_reason: str | None = None,
    **_: Any,
) -> dict[str, Any]:
    if not isinstance(session_seconds, int) or isinstance(session_seconds, bool) or session_seconds <= 0:
        raise DomainError("session_seconds must be a positive integer")
    warmup = _duration_for(segments, "WARMUP")
    cooldown = _duration_for(segments, "COOLDOWN")
    if isinstance(exception_reason, str) and exception_reason.strip():
        finding = "DOCUMENTED_WARMUP_COOLDOWN_EXCEPTION"
    else:
        warmup_ok = _GUIDELINE_MIN_SECONDS <= warmup <= _GUIDELINE_MAX_SECONDS
        cooldown_ok = _GUIDELINE_MIN_SECONDS <= cooldown <= _GUIDELINE_MAX_SECONDS
        if warmup == 0 and cooldown == 0:
            finding = "MISSING_WARMUP_AND_COOLDOWN"
        elif warmup == 0:
            finding = "MISSING_WARMUP"
        elif cooldown == 0:
            finding = "MISSING_COOLDOWN"
        elif not warmup_ok or not cooldown_ok:
            finding = "WARMUP_OR_COOLDOWN_OUTSIDE_GUIDELINE"
        else:
            finding = None
    return {
        "accepted": True,
        "finding": finding,
        "warmup_seconds": warmup,
        "cooldown_seconds": cooldown,
        "severity": "WARNING",
    }


def validate_maf_test(
    *,
    benchmark_identity: str,
    protocol_version: str,
    ceiling_bpm: int,
    warmup_seconds: int,
    measurement_seconds: int,
    cooldown_seconds: int,
    hr_coverage: str,
    conditions: Mapping[str, Any],
    segments: Sequence[Mapping[str, Any]] | None = None,
    **_: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if benchmark_identity != "MAF_TEST":
        reasons.append("EXPLICIT_MAF_TEST_IDENTITY_REQUIRED")
    if not isinstance(protocol_version, str) or not protocol_version.strip():
        reasons.append("PROTOCOL_VERSION_REQUIRED")
    if not isinstance(ceiling_bpm, int) or isinstance(ceiling_bpm, bool) or ceiling_bpm <= 0:
        reasons.append("CONFIRMED_MAF_CEILING_REQUIRED")
    if not isinstance(warmup_seconds, int) or isinstance(warmup_seconds, bool) or not (
        _GUIDELINE_MIN_SECONDS <= warmup_seconds <= _GUIDELINE_MAX_SECONDS
    ):
        reasons.append("WARMUP_OUTSIDE_GUIDELINE")
    valid_measurement = (
        isinstance(measurement_seconds, int)
        and not isinstance(measurement_seconds, bool)
        and measurement_seconds > 0
    )
    if isinstance(protocol_version, str) and protocol_version.startswith("MAF-GPS-"):
        valid_measurement = valid_measurement and 15 * 60 <= measurement_seconds <= 30 * 60
    if not valid_measurement:
        reasons.append("MEASUREMENT_DURATION_OUTSIDE_PROTOCOL")
    if not isinstance(cooldown_seconds, int) or isinstance(cooldown_seconds, bool) or not (
        _GUIDELINE_MIN_SECONDS <= cooldown_seconds <= _GUIDELINE_MAX_SECONDS
    ):
        reasons.append("COOLDOWN_OUTSIDE_GUIDELINE")
    if hr_coverage != "VALID":
        reasons.append("VALID_HR_COVERAGE_REQUIRED")
    if not isinstance(conditions, Mapping) or not conditions:
        reasons.append("COMPARABILITY_CONDITIONS_REQUIRED")
    if segments is not None:
        typed_totals = {
            "WARMUP": _duration_for(segments, "WARMUP"),
            "COOLDOWN": _duration_for(segments, "COOLDOWN"),
        }
        work_segments = [segment for segment in segments if segment.get("kind") == "WORK"]
        work_duration = sum(
            segment["duration_seconds"]
            for segment in work_segments
            if isinstance(segment.get("duration_seconds"), int)
            and not isinstance(segment.get("duration_seconds"), bool)
            and segment["duration_seconds"] > 0
        )
        if typed_totals["WARMUP"] == 0:
            reasons.append("WARMUP_SEGMENT_REQUIRED")
        if not work_segments:
            reasons.append("MEASUREMENT_SEGMENT_REQUIRED")
        if typed_totals["COOLDOWN"] == 0:
            reasons.append("COOLDOWN_SEGMENT_REQUIRED")
        if (
            typed_totals["WARMUP"] != warmup_seconds
            or typed_totals["COOLDOWN"] != cooldown_seconds
            or (
                isinstance(protocol_version, str)
                and protocol_version.startswith("MAF-GPS-")
                and work_duration != measurement_seconds
            )
        ):
            reasons.append("MAF_TEST_SEGMENT_DURATION_MISMATCH")
    result: dict[str, Any] = {
        "valid": not reasons,
        "ordinary_run_auto_labeled": False,
        "severity": "ERROR",
    }
    if reasons:
        result["reasons"] = sorted(reasons)
    return result


def transition_state(
    *,
    current_state: str,
    goal_intent: str,
    user_confirmed: bool,
    evidence_reference: str | None,
    safety_errors: Sequence[Any],
    new_revision: bool = False,
    requested_state: str = "HIGHER_INTENSITY_PERMITTED",
    **_: Any,
) -> dict[str, Any]:
    if current_state not in {"AEROBIC_BASE", "HIGHER_INTENSITY_PERMITTED"}:
        raise DomainError(f"unknown Maffetone state: {current_state!r}")
    if goal_intent not in {"COMPLETION", "PERFORMANCE"}:
        raise DomainError(f"unknown goal intent: {goal_intent!r}")
    if requested_state not in {"AEROBIC_BASE", "HIGHER_INTENSITY_PERMITTED"}:
        raise DomainError(f"unknown requested Maffetone state: {requested_state!r}")
    requirements_met = (
        user_confirmed is True
        and isinstance(evidence_reference, str)
        and bool(evidence_reference.strip())
        and not safety_errors
        and new_revision is True
    )
    accepted = requested_state == "AEROBIC_BASE" or requirements_met
    return {
        "accepted": accepted,
        "state": requested_state if accepted else current_state,
        "new_revision_required": True,
        "severity": "ERROR",
        "reason": None if accepted else "CONFIRMED_EVIDENCE_BACKED_REVISION_REQUIRED",
    }


def apply_goal_intent(
    *, completion: Mapping[str, Any], performance: Mapping[str, Any], **_: Any
) -> dict[str, Any]:
    def ceiling(values: Mapping[str, Any]) -> int:
        result = derive(
            completed_age=values["age"],
            calculation_date=values.get("calculation_date", "2000-01-01"),
            selected_adjustment=values["adjustment"],
            confirmed=True,
        )
        if result.get("blocked"):
            raise DomainError("goal-intent comparison requires usable ordinary MAF parameters")
        return result["ceiling_bpm"]

    completion_ceiling = ceiling(completion)
    performance_ceiling = ceiling(performance)
    completion_method = completion.get("methodology_id", OWNER)
    performance_method = performance.get("methodology_id", OWNER)
    unchanged = completion_method == performance_method == OWNER
    equal = completion_ceiling == performance_ceiling
    return {
        "methodology_id_unchanged": unchanged,
        "ceiling_equal": equal,
        "ceiling_bpm": completion_ceiling if equal else None,
        "accepted": unchanged and equal,
        "severity": "ERROR",
    }


def validate(candidate: Any) -> tuple[ValidationFinding, ...]:
    """Validate frozen structured workouts through the MethodologyPolicy boundary."""

    findings: list[ValidationFinding] = []
    parameter_snapshot = getattr(candidate, "parameter_snapshot", {})
    manifest = getattr(candidate, "manifest", {})
    if isinstance(manifest, Mapping) and manifest.get("methodology_id") not in {None, OWNER}:
        findings.append(
            ValidationFinding(
                "MAF-V1-001",
                ValidationLayer.METHODOLOGY,
                OWNER,
                FindingSeverity.ERROR,
                "Candidate manifest does not match the selected Maffetone methodology.",
                evidence={"manifest_methodology_id": manifest.get("methodology_id")},
            )
        )
    if isinstance(parameter_snapshot, Mapping):
        sensitive_names = {
            "raw_questionnaire_answers",
            "medication",
            "injury",
            "raw_health_answers",
        }

        def sensitive_fields(value: Any) -> set[str]:
            found: set[str] = set()
            if isinstance(value, Mapping):
                for key, item in value.items():
                    if key in sensitive_names:
                        found.add(str(key))
                    found.update(sensitive_fields(item))
            elif isinstance(value, (list, tuple)):
                for item in value:
                    found.update(sensitive_fields(item))
            return found

        sensitive = sorted(sensitive_fields(parameter_snapshot))
        if sensitive:
            findings.append(
                ValidationFinding(
                    "MAF-V1-005",
                    ValidationLayer.METHODOLOGY,
                    OWNER,
                    FindingSeverity.ERROR,
                    "Maffetone policy accepts only the privacy-minimized confirmed parameter snapshot.",
                    evidence={"forbidden_fields": sensitive},
                )
            )
        required = {
            "age_provenance",
            "formula_version",
            "calculation_date",
            "completed_age",
            "selected_adjustment",
            "confirmed",
            "ceiling_bpm",
            "lower_bpm",
            "provenance",
        }
        missing = sorted(required - set(parameter_snapshot))
        if missing:
            findings.append(
                ValidationFinding(
                    "MAF-V1-004",
                    ValidationLayer.METHODOLOGY,
                    OWNER,
                    FindingSeverity.ERROR,
                    "Named Maffetone generation requires a confirmed privacy-minimized parameter snapshot.",
                    evidence={"missing_fields": missing},
                )
            )
        else:
            if not isinstance(parameter_snapshot["formula_version"], str) or not parameter_snapshot[
                "formula_version"
            ].strip():
                findings.append(
                    ValidationFinding(
                        "MAF-V1-004",
                        ValidationLayer.METHODOLOGY,
                        OWNER,
                        FindingSeverity.ERROR,
                        "Maffetone formula version must be frozen before named-method generation.",
                        evidence={"reason": "FORMULA_VERSION_REQUIRED"},
                    )
                )
            if not isinstance(parameter_snapshot["age_provenance"], str) or not parameter_snapshot[
                "age_provenance"
            ].strip():
                findings.append(
                    ValidationFinding(
                        "MAF-V1-006",
                        ValidationLayer.METHODOLOGY,
                        OWNER,
                        FindingSeverity.ERROR,
                        "Completed age requires frozen age-as-of provenance.",
                        evidence={"reason": "AGE_PROVENANCE_REQUIRED"},
                    )
                )
            adjustment = validate_adjustment(
                selected_adjustment=parameter_snapshot["selected_adjustment"],
                confirmed=parameter_snapshot["confirmed"],
                provenance=parameter_snapshot["provenance"],
            )
            if not adjustment["accepted"]:
                findings.append(
                    ValidationFinding(
                        "MAF-V1-004",
                        ValidationLayer.METHODOLOGY,
                        OWNER,
                        FindingSeverity.ERROR,
                        "Maffetone adjustment requires exact user-confirmed frozen provenance.",
                        evidence={"reason": adjustment["reason"]},
                    )
                )
            try:
                derived = derive(
                    completed_age=parameter_snapshot["completed_age"],
                    calculation_date=parameter_snapshot["calculation_date"],
                    selected_adjustment=parameter_snapshot["selected_adjustment"],
                    confirmed=parameter_snapshot["confirmed"],
                    manual_ceiling=parameter_snapshot.get("manual_ceiling"),
                    manual_provenance=parameter_snapshot.get("manual_provenance"),
                )
            except DomainError as exc:
                findings.append(
                    ValidationFinding(
                        "MAF-V1-004",
                        ValidationLayer.METHODOLOGY,
                        OWNER,
                        FindingSeverity.ERROR,
                        "Maffetone parameter snapshot is not usable for named-method generation.",
                        evidence={"reason": str(exc)},
                    )
                )
            else:
                bounds_match = (
                    not derived.get("blocked", False)
                    and derived.get("ceiling_bpm") == parameter_snapshot["ceiling_bpm"]
                    and derived.get("lower_bpm") == parameter_snapshot["lower_bpm"]
                )
                if not bounds_match:
                    findings.append(
                        ValidationFinding(
                            "MAF-V1-003",
                            ValidationLayer.METHODOLOGY,
                            OWNER,
                            FindingSeverity.ERROR,
                            "Maffetone ceiling and aerobic range must match the frozen confirmed parameters.",
                            evidence={
                                "reason": derived.get("reason", "FORMULA_OR_RANGE_MISMATCH"),
                                "expected_ceiling_bpm": derived.get("ceiling_bpm"),
                                "expected_lower_bpm": derived.get("lower_bpm"),
                            },
                        )
                    )
    for workout in tuple(getattr(candidate, "workouts", ())):
        if str(getattr(workout, "sport", "RUNNING")) != "RUNNING":
            continue
        workout_id = getattr(workout, "workout_id", None)
        metadata = getattr(workout, "metadata", None)
        if isinstance(metadata, Mapping) and "fitzgerald_gap_zone_exceptions" in metadata:
            findings.append(
                ValidationFinding(
                    "MAF-V1-007",
                    ValidationLayer.METHODOLOGY,
                    OWNER,
                    FindingSeverity.ERROR,
                    "Maffetone workouts cannot consume Fitzgerald exception metadata.",
                    workout_id=workout_id,
                    evidence={"foreign_field": "fitzgerald_gap_zone_exceptions"},
                )
            )
        raw_segments = [
            {
                "kind": getattr(getattr(segment, "kind", None), "value", getattr(segment, "kind", None)),
                "duration_seconds": getattr(segment, "duration_seconds", None),
            }
            for segment in getattr(workout, "segments", ())
        ]
        duration = sum(
            item["duration_seconds"]
            for item in raw_segments
            if isinstance(item["duration_seconds"], int)
            and not isinstance(item["duration_seconds"], bool)
        )
        guidance = validate_warmup_cooldown(
            session_seconds=duration or 1,
            segments=raw_segments,
            exception_reason=None,
        )
        if guidance["finding"] is not None:
            findings.append(
                ValidationFinding(
                    "MAF-V1-009",
                    ValidationLayer.METHODOLOGY,
                    OWNER,
                    FindingSeverity.WARNING,
                    "Maffetone sessions normally include 12-15 minute warmup and cooldown segments.",
                    workout_id=workout_id,
                    evidence={
                        "finding": guidance["finding"],
                        "warmup_seconds": guidance["warmup_seconds"],
                        "cooldown_seconds": guidance["cooldown_seconds"],
                    },
                )
            )
        ceiling = parameter_snapshot.get("ceiling_bpm") if isinstance(parameter_snapshot, Mapping) else None
        state = parameter_snapshot.get("state", "AEROBIC_BASE") if isinstance(parameter_snapshot, Mapping) else "AEROBIC_BASE"
        if isinstance(ceiling, int) and not isinstance(ceiling, bool):
            prescribed_segments = []
            for segment in getattr(workout, "segments", ()):
                prescription = getattr(segment, "prescription", None)
                primary = getattr(prescription, "primary", None)
                upper = getattr(primary, "upper", None)
                prescribed_segments.append(
                    {
                        "kind": getattr(
                            getattr(segment, "kind", None),
                            "value",
                            getattr(segment, "kind", None),
                        ),
                        "upper_bpm": upper,
                    }
                )
            aerobic = validate_aerobic_base(
                state=state,
                ceiling_bpm=ceiling,
                segments=prescribed_segments,
                event_exception=(
                    getattr(workout, "metadata", {}).get("event_exception")
                    if isinstance(getattr(workout, "metadata", None), Mapping)
                    else None
                ),
            )
            if not aerobic["accepted"]:
                findings.append(
                    ValidationFinding(
                        "MAF-V1-008",
                        ValidationLayer.METHODOLOGY,
                        OWNER,
                        FindingSeverity.ERROR,
                        "Aerobic-base prescriptions may not exceed the confirmed MAF ceiling.",
                        workout_id=workout_id,
                        evidence={"reason": aerobic["reason"], "ceiling_bpm": ceiling},
                    )
                )
        if isinstance(metadata, Mapping) and metadata.get("benchmark_identity") == "MAF_TEST":
            test_result = validate_maf_test(
                benchmark_identity="MAF_TEST",
                protocol_version=metadata.get("protocol_version", ""),
                ceiling_bpm=ceiling,
                warmup_seconds=guidance["warmup_seconds"],
                measurement_seconds=metadata.get("measurement_seconds"),
                cooldown_seconds=guidance["cooldown_seconds"],
                hr_coverage=metadata.get("hr_coverage", ""),
                conditions=metadata.get("conditions", {}),
                segments=raw_segments,
            )
            if not test_result["valid"]:
                findings.append(
                    ValidationFinding(
                        "MAF-V1-010",
                        ValidationLayer.METHODOLOGY,
                        OWNER,
                        FindingSeverity.ERROR,
                        "MAF Test requires explicit protocol identity and complete comparable prescription metadata.",
                        workout_id=workout_id,
                        evidence={"reasons": test_result["reasons"]},
                    )
                )
        for index, segment in enumerate(getattr(workout, "segments", ()), start=1):
            prescription = getattr(segment, "prescription", None)
            if prescription is None or getattr(prescription, "methodology_id", None) != MethodologyId.MAFFETONE_RUNNING_V1:
                findings.append(
                    ValidationFinding(
                        "MAF-V1-007",
                        ValidationLayer.METHODOLOGY,
                        OWNER,
                        FindingSeverity.ERROR,
                        "Running segments require Maffetone-native prescriptions.",
                        workout_id=workout_id,
                        segment_id=f"segment-{index}",
                    )
                )
    return tuple(sorted(findings, key=lambda item: (item.rule_id, item.workout_id or "", item.segment_id or "")))


@dataclass(frozen=True, slots=True)
class MaffetoneMethodologyPolicy:
    methodology_id: MethodologyId = MethodologyId.MAFFETONE_RUNNING_V1

    def validate(self, value: Any) -> tuple[ValidationFinding, ...]:
        return validate(value)


POLICY = MaffetoneMethodologyPolicy()
