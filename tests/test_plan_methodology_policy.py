from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from garmin_data_hub.plan_methodology import fitzgerald_parameters, fitzgerald_policy
from garmin_data_hub.plan_methodology import maffetone_parameters, maffetone_policy
from garmin_data_hub.plan_methodology.domain import DomainError, MethodologyId, exact_decimal
from garmin_data_hub.plan_methodology.registry import resolve_methodology_policy
from garmin_data_hub.plan_methodology.validation import (
    FindingSeverity,
    ValidationFinding,
    ValidationLayer,
    run_validation_pipeline,
    validate_candidate,
)


@pytest.mark.parametrize(
    "pace,lthr,expected_metric,confidence,blocked",
    [
        (
            {"evidence_class": "MEASURED", "value": "4.00"},
            {"evidence_class": "MEASURED", "value": "168"},
            "PACE",
            "HIGH",
            False,
        ),
        (
            {"evidence_class": "MEASURED", "value": "4.00"},
            {"evidence_class": "UNAVAILABLE", "value": None},
            "PACE",
            "HIGH",
            False,
        ),
        (
            {"evidence_class": "ESTIMATED", "value": "4.00"},
            {"evidence_class": "MEASURED", "value": "168"},
            "HEART_RATE",
            "REDUCED",
            False,
        ),
        (
            {"evidence_class": "UNAVAILABLE", "value": None},
            {"evidence_class": "UNAVAILABLE", "value": None},
            None,
            "UNKNOWN",
            True,
        ),
    ],
)
def test_fitzgerald_threshold_readiness_never_invents_targets(
    pace, lthr, expected_metric, confidence, blocked
):
    result = fitzgerald_parameters.select_primary_metric(
        pace=pace, lthr=lthr, context="STEADY_AEROBIC"
    )
    assert result["primary_metric"] == expected_metric
    assert result["confidence"] == confidence
    assert result["blocked"] is blocked
    if expected_metric != "PACE":
        assert result["pace_target"] is None


def test_fitzgerald_all_native_zones_and_foreign_target_remain_distinct():
    result = fitzgerald_policy.classify_native_targets(
        native_targets=[
            "ZONE_1",
            "ZONE_2",
            "ZONE_X",
            "ZONE_3",
            "ZONE_Y",
            "ZONE_4",
            "ZONE_5",
            "MAF.MAF_AEROBIC_RANGE",
        ]
    )
    assert result["categories"] == [
        "LOW",
        "LOW",
        "MODERATE",
        "MODERATE",
        "HIGH",
        "HIGH",
        "HIGH",
        "UNACCOUNTED",
    ]


def test_fitzgerald_mixed_segments_use_leaf_time_and_leave_distance_unaccounted():
    result = fitzgerald_policy.account_segments(
        segments=[
            {"duration_seconds": 1200, "native_target": "ZONE_2"},
            {"duration_seconds": 1200, "native_target": "ZONE_4"},
            {"duration_seconds": 1200, "native_target": "ZONE_2"},
            {"distance_metres": 5000, "native_target": "ZONE_2"},
        ],
        workout_name="misleading hard title",
        tss=999,
    )
    assert result == {
        "low_seconds": 2400,
        "moderate_seconds": 0,
        "high_seconds": 1200,
        "accounted_seconds": 3600,
        "unaccounted_seconds": 0,
        "unaccounted_segments": 1,
    }


def test_fitzgerald_distribution_is_exact_and_unrated_even_with_arbitrary_tolerance():
    result = fitzgerald_policy.interpret_distribution(
        low_seconds=4794,
        moderate_seconds=0,
        high_seconds=1206,
        configured_tolerance={"weekly_percent": 5},
    )
    assert result["low_percent"] == "79.9"
    assert result["hard_percent"] == "20.1"
    assert result["status"] == "UNRATED"
    assert result["tolerance_applied"] is False


def test_fitzgerald_week_is_primary_and_partial_week_is_not_full_comparison():
    result = fitzgerald_policy.distribution_windows(
        local_calendar_weeks=[
            {"week": "2026-W40", "complete": False, "low_seconds": 3600, "high_seconds": 900}
        ],
        include=["REVISION", "WEEK", "PHASE", "ROLLING_FOUR_COMPLETED_WEEKS"],
    )
    assert result["primary_window"] == "WEEK"
    assert result["windows"] == ["WEEK", "ROLLING_FOUR_COMPLETED_WEEKS", "PHASE", "REVISION"]
    assert result["2026-W40"]["coverage"] == "PARTIAL_WINDOW"
    assert result["2026-W40"]["compared_as_full_week"] is False
    assert result["context"]["REVISION"]["status"] == "UNRATED"
    assert result["context"]["ROLLING_FOUR_COMPLETED_WEEKS"]["status"] == "UNRATED"


def test_fitzgerald_zone_x_routine_use_warns_but_explicit_race_exception_is_supported():
    routine = fitzgerald_policy.validate_gap_zone_purpose(
        native_target="ZONE_X", purpose=None
    )
    race_specific = fitzgerald_policy.validate_gap_zone_purpose(
        native_target="ZONE_X",
        purpose="RACE_SPECIFIC",
        exception=True,
        exception_reason="event-specific rehearsal",
        approver="synthetic-user",
    )
    assert routine == {
        "accepted": False,
        "category": "MODERATE",
        "reason": "GAP_ZONE_PURPOSE_REQUIRED",
        "severity": "WARNING",
    }
    assert race_specific["accepted"] is True
    assert race_specific["category"] == "MODERATE"


def test_fitzgerald_vocabulary_rejects_foreign_method_target():
    result = fitzgerald_policy.validate_workout_vocabulary(
        family="AEROBIC",
        description_provenance="ORIGINAL",
        segments=[{"purpose": "aerobic endurance", "native_target": "MAF.MAF_AEROBIC_RANGE"}],
    )
    assert result["valid"] is False
    assert result["official_catalog_claim"] is False
    assert result["reasons"] == ["FITZGERALD_NATIVE_PRESCRIPTION_REQUIRED"]


def test_fitzgerald_goal_intent_does_not_change_math():
    equal = fitzgerald_policy.apply_goal_intent(
        completion={"threshold_speed_mps": "4.00"},
        performance={"threshold_speed_mps": "4.0"},
    )
    unequal = fitzgerald_policy.apply_goal_intent(
        completion={"threshold_speed_mps": "4.00"},
        performance={"threshold_speed_mps": "4.01"},
    )
    assert equal["accepted"] is True
    assert equal["zone_math_equal"] is True
    assert unequal["accepted"] is False


@pytest.mark.parametrize("adjustment,ceiling,lower", [(-10, 130, 120), (-5, 135, 125), (0, 140, 130), (5, 145, 135)])
def test_maffetone_allowed_adjustments_use_exact_formula(adjustment, ceiling, lower):
    result = maffetone_parameters.derive(
        completed_age=40,
        calculation_date="2026-09-30",
        selected_adjustment=adjustment,
        confirmed=True,
    )
    assert (result["ceiling_bpm"], result["lower_bpm"]) == (ceiling, lower)


def test_maffetone_unconfirmed_and_invalid_adjustments_do_not_become_prescriptions():
    unconfirmed = maffetone_parameters.validate_adjustment(
        selected_adjustment=-5, confirmed=False, provenance="GARMIN_OBSERVATION"
    )
    invalid = maffetone_parameters.validate_adjustment(
        selected_adjustment=3, confirmed=True, provenance="USER_SELECTED"
    )
    assert unconfirmed["reason"] == "EXPLICIT_USER_CONFIRMATION_REQUIRED"
    assert invalid["reason"] == "INVALID_ADJUSTMENT"


def test_maffetone_age_exceptions_are_exact_and_never_add_beats():
    young = maffetone_parameters.derive(
        completed_age=16,
        calculation_date="2026-09-30",
        selected_adjustment=0,
        confirmed=True,
    )
    older = maffetone_parameters.derive(
        completed_age=70,
        calculation_date="2026-09-30",
        selected_adjustment=5,
        confirmed=True,
    )
    assert young["reason"] == "MANUAL_AGE_EXCEPTION_REQUIRED"
    assert older["ceiling_bpm"] == 115
    assert older["automatic_extra_beats"] == 0


def test_maffetone_native_range_is_inclusive_and_sub_maf_stays_native():
    result = maffetone_parameters.build_prescriptions(ceiling_bpm=145)
    assert result["MAF_AEROBIC_RANGE"] == {
        "lower": 135,
        "upper": 145,
        "lower_inclusive": True,
        "upper_inclusive": True,
    }
    assert result["SUB_MAF"] == {"upper": 135}
    assert "ZONE_2" not in result


def test_maffetone_warmup_cooldown_guideline_is_warning_not_block():
    missing = maffetone_policy.validate_warmup_cooldown(
        session_seconds=4200,
        segments=[{"kind": "WORK", "duration_seconds": 4200}],
    )
    complete = maffetone_policy.validate_warmup_cooldown(
        session_seconds=4200,
        segments=[
            {"kind": "WARMUP", "duration_seconds": 900},
            {"kind": "WORK", "duration_seconds": 2580},
            {"kind": "COOLDOWN", "duration_seconds": 720},
        ],
    )
    assert missing["accepted"] is True
    assert missing["finding"] == "MISSING_WARMUP_AND_COOLDOWN"
    assert complete["finding"] is None


def test_maf_test_is_explicit_and_ordinary_run_is_not_auto_labeled():
    valid = maffetone_policy.validate_maf_test(
        benchmark_identity="MAF_TEST",
        protocol_version="MAF-GPS-30MIN-V1",
        ceiling_bpm=145,
        warmup_seconds=900,
        measurement_seconds=1800,
        cooldown_seconds=720,
        hr_coverage="VALID",
        conditions={"course": "synthetic-loop", "weather": "synthetic"},
    )
    ordinary = maffetone_policy.validate_maf_test(
        benchmark_identity="AEROBIC_RUN",
        protocol_version="MAF-GPS-30MIN-V1",
        ceiling_bpm=145,
        warmup_seconds=900,
        measurement_seconds=1800,
        cooldown_seconds=720,
        hr_coverage="VALID",
        conditions={"course": "synthetic-loop"},
    )
    assert valid["valid"] is True
    assert valid["ordinary_run_auto_labeled"] is False
    assert ordinary["valid"] is False


def test_maffetone_higher_intensity_requires_confirmation_evidence_and_new_revision():
    rejected = maffetone_policy.transition_state(
        current_state="AEROBIC_BASE",
        goal_intent="PERFORMANCE",
        user_confirmed=False,
        evidence_reference=None,
        safety_errors=[],
    )
    accepted = maffetone_policy.transition_state(
        current_state="AEROBIC_BASE",
        goal_intent="COMPLETION",
        user_confirmed=True,
        evidence_reference="reviewed-maf-evidence-1",
        safety_errors=[],
        new_revision=True,
    )
    assert rejected["state"] == "AEROBIC_BASE"
    assert rejected["accepted"] is False
    assert accepted["state"] == "HIGHER_INTENSITY_PERMITTED"
    assert accepted["accepted"] is True


def test_maffetone_event_day_exception_does_not_unlock_training():
    result = maffetone_policy.validate_aerobic_base(
        state="AEROBIC_BASE",
        ceiling_bpm=145,
        segments=[{"kind": "EVENT", "upper_bpm": None}],
        event_exception={"acknowledged": True, "context": "EVENT_DAY"},
    )
    transition = maffetone_policy.transition_state(
        current_state="AEROBIC_BASE",
        goal_intent="PERFORMANCE",
        user_confirmed=False,
        evidence_reference=None,
        safety_errors=[],
        event_exception={"acknowledged": True},
    )
    assert result["ceiling_changed"] is False
    assert result["state_changed"] is False
    assert transition["accepted"] is False


def test_maffetone_goal_intent_does_not_change_formula():
    result = maffetone_policy.apply_goal_intent(
        completion={"age": 40, "adjustment": 5},
        performance={"age": 40, "adjustment": 5},
    )
    assert result["accepted"] is True
    assert result["ceiling_equal"] is True
    assert result["ceiling_bpm"] == 145


def test_maffetone_privacy_allowlist_excludes_sentinel_values():
    sentinel = "synthetic-sensitive-answer"
    result = maffetone_parameters.persistable_snapshot(
        formula_version="MAF-180-RANGE-1.0.0",
        questionnaire_version="MAF-QUESTIONNAIRE-1.0.0",
        calculation_date="2026-09-30",
        completed_age=40,
        age_provenance="DOB_AS_OF_CALCULATION_DATE",
        selected_adjustment=-5,
        confirmed=True,
        ceiling_bpm=135,
        lower_bpm=125,
        provenance="USER_SELECTED",
        raw_questionnaire_answers={"medication": sentinel, "injury": sentinel},
    )
    assert sentinel not in repr(result)
    assert result["contains_raw_questionnaire_answers"] is False
    assert result["provenance"] == "USER_SELECTED"
    assert result["age_provenance"] == "DOB_AS_OF_CALCULATION_DATE"
    assert result["ceiling_bpm"] == 135
    assert result["lower_bpm"] == 125


def test_registry_dispatches_local_policy_and_findings_sort_stably():
    assert resolve_methodology_policy(
        methodology_id=MethodologyId.FITZGERALD_80_20_RUNNING_V1.value
    ) is fitzgerald_policy.POLICY
    assert resolve_methodology_policy(
        methodology_id=MethodologyId.MAFFETONE_RUNNING_V1.value
    ) is maffetone_policy.POLICY
    with pytest.raises(DomainError):
        resolve_methodology_policy(methodology_id=MethodologyId.LEGACY_UNSPECIFIED.value)

    result = validate_candidate(
        methodology_findings=[
            {"rule_id": "Z-RULE", "severity": "WARNING", "message": "z"},
            {"rule_id": "A-RULE", "severity": "INFORMATIONAL", "message": "a"},
        ]
    )
    assert [finding["rule_id"] for finding in result["findings"]] == ["A-RULE", "Z-RULE"]


def test_maffetone_policy_rejects_foreign_fitzgerald_target_and_omits_sensitive_values():
    sentinel = "do-not-copy-this-answer"
    candidate = SimpleNamespace(
        parameter_snapshot={"raw_questionnaire_answers": {"medication": sentinel}},
        workouts=(
            SimpleNamespace(
                workout_id="w1",
                segments=(
                    SimpleNamespace(
                        kind="WORK",
                        duration_seconds=1800,
                        prescription=SimpleNamespace(
                            methodology_id=MethodologyId.FITZGERALD_80_20_RUNNING_V1
                        ),
                    ),
                ),
            ),
        ),
    )
    findings = maffetone_policy.POLICY.validate(candidate)
    assert any(item.rule_id == "MAF-V1-007" for item in findings)
    assert any(item.rule_id == "MAF-V1-005" for item in findings)
    assert sentinel not in repr([item.to_dict() for item in findings])


def test_fitzgerald_hrmax_garmin_zones_and_rpe_cannot_supply_threshold_authority():
    result = fitzgerald_parameters.select_primary_metric(
        pace={"evidence_class": "UNAVAILABLE", "value": None},
        lthr={"evidence_class": "UNAVAILABLE", "value": None},
        context="STEADY_AEROBIC",
        hrmax_bpm=190,
        garmin_zones=[120, 140, 160],
        rpe=7,
    )
    assert result["blocked"] is True
    assert result["primary_metric"] is None
    assert result["pace_target"] is None


@pytest.mark.parametrize(
    "metric,boundaries",
    [
        (
            "SPEED",
            [
                ("76", "ZONE_1", "ZONE_2"),
                ("87", "ZONE_2", "ZONE_X"),
                ("93", "ZONE_X", "ZONE_3"),
                ("100", "ZONE_3", "ZONE_Y"),
                ("102", "ZONE_Y", "ZONE_4"),
                ("115", "ZONE_4", "ZONE_5"),
            ],
        ),
        (
            "HEART_RATE",
            [
                ("81", "ZONE_1", "ZONE_2"),
                ("90", "ZONE_2", "ZONE_X"),
                ("95", "ZONE_X", "ZONE_3"),
                ("100", "ZONE_3", "ZONE_Y"),
                ("102", "ZONE_Y", "ZONE_4"),
                ("105", "ZONE_4", "ZONE_5"),
            ],
        ),
    ],
)
def test_fitzgerald_all_adjacent_boundaries_are_exact_half_open_decimals(metric, boundaries):
    for boundary, below_target, at_target in boundaries:
        below = str(exact_decimal(boundary, "boundary") - Decimal("0.0001"))
        above = str(exact_decimal(boundary, "boundary") + Decimal("0.0001"))
        assert fitzgerald_parameters.classify_sample(
            metric=metric, threshold="100", sample=below
        )["native_target"] == below_target
        assert fitzgerald_parameters.classify_sample(
            metric=metric, threshold="100", sample=boundary
        )["native_target"] == at_target
        assert fitzgerald_parameters.classify_sample(
            metric=metric, threshold="100", sample=above
        )["native_target"] == at_target


def test_fitzgerald_each_leaf_and_unaccounted_time_are_arithmetically_explicit():
    result = fitzgerald_policy.account_segments(
        segments=[
            {"kind": "WARMUP", "duration_seconds": 600, "native_target": "ZONE_1"},
            {"kind": "WORK", "duration_seconds": 300, "native_target": "ZONE_3"},
            {"kind": "RECOVERY", "duration_seconds": 120, "native_target": "ZONE_1"},
            {"kind": "WORK", "duration_seconds": 180, "native_target": "ZONE_5"},
            {"kind": "COOLDOWN", "duration_seconds": 600, "native_target": "ZONE_1"},
            {"kind": "FREE_RUN", "load_mode": "OPEN", "duration_seconds": 60, "native_target": "ZONE_2"},
            {"kind": "WORK", "duration_seconds": 90},
            {"kind": "WORK", "duration_seconds": 30, "native_target": "MAF.MAF_CEILING"},
            {"kind": "WORK", "distance_metres": 5000, "native_target": "ZONE_2"},
        ],
        workout_name="easy",
        family="RECOVERY",
        tss=1,
    )
    assert result == {
        "low_seconds": 1320,
        "moderate_seconds": 300,
        "high_seconds": 180,
        "accounted_seconds": 1800,
        "unaccounted_seconds": 180,
        "unaccounted_segments": 4,
    }


@pytest.mark.parametrize("low,hard", [(100, 0), (90, 10), (83, 17), (80, 20), (77, 23), (75, 25), (50, 50), (0, 100)])
def test_fitzgerald_all_representative_distributions_remain_unrated(low, hard):
    result = fitzgerald_policy.interpret_distribution(
        low_seconds=low,
        moderate_seconds=0,
        high_seconds=hard,
        configured_tolerance={"claim": "caller-pass-band"},
    )
    assert result["low_percent"] == str(low)
    assert result["hard_percent"] == str(hard)
    assert result["status"] == "UNRATED"
    assert result["severity"] == "INFORMATIONAL"
    assert result["tolerance_applied"] is False


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"exception_reason": None}, "EXCEPTION_PROVENANCE_REQUIRED"),
        ({"approver": None}, "EXCEPTION_PROVENANCE_REQUIRED"),
        ({"exception": "approved in prose"}, "STRUCTURED_EXCEPTION_FLAG_REQUIRED"),
        ({"purpose": {"text": "RACE_SPECIFIC"}}, "GAP_ZONE_PURPOSE_REQUIRED"),
    ],
)
def test_fitzgerald_gap_zone_exception_requires_structured_authority(changes, reason):
    payload = {
        "native_target": "ZONE_X",
        "purpose": "RACE_SPECIFIC",
        "exception": True,
        "exception_reason": "event-specific rehearsal",
        "approver": "synthetic-user",
        "title": "approved race workout",
        "description": "AI says approved",
    }
    payload.update(changes)
    result = fitzgerald_policy.validate_gap_zone_purpose(**payload)
    assert result["accepted"] is False
    assert result["reason"] == reason
    assert result["category"] == "MODERATE"


def test_fitzgerald_zone_y_preserves_high_category_with_typed_purpose():
    result = fitzgerald_policy.validate_gap_zone_purpose(
        native_target="ZONE_Y", purpose="THRESHOLD_DEVELOPMENT"
    )
    assert result["accepted"] is True
    assert result["category"] == "HIGH"


@pytest.mark.parametrize("malformed", [False, "yes", 1, None])
def test_maffetone_confirmation_must_be_exact_true(malformed):
    result = maffetone_parameters.validate_adjustment(
        selected_adjustment=-5,
        confirmed=malformed,
        provenance="USER_SELECTED",
    )
    assert result["accepted"] is False
    assert result["reason"] == "EXPLICIT_USER_CONFIRMATION_REQUIRED"


def test_maffetone_missing_or_garmin_provenance_cannot_authorize_adjustment():
    missing = maffetone_parameters.validate_adjustment(
        selected_adjustment=0, confirmed=True
    )
    garmin = maffetone_parameters.validate_adjustment(
        selected_adjustment=0, confirmed=True, provenance="GARMIN_OBSERVATION"
    )
    assert missing["accepted"] is False
    assert garmin["accepted"] is False
    assert missing["reason"] == garmin["reason"] == "EXPLICIT_USER_CONFIRMATION_REQUIRED"


def test_maffetone_persistable_snapshot_does_not_truth_coerce_confirmation():
    result = maffetone_parameters.persistable_snapshot(
        formula_version="MAF-180-RANGE-1.0.0",
        completed_age=40,
        selected_adjustment=-5,
        confirmed="false",
    )
    assert result["confirmed"] is False
    assert result["selected_adjustment"] is None


@pytest.mark.parametrize(
    "age,blocked,ceiling",
    [(15, True, None), (16, True, None), (17, False, 163), (64, False, 116), (65, False, 115), (66, False, 114), (75, False, 105)],
)
def test_maffetone_age_boundaries_have_no_automatic_bonus(age, blocked, ceiling):
    result = maffetone_parameters.derive(
        completed_age=age,
        calculation_date="2026-09-30",
        selected_adjustment=0,
        confirmed=True,
    )
    assert result["blocked"] is blocked
    assert result["ceiling_bpm"] == ceiling
    if not blocked:
        assert result["lower_bpm"] == ceiling - 10
        assert result["automatic_extra_beats"] == 0


def _maf_test_payload():
    return {
        "benchmark_identity": "MAF_TEST",
        "protocol_version": "MAF-GPS-30MIN-V1",
        "ceiling_bpm": 145,
        "warmup_seconds": 900,
        "measurement_seconds": 1800,
        "cooldown_seconds": 720,
        "hr_coverage": "VALID",
        "conditions": {"course": "synthetic-loop", "weather": "synthetic"},
        "segments": [
            {"kind": "WARMUP", "duration_seconds": 900},
            {"kind": "WORK", "duration_seconds": 1800},
            {"kind": "COOLDOWN", "duration_seconds": 720},
        ],
    }


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("benchmark_identity", "AEROBIC_RUN", "EXPLICIT_MAF_TEST_IDENTITY_REQUIRED"),
        ("protocol_version", "", "PROTOCOL_VERSION_REQUIRED"),
        ("ceiling_bpm", None, "CONFIRMED_MAF_CEILING_REQUIRED"),
        ("warmup_seconds", 0, "WARMUP_OUTSIDE_GUIDELINE"),
        ("measurement_seconds", 0, "MEASUREMENT_DURATION_OUTSIDE_PROTOCOL"),
        ("cooldown_seconds", 0, "COOLDOWN_OUTSIDE_GUIDELINE"),
        ("hr_coverage", "UNAVAILABLE", "VALID_HR_COVERAGE_REQUIRED"),
        ("conditions", {}, "COMPARABILITY_CONDITIONS_REQUIRED"),
    ],
)
def test_maf_test_rejects_missing_or_malformed_structured_fields(field, value, reason):
    payload = _maf_test_payload()
    payload[field] = value
    result = maffetone_policy.validate_maf_test(**payload)
    assert result["valid"] is False
    assert reason in result["reasons"]
    assert result["ordinary_run_auto_labeled"] is False


@pytest.mark.parametrize(
    "missing_kind,reason",
    [
        ("WARMUP", "WARMUP_SEGMENT_REQUIRED"),
        ("WORK", "MEASUREMENT_SEGMENT_REQUIRED"),
        ("COOLDOWN", "COOLDOWN_SEGMENT_REQUIRED"),
    ],
)
def test_maf_test_requires_typed_protocol_segments(missing_kind, reason):
    payload = _maf_test_payload()
    payload["segments"] = [
        segment for segment in payload["segments"] if segment["kind"] != missing_kind
    ]
    result = maffetone_policy.validate_maf_test(**payload)
    assert result["valid"] is False
    assert reason in result["reasons"]


@pytest.mark.parametrize(
    "confirmed,evidence,safety_errors,new_revision,accepted",
    [
        (False, None, [], False, False),
        (True, None, [], False, False),
        (False, "reviewed-evidence", [], False, False),
        (True, "reviewed-evidence", [], False, False),
        (True, "reviewed-evidence", ["SAFETY_ERROR"], True, False),
        (True, "reviewed-evidence", [], True, True),
    ],
)
def test_maffetone_higher_intensity_authorization_matrix(
    confirmed, evidence, safety_errors, new_revision, accepted
):
    result = maffetone_policy.transition_state(
        current_state="AEROBIC_BASE",
        goal_intent="PERFORMANCE",
        user_confirmed=confirmed,
        evidence_reference=evidence,
        safety_errors=safety_errors,
        new_revision=new_revision,
    )
    assert result["accepted"] is accepted
    assert result["state"] == (
        "HIGHER_INTENSITY_PERMITTED" if accepted else "AEROBIC_BASE"
    )


def test_event_like_prose_cannot_create_event_day_exception():
    result = maffetone_policy.validate_aerobic_base(
        state="AEROBIC_BASE",
        ceiling_bpm=145,
        segments=[{"kind": "WORK", "upper_bpm": 155}],
        event_exception={"acknowledged": True, "context": "EVENT_DAY"},
        title="EVENT",
        description="race day exception",
    )
    assert result["accepted"] is False
    assert result["reason"] == "ABOVE_CEILING_PRESCRIPTION"


def test_cross_method_manifest_and_state_metadata_are_rejected():
    fitz_candidate = SimpleNamespace(
        manifest={"methodology_id": MethodologyId.MAFFETONE_RUNNING_V1.value},
        parameter_snapshot={
            "pace": {"evidence_class": "MEASURED", "value": "4"},
            "lthr": {"evidence_class": "UNAVAILABLE", "value": None},
            "state": "HIGHER_INTENSITY_PERMITTED",
        },
        workouts=(),
    )
    maf_candidate = SimpleNamespace(
        manifest={"methodology_id": MethodologyId.FITZGERALD_80_20_RUNNING_V1.value},
        parameter_snapshot={
            "formula_version": "MAF-180-RANGE-1.0.0",
            "calculation_date": "2026-09-30",
            "completed_age": 40,
            "age_provenance": "DOB_AS_OF_CALCULATION_DATE",
            "selected_adjustment": 0,
            "confirmed": True,
            "ceiling_bpm": 140,
            "lower_bpm": 130,
            "provenance": "USER_SELECTED",
        },
        workouts=(
            SimpleNamespace(
                workout_id="maf-1",
                metadata={"fitzgerald_gap_zone_exceptions": {}},
                segments=(),
            ),
        ),
    )
    fitz_findings = fitzgerald_policy.POLICY.validate(fitz_candidate)
    maf_findings = maffetone_policy.POLICY.validate(maf_candidate)
    assert any(item.rule_id == "F80-V1-001" for item in fitz_findings)
    assert any(
        item.rule_id == "F80-V1-012" and "foreign_fields" in item.evidence
        for item in fitz_findings
    )
    assert any(item.rule_id == "MAF-V1-001" for item in maf_findings)
    assert any(
        item.rule_id == "MAF-V1-007" and item.workout_id == "maf-1"
        for item in maf_findings
    )


def test_maffetone_complete_parameter_snapshot_is_ready_and_missing_provenance_blocks():
    complete = {
        "formula_version": "MAF-180-RANGE-1.0.0",
        "calculation_date": "2026-09-30",
        "completed_age": 40,
        "age_provenance": "DOB_AS_OF_CALCULATION_DATE",
        "selected_adjustment": 0,
        "confirmed": True,
        "ceiling_bpm": 140,
        "lower_bpm": 130,
        "provenance": "USER_SELECTED",
    }
    ready = maffetone_policy.POLICY.validate(
        SimpleNamespace(
            manifest={"methodology_id": MethodologyId.MAFFETONE_RUNNING_V1.value},
            parameter_snapshot=complete,
            workouts=(),
        )
    )
    blocked = maffetone_policy.POLICY.validate(
        SimpleNamespace(
            manifest={"methodology_id": MethodologyId.MAFFETONE_RUNNING_V1.value},
            parameter_snapshot={key: value for key, value in complete.items() if key != "provenance"},
            workouts=(),
        )
    )
    assert ready == ()
    assert any(item.rule_id == "MAF-V1-004" for item in blocked)


def test_methodology_findings_are_owned_and_shared_safety_still_precedes():
    method_finding = ValidationFinding(
        "F80-V1-011",
        ValidationLayer.METHODOLOGY,
        MethodologyId.FITZGERALD_80_20_RUNNING_V1.value,
        FindingSeverity.WARNING,
        "deterministic method warning",
        workout_id="w1",
        segment_id="segment-1",
        evidence={"b": 2, "a": 1},
    )
    safety_finding = ValidationFinding(
        "SHARED-SAFETY-ERROR",
        ValidationLayer.SHARED_SAFETY,
        "DATA_HUB_SHARED_SAFETY",
        FindingSeverity.ERROR,
        "shared safety blocks",
    )
    result = run_validation_pipeline(
        object(),
        shared_safety_policy=lambda candidate: (safety_finding,),
        methodology_policy=lambda candidate: (method_finding,),
    )
    assert result["accepted"] is False
    assert result["blocking_layer"] == "SHARED_SAFETY"
    assert result["methodology_waiver_allowed"] is False
    assert result["findings"][1]["owner"] == MethodologyId.FITZGERALD_80_20_RUNNING_V1.value


def test_finding_order_is_stable_for_different_mapping_and_input_order():
    first = validate_candidate(
        methodology_findings=[
            {"message": "second", "severity": "WARNING", "rule_id": "RULE-B", "evidence": {"b": 2, "a": 1}},
            {"evidence": {"a": 1, "b": 2}, "rule_id": "RULE-A", "message": "first", "severity": "INFORMATIONAL"},
        ]
    )
    second = validate_candidate(
        methodology_findings=[
            {"severity": "INFORMATIONAL", "message": "first", "rule_id": "RULE-A", "evidence": {"b": 2, "a": 1}},
            {"rule_id": "RULE-B", "evidence": {"a": 1, "b": 2}, "severity": "WARNING", "message": "second"},
        ]
    )
    assert first == second


def test_finding_order_uses_canonical_evidence_when_scope_keys_are_equal():
    finding_a = {
        "rule_id": "SAME-RULE",
        "severity": "WARNING",
        "message": "same",
        "workout_id": "w1",
        "evidence": {"value": "a"},
    }
    finding_b = {
        "rule_id": "SAME-RULE",
        "severity": "WARNING",
        "message": "same",
        "workout_id": "w1",
        "evidence": {"value": "b"},
    }
    first = validate_candidate(methodology_findings=[finding_b, finding_a])
    second = validate_candidate(methodology_findings=[finding_a, finding_b])
    assert first == second
    assert [item["evidence"]["value"] for item in first["findings"]] == ["a", "b"]


def test_fitzgerald_iso_week_boundaries_are_deterministic_and_wall_clock_free():
    result = fitzgerald_policy.distribution_windows(
        local_calendar_weeks=[
            {"week": "2026-W01", "complete": True, "low_seconds": 80, "high_seconds": 20},
            {"week": "2025-W52", "complete": True, "low_seconds": 90, "high_seconds": 10},
        ],
        include=["WEEK", "ROLLING_FOUR_COMPLETED_WEEKS", "REVISION"],
    )
    assert result["context"]["ROLLING_FOUR_COMPLETED_WEEKS"]["weeks"] == [
        "2025-W52",
        "2026-W01",
    ]
    assert result["context"]["REVISION"]["low_seconds"] == 170
    assert result["context"]["REVISION"]["high_seconds"] == 30
    assert result["context"]["REVISION"]["status"] == "UNRATED"
    with pytest.raises(DomainError, match="valid ISO week"):
        fitzgerald_policy.distribution_windows(
            local_calendar_weeks=[{"week": "2026-W54", "complete": True}],
            include=["WEEK"],
        )
