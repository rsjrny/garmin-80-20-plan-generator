from __future__ import annotations

import pytest

from conftest import assert_contract_result
from methodology_cases import FITZGERALD_CASES, MAFFETONE_CASES


@pytest.mark.parametrize("case", FITZGERALD_CASES, ids=lambda case: case.test_id)
def test_fitzgerald_v1_frozen_contract(case, future_api):
    actual = future_api.call(case)
    assert_contract_result(actual, case.expected)


@pytest.mark.parametrize("case", MAFFETONE_CASES, ids=lambda case: case.test_id)
def test_maffetone_v1_frozen_contract(case, future_api):
    actual = future_api.call(case)
    assert_contract_result(actual, case.expected)


@pytest.mark.parametrize("adjustment,ceiling,lower", [(-10, 130, 120), (-5, 135, 125), (0, 140, 130), (5, 145, 135)])
def test_maf_v1_003_all_adjustments_use_exact_integer_arithmetic(
    adjustment, ceiling, lower, future_api
):
    case = next(case for case in MAFFETONE_CASES if case.test_id == "MAF-V1-003")
    actual = future_api.call(
        type(case)(
            test_id=f"MAF-V1-003-adjustment-{adjustment:+d}",
            source_ids=("MAF-V1-003", "MAF-V1-007"),
            capability=case.capability,
            payload={
                "completed_age": 40,
                "calculation_date": "2026-09-30",
                "selected_adjustment": adjustment,
                "confirmed": True,
            },
            expected={"ceiling_bpm": ceiling, "lower_bpm": lower},
            architecture_requirement="Exact formula and lower aerobic bound",
            expected_red_reason="the four frozen MAF adjustment calculations are absent",
        )
    )
    assert_contract_result(actual, {"ceiling_bpm": ceiling, "lower_bpm": lower})


@pytest.mark.parametrize(
    "evidence_class,prescription_grade",
    [("MEASURED", True), ("DERIVED", True), ("ESTIMATED", False), ("UNAVAILABLE", False)],
)
def test_f80_v1_003_all_threshold_evidence_states(
    evidence_class, prescription_grade, future_api
):
    case = next(case for case in FITZGERALD_CASES if case.test_id == "F80-V1-003")
    actual = future_api.call(
        type(case)(
            test_id=f"F80-V1-003-evidence-{evidence_class}",
            source_ids=("F80-V1-003", "F80-V1-006"),
            capability=case.capability,
            payload={
                "metric": "PACE",
                "value": None if evidence_class == "UNAVAILABLE" else "4.00",
                "unit": "m/s",
                "sport": "RUNNING",
                "method_or_protocol": "APPROVED_DERIVATION" if evidence_class == "DERIVED" else "SYNTHETIC",
                "source_record": "synthetic-threshold",
                "measured_at": "2026-09-01",
                "evidence_class": evidence_class,
                "quality": "VALID",
                "derivation_approved": evidence_class == "DERIVED",
            },
            expected={"evidence_class": evidence_class, "prescription_grade": prescription_grade},
            architecture_requirement="All frozen Fitzgerald threshold evidence states",
            expected_red_reason="threshold evidence classification is absent",
        )
    )
    assert_contract_result(
        actual,
        {"evidence_class": evidence_class, "prescription_grade": prescription_grade},
    )


def test_maf_v1_006_over_65_requires_confirmation_and_never_auto_adds_beats(future_api):
    case = next(case for case in MAFFETONE_CASES if case.test_id == "MAF-V1-006")
    actual = future_api.call(
        type(case)(
            test_id="MAF-V1-006-over-65",
            source_ids=("MAF-V1-006",),
            capability=case.capability,
            payload={
                "completed_age": 70,
                "calculation_date": "2026-09-30",
                "selected_adjustment": 5,
                "confirmed": True,
                "manual_ceiling": None,
            },
            expected={"ceiling_bpm": 115, "lower_bpm": 105, "automatic_extra_beats": 0},
            architecture_requirement="Over-65 explicit confirmation without automatic individualized allowance",
            expected_red_reason="the over-65 MAF branch is absent",
        )
    )
    assert_contract_result(
        actual,
        {"ceiling_bpm": 115, "lower_bpm": 105, "automatic_extra_beats": 0},
    )


def test_maf_v1_008_event_day_exception_stays_separate_from_aerobic_training(future_api):
    case = next(case for case in MAFFETONE_CASES if case.test_id == "MAF-V1-008")
    actual = future_api.call(
        type(case)(
            test_id="MAF-V1-008-event-day",
            source_ids=("MAF-V1-008",),
            capability=case.capability,
            payload={
                "state": "AEROBIC_BASE",
                "ceiling_bpm": 145,
                "segments": [{"kind": "EVENT", "upper_bpm": None}],
                "event_exception": {"acknowledged": True, "context": "EVENT_DAY"},
            },
            expected={
                "accepted": True,
                "aerobic_training_conformance": "NOT_APPLICABLE",
                "ceiling_changed": False,
                "state_changed": False,
            },
            architecture_requirement="Event-day exception separated from training permission",
            expected_red_reason="event-day exception separation is absent",
        )
    )
    assert_contract_result(
        actual,
        {
            "accepted": True,
            "aerobic_training_conformance": "NOT_APPLICABLE",
            "ceiling_changed": False,
            "state_changed": False,
        },
    )
