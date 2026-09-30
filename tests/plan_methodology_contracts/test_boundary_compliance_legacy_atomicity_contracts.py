from __future__ import annotations

import pytest

from architecture_cases import BOUNDARY_PERSISTENCE_CASES
from conftest import assert_contract_result


@pytest.mark.parametrize("case", BOUNDARY_PERSISTENCE_CASES, ids=lambda case: case.test_id)
def test_boundary_compliance_legacy_and_atomicity_contract(case, future_api):
    assert_contract_result(future_api.call(case), case.expected)


@pytest.mark.parametrize(
    "observation_state,quality",
    [
        ("NO_OBSERVATIONS", "UNAVAILABLE"),
        ("INADEQUATE_COVERAGE", "INSUFFICIENT"),
        ("INCOMPLETE_COVERAGE", "PARTIAL"),
    ],
)
def test_missing_data_quality_states_never_default_to_noncompliant(
    observation_state, quality, future_api
):
    template = next(
        case for case in BOUNDARY_PERSISTENCE_CASES if case.test_id == "ARCH-COMP-003"
    )
    case = type(template)(
        test_id=f"ARCH-COMP-003-{quality}",
        source_ids=("F80-V1-015", "MAF-V1-015"),
        capability=template.capability,
        payload={"observations": observation_state, "quality": quality},
        expected={"status": "UNKNOWN", "quality": quality, "noncompliant": False},
        architecture_requirement="Explicit missing-data quality semantics",
        expected_red_reason=f"{quality} data cannot yet remain UNKNOWN rather than NONCOMPLIANT",
    )
    assert_contract_result(future_api.call(case), case.expected)
