from __future__ import annotations

from architecture_cases import ALL_ARCHITECTURE_CASES
from methodology_cases import (
    ALL_METHODOLOGY_CASES,
    FITZGERALD_CASES,
    MAFFETONE_CASES,
)


def test_all_30_plan_1_2_contract_ids_are_mapped_once_to_executable_cases():
    expected_fitzgerald = {f"F80-V1-{number:03d}" for number in range(1, 16)}
    expected_maffetone = {f"MAF-V1-{number:03d}" for number in range(1, 16)}

    assert {case.test_id for case in FITZGERALD_CASES} == expected_fitzgerald
    assert {case.test_id for case in MAFFETONE_CASES} == expected_maffetone
    assert len(ALL_METHODOLOGY_CASES) == 30
    assert all(case.capability and case.expected for case in ALL_METHODOLOGY_CASES)
    assert all(case.architecture_requirement for case in ALL_METHODOLOGY_CASES)
    assert all(case.expected_red_reason for case in ALL_METHODOLOGY_CASES)




def test_architecture_inventory_has_unique_traceable_cases():
    ids = [case.test_id for case in ALL_ARCHITECTURE_CASES]
    assert len(ids) == len(set(ids))
    assert all(case.source_ids[0].startswith("PLAN-2.0:") for case in ALL_ARCHITECTURE_CASES)
    assert all(case.architecture_requirement for case in ALL_ARCHITECTURE_CASES)
    assert all(case.expected_red_reason for case in ALL_ARCHITECTURE_CASES)
