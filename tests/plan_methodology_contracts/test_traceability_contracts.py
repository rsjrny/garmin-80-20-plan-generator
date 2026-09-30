from __future__ import annotations

from architecture_cases import ALL_ARCHITECTURE_CASES
from methodology_cases import (
    ALL_METHODOLOGY_CASES,
    FITZGERALD_CASES,
    MAFFETONE_CASES,
    PLAN_1_2_SHA256,
    PLAN_2_0_SHA256,
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


def test_contract_fixtures_name_the_verified_authoritative_artifacts():
    assert PLAN_1_2_SHA256 == "1a82469c1c9ae5f1763cb5107091a8b3fb35c3c1a864390c77852b39bbde8523"
    assert PLAN_2_0_SHA256 == "2ce0c6507a7be2bcb316180fbc514faf72415c7134589bd72361cdf463be00be"


def test_architecture_inventory_has_unique_traceable_cases():
    ids = [case.test_id for case in ALL_ARCHITECTURE_CASES]
    assert len(ids) == len(set(ids))
    assert all(case.source_ids[0].startswith("PLAN-2.0:") for case in ALL_ARCHITECTURE_CASES)
    assert all(case.architecture_requirement for case in ALL_ARCHITECTURE_CASES)
    assert all(case.expected_red_reason for case in ALL_ARCHITECTURE_CASES)
