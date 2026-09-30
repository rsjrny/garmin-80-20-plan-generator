from __future__ import annotations

import pytest

from architecture_cases import REGISTRY_SNAPSHOT_CASES, REVISION_CASES
from conftest import assert_contract_result


@pytest.mark.parametrize("case", REVISION_CASES, ids=lambda case: case.test_id)
def test_revision_contract(case, future_api):
    assert_contract_result(future_api.call(case), case.expected)


@pytest.mark.parametrize("case", REGISTRY_SNAPSHOT_CASES, ids=lambda case: case.test_id)
def test_registry_and_snapshot_contract(case, future_api):
    assert_contract_result(future_api.call(case), case.expected)
