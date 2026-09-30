from __future__ import annotations

import pytest

from architecture_cases import PRESCRIPTION_SEGMENT_CASES
from conftest import assert_contract_result


@pytest.mark.parametrize("case", PRESCRIPTION_SEGMENT_CASES, ids=lambda case: case.test_id)
def test_prescription_and_segment_contract(case, future_api):
    assert_contract_result(future_api.call(case), case.expected)
