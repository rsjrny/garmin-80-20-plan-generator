from __future__ import annotations

import importlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pytest


FUTURE_PACKAGE = "garmin_data_hub.plan_methodology"


@dataclass(frozen=True)
class ContractCase:
    test_id: str
    source_ids: tuple[str, ...]
    capability: str
    payload: Mapping[str, Any]
    expected: Mapping[str, Any]
    architecture_requirement: str
    expected_red_reason: str


class FutureMethodologyApi:
    """Collection-safe adapter for production APIs deliberately absent in PLAN-2.1."""

    def call(self, case: ContractCase) -> Any:
        module_name, function_name = case.capability.split(":", maxsplit=1)
        qualified_module = f"{FUTURE_PACKAGE}.{module_name}"
        module = None
        try:
            module = importlib.import_module(qualified_module)
        except ModuleNotFoundError as exc:
            if exc.name not in {FUTURE_PACKAGE, qualified_module}:
                raise
        if module is None:
            pytest.fail(
                f"{case.test_id} meaningful RED: {case.expected_red_reason}; "
                f"future capability {case.capability!r} does not exist",
                pytrace=False,
            )

        capability = getattr(module, function_name, None)
        if not callable(capability):
            pytest.fail(
                f"{case.test_id} meaningful RED: {case.expected_red_reason}; "
                f"future callable {case.capability!r} does not exist",
                pytrace=False,
            )
        return capability(**dict(case.payload))


def assert_contract_result(actual: Any, expected: Mapping[str, Any], path: str = "result") -> None:
    """Assert the frozen, relevant subset while allowing diagnostic extensions."""
    assert isinstance(actual, Mapping), f"{path} must be a mapping, got {type(actual).__name__}"
    for key, expected_value in expected.items():
        assert key in actual, f"{path} is missing frozen field {key!r}"
        actual_value = actual[key]
        if isinstance(expected_value, Mapping):
            assert_contract_result(actual_value, expected_value, f"{path}.{key}")
        else:
            assert actual_value == expected_value, (
                f"{path}.{key}: expected {expected_value!r}, got {actual_value!r}"
            )


@pytest.fixture
def future_api() -> FutureMethodologyApi:
    return FutureMethodologyApi()
