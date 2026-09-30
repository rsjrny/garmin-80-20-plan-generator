"""Pure legacy identity classification (conversion is intentionally absent)."""

from __future__ import annotations

from typing import Any


def classify(*, legacy_plan_id: str) -> dict[str, Any]:
    if not legacy_plan_id:
        raise ValueError("legacy_plan_id is required")
    return {
        "usable": True,
        "methodology_id": "legacy_unspecified",
        "fitzgerald_certified": False,
        "maffetone_certified": False,
    }
