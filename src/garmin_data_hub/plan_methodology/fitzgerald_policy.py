"""Small Fitzgerald V1 invariant and planned-time accounting primitives."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from .domain import FitzgeraldCategory


TARGET_CATEGORIES = {
    "ZONE_1": FitzgeraldCategory.LOW,
    "ZONE_2": FitzgeraldCategory.LOW,
    "ZONE_X": FitzgeraldCategory.MODERATE,
    "ZONE_3": FitzgeraldCategory.MODERATE,
    "ZONE_Y": FitzgeraldCategory.HIGH,
    "ZONE_4": FitzgeraldCategory.HIGH,
    "ZONE_5": FitzgeraldCategory.HIGH,
}


def _short_target(value: str) -> str:
    if "." not in value:
        return value
    namespace, target = value.split(".", 1)
    return target if namespace == "F80" else ""


def validate_sport_scope(*, sports: Sequence[str]) -> dict[str, Any]:
    result = {sport: "APPLICABLE" if sport == "RUNNING" else "NOT_APPLICABLE" for sport in sports}
    result["severity"] = "ERROR"
    return result


def classify_native_targets(*, native_targets: Sequence[str]) -> dict[str, Any]:
    categories = [
        TARGET_CATEGORIES.get(_short_target(target), FitzgeraldCategory.UNACCOUNTED).value
        for target in native_targets
    ]
    return {"categories": categories, "severity": "ERROR"}


def account_segments(
    *, segments: Sequence[Mapping[str, Any]], **_: Any
) -> dict[str, Any]:
    totals = {
        FitzgeraldCategory.LOW: 0,
        FitzgeraldCategory.MODERATE: 0,
        FitzgeraldCategory.HIGH: 0,
    }
    unaccounted = 0
    for segment in segments:
        duration = segment.get("duration_seconds")
        category = TARGET_CATEGORIES.get(_short_target(str(segment.get("native_target", ""))))
        valid_duration = isinstance(duration, int) and not isinstance(duration, bool) and duration > 0
        if not valid_duration or category is None:
            if valid_duration:
                unaccounted += duration
            continue
        totals[category] += duration
    accounted = sum(totals.values())
    return {
        "low_seconds": totals[FitzgeraldCategory.LOW],
        "moderate_seconds": totals[FitzgeraldCategory.MODERATE],
        "high_seconds": totals[FitzgeraldCategory.HIGH],
        "accounted_seconds": accounted,
        "unaccounted_seconds": unaccounted,
    }
