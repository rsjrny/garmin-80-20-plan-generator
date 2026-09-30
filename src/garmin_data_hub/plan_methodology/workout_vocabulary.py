"""Application-owned workout vocabulary shared by parsing and policy validation."""

from __future__ import annotations


WORKOUT_FAMILIES = frozenset(
    {
        "RECOVERY",
        "AEROBIC",
        "LONG_AEROBIC",
        "MODERATE_DEVELOPMENT",
        "HIGH_INTENSITY_INTERVALS",
        "RACE_SPECIFIC",
        "EVENT_DAY",
        "STRENGTH",
        "MOBILITY",
        "REST",
        "CUSTOM_STRUCTURED",
    }
)

RUNNING_WORKOUT_FAMILIES = WORKOUT_FAMILIES - {"STRENGTH", "MOBILITY", "REST"}
