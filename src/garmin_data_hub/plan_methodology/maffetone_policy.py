"""Small Maffetone V1 sport and aerobic-base invariants."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from .domain import DomainError


def validate_sport_scope(*, sports: Sequence[str]) -> dict[str, Any]:
    result = {sport: "APPLICABLE" if sport == "RUNNING" else "NOT_APPLICABLE" for sport in sports}
    result["severity"] = "ERROR"
    return result


def validate_aerobic_base(
    *,
    state: str,
    ceiling_bpm: int,
    segments: Sequence[Mapping[str, Any]],
    event_exception: Mapping[str, Any] | None = None,
    **_: Any,
) -> dict[str, Any]:
    if state not in {"AEROBIC_BASE", "HIGHER_INTENSITY_PERMITTED"}:
        raise DomainError(f"unknown Maffetone state: {state!r}")
    if event_exception and event_exception.get("acknowledged") is True:
        event_only = bool(segments) and all(segment.get("kind") == "EVENT" for segment in segments)
        if event_only and event_exception.get("context") == "EVENT_DAY":
            return {
                "accepted": True,
                "aerobic_training_conformance": "NOT_APPLICABLE",
                "ceiling_changed": False,
                "state_changed": False,
            }
    if state == "AEROBIC_BASE":
        for segment in segments:
            upper = segment.get("upper_bpm")
            if upper is not None and upper > ceiling_bpm:
                return {
                    "accepted": False,
                    "reason": "ABOVE_CEILING_PRESCRIPTION",
                    "severity": "ERROR",
                }
    return {"accepted": True, "severity": "ERROR"}
