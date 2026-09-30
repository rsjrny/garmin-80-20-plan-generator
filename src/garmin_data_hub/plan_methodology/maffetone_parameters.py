"""Deterministic, privacy-minimized Maffetone V1 primitives."""

from __future__ import annotations

from datetime import date
from typing import Any

from .domain import DomainError
from .snapshots import ALLOWED_MAF_ADJUSTMENTS


def _allowed_adjustment(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value in ALLOWED_MAF_ADJUSTMENTS


def validate_adjustment(
    *, selected_adjustment: int, confirmed: bool, provenance: str, **_: Any
) -> dict[str, Any]:
    allowed = sorted(ALLOWED_MAF_ADJUSTMENTS)
    if not _allowed_adjustment(selected_adjustment):
        return {
            "accepted": False,
            "allowed_adjustments": allowed,
            "reason": "INVALID_ADJUSTMENT",
            "severity": "ERROR",
        }
    if not confirmed or provenance != "USER_SELECTED":
        return {
            "accepted": False,
            "allowed_adjustments": allowed,
            "reason": "EXPLICIT_USER_CONFIRMATION_REQUIRED",
            "severity": "ERROR",
        }
    return {"accepted": True, "allowed_adjustments": allowed, "severity": "ERROR"}


def derive(
    *,
    completed_age: int,
    calculation_date: str,
    selected_adjustment: int,
    confirmed: bool,
    manual_ceiling: int | None = None,
    manual_provenance: str | None = None,
    **_: Any,
) -> dict[str, Any]:
    try:
        date.fromisoformat(calculation_date)
    except (TypeError, ValueError) as exc:
        raise DomainError("calculation_date must be an ISO date") from exc
    if not isinstance(completed_age, int) or isinstance(completed_age, bool) or completed_age < 0:
        raise DomainError("completed_age cannot be negative")
    if not _allowed_adjustment(selected_adjustment):
        raise DomainError("selected_adjustment must be one of -10, -5, 0, +5")
    if completed_age <= 16:
        if (
            not isinstance(manual_ceiling, int)
            or isinstance(manual_ceiling, bool)
            or manual_ceiling <= 0
            or not confirmed
            or manual_provenance != "MANUAL_AGE_EXCEPTION"
        ):
            return {
                "blocked": True,
                "ceiling_bpm": None,
                "reason": "MANUAL_AGE_EXCEPTION_REQUIRED",
                "severity": "ERROR",
            }
        return {
            "blocked": False,
            "ceiling_bpm": manual_ceiling,
            "lower_bpm": manual_ceiling - 10,
            "formula": "MANUAL_AGE_EXCEPTION",
            "severity": "ERROR",
        }
    if not confirmed:
        return {
            "blocked": True,
            "ceiling_bpm": None,
            "reason": "EXPLICIT_USER_CONFIRMATION_REQUIRED",
            "severity": "ERROR",
        }
    ceiling = 180 - completed_age + selected_adjustment
    return {
        "blocked": False,
        "ceiling_bpm": ceiling,
        "lower_bpm": ceiling - 10,
        "formula": "180-age+adjustment",
        "automatic_extra_beats": 0,
        "severity": "ERROR",
    }


def persistable_snapshot(**values: Any) -> dict[str, Any]:
    # This is deliberately an allowlist. Raw questionnaire/health fields are
    # not copied even if a caller supplies them.
    selected_adjustment = values.get("selected_adjustment")
    confirmed = bool(values.get("confirmed", False))
    if not _allowed_adjustment(selected_adjustment):
        raise DomainError("selected_adjustment must be one of -10, -5, 0, +5")
    if not confirmed:
        selected_adjustment = None
    return {
        "formula_version": values.get("formula_version"),
        "calculation_date": values.get("calculation_date"),
        "completed_age": values.get("completed_age"),
        "selected_adjustment": selected_adjustment,
        "confirmed": confirmed,
        "contains_raw_questionnaire_answers": False,
        "severity": "ERROR",
    }


def build_prescriptions(*, ceiling_bpm: int) -> dict[str, Any]:
    if not isinstance(ceiling_bpm, int) or isinstance(ceiling_bpm, bool):
        raise DomainError("ceiling_bpm must be an integer")
    lower = ceiling_bpm - 10
    return {
        "MAF_AEROBIC_RANGE": {
            "lower": lower,
            "upper": ceiling_bpm,
            "lower_inclusive": True,
            "upper_inclusive": True,
        },
        "MAF_CEILING": {"ceiling": ceiling_bpm, "inclusive": True},
        "SUB_MAF": {"upper": lower},
        "severity": "ERROR",
    }
