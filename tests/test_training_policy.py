from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from garmin_data_hub.services.training_policy import (
    evaluate_training_policy,
    training_policy_constraints,
)


def _session(day: str, **changes):
    values = {
        "iso_date": day,
        "sport": "run",
        "intensity": "easy",
        "workout": "Easy Run",
        "phase": "Base",
        "flags": (),
        "duration_minutes": 45,
        "distance_km": 7,
        "tss": 35,
    }
    values.update(changes)
    return SimpleNamespace(**values)


def test_policy_accepts_balanced_plan_and_reports_distribution():
    sessions = [
        _session("2026-09-01", duration_minutes=60),
        _session("2026-09-03", workout="Tempo", intensity="hard", duration_minutes=20),
        _session("2026-09-05", workout="Long Run", duration_minutes=90),
        _session("2026-09-07", workout="Race", intensity="race", phase="Race"),
    ]

    report = evaluate_training_policy(
        sessions,
        start=date(2026, 9, 1),
        event_date=date(2026, 9, 7),
        age=30,
        run_days_per_week=4,
        preferred_long_session_day="Saturday",
    )

    assert report.is_valid
    assert report.easy_minutes == 150
    assert report.hard_minutes == 20
    assert report.easy_fraction is not None and report.easy_fraction > 0.80


def test_policy_rejects_consecutive_hard_days_and_missing_race():
    sessions = [
        _session("2026-09-01", intensity="hard"),
        _session("2026-09-02", intensity="hard"),
    ]

    report = evaluate_training_policy(
        sessions,
        start=date(2026, 9, 1),
        event_date=date(2026, 9, 7),
        age=20,
        run_days_per_week=5,
    )

    assert not report.is_valid
    assert {issue.code for issue in report.errors} >= {
        "consecutive_hard_days",
        "race_session",
    }


def test_policy_warns_when_known_duration_is_not_80_20():
    sessions = [
        _session("2026-09-01", duration_minutes=60),
        _session("2026-09-03", intensity="moderate", duration_minutes=60),
        _session("2026-09-07", workout="Race", intensity="race", phase="Race"),
    ]
    report = evaluate_training_policy(
        sessions,
        start=date(2026, 9, 1),
        event_date=date(2026, 9, 7),
        age=30,
        run_days_per_week=4,
    )

    assert any(issue.code == "intensity_distribution" for issue in report.warnings)


def test_policy_flags_maffetone_non_race_hard_endurance():
    sessions = [
        _session("2026-09-01", duration_minutes=100),
        _session("2026-09-03", intensity="hard", duration_minutes=30),
        _session("2026-09-07", workout="Race", intensity="race", phase="Race"),
    ]
    report = evaluate_training_policy(
        sessions,
        start=date(2026, 9, 1),
        event_date=date(2026, 9, 7),
        age=42,
        run_days_per_week=4,
        training_method="maffetone",
    )

    assert any(issue.code == "training_method_intensity" for issue in report.errors)
    assert any(issue.code == "intensity_distribution" for issue in report.warnings)


def test_maffetone_policy_constraints_include_maf_cap():
    constraints = training_policy_constraints(
        42,
        4,
        training_method="maffetone",
    )

    assert constraints["training_method"] == "maffetone"
    assert constraints["target_easy_endurance_duration_fraction"] == 1.0
    assert constraints["maf_hr_cap_bpm"] == 138


def test_policy_can_require_strength_in_full_training_weeks():
    sessions = [
        _session("2026-09-01"),
        _session("2026-09-08"),
        _session("2026-09-14", workout="Race", intensity="race", phase="Race"),
    ]
    report = evaluate_training_policy(
        sessions,
        start=date(2026, 9, 1),
        event_date=date(2026, 9, 14),
        age=30,
        run_days_per_week=4,
        minimum_strength_sessions_per_week=1,
    )

    assert any(issue.code == "strength_minimum" for issue in report.errors)
