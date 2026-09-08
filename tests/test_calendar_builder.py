import pytest

from garmin_data_hub.exports.forever.calendar_builder import build_calendar
from garmin_data_hub.exports.forever.training_rules import (
    DISTANCE_PROFILES,
    build_phase_structure,
    distance_to_km,
)


def test_race_day_overrides_weekday_rest_template():
    plans = build_calendar(
        "2026-08-31",
        "2026-09-07",
        "2026-09-07",
        run_days_per_week=5,
    )

    race = plans[-1]
    assert race.day == "Monday"
    assert race.phase == "Race"
    assert race.intensity == "race"
    assert race.sport == "run"
    assert "RACE" in race.flags
    assert race.workout == "Long Trail Run"


def test_sub_week_plan_does_not_divide_by_zero():
    plans = build_calendar(
        "2026-09-04",
        "2026-09-07",
        "2026-09-07",
        run_days_per_week=4,
    )

    assert len(plans) == 4
    assert plans[-1].intensity == "race"
    assert all(plan.sport and plan.intensity for plan in plans)


def test_baseline_places_strength_on_a_rest_day_each_training_week():
    plans = build_calendar(
        "2026-06-01",
        "2026-08-30",
        "2026-08-30",
        run_days_per_week=5,
        long_run_day="Saturday",
    )

    strength = [plan for plan in plans if plan.sport == "strength"]
    assert strength
    assert all(plan.intensity == "moderate" for plan in strength)
    assert {plan.workout for plan in strength} == {
        "Strength A (Legs/Core)",
        "Strength B (Hips/Stability)",
    }


@pytest.mark.parametrize(
    ("distance", "expected_km"),
    [("10M", 16.09344), ("20M", 32.18688)],
)
def test_mile_race_distances_have_complete_training_support(distance, expected_km):
    assert distance in DISTANCE_PROFILES
    assert distance_to_km(distance) == pytest.approx(expected_km)
    assert build_phase_structure(distance, age=40)["Build"].long_run_cap_km > 0

    plans = build_calendar(
        "2026-06-01",
        "2026-08-30",
        "2026-08-30",
        race_distance=distance,
    )
    assert plans[-1].notes == f"RACE DAY: {distance}!"
