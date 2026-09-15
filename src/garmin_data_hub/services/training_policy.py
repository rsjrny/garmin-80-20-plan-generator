"""Shared deterministic policy checks for rule-built and AI-proposed plans."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable

from garmin_data_hub.exports.forever.training_rules import get_intensity_cap

MAX_SESSIONS_PER_DAY = 3
MAX_STRENGTH_SESSIONS_PER_WEEK = 3
MAX_WEEKLY_RUN_INCREASE_FRACTION = 0.10
TARGET_EASY_DURATION_FRACTION = 0.80
DEFAULT_TRAINING_METHOD = "eighty_twenty"


@dataclass(frozen=True)
class TrainingMethod:
    key: str
    label: str
    target_easy_duration_fraction: float
    allow_non_race_hard_endurance: bool
    description: str


TRAINING_METHODS: dict[str, TrainingMethod] = {
    "eighty_twenty": TrainingMethod(
        key="eighty_twenty",
        label="80/20",
        target_easy_duration_fraction=TARGET_EASY_DURATION_FRACTION,
        allow_non_race_hard_endurance=True,
        description="About 80% easy endurance duration with age-based hard-day caps.",
    ),
    "maffetone": TrainingMethod(
        key="maffetone",
        label="Maffetone",
        target_easy_duration_fraction=1.0,
        allow_non_race_hard_endurance=False,
        description="Aerobic-first training capped by the simple MAF heart-rate ceiling.",
    ),
}


def normalize_training_method(value: object) -> str:
    """Return a supported training-method key."""
    normalized = " ".join(str(value or "").strip().casefold().split())
    aliases = {
        "": DEFAULT_TRAINING_METHOD,
        "80/20": "eighty_twenty",
        "8020": "eighty_twenty",
        "80 20": "eighty_twenty",
        "eighty_twenty": "eighty_twenty",
        "eighty twenty": "eighty_twenty",
        "maf": "maffetone",
        "maff": "maffetone",
        "maffetone": "maffetone",
    }
    try:
        return aliases[normalized]
    except KeyError as exc:
        allowed = ", ".join(method.label for method in TRAINING_METHODS.values())
        raise ValueError(f"Unsupported training philosophy. Choose one of: {allowed}") from exc


def maffetone_hr_cap(age: int) -> int:
    """Return the simple MAF ceiling used by this first implementation."""
    return max(0, 180 - int(age))


def training_policy_constraints(
    age: int,
    run_days_per_week: int,
    training_method: object = DEFAULT_TRAINING_METHOD,
) -> dict[str, Any]:
    """Return the stable policy limits shared with plan authors and reviewers."""
    method_key = normalize_training_method(training_method)
    method = TRAINING_METHODS[method_key]
    hard_cap = (
        get_intensity_cap(int(age))
        if method.allow_non_race_hard_endurance
        else 1
    )
    constraints: dict[str, Any] = {
        "training_method": method.key,
        "training_method_label": method.label,
        "max_sessions_per_day": MAX_SESSIONS_PER_DAY,
        "max_run_days_per_week": int(run_days_per_week),
        "max_hard_or_race_sessions_per_week": hard_cap,
        "max_strength_sessions_per_week": MAX_STRENGTH_SESSIONS_PER_WEEK,
        "min_strength_sessions_per_full_base_build_week": 1,
        "max_weekly_run_distance_increase_fraction": MAX_WEEKLY_RUN_INCREASE_FRACTION,
        "target_easy_endurance_duration_fraction": method.target_easy_duration_fraction,
        "consecutive_hard_days_allowed": False,
        "rest_and_active_sessions_same_day_allowed": False,
    }
    if method.key == "maffetone":
        constraints.update(
            {
                "maf_hr_cap_bpm": maffetone_hr_cap(int(age)),
                "non_race_moderate_or_hard_endurance_allowed": False,
            }
        )
    return constraints


@dataclass(frozen=True)
class PolicySession:
    iso_date: str
    sport: str
    intensity: str
    workout: str
    phase: str = ""
    flags: tuple[str, ...] = ()
    duration_minutes: float | None = None
    distance_km: float | None = None
    tss: float | None = None


@dataclass(frozen=True)
class PolicyIssue:
    severity: str
    code: str
    message: str
    week: int | None = None
    iso_date: str | None = None


@dataclass(frozen=True)
class TrainingPolicyReport:
    issues: tuple[PolicyIssue, ...]
    easy_minutes: float
    hard_minutes: float

    @property
    def errors(self) -> tuple[PolicyIssue, ...]:
        return tuple(issue for issue in self.issues if issue.severity == "error")

    @property
    def warnings(self) -> tuple[PolicyIssue, ...]:
        return tuple(issue for issue in self.issues if issue.severity == "warning")

    @property
    def is_valid(self) -> bool:
        return not self.errors

    @property
    def easy_fraction(self) -> float | None:
        total = self.easy_minutes + self.hard_minutes
        return self.easy_minutes / total if total > 0 else None


def _value(item: Any, key: str, default: Any = None) -> Any:
    if isinstance(item, dict):
        return item.get(key, default)
    return getattr(item, key, default)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _duration_from_text(text: str) -> float | None:
    normalized = str(text or "").replace("–", "-").replace("â€“", "-")
    hours = re.search(r"(\d+(?:\.\d+)?)\s*(?:hours?|hrs?|h)\b", normalized, re.I)
    if hours:
        return float(hours.group(1)) * 60.0
    minute_range = re.search(
        r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*(?:minutes?|mins?|m)\b",
        normalized,
        re.I,
    )
    if minute_range:
        return (float(minute_range.group(1)) + float(minute_range.group(2))) / 2
    minutes = re.search(r"(\d+(?:\.\d+)?)\s*(?:minutes?|mins?|m)\b", normalized, re.I)
    return float(minutes.group(1)) if minutes else None


def _infer_sport(workout: str) -> str:
    lowered = workout.casefold()
    if "rest" in lowered or lowered in {"off", "rest day"}:
        return "rest"
    if "mobility" in lowered or "yoga" in lowered:
        return "mobility"
    if "strength a" in lowered or "strength b" in lowered:
        return "strength"
    return "run"


def _infer_intensity(workout: str, sport: str) -> str:
    lowered = workout.casefold()
    if sport == "rest":
        return "rest"
    if "recovery" in lowered:
        return "recovery"
    if any(word in lowered for word in ("race", "interval", "tempo", "hill repeat")):
        return "hard"
    if sport in {"strength", "mobility"}:
        return "moderate" if sport == "strength" else "recovery"
    return "easy"


def normalize_policy_sessions(items: Iterable[Any]) -> tuple[PolicySession, ...]:
    """Normalize imported workouts or legacy ``DayPlan`` objects."""
    sessions: list[PolicySession] = []
    for item in items:
        workout = str(_value(item, "workout", "") or "")
        sport = str(_value(item, "sport", "") or "").casefold()
        if not sport:
            sport = _infer_sport(workout)
        intensity = str(_value(item, "intensity", "") or "").casefold()
        if not intensity:
            intensity = _infer_intensity(workout, sport)
        raw_flags = _value(item, "flags", ())
        if isinstance(raw_flags, str):
            flags = tuple(part.strip() for part in raw_flags.split(",") if part.strip())
        elif isinstance(raw_flags, (list, tuple, set)):
            flags = tuple(str(flag) for flag in raw_flags)
        else:
            flags = ()
        duration = _float_or_none(_value(item, "duration_minutes"))
        if duration is None:
            duration = _duration_from_text(str(_value(item, "notes", "") or ""))
        iso_date = str(_value(item, "iso_date", _value(item, "date", "")) or "")
        sessions.append(
            PolicySession(
                iso_date=iso_date,
                sport=sport,
                intensity=intensity,
                workout=workout,
                phase=str(_value(item, "phase", "") or ""),
                flags=flags,
                duration_minutes=duration,
                distance_km=_float_or_none(_value(item, "distance_km")),
                tss=_float_or_none(_value(item, "tss")),
            )
        )
    return tuple(sorted(sessions, key=lambda item: (item.iso_date, item.workout)))


def evaluate_training_policy(
    items: Iterable[Any],
    *,
    start: date,
    event_date: date,
    age: int,
    run_days_per_week: int,
    preferred_long_session_day: str | None = None,
    minimum_strength_sessions_per_week: int = 0,
    training_method: object = DEFAULT_TRAINING_METHOD,
) -> TrainingPolicyReport:
    """Evaluate common safety invariants without generating or changing a plan."""
    method_key = normalize_training_method(training_method)
    method = TRAINING_METHODS[method_key]
    sessions = normalize_policy_sessions(items)
    issues: list[PolicyIssue] = []
    by_date: dict[date, list[PolicySession]] = {}
    for session in sessions:
        try:
            session_date = date.fromisoformat(session.iso_date)
        except ValueError:
            issues.append(
                PolicyIssue("error", "invalid_date", f"Invalid workout date: {session.iso_date!r}.")
            )
            continue
        if not start <= session_date <= event_date:
            issues.append(
                PolicyIssue(
                    "error",
                    "outside_window",
                    f"Workout date {session.iso_date} is outside the plan window.",
                    iso_date=session.iso_date,
                )
            )
        by_date.setdefault(session_date, []).append(session)

    hard_dates: list[date] = []
    for session_date, entries in sorted(by_date.items()):
        active = [entry for entry in entries if entry.intensity != "rest" and entry.sport != "rest"]
        rest = [entry for entry in entries if entry.intensity == "rest" or entry.sport == "rest"]
        if len(entries) > MAX_SESSIONS_PER_DAY:
            issues.append(
                PolicyIssue(
                    "error",
                    "daily_session_cap",
                    f"{session_date.isoformat()} has more than 3 sessions.",
                    iso_date=session_date.isoformat(),
                )
            )
        if active and rest:
            issues.append(
                PolicyIssue(
                    "error",
                    "rest_active_mix",
                    f"{session_date.isoformat()} cannot mix rest and active sessions.",
                    iso_date=session_date.isoformat(),
                )
            )
        hard_entries = [entry for entry in entries if entry.intensity in {"hard", "race"}]
        if len(hard_entries) > 1:
            issues.append(
                PolicyIssue(
                    "error",
                    "daily_hard_cap",
                    f"{session_date.isoformat()} has more than one hard/race session.",
                    iso_date=session_date.isoformat(),
                )
            )
        if hard_entries:
            hard_dates.append(session_date)

    race_entries = [
        session
        for session in by_date.get(event_date, [])
        if session.intensity == "race"
    ]
    if len(race_entries) != 1:
        issues.append(
            PolicyIssue(
                "error",
                "race_session",
                f"Event date {event_date.isoformat()} must have exactly one race session.",
                iso_date=event_date.isoformat(),
            )
        )

    by_week: dict[int, list[PolicySession]] = {}
    for session in sessions:
        try:
            session_date = date.fromisoformat(session.iso_date)
        except ValueError:
            continue
        week = ((session_date - start).days // 7) + 1
        by_week.setdefault(week, []).append(session)

    hard_cap = (
        get_intensity_cap(age)
        if method.allow_non_race_hard_endurance
        else 1
    )
    baseline_run_km: float | None = None
    easy_minutes = 0.0
    hard_minutes = 0.0
    for week in sorted(by_week):
        entries = by_week[week]
        run_entries = [
            entry for entry in entries if entry.sport == "run" and entry.intensity != "rest"
        ]
        run_dates = {entry.iso_date for entry in run_entries}
        if len(run_dates) > run_days_per_week:
            issues.append(
                PolicyIssue(
                    "error",
                    "run_day_cap",
                    f"Week {week} has {len(run_dates)} run days; configured maximum is {run_days_per_week}.",
                    week=week,
                )
            )
        hard_entries = [entry for entry in entries if entry.intensity in {"hard", "race"}]
        if len(hard_entries) > hard_cap:
            issues.append(
                PolicyIssue(
                    "error",
                    "hard_day_cap",
                    f"Week {week} has {len(hard_entries)} hard/race sessions; age-based maximum is {hard_cap}.",
                    week=week,
                )
            )
        strength_count = sum(entry.sport == "strength" for entry in entries)
        if strength_count > MAX_STRENGTH_SESSIONS_PER_WEEK:
            issues.append(
                PolicyIssue(
                    "error",
                    "strength_cap",
                    f"Week {week} has more than 3 strength sessions.",
                    week=week,
                )
            )
        week_start = date.fromordinal(start.toordinal() + (week - 1) * 7)
        week_end = date.fromordinal(week_start.toordinal() + 6)
        full_training_week = week_end <= event_date
        strength_phase = any(
            entry.phase in {"Base", "Build", "Peak", "Maintenance"}
            for entry in entries
        ) and not any(
            entry.phase in {"Taper", "Race", "Recovery"} for entry in entries
        )
        if (
            minimum_strength_sessions_per_week > 0
            and full_training_week
            and strength_phase
            and strength_count < minimum_strength_sessions_per_week
        ):
            issues.append(
                PolicyIssue(
                    "error",
                    "strength_minimum",
                    f"Week {week} has {strength_count} strength sessions; minimum is {minimum_strength_sessions_per_week}.",
                    week=week,
                )
            )
        total_duration = sum(
            entry.duration_minutes or 0
            for entry in entries
            if entry.intensity != "race"
        )
        if total_duration > 2_400:
            issues.append(
                PolicyIssue("error", "duration_cap", f"Week {week} exceeds the 40-hour training cap.", week=week)
            )
        total_tss = sum(entry.tss or 0 for entry in entries if entry.intensity != "race")
        if total_tss > 1_500:
            issues.append(
                PolicyIssue("error", "tss_cap", f"Week {week} exceeds the 1,500 TSS training cap.", week=week)
            )

        has_cutback = any("CUTBACK" in entry.flags for entry in entries)
        has_taper_or_race = any(
            entry.phase in {"Taper", "Race", "Recovery"} for entry in entries
        )
        has_all_run_distances = bool(run_entries) and all(
            entry.distance_km not in (None, 0.0) for entry in run_entries
        )
        if has_all_run_distances and not has_cutback and not has_taper_or_race:
            run_km = sum(entry.distance_km or 0 for entry in run_entries)
            if baseline_run_km is not None and baseline_run_km >= 10:
                if run_km > baseline_run_km * (1 + MAX_WEEKLY_RUN_INCREASE_FRACTION) + 1e-9:
                    increase = ((run_km / baseline_run_km) - 1) * 100
                    issues.append(
                        PolicyIssue(
                            "error",
                            "run_progression",
                            f"Week {week} run distance increases {increase:.1f}%; maximum is 10%.",
                            week=week,
                        )
                    )
            baseline_run_km = run_km

        known_run_minutes = [
            entry.duration_minutes
            for entry in run_entries
            if entry.duration_minutes not in (None, 0.0)
        ]
        if len(known_run_minutes) == len(run_entries) and known_run_minutes:
            longest = max(known_run_minutes)
            total_run = sum(known_run_minutes)
            if total_run >= 120 and longest / total_run > 0.50:
                issues.append(
                    PolicyIssue(
                        "warning",
                        "long_session_share",
                        f"Week {week}'s longest run is more than 50% of known run duration.",
                        week=week,
                    )
                )

        for entry in entries:
            if entry.sport not in {"run", "cycle", "swim", "hike", "cross_training"}:
                continue
            minutes = entry.duration_minutes or 0
            if (
                not method.allow_non_race_hard_endurance
                and entry.intensity in {"moderate", "hard"}
            ):
                issues.append(
                    PolicyIssue(
                        "error",
                        "training_method_intensity",
                        (
                            f"{entry.iso_date} {entry.workout} is {entry.intensity}; "
                            f"{method.label} allows only easy/recovery endurance outside race day."
                        ),
                        week=week,
                        iso_date=entry.iso_date,
                    )
                )
            if entry.intensity in {"easy", "recovery"}:
                easy_minutes += minutes
            elif entry.intensity in {"moderate", "hard"}:
                hard_minutes += minutes
            if (
                preferred_long_session_day
                and "long" in entry.workout.casefold()
                and entry.intensity != "race"
            ):
                actual_day = date.fromisoformat(entry.iso_date).strftime("%A")
                if actual_day.casefold() != preferred_long_session_day.casefold():
                    issues.append(
                        PolicyIssue(
                            "warning",
                            "preferred_long_day",
                            f"Long session on {entry.iso_date} is {actual_day}; preference is {preferred_long_session_day}.",
                            week=week,
                            iso_date=entry.iso_date,
                        )
                    )

    for previous, current in zip(sorted(set(hard_dates)), sorted(set(hard_dates))[1:]):
        if (current - previous).days == 1:
            issues.append(
                PolicyIssue(
                    "error",
                    "consecutive_hard_days",
                    f"Hard/race sessions on {previous.isoformat()} and {current.isoformat()} are consecutive.",
                    iso_date=current.isoformat(),
                )
            )

    intensity_total = easy_minutes + hard_minutes
    target_easy_fraction = method.target_easy_duration_fraction
    if (
        intensity_total >= 120
        and easy_minutes / intensity_total < target_easy_fraction - 0.05
    ):
        hard_percent = hard_minutes / intensity_total * 100
        target_label = (
            "Maffetone aerobic target"
            if method.key == "maffetone"
            else "80/20 target"
        )
        issues.append(
            PolicyIssue(
                "warning",
                "intensity_distribution",
                f"Known endurance duration is {hard_percent:.0f}% moderate/hard; review the {target_label}.",
            )
        )

    return TrainingPolicyReport(tuple(issues), easy_minutes, hard_minutes)
