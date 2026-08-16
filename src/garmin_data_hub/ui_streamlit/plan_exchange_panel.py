"""Manual ChatGPT training-plan exchange for the Build Plan page.

This module deliberately contains no model client.  The application prepares a
privacy-minimized JSON packet, the athlete uploads it to ChatGPT themselves, and
the returned JSON remains a preview until an explicit database save.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.services.ai_plan_import import (
    ImportedTrainingPlan,
    PlanImportError,
    parse_chatgpt_plan,
)
from garmin_data_hub.services.coaching_packet import (
    build_coaching_packet,
    coaching_packet_to_json,
)
from garmin_data_hub.services.plan_persistence import (
    PlanPersistenceError,
    StalePlanWriteError,
    save_imported_plan,
)


_IMPORT_NOTICE_KEY = "chatgpt_plan_import_notice"
_UPLOAD_GENERATION_KEY = "chatgpt_plan_response_upload_generation"
_SESSION_FIELDS = (
    "sport",
    "phase",
    "workout",
    "intensity",
    "duration_minutes",
    "distance_km",
    "tss",
    "flags",
    "notes",
)
_SESSION_FIELD_LABELS = {
    "sport": "Sport",
    "phase": "Phase",
    "workout": "Workout",
    "intensity": "Intensity",
    "duration_minutes": "Duration",
    "distance_km": "Distance",
    "tss": "TSS",
    "flags": "Flags",
    "notes": "Notes",
}


def _existing_workouts(
    db_path: Path, start_date: str, end_date: str
) -> list[dict[str, Any]]:
    conn = connect_sqlite(db_path)
    try:
        rows = conn.execute(
            """
            SELECT scheduled_date, workout_name, planned_duration_s,
                   planned_distance_m, planned_tss, description, structure_json
            FROM planned_workout
            WHERE scheduled_date BETWEEN ? AND ?
            ORDER BY scheduled_date, planned_workout_id
            """,
            (start_date, end_date),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _workout_rows(plan: ImportedTrainingPlan) -> list[dict[str, Any]]:
    return [
        {
            "Date": workout.iso_date,
            "Sport": workout.sport.replace("_", " ").title(),
            "Phase": workout.phase,
            "Workout": workout.workout,
            "Intensity": workout.intensity.title(),
            "Duration (min)": workout.duration_minutes,
            "Distance (km)": workout.distance_km,
            "TSS": workout.tss,
            "Flags": ", ".join(workout.flags),
            "Notes": workout.notes,
        }
        for workout in plan.workouts
    ]


def _finite_number(value: Any, *, divisor: float = 1.0) -> float | None:
    if value is None:
        return None
    try:
        result = float(value) / divisor
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    if not math.isfinite(result):
        return None
    return round(result, 6)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _flags(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(sorted(_text(item) for item in value if _text(item)))


def _structure_workout(row: dict[str, Any]) -> dict[str, Any]:
    raw_structure = row.get("structure_json")
    if isinstance(raw_structure, str) and raw_structure:
        try:
            raw_structure = json.loads(raw_structure)
        except json.JSONDecodeError:
            return {}
    if not isinstance(raw_structure, dict):
        return {}
    workout = raw_structure.get("workout")
    return workout if isinstance(workout, dict) else {}


def _existing_session(row: dict[str, Any]) -> dict[str, Any]:
    structured = _structure_workout(row)
    return {
        "sport": _text(structured.get("sport")),
        "phase": _text(structured.get("phase")),
        "workout": _text(structured.get("workout") or row.get("workout_name")),
        "intensity": _text(structured.get("intensity")),
        "duration_minutes": _finite_number(
            structured.get("duration_minutes")
            if "duration_minutes" in structured
            else row.get("planned_duration_s"),
            divisor=1.0 if "duration_minutes" in structured else 60.0,
        ),
        "distance_km": _finite_number(
            structured.get("distance_km")
            if "distance_km" in structured
            else row.get("planned_distance_m"),
            divisor=1.0 if "distance_km" in structured else 1000.0,
        ),
        "tss": _finite_number(
            structured.get("tss")
            if "tss" in structured
            else row.get("planned_tss")
        ),
        "flags": _flags(structured.get("flags")),
        "notes": _text(
            structured.get("notes")
            if "notes" in structured
            else row.get("description")
        ),
    }


def _imported_session(workout: Any) -> dict[str, Any]:
    return {
        "sport": _text(workout.sport),
        "phase": _text(workout.phase),
        "workout": _text(workout.workout),
        "intensity": _text(workout.intensity),
        "duration_minutes": _finite_number(workout.duration_minutes),
        "distance_km": _finite_number(workout.distance_km),
        "tss": _finite_number(workout.tss),
        "flags": _flags(workout.flags),
        "notes": _text(workout.notes),
    }


def _session_sort_key(session: dict[str, Any]) -> str:
    return json.dumps(session, ensure_ascii=False, sort_keys=True)


def _changed_session_fields(
    current: list[dict[str, Any]], imported: list[dict[str, Any]]
) -> list[str]:
    changed: list[str] = []
    for field in _SESSION_FIELDS:
        old_values = sorted(repr(session[field]) for session in current)
        new_values = sorted(repr(session[field]) for session in imported)
        if old_values != new_values:
            changed.append(_SESSION_FIELD_LABELS[field])
    return changed


def _session_summary(session: dict[str, Any]) -> str:
    parts = [session["workout"] or "(unnamed workout)"]
    for field, suffix in (
        ("sport", "sport"),
        ("phase", "phase"),
        ("intensity", "intensity"),
    ):
        if session[field]:
            parts.append(f"{suffix}: {session[field]}")
    if session["duration_minutes"] is not None:
        parts.append(f"{session['duration_minutes']:g} min")
    if session["distance_km"] is not None:
        parts.append(f"{session['distance_km']:g} km")
    if session["tss"] is not None:
        parts.append(f"TSS {session['tss']:g}")
    if session["flags"]:
        parts.append(f"flags: {', '.join(session['flags'])}")
    if session["notes"]:
        parts.append(f"notes: {session['notes']}")
    return " | ".join(parts)


def _date_level_diff(
    existing: list[dict[str, Any]], plan: ImportedTrainingPlan
) -> list[dict[str, str]]:
    old_by_date: dict[str, list[dict[str, Any]]] = {}
    for row in existing:
        old_by_date.setdefault(str(row["scheduled_date"]), []).append(
            _existing_session(row)
        )

    new_by_date: dict[str, list[dict[str, Any]]] = {}
    for workout in plan.workouts:
        new_by_date.setdefault(workout.iso_date, []).append(
            _imported_session(workout)
        )

    diff: list[dict[str, str]] = []
    for iso_date in sorted(set(old_by_date) | set(new_by_date)):
        old = sorted(old_by_date.get(iso_date, []), key=_session_sort_key)
        new = sorted(new_by_date.get(iso_date, []), key=_session_sort_key)
        if old == new:
            continue
        if old and new:
            change = "Changed"
        elif new:
            change = "Added"
        else:
            change = "Removed"
        diff.append(
            {
                "Date": iso_date,
                "Change": change,
                "Changed fields": (
                    ", ".join(_changed_session_fields(old, new))
                    if old and new
                    else "Sessions"
                ),
                "Current": " ; ".join(_session_summary(item) for item in old) or "—",
                "Imported": " ; ".join(_session_summary(item) for item in new)
                or "Rest / no scheduled row",
            }
        )
    return diff


def _context_errors(
    plan: ImportedTrainingPlan,
    *,
    expected_start_date: date,
    expected_event_date: date,
    expected_age: int,
    expected_distance: str,
    expected_run_days: int,
    expected_hrmax: int | None,
    expected_lthr: int | None,
    expected_sodium: int | None,
    expected_primary_sport: str,
    expected_event_sport: str,
) -> list[str]:
    """Reject a structurally valid response that changed locked request inputs."""
    errors: list[str] = []
    athlete = plan.inputs.athlete
    event = plan.inputs.event

    comparisons = (
        (event.start_date, expected_start_date.isoformat(), "plan start date"),
        (event.event_date, expected_event_date.isoformat(), "event date"),
        (event.distance, expected_distance, "event distance"),
        (event.run_days_per_week, expected_run_days, "run-days setting"),
        (athlete.age, expected_age, "athlete age"),
        (athlete.hrmax, expected_hrmax, "HRmax"),
        (athlete.lthr, expected_lthr, "LTHR"),
        (
            athlete.sodium_mg_per_hr_hot,
            expected_sodium,
            "sodium setting",
        ),
        (plan.primary_sport, expected_primary_sport, "primary sport"),
        (plan.event_sport, expected_event_sport, "event sport"),
    )
    for actual, expected, label in comparisons:
        if actual != expected:
            errors.append(
                f"The response changed {label}: expected {expected!r}, got {actual!r}."
            )

    return errors


def _schedule_warnings(
    plan: ImportedTrainingPlan, *, expected_long_run_day: str | None
) -> list[str]:
    """Flag likely long-run conflicts for review without trusting name heuristics."""
    if not expected_long_run_day:
        return []
    warnings: list[str] = []
    for workout in plan.workouts:
        if (
            workout.sport == "run"
            and workout.intensity != "race"
            and re.search(r"\blong\b", workout.workout, flags=re.IGNORECASE)
        ):
            workout_day = date.fromisoformat(workout.iso_date).strftime("%A")
            if workout_day.casefold() != expected_long_run_day.casefold():
                warnings.append(
                    f"Review the run labelled '{workout.workout}' on "
                    f"{workout.iso_date} ({workout_day}); your preferred long-run "
                    f"day is {expected_long_run_day}."
                )
    return warnings


def render_plan_exchange_panel(
    db_path: Path,
    *,
    plan_start_date: date,
    event_date: date,
    age: int,
    distance: str,
    run_days_per_week: int,
    long_run_day: str,
    sodium_mg_per_hour: int | None,
    hrmax: int | None,
    lthr: int | None,
) -> None:
    """Render export, validation preview, and explicit atomic plan import."""
    st.subheader("ChatGPT Plan Exchange (manual)")
    st.caption(
        "No API key is used. Garmin Data Hub creates a local JSON file; you "
        "choose whether to upload it to ChatGPT and whether to save the result."
    )

    import_notice = st.session_state.pop(_IMPORT_NOTICE_KEY, None)
    if isinstance(import_notice, dict):
        if import_notice.get("duplicate"):
            st.info(
                "This exact response was already applied as import "
                f"#{import_notice['plan_import_id']}."
            )
        else:
            st.success(
                f"Imported plan #{import_notice['plan_import_id']}: saved "
                f"{import_notice['workout_count']} sessions and replaced "
                f"{import_notice['replaced_workout_count']} prior rows."
            )
        st.page_link(
            "pages/1_Plan_Review.py",
            label="Open Plan Review",
            icon="📅",
        )

    if hrmax is not None and lthr is not None and lthr >= hrmax:
        st.warning(
            "LTHR must be lower than HRmax before exchanging a plan. Update "
            "Athlete HR Settings above, save the override, and try again."
        )
        return

    exchange_start = max(date.today(), plan_start_date)
    if event_date < exchange_start:
        st.warning("The race date must be today or later before a plan can be exchanged.")
        return

    horizon_days = (event_date - exchange_start).days + 1
    if horizon_days > 366:
        st.warning(
            "The manual plan format currently supports at most 366 days. "
            "Move the plan start date closer to the event."
        )
        return

    export_tab, import_tab = st.tabs(
        ["1 · Export coaching packet", "2 · Import ChatGPT plan"]
    )

    with export_tab:
        st.markdown(
            "Add details Garmin cannot know. Leave any field blank when it does "
            "not apply. These values are included only in the downloaded packet."
        )
        limits_col, strength_col, nutrition_col = st.columns(3)
        with limits_col:
            injuries = st.text_area(
                "Injuries or limitations",
                key="chatgpt_exchange_injuries",
                height=100,
                placeholder="Example: avoid deep knee flexion",
            )
            scheduling = st.text_area(
                "Scheduling notes",
                key="chatgpt_exchange_schedule",
                height=100,
                placeholder="Example: no training Wednesday evenings",
            )
        with strength_col:
            equipment = st.text_area(
                "Strength equipment",
                key="chatgpt_exchange_equipment",
                height=100,
                placeholder="Example: dumbbells, bands, pull-up bar",
            )
            strength_experience = st.text_area(
                "Strength experience",
                key="chatgpt_exchange_strength_experience",
                height=100,
                placeholder="Example: beginner, twice weekly",
            )
        with nutrition_col:
            dietary = st.text_area(
                "Dietary preferences",
                key="chatgpt_exchange_diet",
                height=68,
                placeholder="Example: vegetarian",
            )
            allergies = st.text_area(
                "Allergies or intolerances",
                key="chatgpt_exchange_allergies",
                height=68,
            )
            gi_notes = st.text_area(
                "GI considerations",
                key="chatgpt_exchange_gi",
                height=68,
                placeholder="Example: sensitive during long runs",
            )

        lookback_weeks = st.select_slider(
            "Training-history window",
            options=[4, 8, 12, 16, 24, 52],
            value=12,
            format_func=lambda value: f"{value} weeks",
            key="chatgpt_exchange_lookback_weeks",
        )
        preferences = {
            "injuries_or_limitations": injuries,
            "strength_equipment": equipment,
            "strength_experience": strength_experience,
            "dietary_preferences": dietary,
            "allergies_or_intolerances": allergies,
            "gi_considerations": gi_notes,
            "scheduling_notes": scheduling,
        }

        try:
            packet = build_coaching_packet(
                db_path,
                as_of=date.today(),
                plan_start=exchange_start,
                lookback_days=int(lookback_weeks) * 7,
                recent_activity_limit=40,
                plan_horizon_days=horizon_days,
                preferences=preferences,
                plan_context={
                    "age": int(age),
                    "run_days_per_week": int(run_days_per_week),
                    "distance": str(distance),
                    "long_run_day": str(long_run_day),
                    "sodium_mg_per_hour": sodium_mg_per_hour,
                },
            )
            packet_json = coaching_packet_to_json(packet)
        except (OSError, TypeError, ValueError) as exc:
            st.error(f"Could not create the coaching packet: {exc}")
            packet = None
            packet_json = ""

        if packet is not None:
            activity_count = packet["context"]["training_history"]["summary"][
                "activities"
            ]
            st.info(
                f"Packet ready: {activity_count} summarized activities, "
                f"{horizon_days} plan days, GPS and raw trackpoints excluded."
            )
            st.download_button(
                "Download coaching packet JSON",
                data=packet_json,
                file_name=f"chatgpt_coaching_packet_{exchange_start.isoformat()}.json",
                mime="application/json",
                type="primary",
                use_container_width=True,
            )
            st.markdown("**Copy this prompt into the ChatGPT conversation:**")
            st.code(packet["chatgpt"]["copyable_prompt"], language=None)
            with st.expander("Review exactly what the packet contains"):
                st.json(
                    {
                        "privacy": packet["privacy"],
                        "context": packet["context"],
                    },
                    expanded=False,
                )

    with import_tab:
        st.markdown(
            "Ask ChatGPT to save or provide its response as JSON, then upload "
            "that response here. Uploading only creates a preview."
        )
        if packet is None:
            st.info("Resolve the export-packet error before importing a response.")
            return

        upload_generation = int(st.session_state.get(_UPLOAD_GENERATION_KEY, 0))
        uploaded = st.file_uploader(
            "ChatGPT plan response (.json)",
            type=["json"],
            key=f"chatgpt_plan_response_upload_{upload_generation}",
        )
        if uploaded is None:
            return

        try:
            plan = parse_chatgpt_plan(
                uploaded.getvalue(),
                expected_request_id=packet["request_id"],
                expected_active_plan_sha256=packet["active_plan_sha256"],
            )
        except PlanImportError as exc:
            st.error(f"This response was not accepted: {exc}")
            st.caption(
                "Upload the response produced from the currently displayed packet. "
                "If inputs or the active plan changed, export a fresh packet."
            )
            return

        locked_input_errors = _context_errors(
            plan,
            expected_start_date=exchange_start,
            expected_event_date=event_date,
            expected_age=int(age),
            expected_distance=str(distance),
            expected_run_days=int(run_days_per_week),
            expected_hrmax=int(hrmax) if hrmax is not None else None,
            expected_lthr=int(lthr) if lthr is not None else None,
            expected_sodium=packet["context"]["athlete"][
                "sodium_mg_per_hour"
            ],
            expected_primary_sport=str(
                packet["context"]["athlete"]["primary_sport"]
            ),
            expected_event_sport=str(packet["context"]["event"]["sport"]),
        )
        schedule_warnings = _schedule_warnings(
            plan,
            expected_long_run_day=packet["context"]["training_constraints"][
                "preferred_long_session_day"
            ],
        )
        if locked_input_errors:
            st.error("The response changed locked plan inputs and cannot be saved.")
            for message in locked_input_errors:
                st.write(f"- {message}")

        first_date = plan.day_plans[0].iso_date
        last_date = plan.day_plans[-1].iso_date
        existing = _existing_workouts(db_path, first_date, last_date)
        diff = _date_level_diff(existing, plan)

        metric_cols = st.columns(4)
        metric_cols[0].metric("Plan days", len(plan.day_plans))
        metric_cols[1].metric("Scheduled sessions", len(plan.workouts))
        metric_cols[2].metric("Rows currently in range", len(existing))
        metric_cols[3].metric("Changed dates", len(diff))

        if plan.analysis.notes:
            st.markdown("**ChatGPT analysis**")
            st.write(plan.analysis.notes)
        if plan.rationale:
            st.markdown("**Plan rationale**")
            st.write(plan.rationale)

        if plan.warnings:
            st.warning("\n\n".join(plan.warnings))
        if schedule_warnings:
            st.warning("\n\n".join(schedule_warnings))

        preview_tab, weeks_tab, changes_tab, guidance_tab = st.tabs(
            ["Daily plan", "Weekly summary", "Changes", "Strength & nutrition"]
        )
        with preview_tab:
            st.dataframe(
                pd.DataFrame(_workout_rows(plan)), use_container_width=True
            )
        with weeks_tab:
            st.dataframe(pd.DataFrame(plan.weekly_rows), use_container_width=True)
        with changes_tab:
            if diff:
                st.dataframe(pd.DataFrame(diff), use_container_width=True)
            else:
                st.info("No date-level workout changes were detected.")
        with guidance_tab:
            st.markdown("**Strength guidance**")
            if plan.strength_guidance:
                for item in plan.strength_guidance:
                    st.write(f"- {item}")
            else:
                st.caption("No separate strength guidance was returned.")
            st.markdown("**Nutrition guidance**")
            if plan.nutrition_guidance:
                for item in plan.nutrition_guidance:
                    st.write(f"- {item}")
            else:
                st.caption("No separate nutrition guidance was returned.")
            st.caption(
                "Strength and nutrition material is educational and is not a "
                "medical diagnosis or treatment plan."
            )

        st.divider()
        st.warning(
            f"Applying this plan will replace {len(existing)} existing database "
            f"row(s) from {first_date} through {last_date}. Workouts outside this "
            "range are preserved."
        )
        st.caption(
            "When accepted, the response JSON and guidance plus a recovery "
            "snapshot of the prior plan are retained locally in "
            "plan_import_history."
        )
        review_id = hashlib.sha256(plan.canonical_payload.encode("utf-8")).hexdigest()[:16]
        with st.form(f"apply_chatgpt_plan_form_{review_id}"):
            acknowledged = st.checkbox(
                "I reviewed the plan, changes, warnings, and replacement range.",
                key=f"apply_chatgpt_plan_ack_{review_id}",
            )
            apply_clicked = st.form_submit_button(
                "Apply imported plan to database",
                type="primary",
                disabled=bool(locked_input_errors),
                use_container_width=True,
            )

        if apply_clicked:
            if not acknowledged:
                st.warning("Review the preview and check the confirmation box first.")
            else:
                try:
                    result = save_imported_plan(db_path, plan)
                except StalePlanWriteError as exc:
                    st.error(str(exc))
                    st.info("Export a fresh packet and ask ChatGPT for a new response.")
                except (PlanPersistenceError, OSError) as exc:
                    st.error(
                        "The plan was not saved; the database was rolled back: "
                        f"{exc}"
                    )
                else:
                    st.session_state.plan_refresh_token = datetime.now(
                        timezone.utc
                    ).isoformat()
                    st.session_state[_UPLOAD_GENERATION_KEY] = upload_generation + 1
                    st.session_state[_IMPORT_NOTICE_KEY] = {
                        "duplicate": result.duplicate,
                        "plan_import_id": result.plan_import_id,
                        "workout_count": result.workout_count,
                        "replaced_workout_count": result.replaced_workout_count,
                    }
                    st.cache_data.clear()
                    st.rerun()
