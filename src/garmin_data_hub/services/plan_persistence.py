from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import sqlite3
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime, timezone
from numbers import Integral, Real
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

from garmin_data_hub.db import queries as db_queries
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.services.season_plans import assert_legacy_write_allowed

logger = logging.getLogger(__name__)


IMPORTED_PLAN_SOURCE = "codex_cli"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ACTIVE_PLAN_SQL = """
    SELECT scheduled_date, workout_name, description, planned_distance_m,
           planned_duration_s, planned_tss, structure_json
    FROM active_planned_workout
"""


class PlanPersistenceError(RuntimeError):
    """Base class for an imported plan that could not be persisted safely."""


class InvalidImportedPlanError(PlanPersistenceError):
    """The parsed plan object is incomplete or contains unsafe values."""


class StalePlanWriteError(PlanPersistenceError):
    """The active plan changed while a replacement was being prepared."""

    def __init__(self, *, expected_sha256: str, current_sha256: str) -> None:
        self.expected_sha256 = expected_sha256
        self.current_sha256 = current_sha256
        super().__init__(
            "The active training plan changed while this update was being "
            "prepared. Refresh the plan and try again."
        )


@dataclass(frozen=True)
class PlanImportSaveResult:
    """Outcome of an explicit imported-plan save operation."""

    status: Literal["applied", "duplicate"]
    plan_import_id: int
    content_sha256: str
    active_plan_sha256: str
    replace_start_date: str
    replace_end_date: str
    workout_count: int
    replaced_workout_count: int

    @property
    def applied(self) -> bool:
        return self.status == "applied"

    @property
    def duplicate(self) -> bool:
        return self.status == "duplicate"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def active_plan_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    """Return the semantic ``planned_workout`` state used for stale checks.

    Database identifiers and creation timestamps are deliberately excluded. Rows
    sharing a date are sorted by their canonical semantic contents, so the hash
    does not depend on insertion order.
    """
    workouts: list[dict[str, Any]] = []
    for row in conn.execute(_ACTIVE_PLAN_SQL).fetchall():
        structure: Any = row["structure_json"]
        if structure:
            try:
                structure = json.loads(structure)
            except (TypeError, json.JSONDecodeError):
                structure = str(structure)

        workouts.append(
            {
                "scheduled_date": row["scheduled_date"],
                "workout_name": row["workout_name"],
                "description": row["description"],
                "planned_distance_m": _finite_float_or_none(
                    row["planned_distance_m"]
                ),
                "planned_duration_s": _finite_float_or_none(
                    row["planned_duration_s"]
                ),
                "planned_tss": _finite_float_or_none(row["planned_tss"]),
                "structure": structure,
            }
        )

    workouts.sort(
        key=lambda workout: (
            str(workout.get("scheduled_date") or ""),
            _canonical_json(workout),
        )
    )
    return {"planned_workouts": workouts}


def fingerprint_active_plan_snapshot(snapshot: Mapping[str, Any]) -> str:
    """Return the canonical SHA-256 fingerprint for an active-plan snapshot."""
    return hashlib.sha256(_canonical_json(snapshot).encode("utf-8")).hexdigest()


def active_plan_sha256(conn: sqlite3.Connection) -> str:
    """Fingerprint the authoritative plan using an existing connection."""
    return fingerprint_active_plan_snapshot(active_plan_snapshot(conn))


def get_active_plan_snapshot(db_path: Path | str) -> dict[str, Any]:
    """Load the canonical semantic active-plan snapshot from ``db_path``."""
    path = Path(db_path)
    if not path.is_file():
        raise FileNotFoundError(f"Garmin database does not exist: {path}")
    conn = connect_sqlite(path)
    try:
        return active_plan_snapshot(conn)
    finally:
        conn.close()


def get_active_plan_sha256(db_path: Path | str) -> str:
    """Fingerprint the authoritative active plan stored in ``db_path``."""
    return fingerprint_active_plan_snapshot(get_active_plan_snapshot(db_path))


def save_imported_plan(
    db_path: Path | str,
    plan: Any,
) -> PlanImportSaveResult:
    """Atomically replace the imported plan window after a stale-plan check.

    ``plan`` is the validated ``ImportedTrainingPlan`` returned by
    :mod:`garmin_data_hub.services.ai_plan_import`. Persistence intentionally uses
    a small structural interface so this module does not create an import cycle
    with the parser.

    An identical response is an idempotent no-op. For a new response, SQLite's
    write lock is acquired before checking ``active_plan_sha256``; the range
    delete, exact numeric inserts, legacy plan cache, and audit row then commit as
    one transaction.
    """
    path = Path(db_path)
    if not path.is_file():
        raise FileNotFoundError(f"Garmin database does not exist: {path}")

    prepared = _prepare_import(plan)
    conn = connect_sqlite(path)
    transaction_started = False
    try:
        conn.execute("BEGIN IMMEDIATE")
        transaction_started = True

        duplicate_row = conn.execute(
            """
            SELECT plan_import_id, replace_start_date, replace_end_date,
                   workout_count
            FROM plan_import_history
            WHERE content_sha256 = ?
            """,
            (prepared["content_sha256"],),
        ).fetchone()
        if duplicate_row is not None:
            current_sha256 = active_plan_sha256(conn)
            conn.rollback()
            transaction_started = False
            return PlanImportSaveResult(
                status="duplicate",
                plan_import_id=int(duplicate_row["plan_import_id"]),
                content_sha256=prepared["content_sha256"],
                active_plan_sha256=current_sha256,
                replace_start_date=str(duplicate_row["replace_start_date"]),
                replace_end_date=str(duplicate_row["replace_end_date"]),
                workout_count=int(duplicate_row["workout_count"]),
                replaced_workout_count=0,
            )

        previous_snapshot = active_plan_snapshot(conn)
        current_sha256 = fingerprint_active_plan_snapshot(previous_snapshot)
        expected_sha256 = prepared["expected_active_plan_sha256"]
        if current_sha256 != expected_sha256:
            raise StalePlanWriteError(
                expected_sha256=expected_sha256,
                current_sha256=current_sha256,
            )

        replace_start = prepared["replace_start_date"]
        replace_end = prepared["replace_end_date"]
        assert_legacy_write_allowed(conn, replace_start, replace_end)
        replaced_count = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM active_planned_workout
                WHERE scheduled_date BETWEEN ? AND ?
                """,
                (replace_start, replace_end),
            ).fetchone()[0]
        )
        previous_setting_row = conn.execute(
            "SELECT value FROM app_settings WHERE key = 'last_generated_plan'"
        ).fetchone()
        previous_setting_value = (
            previous_setting_row["value"] if previous_setting_row else None
        )
        previous_generated_plan = _decode_last_generated_plan(previous_setting_value)
        previous_plan_json = _canonical_json(
            {
                "active_plan_sha256": current_sha256,
                "active_plan": previous_snapshot,
                "last_generated_plan": previous_generated_plan,
                "last_generated_plan_setting_value": previous_setting_value,
            }
        )

        conn.execute(
            """
            DELETE FROM planned_workout
            WHERE scheduled_date BETWEEN ? AND ?
            """,
            (replace_start, replace_end),
        )
        conn.executemany(
            """
            INSERT INTO planned_workout(
                scheduled_date,
                workout_name,
                description,
                planned_distance_m,
                planned_duration_s,
                planned_tss,
                structure_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            prepared["workout_rows"],
        )

        new_active_sha256 = active_plan_sha256(conn)
        applied_at = _utc_now_iso()
        provenance = {
            "source": IMPORTED_PLAN_SOURCE,
            "contract": prepared["contract"],
            "version": prepared["version"],
            "request_id": prepared["request_id"],
            "response_content_sha256": prepared["content_sha256"],
            "active_plan_sha256_before": current_sha256,
            "active_plan_sha256_after": new_active_sha256,
            "applied_at": applied_at,
        }
        legacy_blob = _build_imported_legacy_blob(
            plan,
            provenance,
            previous_generated_plan=previous_generated_plan,
            replace_start_date=replace_start,
            replace_end_date=replace_end,
        )
        legacy_setting_value = json.dumps(legacy_blob, ensure_ascii=False)
        conn.execute(
            """
            INSERT INTO app_settings(key, value, updated_at)
            VALUES ('last_generated_plan', ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (legacy_setting_value, applied_at),
        )

        cursor = conn.execute(
            """
            INSERT INTO plan_import_history(
                source,
                schema_version,
                content_sha256,
                plan_name,
                replace_start_date,
                replace_end_date,
                workout_count,
                payload_json,
                previous_plan_json,
                validation_warnings_json,
                applied_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                IMPORTED_PLAN_SOURCE,
                prepared["schema_version"],
                prepared["content_sha256"],
                prepared["plan_name"],
                replace_start,
                replace_end,
                len(prepared["workout_rows"]),
                prepared["canonical_payload"],
                previous_plan_json,
                _canonical_json(prepared["warnings"]),
                applied_at,
            ),
        )
        plan_import_id = int(cursor.lastrowid)
        conn.commit()
        transaction_started = False
        return PlanImportSaveResult(
            status="applied",
            plan_import_id=plan_import_id,
            content_sha256=prepared["content_sha256"],
            active_plan_sha256=new_active_sha256,
            replace_start_date=replace_start,
            replace_end_date=replace_end,
            workout_count=len(prepared["workout_rows"]),
            replaced_workout_count=replaced_count,
        )
    except BaseException:
        if transaction_started and conn.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.close()


def _prepare_import(plan: Any) -> dict[str, Any]:
    request_id = _required_sha256(getattr(plan, "request_id", None), "request_id")
    expected_hash = _required_sha256(
        getattr(plan, "active_plan_sha256", None), "active_plan_sha256"
    )
    contract = _required_text(getattr(plan, "contract", None), "contract")
    version = getattr(plan, "version", None)
    if isinstance(version, bool) or not isinstance(version, Integral) or int(version) < 1:
        raise InvalidImportedPlanError("version must be a positive integer")
    version = int(version)

    workouts = tuple(getattr(plan, "workouts", ()) or ())
    if not workouts:
        raise InvalidImportedPlanError("an imported plan must contain workouts")

    day_plans = tuple(getattr(plan, "day_plans", ()) or ())
    day_plan_dates = [
        _required_iso_date(
            getattr(day_plan, "iso_date", None), f"day_plans[{index}].iso_date"
        )
        for index, day_plan in enumerate(day_plans)
    ]

    canonical_payload = _canonical_import_payload(plan)
    content_sha256 = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()
    schema_version = f"{contract}.v{version}"
    workout_rows: list[tuple[Any, ...]] = []
    workout_dates: list[str] = []
    for index, workout in enumerate(workouts):
        prefix = f"workouts[{index}]"
        iso_date = _required_iso_date(
            getattr(workout, "iso_date", None), f"{prefix}.iso_date"
        )
        workout_name = _required_text(
            getattr(workout, "workout", None), f"{prefix}.workout"
        )
        notes = _optional_text(getattr(workout, "notes", None), f"{prefix}.notes")
        sport = _required_text(getattr(workout, "sport", None), f"{prefix}.sport")
        phase = _optional_text(getattr(workout, "phase", None), f"{prefix}.phase")
        intensity = _optional_text(
            getattr(workout, "intensity", None), f"{prefix}.intensity"
        )
        flags_value = getattr(workout, "flags", ()) or ()
        if isinstance(flags_value, (str, bytes)) or not isinstance(
            flags_value, Sequence
        ):
            raise InvalidImportedPlanError(f"{prefix}.flags must be a sequence")
        flags = [
            _required_text(flag, f"{prefix}.flags[{flag_index}]")
            for flag_index, flag in enumerate(flags_value)
        ]
        duration_minutes = _optional_nonnegative_number(
            getattr(workout, "duration_minutes", None),
            f"{prefix}.duration_minutes",
        )
        distance_km = _optional_nonnegative_number(
            getattr(workout, "distance_km", None), f"{prefix}.distance_km"
        )
        tss = _optional_nonnegative_number(
            getattr(workout, "tss", None), f"{prefix}.tss"
        )
        structure_json = _canonical_json(
            {
                "schema_version": schema_version,
                "source": IMPORTED_PLAN_SOURCE,
                "request_id": request_id,
                "response_content_sha256": content_sha256,
                "workout": {
                    "date": iso_date,
                    "sport": sport,
                    "phase": phase,
                    "workout": workout_name,
                    "notes": notes,
                    "flags": flags,
                    "intensity": intensity,
                    "duration_minutes": duration_minutes,
                    "distance_km": distance_km,
                    "tss": tss,
                },
            }
        )
        workout_dates.append(iso_date)
        workout_rows.append(
            (
                iso_date,
                workout_name,
                notes,
                distance_km * 1000.0 if distance_km is not None else None,
                duration_minutes * 60.0 if duration_minutes is not None else None,
                tss,
                structure_json,
            )
        )

    replace_dates = day_plan_dates or workout_dates
    replace_start = min(replace_dates)
    replace_end = max(replace_dates)
    outside_window = [
        workout_date
        for workout_date in workout_dates
        if not replace_start <= workout_date <= replace_end
    ]
    if outside_window:
        raise InvalidImportedPlanError(
            "all imported workouts must fall inside the day-plan replacement window"
        )

    inputs = getattr(plan, "inputs", None)
    event = getattr(inputs, "event", None)
    plan_name = str(getattr(event, "event_name", "") or "Imported training plan")
    warnings_value = getattr(plan, "warnings", ()) or ()
    if isinstance(warnings_value, (str, bytes)) or not isinstance(
        warnings_value, Sequence
    ):
        raise InvalidImportedPlanError("warnings must be a sequence")
    warnings = [
        _required_text(warning, f"warnings[{index}]")
        for index, warning in enumerate(warnings_value)
    ]
    return {
        "request_id": request_id,
        "expected_active_plan_sha256": expected_hash,
        "contract": contract,
        "version": version,
        "schema_version": schema_version,
        "plan_name": plan_name,
        "canonical_payload": canonical_payload,
        "content_sha256": content_sha256,
        "replace_start_date": replace_start,
        "replace_end_date": replace_end,
        "workout_rows": workout_rows,
        "warnings": warnings,
    }


def _canonical_import_payload(plan: Any) -> str:
    payload = getattr(plan, "canonical_payload", None)
    if callable(payload):
        payload = payload()
    if payload is None:
        to_dict = getattr(plan, "to_dict", None)
        if not callable(to_dict):
            raise InvalidImportedPlanError(
                "imported plan must expose canonical_payload or to_dict()"
            )
        payload = to_dict()

    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise InvalidImportedPlanError(
                "canonical_payload must contain valid JSON"
            ) from exc
    if not isinstance(payload, Mapping):
        raise InvalidImportedPlanError("the imported plan payload must be an object")
    try:
        return _canonical_json(payload)
    except (TypeError, ValueError) as exc:
        raise InvalidImportedPlanError(
            "the imported plan payload is not canonical JSON data"
        ) from exc


def _build_imported_legacy_blob(
    plan: Any,
    provenance: Mapping[str, Any],
    *,
    previous_generated_plan: Any = None,
    replace_start_date: str,
    replace_end_date: str,
) -> dict[str, Any]:
    analysis_data = _json_compatible(getattr(plan, "analysis", None))
    if not isinstance(analysis_data, dict):
        analysis_data = {"notes": str(analysis_data or "")}

    nutrition_guidance = _guidance_list(
        getattr(plan, "nutrition_guidance", ()), "nutrition_guidance"
    )
    nutrition_targets = _json_compatible(
        list(getattr(plan, "nutrition_targets", ()) or ())
    )
    strength_guidance = _guidance_list(
        getattr(plan, "strength_guidance", ()), "strength_guidance"
    )
    warnings = _guidance_list(getattr(plan, "warnings", ()), "warnings")
    rationale = str(getattr(plan, "rationale", "") or "")
    analysis_data.update(
        {
            "nutrition_guidance": nutrition_guidance,
            "nutrition_targets": nutrition_targets,
            "strength_guidance": strength_guidance,
            "rationale": rationale,
            "warnings": warnings,
            "provenance": dict(provenance),
        }
    )
    return {
        "day_plans": _merge_cached_day_plans(
            previous_generated_plan,
            getattr(plan, "day_plans", ()) or (),
            replace_start_date=replace_start_date,
            replace_end_date=replace_end_date,
        ),
        "weekly_rows": _json_compatible(
            list(getattr(plan, "weekly_rows", ()) or ())
        ),
        "analysis": analysis_data,
        "inputs": _legacy_inputs(getattr(plan, "inputs", None)),
        "nutrition_guidance": nutrition_guidance,
        "nutrition_targets": nutrition_targets,
        "strength_guidance": strength_guidance,
        "rationale": rationale,
        "warnings": warnings,
        "provenance": dict(provenance),
        "generated_at": provenance["applied_at"],
    }


def _merge_cached_day_plans(
    previous_generated_plan: Any,
    imported_day_plans: Sequence[Any],
    *,
    replace_start_date: str,
    replace_end_date: str,
) -> list[dict[str, Any]]:
    """Merge imported days into the cached plan without erasing other dates."""
    merged_by_date: dict[str, dict[str, Any]] = {}
    previous_days: Any = ()
    if isinstance(previous_generated_plan, Mapping):
        previous_days = previous_generated_plan.get("day_plans", ())
    if isinstance(previous_days, Sequence) and not isinstance(
        previous_days, (str, bytes)
    ):
        for cached_day in previous_days:
            compatible_day = _json_compatible(cached_day)
            if not isinstance(compatible_day, dict):
                continue
            iso_date = compatible_day.get("iso_date")
            if not _is_canonical_iso_date(iso_date):
                continue
            if replace_start_date <= iso_date <= replace_end_date:
                continue
            merged_by_date[iso_date] = compatible_day

    for index, imported_day in enumerate(imported_day_plans):
        compatible_day = _json_compatible(imported_day)
        if not isinstance(compatible_day, dict):
            raise InvalidImportedPlanError(
                f"day_plans[{index}] must be a JSON object"
            )
        iso_date = _required_iso_date(
            compatible_day.get("iso_date"), f"day_plans[{index}].iso_date"
        )
        merged_by_date[iso_date] = compatible_day

    return [merged_by_date[iso_date] for iso_date in sorted(merged_by_date)]


def _is_canonical_iso_date(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _legacy_inputs(inputs: Any) -> dict[str, Any]:
    """Serialize imported inputs using the keys consumed by Plan Review."""
    athlete = getattr(inputs, "athlete", None)
    event = getattr(inputs, "event", None)
    return {
        "athlete": {
            "athlete_name": getattr(athlete, "athlete_name", ""),
            "age": getattr(athlete, "age", None),
            "hrmax": getattr(athlete, "hrmax", None),
            "lthr": getattr(athlete, "lthr", None),
            "sodium": getattr(athlete, "sodium_mg_per_hr_hot", None),
        },
        "event": {
            "event_name": getattr(event, "event_name", ""),
            "event_date": getattr(event, "event_date", ""),
            "distance": getattr(event, "distance", ""),
            "start_date": getattr(event, "start_date", ""),
            "run_days_per_week": getattr(event, "run_days_per_week", None),
        },
    }


def _decode_last_generated_plan(value: Any) -> Any:
    decoded = value
    for _ in range(2):
        if not isinstance(decoded, str):
            break
        try:
            decoded = json.loads(decoded)
        except (TypeError, json.JSONDecodeError):
            break
    return decoded


def _guidance_list(value: Any, field: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise InvalidImportedPlanError(f"{field} must be a sequence")
    return [_required_text(item, f"{field}[{index}]") for index, item in enumerate(value)]


def _required_sha256(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise InvalidImportedPlanError(f"{field} must be a lowercase SHA-256 value")
    return value


def _required_iso_date(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise InvalidImportedPlanError(f"{field} must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise InvalidImportedPlanError(f"{field} must be an ISO date") from exc
    if parsed.isoformat() != value:
        raise InvalidImportedPlanError(f"{field} must use YYYY-MM-DD format")
    return value


def _required_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InvalidImportedPlanError(f"{field} must be a non-empty string")
    return value


def _optional_text(value: Any, field: str) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise InvalidImportedPlanError(f"{field} must be a string")
    return value


def _optional_nonnegative_number(value: Any, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, Real):
        raise InvalidImportedPlanError(f"{field} must be a number or null")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise InvalidImportedPlanError(f"{field} must be a finite non-negative number")
    return parsed


def _finite_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _canonical_json(value: Any) -> str:
    return json.dumps(
        _json_compatible(value),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _json_compatible(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _json_compatible(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_compatible(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_compatible(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, Integral):
        return int(value)
    if isinstance(value, Real):
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("non-finite numbers are not valid JSON")
        return parsed
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def save_generated_plan(
    db_path: Path,
    inputs: Any,
    analysis: Any,
    day_plans: list[Any],
    weekly_rows: list[dict[str, Any]],
    *,
    expected_active_plan_sha256: str | None = None,
) -> None:
    """Atomically persist a generated baseline after an optional stale check."""
    if (
        expected_active_plan_sha256 is not None
        and _SHA256_RE.fullmatch(expected_active_plan_sha256) is None
    ):
        raise ValueError("expected active-plan SHA-256 is invalid")
    day_plans_data = [
        {
            "iso_date": dp.iso_date,
            "day": dp.day,
            "week": dp.week,
            "phase": dp.phase,
            "flags": dp.flags,
            "workout": dp.workout,
            "notes": dp.notes,
            "sport": getattr(dp, "sport", None),
            "intensity": getattr(dp, "intensity", None),
            "session_count": getattr(dp, "session_count", None),
        }
        for dp in day_plans
    ]

    generated_at = _utc_now_iso()
    analysis_data = {
        "hrmax_observed": analysis.hrmax_observed,
        "hrmax_robust": analysis.hrmax_robust,
        "lthr_suggested": analysis.lthr_suggested,
        "avg_weekly_hours": analysis.avg_weekly_hours,
        "avg_weekly_miles": analysis.avg_weekly_miles,
        "z2_fraction": analysis.z2_fraction,
        "notes": analysis.notes,
        "provenance": {
            "source": "rule_based_baseline",
            "generated_at": generated_at,
        },
    }

    inputs_data = {
        "athlete": {
            "athlete_name": inputs.athlete.athlete_name,
            "age": inputs.athlete.age,
            "hrmax": inputs.athlete.hrmax,
            "lthr": inputs.athlete.lthr,
            "sodium": inputs.athlete.sodium_mg_per_hr_hot,
        },
        "event": {
            "event_name": inputs.event.event_name,
            "event_date": inputs.event.event_date,
            "distance": inputs.event.distance,
        },
    }

    plan_blob = json.dumps(
        {
            "day_plans": day_plans_data,
            "weekly_rows": weekly_rows,
            "analysis": analysis_data,
            "inputs": inputs_data,
            "provenance": {
                "source": "rule_based_baseline",
                "generated_at": generated_at,
            },
            "generated_at": generated_at,
        }
    )

    rows_to_insert: list[tuple[Any, ...]] = []
    for dp in day_plans:
        if not dp.workout:
            continue
        text_to_parse = (dp.workout + " " + (dp.notes or "")).lower()
        planned_dist, planned_dur = _parse_planned_workout_metrics(text_to_parse)
        structure_json = json.dumps(
            {
                "source": "rule_based_baseline",
                "workout": {
                    "sport": getattr(dp, "sport", None),
                    "phase": dp.phase,
                    "workout": dp.workout,
                    "intensity": getattr(dp, "intensity", None),
                    "duration_minutes": (
                        planned_dur / 60.0 if planned_dur is not None else None
                    ),
                    "distance_km": (
                        planned_dist / 1000.0 if planned_dist is not None else None
                    ),
                    "tss": None,
                    "flags": [
                        flag.strip()
                        for flag in str(dp.flags or "").split(",")
                        if flag.strip()
                    ],
                    "notes": dp.notes,
                },
            },
            ensure_ascii=False,
        )
        rows_to_insert.append(
            (
                dp.iso_date,
                dp.workout,
                dp.notes,
                planned_dist,
                planned_dur,
                None,
                structure_json,
            )
        )

    conn = connect_sqlite(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        plan_dates = [dp.iso_date for dp in day_plans]
        if plan_dates:
            assert_legacy_write_allowed(conn, min(plan_dates), max(plan_dates))
        if expected_active_plan_sha256 is not None:
            current_sha256 = active_plan_sha256(conn)
            if current_sha256 != expected_active_plan_sha256:
                raise StalePlanWriteError(
                    expected_sha256=expected_active_plan_sha256,
                    current_sha256=current_sha256,
                )
        conn.execute(
            """
            INSERT OR REPLACE INTO app_settings(key, value)
            VALUES ('last_generated_plan', ?)
            """,
            (json.dumps(plan_blob),),
        )
        plan_dates = [dp.iso_date for dp in day_plans]
        if plan_dates:
            conn.execute(
                """
                DELETE FROM planned_workout
                WHERE scheduled_date >= ? AND scheduled_date <= ?
                """,
                (min(plan_dates), max(plan_dates)),
            )
        conn.executemany(
            """
            INSERT INTO planned_workout(
                scheduled_date, workout_name, description,
                planned_distance_m, planned_duration_s, planned_tss,
                structure_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows_to_insert,
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def _parse_planned_workout_metrics(text: str) -> tuple[float | None, float | None]:
    """Extract distance in metres and duration in seconds from workout text.

    Unit names require a word boundary so duration strings such as ``60 min``
    cannot be interpreted as ``60 mi``. Both ASCII and typographic range dashes
    are supported because generated workout descriptions use both forms.
    """
    planned_dist: float | None = None
    planned_dur: float | None = None
    normalized = str(text or "").lower()

    try:
        hour_match = re.search(
            r"(\d+(?:\.\d+)?)\s*(?:hours?|hrs?)\b", normalized
        )
        min_range_match = re.search(
            r"(\d+(?:\.\d+)?)\s*[-–—]\s*(\d+(?:\.\d+)?)\s*"
            r"(?:minutes?|mins?)\b",
            normalized,
        )
        min_match = re.search(
            r"(\d+(?:\.\d+)?)\s*(?:minutes?|mins?)\b", normalized
        )

        if hour_match:
            planned_dur = float(hour_match.group(1)) * 3600
        elif min_range_match:
            avg_mins = (
                float(min_range_match.group(1)) + float(min_range_match.group(2))
            ) / 2
            planned_dur = avg_mins * 60
        elif min_match:
            planned_dur = float(min_match.group(1)) * 60

        mile_match = re.search(
            r"(\d+(?:\.\d+)?)\s*(?:mi|miles?)\b", normalized
        )
        km_match = re.search(
            r"(\d+(?:\.\d+)?)\s*(?:km|kilomet(?:er|re)s?)\b", normalized
        )

        if mile_match:
            planned_dist = float(mile_match.group(1)) * 1609.34
        elif km_match:
            planned_dist = float(km_match.group(1)) * 1000
    except (TypeError, ValueError):
        logger.warning("Could not parse workout metrics from %r", text)

    return planned_dist, planned_dur


def load_generated_plan(db_path: Path):
    """Load the most recently generated plan from the database."""
    conn = connect_sqlite(db_path)
    try:
        blob = db_queries.get_setting(conn, "last_generated_plan", "")
    finally:
        conn.close()

    if not blob:
        return None, None, None, None

    try:
        # Legacy generated plans were stored as a JSON string inside the JSON
        # app-setting value. Imported plans use the setting's native JSON object
        # representation. Accept both so Plan Review can reload either source.
        data = blob if isinstance(blob, dict) else json.loads(blob)
        return (
            data.get("inputs"),
            data.get("analysis"),
            data.get("day_plans"),
            data.get("weekly_rows"),
        )
    except (AttributeError, TypeError, json.JSONDecodeError):
        logger.exception("Failed to decode persisted generated plan from %s", db_path)
        return None, None, None, None


def load_plan_settings(db_path: Path) -> dict[str, Any]:
    """Load persisted Build Plan UI settings in one round-trip."""
    conn = connect_sqlite(db_path)
    try:
        athlete_name = db_queries.get_setting(conn, "plan_athlete_name", "Runner")
        distance = db_queries.get_setting(conn, "plan_distance", "50K")
        today_iso = datetime.now(timezone.utc).date().isoformat()
        return {
            "plan_athlete_name": athlete_name,
            "plan_age": db_queries.get_setting(conn, "plan_age", 50),
            "plan_run_days": db_queries.get_setting(conn, "plan_run_days", 5),
            "plan_sodium": db_queries.get_setting(conn, "plan_sodium", 900),
            "plan_distance": distance,
            "plan_event_name": db_queries.get_setting(
                conn, "plan_event_name", f"{distance} Training Plan"
            ),
            "plan_long_run_day": db_queries.get_setting(
                conn, "plan_long_run_day", "Saturday"
            ),
            "plan_event_date": db_queries.get_setting(
                conn, "plan_event_date", today_iso
            ),
            "plan_start_date": db_queries.get_setting(
                conn, "plan_start_date", today_iso
            ),
            "plan_training_method": db_queries.get_setting(
                conn, "plan_training_method", "eighty_twenty"
            ),
            "plan_out_dir": db_queries.get_setting(
                conn, "plan_out_dir", str(Path.home() / "Documents")
            ),
            "plan_out_name": db_queries.get_setting(
                conn, "plan_out_name", f"{athlete_name}_master_workbook.xlsx"
            ),
        }
    finally:
        conn.close()


def load_chatgpt_exchange_settings(db_path: Path) -> dict[str, Any]:
    """Load the persisted Codex workspace preferences."""
    defaults: dict[str, Any] = {
        "chatgpt_exchange_injuries": "",
        "chatgpt_exchange_schedule": "",
        "chatgpt_exchange_equipment": "",
        "chatgpt_exchange_strength_experience": "",
        "chatgpt_exchange_diet": "",
        "chatgpt_exchange_allergies": "",
        "chatgpt_exchange_gi": "",
        "chatgpt_exchange_lookback_weeks": 12,
    }
    conn = connect_sqlite(db_path)
    try:
        return {
            key: db_queries.get_setting(conn, key, default)
            for key, default in defaults.items()
        }
    finally:
        conn.close()


def save_plan_setting(db_path: Path, key: str, value: Any) -> None:
    """Persist a single Build Plan UI setting."""
    conn = connect_sqlite(db_path)
    try:
        db_queries.set_setting(conn, key, value)
    finally:
        conn.close()
