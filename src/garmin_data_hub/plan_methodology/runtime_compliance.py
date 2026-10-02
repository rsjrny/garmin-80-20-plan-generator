"""End-to-end native compliance resolution through an explicit confirmed match."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from garmin_data_hub.db.queries import load_runtime_activity_evidence
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.services.thresholds import RUNNING_SPORTS

from .activity_workout_match import MatchPersistenceError, load_confirmed_match
from .domain import DomainError, MethodologyId, Metric, Sport
from .fitzgerald_compliance import evaluate as evaluate_fitzgerald
from .maffetone_compliance import evaluate as evaluate_maffetone
from .maffetone_compliance import evaluate_test_observation
from .revision_repository import load_revision


def _fitzgerald_threshold(parameters: Mapping[str, Any], metric: Metric) -> Any:
    if metric is Metric.SPEED:
        direct = parameters.get("threshold_speed_mps")
        nested = parameters.get("pace")
    else:
        direct = parameters.get("lthr_bpm") or parameters.get("threshold_lthr_bpm")
        nested = parameters.get("lthr")
    if direct is not None:
        return direct
    return nested.get("value") if isinstance(nested, Mapping) else None


def evaluate_confirmed_match(
    db_path: Path | str,
    *,
    revision_id: str | None = None,
    workout_id: str | None = None,
    activity_id: int | None = None,
) -> dict[str, Any]:
    """Resolve immutable plan facts and targeted actual evidence, then dispatch."""
    path = Path(db_path)
    conn = connect_sqlite(path)
    try:
        match = load_confirmed_match(
            conn,
            revision_id=revision_id,
            workout_id=workout_id,
            activity_id=activity_id,
        )
        if match is None:
            raise MatchPersistenceError("no confirmed activity/workout match exists")
        activity = load_runtime_activity_evidence(conn, match.activity_id)
    finally:
        conn.close()
    if activity is None:
        raise MatchPersistenceError("matched upstream activity is missing")

    revision = load_revision(path, match.revision_id)
    if revision is None:
        raise MatchPersistenceError("matched revision is missing")
    workout = next(
        (item for item in revision.candidate.workouts if item.workout_id == match.workout_id),
        None,
    )
    if workout is None:
        raise MatchPersistenceError("matched immutable workout is missing")
    if workout.sport is not Sport.RUNNING:
        return {"activity_workout_match_id":match.activity_workout_match_id,
                "revision_id":match.revision_id,"workout_id":match.workout_id,
                "activity_id":match.activity_id,"sport":workout.sport.value,
                "status":"NOT_APPLICABLE","reason":"AUXILIARY_SESSION",
                "excluded_from_running_distribution":True}
    if activity.activity_type is None or activity.activity_type.lower() not in RUNNING_SPORTS:
        raise DomainError("named running methodology cannot evaluate a non-running activity")
    prescriptions = [segment.prescription for segment in workout.segments if segment.prescription is not None]
    if not prescriptions:
        raise DomainError("matched workout has no runtime prescription")
    methodology = revision.candidate.manifest.get("methodology_id")
    if any(item.methodology_id.value != methodology for item in prescriptions):
        raise DomainError("revision and prescription methodology namespaces differ")

    envelope = {
        "activity_workout_match_id": match.activity_workout_match_id,
        "revision_id": match.revision_id,
        "workout_id": match.workout_id,
        "activity_id": match.activity_id,
        "local_activity_date": activity.local_calendar_date,
        "sport": activity.activity_type,
        "phase": workout.phase,
        "elapsed_duration_seconds": activity.elapsed_duration_seconds,
        "moving_duration_seconds": activity.moving_duration_seconds,
    }
    if methodology == MethodologyId.FITZGERALD_80_20_RUNNING_V1.value:
        primary = prescriptions[0]
        if any(item.primary.metric is not primary.primary.metric for item in prescriptions):
            raise DomainError("mixed Fitzgerald primary metrics require explicit segment alignment")
        parameters = revision.candidate.parameter_snapshot
        threshold = _fitzgerald_threshold(parameters, primary.primary.metric)
        secondary_threshold = (
            None
            if primary.secondary is None
            else _fitzgerald_threshold(parameters, primary.secondary.metric)
        )
        targets = {item.native_target.value for item in prescriptions}
        result = evaluate_fitzgerald(
            prescription=primary,
            samples=activity.samples,
            threshold=threshold,
            secondary_threshold=secondary_threshold,
            target_alignment_available=len(targets) == 1,
        )
    elif methodology == MethodologyId.MAFFETONE_RUNNING_V1.value:
        parameters = revision.candidate.parameter_snapshot
        primary = prescriptions[0]
        result = evaluate_maffetone(
            ceiling_bpm=parameters.get("ceiling_bpm"),
            lower_bpm=parameters.get("lower_bpm"),
            samples=activity.samples,
            native_target=primary.native_target.value,
            methodology_state=parameters.get("state"),
            event_context=(workout.metadata or {}).get("event_exception"),
        )
        metadata = workout.metadata or {}
        if metadata.get("benchmark_identity") == "MAF_TEST":
            warmup = sum(
                segment.duration_seconds or 0
                for segment in workout.segments
                if segment.kind.value == "WARMUP"
            )
            cooldown = sum(
                segment.duration_seconds or 0
                for segment in workout.segments
                if segment.kind.value == "COOLDOWN"
            )
            segments = [
                {"kind": segment.kind.value, "duration_seconds": segment.duration_seconds}
                for segment in workout.segments
            ]
            result["maf_test_observation"] = evaluate_test_observation(
                samples=activity.samples,
                benchmark_identity="MAF_TEST",
                ceiling_bpm=parameters.get("ceiling_bpm"),
                lower_bpm=parameters.get("lower_bpm"),
                protocol_version=metadata.get("protocol_version", ""),
                warmup_seconds=warmup,
                measurement_seconds=metadata.get("measurement_seconds"),
                cooldown_seconds=cooldown,
                conditions=metadata.get("conditions", {}),
                test_date=activity.local_calendar_date or str(workout.scheduled_date),
                segments=segments,
                course_key=metadata.get("course_key"),
            )
    else:
        raise DomainError("legacy or unknown methodologies have no native runtime evaluator")
    return {**envelope, **result}
