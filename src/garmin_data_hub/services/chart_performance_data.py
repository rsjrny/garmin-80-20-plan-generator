"""Read-only temporal support and original workout context for advanced Charts."""
from collections import defaultdict
from pathlib import Path
import sqlite3

from garmin_data_hub.analytics.temporal_metrics import TemporalSample, build_temporal_evidence
from garmin_data_hub.db.queries import get_athlete_metrics
from garmin_data_hub.plan_methodology.activity_workout_match import load_confirmed_match
from garmin_data_hub.plan_methodology.revision_repository import load_revision, CorruptRevisionError


def summarize_samples(samples):
    """Retain exact weighted signal bands without retaining every trackpoint."""
    evidence = build_temporal_evidence(samples)
    intervals = evidence.intervals
    paired = defaultdict(float)
    power_seconds, power_hr_seconds = 0.0, 0.0
    power_run, longest_power_run, last_power_end = 0.0, 0.0, None
    for interval in intervals:
        if interval.power_w is not None:
            power_seconds += interval.duration_s
            power_run = power_run + interval.duration_s if last_power_end == interval.start_s else interval.duration_s
            last_power_end = interval.end_s
            longest_power_run = max(longest_power_run,power_run)
            if interval.heart_rate_bpm is not None:
                power_hr_seconds += interval.duration_s
        else:
            power_run, last_power_end = 0.0, None
        speed, hr = interval.speed_mps, interval.heart_rate_bpm
        if speed is not None and .5 <= speed <= 12 and hr is not None:
            paired[speed, hr] += interval.duration_s
    records = [(speed,hr,seconds) for (speed,hr),seconds in paired.items()]
    seconds = sum(row[2] for row in records)
    mean_speed = sum(speed*dt for speed,hr,dt in records)/seconds if seconds else None
    mean_hr = sum(hr*dt for speed,hr,dt in records)/seconds if seconds else None
    variance = sum(dt*(speed-mean_speed)**2 for speed,hr,dt in records)/seconds if seconds else None
    speed_cv = variance**.5/mean_speed if seconds and mean_speed else None
    midpoint = (intervals[0].start_s+intervals[-1].end_s)/2 if intervals else None
    half_seconds = [0.0,0.0]
    if midpoint is not None:
        for interval in intervals:
            if interval.speed_mps is None or not .5 <= interval.speed_mps <= 12 or interval.heart_rate_bpm is None:
                continue
            half_seconds[0] += max(0,min(interval.end_s,midpoint)-interval.start_s)
            half_seconds[1] += max(0,interval.end_s-max(interval.start_s,midpoint))
    span = intervals[-1].end_s-intervals[0].start_s if intervals else 0
    observed_halves = [0.0,0.0]
    if evidence.observed_window_seconds:
        observed_midpoint = (evidence.observed_start_seconds+evidence.observed_end_seconds)/2
        for interval in intervals:
            if interval.speed_mps is None or not .5 <= interval.speed_mps <= 12 or interval.heart_rate_bpm is None:
                continue
            observed_halves[0] += max(0,min(interval.end_s,observed_midpoint)-interval.start_s)
            observed_halves[1] += max(0,interval.end_s-max(interval.start_s,observed_midpoint))
    return dict(observed_s=evidence.observed_window_seconds, supported_s=evidence.temporal_supported_seconds,
        paired_s=seconds, mean_speed=mean_speed, mean_hr=mean_hr, speed_cv=speed_cv,
        power_s=power_seconds, power_hr_s=power_hr_seconds, longest_power_run_s=longest_power_run,
        half_coverage=min(half_seconds)/(span/2) if span else 0,
        min_observed_half_paired_s=min(observed_halves),
        bands=records)


def advanced_chart_evidence(db_path, activity_ids):
    """Stream one activity at a time; store only qualification summaries/bands.

    A read transaction keeps thresholds, tracks and original matched prescriptions
    consistent. It never refreshes metrics, confirms matches or changes a schedule.
    """
    ids = sorted({int(aid) for aid in activity_ids})
    conn = sqlite3.connect(Path(db_path).resolve().as_uri()+"?mode=ro",uri=True)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("BEGIN")
        thresholds = get_athlete_metrics(conn)
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        track_columns = {r[1] for r in conn.execute("PRAGMA table_info(activity_trackpoints)")}
        needed = {"activity_id","seq","timestamp_utc","speed_mps","heart_rate_bpm","power_w"}
        summaries, contexts, revisions = {}, {}, {}
        for offset in range(0,len(ids),500):
            chunk = ids[offset:offset+500]
            if needed <= track_columns:
                cursor = conn.execute(f"SELECT activity_id,seq,timestamp_utc,speed_mps,heart_rate_bpm,power_w FROM activity_trackpoints WHERE activity_id IN ({','.join('?' for _ in chunk)}) ORDER BY activity_id,seq",chunk)
                samples, current_id = [], None
                for row in cursor:
                    aid = row["activity_id"]
                    if aid != current_id:
                        if current_id is not None:
                            summaries[current_id] = summarize_samples(samples)
                        current_id, samples = aid, []
                    samples.append(TemporalSample(row["timestamp_utc"],row["speed_mps"],row["heart_rate_bpm"],row["power_w"],row["seq"]))
                if current_id is not None:
                    summaries[current_id] = summarize_samples(samples)
            if "activity_workout_match" in tables:
                for aid in chunk:
                    match = load_confirmed_match(conn,activity_id=aid)
                    if match is None:
                        continue
                    if match.revision_id not in revisions:
                        revisions[match.revision_id] = load_revision(None,match.revision_id,_connection=conn)
                    revision = revisions[match.revision_id]
                    workout = next((w for w in revision.candidate.workouts if w.workout_id == match.workout_id),None) if revision else None
                    if workout is None:
                        raise CorruptRevisionError("Confirmed workout context is missing")
                    contexts[aid] = dict(family=workout.family, hard=workout.quality_flag or workout.event_flag,
                                         long_run=workout.long_run_flag, sport=workout.sport.value)
        return dict(thresholds=thresholds, summaries=summaries, contexts=contexts,
                    tracks_available=needed <= track_columns)
    finally:
        conn.close()
