"""Elapsed-time metrics derived from trackpoint observations.

Trackpoints are interpreted as left-held observations: the values recorded at
``t`` apply until the next distinct timestamp.  No support is inferred after
the final observation, and gaps longer than ``MAX_CONTIGUOUS_GAP_SECONDS`` do
not produce an interval.
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import chain
import math
from typing import Iterable


MAX_CONTIGUOUS_GAP_SECONDS = 30.0
POWER_PEAK_DURATIONS_SECONDS = (5, 30, 60, 300, 1200)
POWER_COVERAGE_THRESHOLD = 0.95
HR_MIN_BPM = 35.0
HR_MAX_BPM = 220.0
_TIME_EPSILON_SECONDS = 1e-9


@dataclass(frozen=True)
class TemporalSample:
    """One timestamped trackpoint observation used by temporal metrics."""

    timestamp_utc: str | datetime | float | None
    speed_mps: float | None = None
    heart_rate_bpm: float | None = None
    power_w: float | None = None
    seq: int = 0


@dataclass(frozen=True)
class TemporalInterval:
    """A supported left-held interval between consecutive observations."""

    start_s: float
    end_s: float
    speed_mps: float | None
    heart_rate_bpm: float | None
    power_w: float | None

    @property
    def duration_s(self) -> float:
        return self.end_s - self.start_s


@dataclass(frozen=True)
class TemporalMetricResult:
    """All elapsed-time metrics produced from one activity's trackpoints."""

    power_peaks_w: dict[int, float | None]
    aerobic_decoupling_pct: float | None
    pace_decoupling_pct: float | None
    hr_drift_pct: float | None
    aerobic_workload_signal: str
    paired_power_coverage: float | None


@dataclass(frozen=True)
class TemporalEvidence:
    """Normalized observations and exact support supplied by the temporal engine.

    This is deliberately metric-neutral. Methodology evaluators decide which
    signal is authoritative, while this value remains the single owner of
    timestamp parsing, duplicate handling, the gap rule, and final-sample
    behavior.
    """

    observations_received: int
    parseable_observations: int
    observed_start_seconds: float | None
    observed_end_seconds: float | None
    observed_window_seconds: float | None
    temporal_supported_seconds: float
    temporal_unsupported_seconds: float | None
    intervals: tuple[TemporalInterval, ...]


def build_temporal_intervals(
    samples: Iterable[TemporalSample],
) -> list[TemporalInterval]:
    """Return deterministic, gap-limited intervals sorted by actual time.

    Samples with unparseable timestamps are ignored.  For duplicate timestamps,
    the highest-sequence (then latest-input) observation is retained.  Sorting
    by time means an out-of-order ``seq`` cannot create negative duration.
    """

    _received, distinct = _normalize_temporal_samples(samples)
    return _build_intervals(distinct)


def _build_intervals(
    distinct: list[tuple[float, TemporalSample]],
) -> list[TemporalInterval]:
    intervals: list[TemporalInterval] = []
    for index in range(len(distinct) - 1):
        start_s, sample = distinct[index]
        end_s = distinct[index + 1][0]
        duration_s = end_s - start_s
        if duration_s <= _TIME_EPSILON_SECONDS:
            continue
        if duration_s > MAX_CONTIGUOUS_GAP_SECONDS:
            continue
        intervals.append(
            TemporalInterval(
                start_s=start_s,
                end_s=end_s,
                speed_mps=_nonnegative_or_none(sample.speed_mps),
                heart_rate_bpm=_hr_or_none(sample.heart_rate_bpm),
                power_w=_nonnegative_or_none(sample.power_w),
            )
        )
    return intervals


def build_temporal_evidence(
    samples: Iterable[TemporalSample],
) -> TemporalEvidence:
    """Return interval support plus known unsupported time for one sample stream."""
    materialized = tuple(samples)
    received, distinct = _normalize_temporal_samples(materialized)
    intervals = tuple(_build_intervals(distinct))
    observed_window = (
        distinct[-1][0] - distinct[0][0] if len(distinct) >= 2 else None
    )
    supported = sum(interval.duration_s for interval in intervals)
    unsupported = (
        max(0.0, observed_window - supported)
        if observed_window is not None
        else None
    )
    return TemporalEvidence(
        observations_received=received,
        parseable_observations=len(distinct),
        observed_start_seconds=None if not distinct else distinct[0][0],
        observed_end_seconds=None if not distinct else distinct[-1][0],
        observed_window_seconds=observed_window,
        temporal_supported_seconds=supported,
        temporal_unsupported_seconds=unsupported,
        intervals=intervals,
    )


def _normalize_temporal_samples(
    samples: Iterable[TemporalSample],
) -> tuple[int, list[tuple[float, TemporalSample]]]:
    parsed: list[tuple[float, int, int, TemporalSample]] = []
    received = 0
    for input_order, sample in enumerate(samples):
        received += 1
        timestamp_s = _timestamp_seconds(sample.timestamp_utc)
        if timestamp_s is None:
            continue
        try:
            sequence = int(sample.seq)
        except (TypeError, ValueError, OverflowError):
            sequence = input_order
        parsed.append((timestamp_s, sequence, input_order, sample))

    parsed.sort(key=lambda item: (item[0], item[1], item[2]))
    distinct: list[tuple[float, TemporalSample]] = []
    for timestamp_s, _sequence, _input_order, sample in parsed:
        if distinct and abs(timestamp_s - distinct[-1][0]) <= _TIME_EPSILON_SECONDS:
            distinct[-1] = (timestamp_s, sample)
        else:
            distinct.append((timestamp_s, sample))

    return received, distinct


def calculate_temporal_metrics(
    samples: Iterable[TemporalSample],
) -> TemporalMetricResult:
    """Calculate canonical elapsed-time power, decoupling, and HR-drift metrics."""

    intervals = build_temporal_intervals(samples)
    peaks = calculate_power_peaks(intervals)
    midpoint_s = _observed_midpoint(intervals)

    hr_supported_s = sum(
        interval.duration_s
        for interval in intervals
        if interval.heart_rate_bpm is not None
    )
    paired_power_s = sum(
        interval.duration_s
        for interval in intervals
        if interval.heart_rate_bpm is not None and interval.power_w is not None
    )
    coverage = (
        paired_power_s / hr_supported_s if hr_supported_s > 0.0 else None
    )
    workload_signal = (
        "power"
        if coverage is not None
        and coverage + _TIME_EPSILON_SECONDS >= POWER_COVERAGE_THRESHOLD
        else "speed"
    )

    return TemporalMetricResult(
        power_peaks_w=peaks,
        aerobic_decoupling_pct=_decoupling_pct(
            intervals, midpoint_s, workload_signal
        ),
        pace_decoupling_pct=_decoupling_pct(intervals, midpoint_s, "speed"),
        hr_drift_pct=_hr_drift_pct(intervals, midpoint_s),
        aerobic_workload_signal=workload_signal,
        paired_power_coverage=coverage,
    )


def calculate_power_peaks(
    intervals: Iterable[TemporalInterval],
    durations_s: Iterable[int] = POWER_PEAK_DURATIONS_SECONDS,
) -> dict[int, float | None]:
    """Return maximum exact-duration time-weighted power means.

    Only contiguous intervals with measured power participate.  A missing-power
    interval or an omitted timestamp gap ends a run, so unsupported time cannot
    be compressed out of a candidate window.
    """

    runs: list[list[TemporalInterval]] = []
    current: list[TemporalInterval] = []
    for interval in intervals:
        if interval.power_w is None:
            if current:
                runs.append(current)
                current = []
            continue
        if current and abs(current[-1].end_s - interval.start_s) > _TIME_EPSILON_SECONDS:
            runs.append(current)
            current = []
        current.append(interval)
    if current:
        runs.append(current)

    prepared_runs = [_prepare_power_run(run) for run in runs]
    results: dict[int, float | None] = {}
    for raw_duration in durations_s:
        duration_s = float(raw_duration)
        best: float | None = None
        if math.isfinite(duration_s) and duration_s > 0.0:
            for starts, ends, powers, cumulative_work in prepared_runs:
                run_start = starts[0]
                run_end = ends[-1]
                if run_end - run_start + _TIME_EPSILON_SECONDS < duration_s:
                    continue

                latest_start = run_end - duration_s
                # A fixed-window integral is linear between points where the
                # window start or end crosses a signal boundary.  Those
                # candidates are interval starts and interval ends minus N.
                candidates = chain(starts, (end - duration_s for end in ends))
                for candidate in candidates:
                    if not (
                        run_start - _TIME_EPSILON_SECONDS
                        <= candidate
                        <= latest_start + _TIME_EPSILON_SECONDS
                    ):
                        continue
                    window_start = min(max(candidate, run_start), latest_start)
                    window_end = window_start + duration_s
                    work = _integrated_work(
                        window_end, starts, ends, powers, cumulative_work
                    ) - _integrated_work(
                        window_start, starts, ends, powers, cumulative_work
                    )
                    mean = work / duration_s
                    if math.isfinite(mean) and (best is None or mean > best):
                        best = mean
        results[int(raw_duration)] = round(best, 6) if best is not None else None
    return results


def _prepare_power_run(
    run: list[TemporalInterval],
) -> tuple[list[float], list[float], list[float], list[float]]:
    starts = [interval.start_s for interval in run]
    ends = [interval.end_s for interval in run]
    powers = [float(interval.power_w) for interval in run if interval.power_w is not None]
    cumulative_work = [0.0]
    for interval, power in zip(run, powers):
        cumulative_work.append(cumulative_work[-1] + interval.duration_s * power)
    return starts, ends, powers, cumulative_work


def _integrated_work(
    time_s: float,
    starts: list[float],
    ends: list[float],
    powers: list[float],
    cumulative_work: list[float],
) -> float:
    if time_s <= starts[0] + _TIME_EPSILON_SECONDS:
        return 0.0
    if time_s >= ends[-1] - _TIME_EPSILON_SECONDS:
        return cumulative_work[-1]
    index = bisect_right(starts, time_s) - 1
    index = max(0, min(index, len(powers) - 1))
    held_duration_s = max(0.0, min(time_s, ends[index]) - starts[index])
    return cumulative_work[index] + held_duration_s * powers[index]


def _observed_midpoint(intervals: list[TemporalInterval]) -> float | None:
    if not intervals:
        return None
    return intervals[0].start_s + (intervals[-1].end_s - intervals[0].start_s) / 2.0


def _decoupling_pct(
    intervals: list[TemporalInterval],
    midpoint_s: float | None,
    workload_signal: str,
) -> float | None:
    if midpoint_s is None:
        return None
    first = _paired_half_totals(intervals, midpoint_s, workload_signal, first=True)
    second = _paired_half_totals(intervals, midpoint_s, workload_signal, first=False)
    if first is None or second is None:
        return None
    first_duration, first_work, first_hr = first
    second_duration, second_work, second_hr = second
    if first_duration <= 0.0 or second_duration <= 0.0:
        return None
    first_work_avg = first_work / first_duration
    second_work_avg = second_work / second_duration
    first_hr_avg = first_hr / first_duration
    second_hr_avg = second_hr / second_duration
    if first_hr_avg <= 0.0 or second_hr_avg <= 0.0 or first_work_avg <= 0.0:
        return None
    first_efficiency = first_work_avg / first_hr_avg
    second_efficiency = second_work_avg / second_hr_avg
    value = ((first_efficiency - second_efficiency) / first_efficiency) * 100.0
    return _rounded_finite(value)


def _paired_half_totals(
    intervals: list[TemporalInterval],
    midpoint_s: float,
    workload_signal: str,
    *,
    first: bool,
) -> tuple[float, float, float] | None:
    duration_s = 0.0
    workload_total = 0.0
    hr_total = 0.0
    for interval in intervals:
        workload = (
            interval.power_w if workload_signal == "power" else interval.speed_mps
        )
        if workload is None or interval.heart_rate_bpm is None:
            continue
        overlap_s = _half_overlap(interval, midpoint_s, first=first)
        if overlap_s <= 0.0:
            continue
        duration_s += overlap_s
        workload_total += workload * overlap_s
        hr_total += interval.heart_rate_bpm * overlap_s
    if duration_s <= 0.0:
        return None
    return duration_s, workload_total, hr_total


def _hr_drift_pct(
    intervals: list[TemporalInterval], midpoint_s: float | None
) -> float | None:
    if midpoint_s is None:
        return None
    halves: list[tuple[float, float]] = []
    for first in (True, False):
        duration_s = 0.0
        hr_total = 0.0
        for interval in intervals:
            if interval.heart_rate_bpm is None:
                continue
            overlap_s = _half_overlap(interval, midpoint_s, first=first)
            if overlap_s <= 0.0:
                continue
            duration_s += overlap_s
            hr_total += interval.heart_rate_bpm * overlap_s
        if duration_s <= 0.0:
            return None
        halves.append((duration_s, hr_total))
    first_hr = halves[0][1] / halves[0][0]
    second_hr = halves[1][1] / halves[1][0]
    if first_hr <= 0.0 or second_hr <= 0.0:
        return None
    return _rounded_finite(((second_hr - first_hr) / first_hr) * 100.0)


def _half_overlap(
    interval: TemporalInterval, midpoint_s: float, *, first: bool
) -> float:
    if first:
        return max(0.0, min(interval.end_s, midpoint_s) - interval.start_s)
    return max(0.0, interval.end_s - max(interval.start_s, midpoint_s))


def _timestamp_seconds(value: str | datetime | float | None) -> float | None:
    try:
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, (int, float)):
            seconds = float(value)
            return seconds if math.isfinite(seconds) else None
        else:
            text = str(value).strip()
            if not text:
                return None
            if text.endswith(("Z", "z")):
                text = f"{text[:-1]}+00:00"
            parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        seconds = parsed.timestamp()
        return seconds if math.isfinite(seconds) else None
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _finite_float(value: float | None) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _nonnegative_or_none(value: float | None) -> float | None:
    number = _finite_float(value)
    return number if number is not None and number >= 0.0 else None


def _hr_or_none(value: float | None) -> float | None:
    number = _finite_float(value)
    if number is None or number < HR_MIN_BPM or number > HR_MAX_BPM:
        return None
    return number


def _rounded_finite(value: float) -> float | None:
    return round(value, 2) if math.isfinite(value) else None
