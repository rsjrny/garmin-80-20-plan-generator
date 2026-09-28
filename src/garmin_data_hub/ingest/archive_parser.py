"""Canonical FIT/GPX/TCX archive parsing and identity validation.

The parser is deliberately side-effect free.  It resolves one archive to one
target activity, validates that relationship, and only then returns canonical
``activity_trackpoints`` rows.  Database policy belongs to the caller.
"""

from __future__ import annotations

import math
import re
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from fitparse.utils import FitParseError
from garmin_mcp.parse_activity_files import (
    _activity_id_from_zip_filename,
    _extract_activity_id_from_member,
    _track_rows_from_fit_bytes,
)


PARSER_ALGORITHM_VERSION = "archive-v1"
IDENTITY_TOLERANCE_SECONDS = 300.0

GPX_NS = "http://www.topografix.com/GPX/1/1"
GPX_TRACKPOINT_NS = "http://www.garmin.com/xmlschemas/TrackPointExtension/v1"
TCX_NS = "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"
TCX_ACTIVITY_NS = "http://www.garmin.com/xmlschemas/ActivityExtension/v2"

_XML_MEMBER_ID_RE = re.compile(
    r"(?:^|/)(\d{7,})_(?:unknown|activity)\.(?:gpx|tcx|fit)$",
    re.IGNORECASE,
)


class ArchiveParseStatus(str, Enum):
    PARSED = "parsed"
    RECOGNIZED_NO_RECORDS = "recognized_no_records"
    UNSUPPORTED = "unsupported"
    MALFORMED = "malformed"
    MISMATCH = "mismatch"
    AMBIGUOUS = "ambiguous"


class ArchiveFormat(str, Enum):
    FIT = "fit"
    GPX = "gpx"
    TCX = "tcx"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ActivityIdentity:
    activity_id: int
    start_time_utc: str
    duration_s: float | None = None
    activity_type: str | None = None


@dataclass(frozen=True)
class ArchiveParseResult:
    status: ArchiveParseStatus
    archive_format: ArchiveFormat
    activity_id: int | None
    rows: list[tuple]
    reason_code: str | None = None
    identity_evidence: dict[str, Any] = field(default_factory=dict)
    member_name: str | None = None
    algorithm_version: str = PARSER_ALGORITHM_VERSION


class _MalformedArchive(ValueError):
    pass


def extract_activity_id_from_member(name: str) -> int | None:
    """Resolve IDs from current FIT and legacy ``*_UNKNOWN`` members."""
    fit_id = _extract_activity_id_from_member(name)
    if fit_id is not None:
        return int(fit_id)
    match = _XML_MEMBER_ID_RE.search(name.replace("\\", "/"))
    return int(match.group(1)) if match else None


def _result(
    status: ArchiveParseStatus,
    archive_format: ArchiveFormat,
    target: ActivityIdentity,
    *,
    rows: list[tuple] | None = None,
    reason_code: str | None = None,
    evidence: dict[str, Any] | None = None,
    member_name: str | None = None,
) -> ArchiveParseResult:
    return ArchiveParseResult(
        status=status,
        archive_format=archive_format,
        activity_id=(
            int(target.activity_id)
            if status
            in {
                ArchiveParseStatus.PARSED,
                ArchiveParseStatus.RECOGNIZED_NO_RECORDS,
            }
            else None
        ),
        rows=list(rows or []),
        reason_code=reason_code,
        identity_evidence=dict(evidence or {}),
        member_name=member_name,
    )


def _namespace(tag: str) -> str:
    if tag.startswith("{") and "}" in tag:
        return tag[1:].split("}", 1)[0]
    return ""


def _local_name(tag: str) -> str:
    return tag.split("}", 1)[-1]


def _parse_datetime(value: object, *, source_requires_timezone: bool) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            raise _MalformedArchive("missing timestamp")
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise _MalformedArchive("invalid timestamp") from exc
    if parsed.tzinfo is None:
        if source_requires_timezone:
            raise _MalformedArchive("source timestamp has no timezone")
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_utc(value: datetime) -> str:
    normalized = value.astimezone(timezone.utc)
    if normalized.microsecond:
        base = normalized.strftime("%Y-%m-%dT%H:%M:%S.%f").rstrip("0")
    else:
        base = normalized.strftime("%Y-%m-%dT%H:%M:%S")
    return base + "Z"


def normalize_timestamp(value: object, *, source_requires_timezone: bool = True) -> str:
    return _format_utc(
        _parse_datetime(value, source_requires_timezone=source_requires_timezone)
    )


def _optional_float(value: str | None, field_name: str) -> float | None:
    if value is None or not value.strip():
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise _MalformedArchive(f"invalid {field_name}") from exc
    if not math.isfinite(parsed):
        raise _MalformedArchive(f"non-finite {field_name}")
    return parsed


def _optional_int(value: str | None, field_name: str) -> int | None:
    parsed = _optional_float(value, field_name)
    if parsed is None:
        return None
    if not parsed.is_integer():
        raise _MalformedArchive(f"non-integral {field_name}")
    return int(parsed)


def _text(element: ElementTree.Element | None) -> str | None:
    if element is None or element.text is None:
        return None
    value = element.text.strip()
    return value or None


def _target_window(target: ActivityIdentity) -> tuple[datetime, datetime]:
    start = _parse_datetime(target.start_time_utc, source_requires_timezone=False)
    duration = float(target.duration_s or 0.0)
    if not math.isfinite(duration) or duration < 0:
        duration = 0.0
    return start, start + timedelta(seconds=duration)


def _time_matches(
    candidate_start: datetime,
    candidate_end: datetime,
    target: ActivityIdentity,
) -> tuple[bool, float]:
    target_start, target_end = _target_window(target)
    delta = abs((candidate_start - target_start).total_seconds())
    if delta > IDENTITY_TOLERANCE_SECONDS:
        return False, delta
    if target_end > target_start:
        tolerance = timedelta(seconds=IDENTITY_TOLERANCE_SECONDS)
        if candidate_end < target_start - tolerance:
            return False, delta
        if candidate_start > target_end + tolerance:
            return False, delta
    return True, delta


def _routing_evidence(
    archive_path: Path,
    member_name: str,
    target: ActivityIdentity,
) -> tuple[ArchiveParseStatus | None, dict[str, Any]]:
    outer_id = _activity_id_from_zip_filename(archive_path)
    member_id = extract_activity_id_from_member(member_name)
    evidence: dict[str, Any] = {
        "target_activity_id": int(target.activity_id),
        "archive_activity_id": int(outer_id) if outer_id is not None else None,
        "member_activity_id": int(member_id) if member_id is not None else None,
    }
    available = [int(value) for value in (outer_id, member_id) if value is not None]
    if not available:
        return ArchiveParseStatus.AMBIGUOUS, evidence
    if len(set(available)) > 1 or any(
        value != int(target.activity_id) for value in available
    ):
        return ArchiveParseStatus.MISMATCH, evidence
    return None, evidence


_RUNNING_ACTIVITY_TYPES = frozenset(
    {"running", "trail_running", "indoor_running"}
)
_CYCLING_ACTIVITY_TYPES = frozenset(
    {
        "biking",
        "cycling",
        "mountain_biking",
        "gravel_cycling",
        "indoor_cycling",
    }
)


def _sport_family(value: str | None) -> str | None:
    """Normalize supported TCX and Data Hub sport names to strict families."""
    if not value:
        return None
    normalized = value.strip().casefold().replace("-", "_").replace(" ", "_")
    if normalized in _RUNNING_ACTIVITY_TYPES:
        return "running"
    if normalized in _CYCLING_ACTIVITY_TYPES:
        return "cycling"
    return None


def _sport_matches(tcx_sport: str | None, activity_type: str | None) -> bool:
    if not tcx_sport or not activity_type:
        return True
    sport = tcx_sport.strip().casefold()
    if sport == "other":
        return True
    source_family = _sport_family(sport)
    target_family = _sport_family(activity_type)
    return source_family is not None and source_family == target_family


def _parse_gpx(
    payload: bytes,
    target: ActivityIdentity,
    evidence: dict[str, Any],
    member_name: str,
) -> ArchiveParseResult:
    try:
        root = ElementTree.fromstring(payload)
    except ElementTree.ParseError:
        return _result(
            ArchiveParseStatus.MALFORMED,
            ArchiveFormat.GPX,
            target,
            reason_code="malformed_xml",
            evidence=evidence,
            member_name=member_name,
        )
    if _local_name(root.tag) != "gpx" or _namespace(root.tag) != GPX_NS:
        return _result(
            ArchiveParseStatus.UNSUPPORTED,
            ArchiveFormat.GPX,
            target,
            reason_code="unsupported_gpx_root",
            evidence=evidence,
            member_name=member_name,
        )

    segments = root.findall(f".//{{{GPX_NS}}}trkseg")
    if not segments:
        return _result(
            ArchiveParseStatus.AMBIGUOUS,
            ArchiveFormat.GPX,
            target,
            reason_code="no_identifiable_gpx_segment",
            evidence=evidence,
            member_name=member_name,
        )

    candidates: list[tuple[ElementTree.Element, list[ElementTree.Element], float]] = []
    empty_segments = 0
    try:
        for segment in segments:
            points = segment.findall(f"{{{GPX_NS}}}trkpt")
            if not points:
                empty_segments += 1
                continue
            timestamps = [
                _parse_datetime(
                    _text(point.find(f"{{{GPX_NS}}}time")),
                    source_requires_timezone=True,
                )
                for point in points
            ]
            matches, delta = _time_matches(timestamps[0], timestamps[-1], target)
            if matches:
                candidates.append((segment, points, delta))
    except _MalformedArchive as exc:
        return _result(
            ArchiveParseStatus.MALFORMED,
            ArchiveFormat.GPX,
            target,
            reason_code=str(exc).replace(" ", "_"),
            evidence=evidence,
            member_name=member_name,
        )

    match_evidence = {
        **evidence,
        "candidate_count": len(segments),
        "matching_candidate_count": len(candidates),
        "unresolved_empty_candidate_count": empty_segments,
    }
    if empty_segments:
        return _result(
            ArchiveParseStatus.AMBIGUOUS,
            ArchiveFormat.GPX,
            target,
            reason_code="unresolved_empty_gpx_segment",
            evidence=match_evidence,
            member_name=member_name,
        )
    if not candidates:
        return _result(
            ArchiveParseStatus.MISMATCH,
            ArchiveFormat.GPX,
            target,
            reason_code="no_temporal_match",
            evidence=match_evidence,
            member_name=member_name,
        )
    if len(candidates) > 1:
        return _result(
            ArchiveParseStatus.AMBIGUOUS,
            ArchiveFormat.GPX,
            target,
            reason_code="multiple_temporal_matches",
            evidence=match_evidence,
            member_name=member_name,
        )

    _segment, points, delta = candidates[0]
    rows: list[tuple] = []
    try:
        for seq, point in enumerate(points):
            extensions = point.find(f"{{{GPX_NS}}}extensions")
            hr = cadence = power = None
            if extensions is not None:
                extension = extensions.find(
                    f"{{{GPX_TRACKPOINT_NS}}}TrackPointExtension"
                )
                if extension is not None:
                    hr = _optional_int(
                        _text(extension.find(f"{{{GPX_TRACKPOINT_NS}}}hr")),
                        "heart_rate_bpm",
                    )
                    cadence = _optional_int(
                        _text(extension.find(f"{{{GPX_TRACKPOINT_NS}}}cad")),
                        "cadence",
                    )
                power = _optional_int(
                    _text(extensions.find(f"{{{GPX_NS}}}power")),
                    "power_w",
                )
            rows.append(
                (
                    seq,
                    normalize_timestamp(
                        _text(point.find(f"{{{GPX_NS}}}time")),
                        source_requires_timezone=True,
                    ),
                    _optional_float(point.get("lat"), "latitude"),
                    _optional_float(point.get("lon"), "longitude"),
                    _optional_float(
                        _text(point.find(f"{{{GPX_NS}}}ele")), "altitude_m"
                    ),
                    None,
                    None,
                    hr,
                    cadence,
                    power,
                    None,
                )
            )
    except _MalformedArchive as exc:
        return _result(
            ArchiveParseStatus.MALFORMED,
            ArchiveFormat.GPX,
            target,
            reason_code=str(exc).replace(" ", "_"),
            evidence=match_evidence,
            member_name=member_name,
        )

    match_evidence["start_delta_seconds"] = delta
    return _result(
        ArchiveParseStatus.PARSED,
        ArchiveFormat.GPX,
        target,
        rows=rows,
        evidence=match_evidence,
        member_name=member_name,
    )


def _parse_tcx(
    payload: bytes,
    target: ActivityIdentity,
    evidence: dict[str, Any],
    member_name: str,
) -> ArchiveParseResult:
    try:
        root = ElementTree.fromstring(payload)
    except ElementTree.ParseError:
        return _result(
            ArchiveParseStatus.MALFORMED,
            ArchiveFormat.TCX,
            target,
            reason_code="malformed_xml",
            evidence=evidence,
            member_name=member_name,
        )
    if _local_name(root.tag) != "TrainingCenterDatabase" or _namespace(root.tag) != TCX_NS:
        return _result(
            ArchiveParseStatus.UNSUPPORTED,
            ArchiveFormat.TCX,
            target,
            reason_code="unsupported_tcx_root",
            evidence=evidence,
            member_name=member_name,
        )

    activities = root.findall(f".//{{{TCX_NS}}}Activities/{{{TCX_NS}}}Activity")
    if not activities:
        return _result(
            ArchiveParseStatus.AMBIGUOUS,
            ArchiveFormat.TCX,
            target,
            reason_code="no_tcx_activity",
            evidence=evidence,
            member_name=member_name,
        )

    candidates: list[tuple[ElementTree.Element, list[ElementTree.Element], float]] = []
    invalid_identity = False
    sport_conflicts = 0
    for activity in activities:
        try:
            identity_time = _parse_datetime(
                _text(activity.find(f"{{{TCX_NS}}}Id")),
                source_requires_timezone=True,
            )
        except _MalformedArchive:
            invalid_identity = True
            continue
        points = activity.findall(f".//{{{TCX_NS}}}Trackpoint")
        try:
            end_time = (
                _parse_datetime(
                    _text(points[-1].find(f"{{{TCX_NS}}}Time")),
                    source_requires_timezone=True,
                )
                if points
                else identity_time
            )
        except _MalformedArchive:
            return _result(
                ArchiveParseStatus.MALFORMED,
                ArchiveFormat.TCX,
                target,
                reason_code="invalid_timestamp",
                evidence=evidence,
                member_name=member_name,
            )
        matches, delta = _time_matches(identity_time, end_time, target)
        if not matches:
            continue
        if not _sport_matches(activity.get("Sport"), target.activity_type):
            sport_conflicts += 1
            continue
        candidates.append((activity, points, delta))

    match_evidence = {
        **evidence,
        "candidate_count": len(activities),
        "matching_candidate_count": len(candidates),
        "invalid_identity_count": int(invalid_identity),
        "sport_conflict_count": sport_conflicts,
    }
    if invalid_identity:
        return _result(
            ArchiveParseStatus.AMBIGUOUS,
            ArchiveFormat.TCX,
            target,
            reason_code="unresolved_tcx_identity",
            evidence=match_evidence,
            member_name=member_name,
        )
    if not candidates:
        return _result(
            ArchiveParseStatus.MISMATCH,
            ArchiveFormat.TCX,
            target,
            reason_code=("sport_mismatch" if sport_conflicts else "no_temporal_match"),
            evidence=match_evidence,
            member_name=member_name,
        )
    if len(candidates) > 1:
        return _result(
            ArchiveParseStatus.AMBIGUOUS,
            ArchiveFormat.TCX,
            target,
            reason_code="multiple_temporal_matches",
            evidence=match_evidence,
            member_name=member_name,
        )

    _activity, points, delta = candidates[0]
    if not points:
        match_evidence["start_delta_seconds"] = delta
        return _result(
            ArchiveParseStatus.RECOGNIZED_NO_RECORDS,
            ArchiveFormat.TCX,
            target,
            reason_code="activity_has_no_trackpoints",
            evidence=match_evidence,
            member_name=member_name,
        )

    rows: list[tuple] = []
    try:
        for seq, point in enumerate(points):
            position = point.find(f"{{{TCX_NS}}}Position")
            heart_rate = point.find(f"{{{TCX_NS}}}HeartRateBpm")
            extensions = point.find(f"{{{TCX_NS}}}Extensions")
            tpx = (
                extensions.find(f"{{{TCX_ACTIVITY_NS}}}TPX")
                if extensions is not None
                else None
            )
            rows.append(
                (
                    seq,
                    normalize_timestamp(
                        _text(point.find(f"{{{TCX_NS}}}Time")),
                        source_requires_timezone=True,
                    ),
                    _optional_float(
                        _text(
                            position.find(f"{{{TCX_NS}}}LatitudeDegrees")
                            if position is not None
                            else None
                        ),
                        "latitude",
                    ),
                    _optional_float(
                        _text(
                            position.find(f"{{{TCX_NS}}}LongitudeDegrees")
                            if position is not None
                            else None
                        ),
                        "longitude",
                    ),
                    _optional_float(
                        _text(point.find(f"{{{TCX_NS}}}AltitudeMeters")),
                        "altitude_m",
                    ),
                    _optional_float(
                        _text(point.find(f"{{{TCX_NS}}}DistanceMeters")),
                        "distance_m",
                    ),
                    _optional_float(
                        _text(
                            tpx.find(f"{{{TCX_ACTIVITY_NS}}}Speed")
                            if tpx is not None
                            else None
                        ),
                        "speed_mps",
                    ),
                    _optional_int(
                        _text(
                            heart_rate.find(f"{{{TCX_NS}}}Value")
                            if heart_rate is not None
                            else None
                        ),
                        "heart_rate_bpm",
                    ),
                    _optional_int(
                        _text(point.find(f"{{{TCX_NS}}}Cadence")), "cadence"
                    ),
                    _optional_int(
                        _text(
                            tpx.find(f"{{{TCX_ACTIVITY_NS}}}Watts")
                            if tpx is not None
                            else None
                        ),
                        "power_w",
                    ),
                    None,
                )
            )
    except _MalformedArchive as exc:
        return _result(
            ArchiveParseStatus.MALFORMED,
            ArchiveFormat.TCX,
            target,
            reason_code=str(exc).replace(" ", "_"),
            evidence=match_evidence,
            member_name=member_name,
        )

    match_evidence["start_delta_seconds"] = delta
    return _result(
        ArchiveParseStatus.PARSED,
        ArchiveFormat.TCX,
        target,
        rows=rows,
        evidence=match_evidence,
        member_name=member_name,
    )


def _parse_fit(
    payload: bytes,
    target: ActivityIdentity,
    evidence: dict[str, Any],
    member_name: str,
) -> ArchiveParseResult:
    try:
        upstream_rows = _track_rows_from_fit_bytes(payload)
    except FitParseError:
        return _result(
            ArchiveParseStatus.MALFORMED,
            ArchiveFormat.FIT,
            target,
            reason_code="fit_parser_error",
            evidence=evidence,
            member_name=member_name,
        )

    try:
        rows = [
            (
                row[0],
                normalize_timestamp(row[1], source_requires_timezone=False),
                *row[2:],
            )
            for row in upstream_rows
        ]
    except _MalformedArchive:
        return _result(
            ArchiveParseStatus.MALFORMED,
            ArchiveFormat.FIT,
            target,
            reason_code="fit_parser_error",
            evidence=evidence,
            member_name=member_name,
        )
    if not rows:
        return _result(
            ArchiveParseStatus.RECOGNIZED_NO_RECORDS,
            ArchiveFormat.FIT,
            target,
            reason_code="activity_has_no_trackpoints",
            evidence=evidence,
            member_name=member_name,
        )

    try:
        first = _parse_datetime(rows[0][1], source_requires_timezone=True)
        last = _parse_datetime(rows[-1][1], source_requires_timezone=True)
        target_start, target_end = _target_window(target)
    except _MalformedArchive:
        return _result(
            ArchiveParseStatus.MALFORMED,
            ArchiveFormat.FIT,
            target,
            reason_code="invalid_timestamp",
            evidence=evidence,
            member_name=member_name,
        )
    tolerance = timedelta(seconds=IDENTITY_TOLERANCE_SECONDS)
    if target_end > target_start:
        disjoint = last < target_start - tolerance or first > target_end + tolerance
    else:
        disjoint = abs((first - target_start).total_seconds()) > 86400.0
    if disjoint:
        return _result(
            ArchiveParseStatus.MISMATCH,
            ArchiveFormat.FIT,
            target,
            reason_code="fit_time_range_mismatch",
            evidence=evidence,
            member_name=member_name,
        )
    return _result(
        ArchiveParseStatus.PARSED,
        ArchiveFormat.FIT,
        target,
        rows=rows,
        evidence=evidence,
        member_name=member_name,
    )


def parse_activity_archive(
    archive_path: Path,
    target: ActivityIdentity,
) -> ArchiveParseResult:
    """Parse one archive into an explicit, identity-validated result."""
    path = Path(archive_path)
    evidence: dict[str, Any] = {"target_activity_id": int(target.activity_id)}
    try:
        with zipfile.ZipFile(path, "r") as archive:
            bad_member = archive.testzip()
            if bad_member is not None:
                return _result(
                    ArchiveParseStatus.MALFORMED,
                    ArchiveFormat.UNKNOWN,
                    target,
                    reason_code="zip_crc_error",
                    evidence={**evidence, "bad_member": bad_member},
                )
            supported = [
                name
                for name in archive.namelist()
                if Path(name).suffix.casefold() in {".fit", ".gpx", ".tcx"}
                and not name.endswith("/")
            ]
            if not supported:
                return _result(
                    ArchiveParseStatus.UNSUPPORTED,
                    ArchiveFormat.UNKNOWN,
                    target,
                    reason_code="no_supported_member",
                    evidence=evidence,
                )
            if len(supported) > 1:
                return _result(
                    ArchiveParseStatus.AMBIGUOUS,
                    ArchiveFormat.UNKNOWN,
                    target,
                    reason_code="multiple_supported_members",
                    evidence={**evidence, "supported_member_count": len(supported)},
                )
            member_name = supported[0]
            suffix = Path(member_name).suffix.casefold()
            archive_format = ArchiveFormat(suffix[1:])
            routing_status, routing = _routing_evidence(path, member_name, target)
            if routing_status is not None:
                return _result(
                    routing_status,
                    archive_format,
                    target,
                    reason_code=(
                        "activity_id_mismatch"
                        if routing_status is ArchiveParseStatus.MISMATCH
                        else "unresolved_activity_id"
                    ),
                    evidence=routing,
                    member_name=member_name,
                )
            payload = archive.read(member_name)
    except (
        OSError,
        RuntimeError,
        NotImplementedError,
        zipfile.BadZipFile,
        zipfile.LargeZipFile,
    ):
        return _result(
            ArchiveParseStatus.MALFORMED,
            ArchiveFormat.UNKNOWN,
            target,
            reason_code="malformed_zip",
            evidence=evidence,
        )

    if archive_format is ArchiveFormat.GPX:
        return _parse_gpx(payload, target, routing, member_name)
    if archive_format is ArchiveFormat.TCX:
        return _parse_tcx(payload, target, routing, member_name)
    return _parse_fit(payload, target, routing, member_name)
