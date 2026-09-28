"""Phase 3M.5A regressions for legacy TCX identity and GPX tracks.

All fixtures are synthetic.  They reproduce only the structural identity
signals observed in the read-only 3M.5 rehearsal copies.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from scripts.phase3m5a_classify import _timestamps_are_nondecreasing
from garmin_data_hub.ingest.archive_parser import (
    PARSER_ALGORITHM_VERSION,
    ActivityIdentity,
    ArchiveParseStatus,
    parse_activity_archive,
)


ACTIVITY_ID = 9_500_001
OTHER_ACTIVITY_ID = 9_500_002
START_UTC = "2042-03-04T05:06:07Z"


def test_semantic_correction_advances_parser_algorithm_version() -> None:
    assert PARSER_ALGORITHM_VERSION == "archive-v2"


def test_classification_harness_orders_timestamp_instants_not_strings() -> None:
    assert _timestamps_are_nondecreasing(
        ["2042-03-04T05:06:07Z", "2042-03-04T05:06:07.5Z"]
    )
    assert not _timestamps_are_nondecreasing(
        ["2042-03-04T05:06:07.5Z", "2042-03-04T05:06:07Z"]
    )


def _archive(
    directory: Path,
    payload: str,
    *,
    extension: str,
    archive_activity_id: int = ACTIVITY_ID,
    member_activity_id: int | None = ACTIVITY_ID,
) -> Path:
    path = directory / f"2042-03-04_{archive_activity_id}_synthetic.zip"
    member = (
        f"{member_activity_id}_UNKNOWN.{extension}"
        if member_activity_id is not None
        else f"activity.{extension}"
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr(member, payload)
    return path


def _target(*, activity_type: str = "running") -> ActivityIdentity:
    return ActivityIdentity(
        activity_id=ACTIVITY_ID,
        start_time_utc=START_UTC,
        duration_s=3_600.0,
        activity_type=activity_type,
    )


def _target_at(
    start_time_utc: str,
    *,
    activity_type: str = "running",
    duration_s: float | None = 3_600.0,
) -> ActivityIdentity:
    return ActivityIdentity(
        activity_id=ACTIVITY_ID,
        start_time_utc=start_time_utc,
        duration_s=duration_s,
        activity_type=activity_type,
    )


def _tcx(
    *,
    sport: str | None = "Running",
    identity: str = START_UTC,
    points: tuple[str, ...] | None = None,
    lap_duration_s: float | None = 3_600.0,
) -> str:
    point_times = points if points is not None else (identity, identity)
    sport_attribute = f' Sport="{sport}"' if sport is not None else ""
    trackpoints = "".join(
        f"<Trackpoint><Time>{timestamp}</Time></Trackpoint>"
        for timestamp in point_times
    )
    total_time = (
        f"<TotalTimeSeconds>{lap_duration_s}</TotalTimeSeconds>"
        if lap_duration_s is not None
        else ""
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
    <TrainingCenterDatabase
      xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
      <Activities><Activity{sport_attribute}>
        <Id>{identity}</Id>
        <Lap StartTime="{identity}">{total_time}<Track>{trackpoints}</Track></Lap>
      </Activity></Activities>
    </TrainingCenterDatabase>
    """


def _gpx_track(name: str, *segments: tuple[str | None, ...]) -> str:
    rendered_segments: list[str] = []
    for segment_index, timestamps in enumerate(segments):
        points = []
        for point_index, timestamp in enumerate(timestamps):
            time_element = f"<time>{timestamp}</time>" if timestamp else ""
            measurements = (
                "<ele>123.5</ele>"
                if segment_index == 0 and point_index == 0
                else ""
            )
            points.append(
                f'<trkpt lat="38.5" lon="-77.5">'
                f"{measurements}{time_element}</trkpt>"
            )
        rendered_segments.append(f"<trkseg>{''.join(points)}</trkseg>")
    return f"<trk><name>{name}</name>{''.join(rendered_segments)}</trk>"


def _gpx(*tracks: str) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
    <gpx xmlns="http://www.topografix.com/GPX/1/1"
         version="1.1" creator="synthetic-3m5a">
      {''.join(tracks)}
    </gpx>
    """


@pytest.mark.parametrize(
    "activity_type",
    [
        "indoor_running",
        "trail_running",
        "road_biking",
        "strength_training",
        "walking",
    ],
)
def test_strong_exact_tcx_identity_outweighs_conflicting_legacy_sport(
    tmp_path: Path,
    activity_type: str,
) -> None:
    archive = _archive(tmp_path, _tcx(), extension="tcx")

    result = parse_activity_archive(archive, _target(activity_type=activity_type))

    assert result.status is ArchiveParseStatus.PARSED
    assert result.identity_evidence["identity_strength"] == "strong"
    if activity_type in {"road_biking", "strength_training", "walking"}:
        assert result.identity_evidence["sport_evidence"] == (
            "conflict_overridden_by_strong_identity"
        )


@pytest.mark.parametrize("activity_type", ["mountain_biking", "gravel_cycling"])
def test_strong_exact_tcx_identity_intentionally_outweighs_running_for_cycling(
    tmp_path: Path,
    activity_type: str,
) -> None:
    archive = _archive(tmp_path, _tcx(), extension="tcx")

    result = parse_activity_archive(archive, _target(activity_type=activity_type))

    assert result.status is ArchiveParseStatus.PARSED
    assert result.identity_evidence["identity_strength"] == "strong"
    assert (
        result.identity_evidence["sport_evidence"]
        == "conflict_overridden_by_strong_identity"
    )


def test_filename_match_cannot_override_conflicting_internal_id(
    tmp_path: Path,
) -> None:
    archive = _archive(
        tmp_path,
        _tcx(),
        extension="tcx",
        member_activity_id=OTHER_ACTIVITY_ID,
    )

    result = parse_activity_archive(archive, _target(activity_type="road_biking"))

    assert result.status is ArchiveParseStatus.MISMATCH
    assert result.reason_code == "activity_id_mismatch"


def test_exact_timestamp_without_sport_is_still_positive_identity(
    tmp_path: Path,
) -> None:
    archive = _archive(tmp_path, _tcx(sport=None), extension="tcx")

    result = parse_activity_archive(archive, _target(activity_type="walking"))

    assert result.status is ArchiveParseStatus.PARSED


def test_weak_nearby_time_and_filename_only_do_not_override_sport_conflict(
    tmp_path: Path,
) -> None:
    nearby = "2042-03-04T05:10:07Z"
    archive = _archive(
        tmp_path,
        _tcx(identity=nearby),
        extension="tcx",
        member_activity_id=None,
    )

    result = parse_activity_archive(archive, _target(activity_type="road_biking"))

    assert result.status is ArchiveParseStatus.MISMATCH
    assert result.reason_code == "sport_mismatch"


def test_filename_and_same_calendar_date_are_not_sufficient_tcx_identity(
    tmp_path: Path,
) -> None:
    elsewhere_same_day = "2042-03-04T17:06:07Z"
    archive = _archive(
        tmp_path,
        _tcx(identity=elsewhere_same_day),
        extension="tcx",
        member_activity_id=None,
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.MISMATCH
    assert result.reason_code == "no_temporal_match"


def test_filename_and_broad_tolerance_do_not_choose_close_wrong_tcx_target(
    tmp_path: Path,
) -> None:
    other_activity_start = "2042-03-04T05:10:07Z"
    archive = _archive(
        tmp_path,
        _tcx(
            identity=other_activity_start,
            points=(other_activity_start, "2042-03-04T05:11:07Z"),
        ),
        extension="tcx",
        member_activity_id=None,
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.MISMATCH
    assert result.reason_code == "insufficient_temporal_identity"


def test_nearby_tcx_time_can_match_when_both_routing_ids_corroborate(
    tmp_path: Path,
) -> None:
    nearby = "2042-03-04T05:10:07Z"
    archive = _archive(
        tmp_path,
        _tcx(identity=nearby, points=(START_UTC, "2042-03-04T05:11:07Z")),
        extension="tcx",
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.PARSED


def test_unknown_tcx_sport_remains_non_permissive_with_strong_identity(
    tmp_path: Path,
) -> None:
    archive = _archive(
        tmp_path,
        _tcx(sport="SpaceWalking"),
        extension="tcx",
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.MISMATCH
    assert result.reason_code == "sport_mismatch"


@pytest.mark.parametrize("sport", ["SpaceWalking", "UnsupportedLegacyAlias"])
def test_unknown_or_unsupported_tcx_sport_with_weak_identity_does_not_match(
    tmp_path: Path,
    sport: str,
) -> None:
    nearby = "2042-03-04T05:10:07Z"
    archive = _archive(
        tmp_path,
        _tcx(sport=sport, identity=nearby),
        extension="tcx",
        member_activity_id=None,
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.MISMATCH
    assert result.reason_code == "sport_mismatch"


def test_tcx_activity_id_cannot_hide_trackpoint_span_for_another_activity(
    tmp_path: Path,
) -> None:
    archive = _archive(
        tmp_path,
        _tcx(
            points=(
                "2042-03-06T05:06:07Z",
                "2042-03-06T05:07:07Z",
            )
        ),
        extension="tcx",
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.MISMATCH
    assert result.reason_code == "trackpoint_time_range_mismatch"


def test_tcx_strong_start_cannot_hide_trackpoint_end_beyond_target_window(
    tmp_path: Path,
) -> None:
    archive = _archive(
        tmp_path,
        _tcx(points=(START_UTC, "2042-03-04T07:06:07Z")),
        extension="tcx",
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.MISMATCH
    assert result.reason_code == "trackpoint_time_range_mismatch"


def test_tcx_declared_lap_rejects_unrelated_tail_when_db_duration_is_missing(
    tmp_path: Path,
) -> None:
    archive = _archive(
        tmp_path,
        _tcx(points=(START_UTC, "2042-03-04T07:06:07Z")),
        extension="tcx",
    )

    result = parse_activity_archive(
        archive,
        _target_at(START_UTC, activity_type="road_biking", duration_s=None),
    )

    assert result.status is ArchiveParseStatus.MISMATCH
    assert result.reason_code == "sport_mismatch"


def test_sport_conflict_override_requires_a_declared_tcx_lap_window(
    tmp_path: Path,
) -> None:
    archive = _archive(
        tmp_path,
        _tcx(lap_duration_s=None),
        extension="tcx",
    )

    result = parse_activity_archive(archive, _target(activity_type="road_biking"))

    assert result.status is ArchiveParseStatus.MISMATCH
    assert result.reason_code == "sport_mismatch"


def test_tcx_out_of_order_trackpoints_are_malformed(tmp_path: Path) -> None:
    archive = _archive(
        tmp_path,
        _tcx(points=(START_UTC, "2042-03-04T05:06:09Z", "2042-03-04T05:06:08Z")),
        extension="tcx",
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.MALFORMED
    assert result.reason_code == "non_monotonic_timestamps"


def test_tcx_timezone_offset_normalizes_to_exact_strong_identity(
    tmp_path: Path,
) -> None:
    offset_start = "2042-03-04T00:06:07-05:00"
    archive = _archive(
        tmp_path,
        _tcx(identity=offset_start, points=(offset_start, "2042-03-04T00:06:08-05:00")),
        extension="tcx",
    )

    result = parse_activity_archive(archive, _target(activity_type="indoor_running"))

    assert result.status is ArchiveParseStatus.PARSED
    assert result.identity_evidence["identity_strength"] == "strong"


def test_tcx_local_clock_with_offset_cannot_masquerade_as_utc(tmp_path: Path) -> None:
    shifted = "2042-03-04T05:06:07-05:00"
    archive = _archive(
        tmp_path,
        _tcx(identity=shifted, points=(shifted, "2042-03-04T05:06:08-05:00")),
        extension="tcx",
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.MISMATCH
    assert result.reason_code == "no_temporal_match"


def test_naive_database_gmt_is_intentionally_interpreted_as_utc(
    tmp_path: Path,
) -> None:
    archive = _archive(tmp_path, _tcx(), extension="tcx")

    result = parse_activity_archive(
        archive,
        _target_at("2042-03-04 05:06:07", activity_type="indoor_running"),
    )

    assert result.status is ArchiveParseStatus.PARSED
    assert result.identity_evidence["identity_strength"] == "strong"


def test_tcx_naive_source_timestamp_does_not_gain_utc_identity(tmp_path: Path) -> None:
    naive = "2042-03-04T05:06:07"
    archive = _archive(tmp_path, _tcx(identity=naive), extension="tcx")

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.AMBIGUOUS
    assert result.reason_code == "unresolved_tcx_identity"


@pytest.mark.parametrize(
    ("candidate_start", "expected_status"),
    [
        ("2042-03-04T05:11:07Z", ArchiveParseStatus.PARSED),
        ("2042-03-04T05:11:07.001Z", ArchiveParseStatus.MISMATCH),
    ],
)
def test_tcx_timestamp_tolerance_boundary_is_explicit(
    tmp_path: Path,
    candidate_start: str,
    expected_status: ArchiveParseStatus,
) -> None:
    archive = _archive(
        tmp_path,
        _tcx(identity=candidate_start, points=(candidate_start, candidate_start)),
        extension="tcx",
    )

    result = parse_activity_archive(archive, _target_at(START_UTC, duration_s=None))

    assert result.status is expected_status


def test_genuine_mismatch_shape_remains_rejected(tmp_path: Path) -> None:
    old_identity = "2036-03-04T05:06:07Z"
    archive = _archive(
        tmp_path,
        _tcx(identity=old_identity),
        extension="tcx",
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.MISMATCH
    assert result.reason_code == "no_temporal_match"
    assert result.rows == []


def test_one_gpx_track_concatenates_two_segments_deterministically(
    tmp_path: Path,
) -> None:
    archive = _archive(
        tmp_path,
        _gpx(
            _gpx_track(
                "Synthetic ride",
                (START_UTC, "2042-03-04T05:06:08Z"),
                ("2042-03-04T05:16:07Z", "2042-03-04T05:16:08Z"),
            )
        ),
        extension="gpx",
    )

    result = parse_activity_archive(archive, _target(activity_type="road_biking"))

    assert result.status is ArchiveParseStatus.PARSED
    assert len(result.rows) == 4
    assert [(row[0], row[1]) for row in result.rows] == [
        (0, START_UTC),
        (1, "2042-03-04T05:06:08Z"),
        (2, "2042-03-04T05:16:07Z"),
        (3, "2042-03-04T05:16:08Z"),
    ]
    assert result.rows[1][4:] == (None, None, None, None, None, None, None)
    assert result.identity_evidence["candidate_count"] == 1
    assert result.identity_evidence["selected_track_segment_count"] == 2


@pytest.mark.parametrize("segment_count", [1, 2, 4])
def test_one_gpx_track_concatenates_any_number_of_segments(
    tmp_path: Path,
    segment_count: int,
) -> None:
    segments = tuple(
        (f"2042-03-04T05:06:{7 + index:02d}Z",)
        for index in range(segment_count)
    )
    archive = _archive(
        tmp_path,
        _gpx(_gpx_track("Segmented activity", *segments)),
        extension="gpx",
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.PARSED
    assert len(result.rows) == segment_count
    assert [row[0] for row in result.rows] == list(range(segment_count))
    assert result.identity_evidence["selected_track_segment_count"] == segment_count


def test_empty_gpx_segment_inside_populated_track_is_ignored_deterministically(
    tmp_path: Path,
) -> None:
    archive = _archive(
        tmp_path,
        _gpx(
            _gpx_track(
                "Empty separator",
                (START_UTC,),
                (),
                ("2042-03-04T05:06:08Z",),
            )
        ),
        extension="gpx",
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.PARSED
    assert [(row[0], row[1]) for row in result.rows] == [
        (0, START_UTC),
        (1, "2042-03-04T05:06:08Z"),
    ]
    assert result.identity_evidence["empty_segment_count"] == 1


def test_gpx_segment_gap_preserves_source_order_and_optional_nulls(
    tmp_path: Path,
) -> None:
    archive = _archive(
        tmp_path,
        _gpx(
            _gpx_track(
                "Gap",
                (START_UTC, "2042-03-04T05:06:08Z"),
                ("2042-03-04T05:36:07Z",),
            )
        ),
        extension="gpx",
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.PARSED
    assert [row[1] for row in result.rows] == [
        START_UTC,
        "2042-03-04T05:06:08Z",
        "2042-03-04T05:36:07Z",
    ]
    assert result.rows[-1][4:] == (None, None, None, None, None, None, None)


def test_duplicate_gpx_boundary_timestamp_is_preserved_once_per_source_point(
    tmp_path: Path,
) -> None:
    boundary = "2042-03-04T05:06:08Z"
    archive = _archive(
        tmp_path,
        _gpx(
            _gpx_track(
                "Duplicate boundary",
                (START_UTC, boundary),
                (boundary, "2042-03-04T05:06:09Z"),
            )
        ),
        extension="gpx",
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.PARSED
    assert [row[1] for row in result.rows] == [
        START_UTC,
        boundary,
        boundary,
        "2042-03-04T05:06:09Z",
    ]
    assert [row[0] for row in result.rows] == [0, 1, 2, 3]


def test_out_of_order_gpx_segments_are_malformed(tmp_path: Path) -> None:
    archive = _archive(
        tmp_path,
        _gpx(
            _gpx_track(
                "Out of order",
                (START_UTC, "2042-03-04T05:06:09Z"),
                ("2042-03-04T05:06:08Z",),
            )
        ),
        extension="gpx",
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.MALFORMED
    assert result.reason_code == "non_monotonic_timestamps"


def test_multiple_gpx_tracks_are_not_collapsed_as_segments(tmp_path: Path) -> None:
    archive = _archive(
        tmp_path,
        _gpx(
            _gpx_track("First activity", (START_UTC,)),
            _gpx_track("Second activity", ("2042-03-04T05:06:09Z",)),
        ),
        extension="gpx",
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.AMBIGUOUS
    assert result.reason_code == "multiple_temporal_matches"
    assert result.identity_evidence["candidate_count"] == 2
    assert result.rows == []


def test_gpx_selects_one_matching_track_without_collapsing_conflicting_track(
    tmp_path: Path,
) -> None:
    archive = _archive(
        tmp_path,
        _gpx(
            _gpx_track("Target", (START_UTC, "2042-03-04T05:06:08Z")),
            _gpx_track(
                "Different activity",
                ("2042-03-06T05:06:07Z", "2042-03-06T05:06:08Z"),
            ),
        ),
        extension="gpx",
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.PARSED
    assert len(result.rows) == 2
    assert result.identity_evidence["track_count"] == 2
    assert result.identity_evidence["matching_candidate_count"] == 1


def test_overlapping_gpx_tracks_remain_ambiguous(tmp_path: Path) -> None:
    archive = _archive(
        tmp_path,
        _gpx(
            _gpx_track("First", (START_UTC, "2042-03-04T05:06:08Z")),
            _gpx_track("Second", (START_UTC, "2042-03-04T05:06:09Z")),
        ),
        extension="gpx",
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.AMBIGUOUS
    assert result.reason_code == "multiple_temporal_matches"
    assert result.rows == []


def test_malformed_later_gpx_segment_fails_without_partial_rows(
    tmp_path: Path,
) -> None:
    archive = _archive(
        tmp_path,
        _gpx(
            _gpx_track(
                "Malformed second segment",
                (START_UTC,),
                (None,),
            )
        ),
        extension="gpx",
    )

    result = parse_activity_archive(archive, _target())

    assert result.status is ArchiveParseStatus.MALFORMED
    assert result.reason_code == "missing_timestamp"
    assert result.rows == []
