from __future__ import annotations

from pathlib import Path

import pytest

from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services import baseline_plan_builder as builder
from garmin_data_hub.services.plan_persistence import (
    StalePlanWriteError,
    get_active_plan_sha256,
    get_active_plan_snapshot,
)
from garmin_data_hub.services.training_policy import PolicyIssue, TrainingPolicyReport
from garmin_data_hub.ui_nicegui.data import plan_rows


def _database(tmp_path: Path) -> Path:
    db_path = tmp_path / "garmin.db"
    conn = connect_sqlite(db_path)
    try:
        apply_schema(conn, schema_sql_path())
    finally:
        conn.close()
    return db_path


def _request(*, distance: str = "10K") -> builder.BaselinePlanRequest:
    return builder.BaselinePlanRequest(
        athlete_name="Offline Athlete",
        age=42,
        lthr=165,
        hrmax=190,
        sodium_mg_per_hour=700,
        event_name="Autumn 10K",
        distance=distance,
        start_date="2026-08-24",
        event_date="2026-11-16",
        run_days_per_week=4,
        long_run_day="Saturday",
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("5K", "5K"),
        ("10K", "10K"),
        ("10 Miler", "10M"),
        ("Half Marathon", "HM"),
        ("20 Miler", "20M"),
        ("Marathon", "MAR"),
        ("50K", "50K"),
        ("50 Mile", "50M"),
        ("100K", "100K"),
        ("100 Mile", "100M"),
    ],
)
def test_friendly_distances_map_to_rule_engine_codes(value, expected):
    assert builder.normalize_baseline_distance(value) == expected


def test_unknown_distance_is_rejected_instead_of_using_50k_fallback():
    with pytest.raises(ValueError, match="Unsupported race distance"):
        builder.normalize_baseline_distance("custom adventure")


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"hrmax": 79}, "HRmax must be between"),
        ({"lthr": 221}, "LTHR must be between"),
        ({"hrmax": 165, "lthr": 165}, "LTHR must be below HRmax"),
    ],
)
def test_invalid_heart_rate_thresholds_are_rejected(overrides, message):
    request = builder.BaselinePlanRequest(
        **{**_request().__dict__, **overrides}
    )

    with pytest.raises(ValueError, match=message):
        builder.build_and_save_baseline(Path("unused.db"), request)


def test_unbounded_plan_horizon_is_rejected_before_generation(monkeypatch):
    request = builder.BaselinePlanRequest(
        **{**_request().__dict__, "event_date": "2029-08-24"}
    )
    monkeypatch.setattr(
        builder,
        "generate_plan_data",
        lambda **_kwargs: pytest.fail("generation must not start"),
    )

    with pytest.raises(ValueError, match="cannot exceed 730 days"):
        builder.build_and_save_baseline(Path("unused.db"), request)


def test_baseline_generates_once_persists_and_exports_validated_workbook(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    db_path = _database(tmp_path)
    workbook_path = tmp_path / "exports" / "offline.xlsx"
    original_generate = builder.generate_plan_data
    calls: list[str] = []

    def generate_once(**kwargs):
        calls.append(kwargs["distance"])
        return original_generate(**kwargs)

    monkeypatch.setattr(builder, "generate_plan_data", generate_once)

    result = builder.build_and_save_baseline(
        db_path,
        _request(distance="Half Marathon"),
        workbook_path=workbook_path,
    )

    assert calls == ["HM"]
    assert result.workbook_path == workbook_path.resolve()
    assert result.workbook_error is None
    assert result.plan_day_count == 85
    assert result.schedule_row_count == len(plan_rows(db_path))
    assert result.active_session_count > 0
    assert workbook_path.is_file()
    assert plan_rows(db_path)[-1]["intensity"] == "race"


def test_eighty_twenty_baseline_preserves_quality_sessions(tmp_path: Path):
    db_path = _database(tmp_path)

    builder.build_and_save_baseline(
        db_path,
        _request(distance="Half Marathon"),
    )

    rows = plan_rows(db_path)
    assert any(
        row["sport"] == "run" and row["intensity"] == "hard"
        for row in rows
    )


def test_maffetone_baseline_replaces_quality_runs_with_maf_capped_easy_runs(
    tmp_path: Path,
):
    db_path = _database(tmp_path)
    request = builder.BaselinePlanRequest(
        **{**_request(distance="Half Marathon").__dict__, "training_method": "maffetone"}
    )

    builder.build_and_save_baseline(db_path, request)

    rows = plan_rows(db_path)
    assert all(
        row["intensity"] != "hard" or row["date"] == request.event_date
        for row in rows
        if row["sport"] == "run"
    )
    assert any(
        row["sport"] == "run"
        and row["intensity"] in {"easy", "recovery"}
        and "MAF cap: 138 bpm" in str(row["notes"])
        for row in rows
    )


def test_policy_error_writes_neither_database_nor_workbook(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    db_path = _database(tmp_path)
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            "INSERT INTO planned_workout(scheduled_date, workout_name) VALUES (?, ?)",
            ("2026-09-01", "Keep me"),
        )
        conn.commit()
    finally:
        conn.close()
    before = get_active_plan_snapshot(db_path)
    workbook_path = tmp_path / "must-not-exist.xlsx"
    report = TrainingPolicyReport(
        (PolicyIssue("error", "test", "Unsafe test baseline"),),
        0,
        0,
    )
    monkeypatch.setattr(builder, "evaluate_training_policy", lambda *_args, **_kwargs: report)

    with pytest.raises(builder.BaselinePolicyError, match="Unsafe test baseline"):
        builder.build_and_save_baseline(
            db_path,
            _request(),
            workbook_path=workbook_path,
        )

    assert get_active_plan_snapshot(db_path) == before
    assert not workbook_path.exists()


def test_workbook_failure_happens_before_database_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    db_path = _database(tmp_path)
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            "INSERT INTO planned_workout(scheduled_date, workout_name) VALUES (?, ?)",
            ("2026-09-01", "Keep me"),
        )
        conn.commit()
    finally:
        conn.close()
    before = get_active_plan_snapshot(db_path)
    monkeypatch.setattr(
        builder,
        "write_master_workbook",
        lambda **_kwargs: (_ for _ in ()).throw(OSError("disk full")),
    )

    with pytest.raises(builder.BaselineWorkbookError, match="disk full"):
        builder.build_and_save_baseline(
            db_path,
            _request(),
            workbook_path=tmp_path / "offline.xlsx",
        )

    assert get_active_plan_snapshot(db_path) == before
    assert not list(tmp_path.glob(".garmin-data-hub-plan-*.xlsx"))


def test_unexpected_workbook_failure_is_wrapped_and_cleans_temporary_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    db_path = _database(tmp_path)
    before = get_active_plan_snapshot(db_path)
    monkeypatch.setattr(
        builder,
        "write_master_workbook",
        lambda **_kwargs: (_ for _ in ()).throw(AttributeError("bad workbook")),
    )

    with pytest.raises(builder.BaselineWorkbookError, match="bad workbook"):
        builder.build_and_save_baseline(
            db_path,
            _request(),
            workbook_path=tmp_path / "offline.xlsx",
        )

    assert get_active_plan_snapshot(db_path) == before
    assert not list(tmp_path.glob(".garmin-data-hub-plan-*.xlsx"))


def test_stale_confirmation_does_not_replace_plan_or_leave_workbook(tmp_path):
    db_path = _database(tmp_path)
    expected_hash = get_active_plan_sha256(db_path)
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            "INSERT INTO planned_workout(scheduled_date, workout_name) VALUES (?, ?)",
            ("2026-09-01", "Added from another page"),
        )
        conn.commit()
    finally:
        conn.close()
    before = get_active_plan_snapshot(db_path)
    workbook_path = tmp_path / "stale.xlsx"

    with pytest.raises(StalePlanWriteError):
        builder.build_and_save_baseline(
            db_path,
            _request(),
            workbook_path=workbook_path,
            expected_active_plan_sha256=expected_hash,
        )

    assert get_active_plan_snapshot(db_path) == before
    assert not workbook_path.exists()
    assert not list(tmp_path.glob(".garmin-data-hub-plan-*.xlsx"))
