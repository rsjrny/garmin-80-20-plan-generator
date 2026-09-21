import json

from garmin_data_hub.ui_nicegui.mcp_results import result_sections, series_for


def test_nested_tool_summary_keeps_zero_null_and_error_distinct():
    sections = result_sections(json.dumps({"latest": {"score": 0, "hrv": None}, "error": "No sleep", "daily": []}))
    fields = next(s["fields"] for s in sections if s["kind"] == "fields")
    assert fields == [{"label": "Score", "value": "0"}, {"label": "Hrv", "value": "Not reported"}]
    assert any(s.get("error") and s["text"] == "No sleep" for s in sections)
    assert any(s.get("text") == "No records returned." for s in sections)


def test_unknown_tools_and_schema_have_readable_fallbacks():
    assert result_sections("Sync started")[0]["text"] == "Sync started"
    table = result_sections('{"sleep":{"columns":["date","hours"],"row_count":12}}')[0]
    assert table["title"] == "Database tables"
    assert table["rows"][0]["row_count"] == 12
    rows = result_sections('[{"first":1},{"second":2}]')[0]["rows"]
    assert rows == [{"first": 1, "second": None}, {"first": None, "second": 2}]


def test_chart_uses_dated_values_without_missing_zero_or_id_confusion():
    result = series_for([
        {"date": "2026-09-20", "hrv": 60, "activity_id": 17, "ready": True},
        {"date": "2026-09-18", "hrv": 0},
        {"date": "2026-09-19", "hrv": None},
        {"date": "invalid", "hrv": 200},
        {"date": "2026-09-21", "hrv": float("nan")},
    ])
    assert list(result) == ["hrv"]
    assert result["hrv"]["mean"] == 30
    assert result["hrv"]["latest"] == 60
    assert result["hrv"]["count"] == 2
    assert result["hrv"]["plot_points"][1] == ("2026-09-19", None)
    assert series_for([{"id": 7, "value": 4}]) == {}


def test_large_result_is_bounded_and_total_is_preserved():
    table = result_sections(json.dumps([{"score": n} for n in range(1100)]))[0]
    assert table["count"] == 1100
    assert len(table["rows"]) == 1000
