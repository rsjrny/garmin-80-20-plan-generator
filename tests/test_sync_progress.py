from garmin_data_hub.ui_streamlit.sync_status import (
    derived_refresh_summary_from_log,
    progress_from_log,
    sync_completed_from_state,
)


SAMPLE_RUNNING_LOG = """
Started: python cli_backup_ingest.py --db garmin.db --visible --chrome
============================================================
Garmin Data Sync  (garmin-givemydata)
============================================================
Already logged in (session restored)
Fetching 2026-04-04 to 2026-04-07 (3 days)...
Activities page: fetched 100 more (offset 100)
[OK] Sync completed
[OK] App schema applied
[trackpoints] done: 1 activities, 3716 points, 0 errors
[OK] Derived tables refreshed: targets=1, upserted=1, zones=1
"""

SAMPLE_SUCCESS_LOG = SAMPLE_RUNNING_LOG + "[SUCCESS] Sync complete\n"


def test_progress_from_log_reaches_near_complete_for_post_sync_steps():
    progress = progress_from_log(SAMPLE_RUNNING_LOG)
    assert progress >= 0.95


def test_progress_from_log_reaches_complete_on_success_marker():
    assert progress_from_log(SAMPLE_SUCCESS_LOG) == 1.0


def test_sync_completed_from_state_uses_success_log_even_if_return_code_missing():
    assert sync_completed_from_state(None, SAMPLE_SUCCESS_LOG) is True
    assert sync_completed_from_state(None, SAMPLE_RUNNING_LOG) is False


def test_derived_refresh_summary_from_log_parses_counts():
    assert derived_refresh_summary_from_log(SAMPLE_SUCCESS_LOG) == {
        "target_activities": 1,
        "rows_upserted": 1,
        "zones_updated": 1,
    }
