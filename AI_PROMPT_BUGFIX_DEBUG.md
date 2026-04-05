# Garmin Data Hub AI Prompt: Bug Fix and Debug

Use this for troubleshooting regressions, errors, and data issues.

## Copy/Paste Prompt

```md
You are debugging and fixing a bug in Garmin Data Hub.

Project context:
- Python >= 3.10, package in src/garmin_data_hub.
- Streamlit UI: src/garmin_data_hub/ui_streamlit
- CLI sync tool: src/garmin_data_hub/cli_backup_ingest.py
- DB/query code: src/garmin_data_hub/db
- Ingest/trackpoints: src/garmin_data_hub/ingest
- Analytics/business logic: src/garmin_data_hub/analytics

Debug objectives:
1. Identify root cause (not just symptoms).
2. Provide minimal, low-risk fix.
3. Add guards/tests to prevent recurrence.

Required workflow:
1. Re-state the failure scenario and expected behavior.
2. Trace execution path and identify likely fault points.
3. Confirm root cause with evidence from code/logs/tests.
4. Implement fix with the smallest safe diff.
5. Add/update tests for failing case and nearby edge cases.
6. Run focused validation and report exact outcomes.

Fix principles:
- Do not silently swallow errors.
- Preserve existing contracts unless task says otherwise.
- If changing behavior, document before/after clearly.
- If uncertain, include instrumentation or assertions.

Validation options:
- python -m garmin_data_hub.cli_backup_ingest --help
- pytest
- streamlit run src/garmin_data_hub/ui_streamlit/app.py

Output format:
- Root cause
- Fix summary
- Files changed (and why)
- Tests added/updated
- Validation run
- Remaining risks

Bug report / task:
[REPLACE THIS WITH BUG DETAILS]
```
