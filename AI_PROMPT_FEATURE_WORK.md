# Garmin Data Hub AI Prompt: Feature Work

Use this when implementing new functionality.

## Copy/Paste Prompt

```md
You are implementing a new feature in Garmin Data Hub.

Project context:
- Python >= 3.10, package in src/garmin_data_hub.
- Streamlit app entry: src/garmin_data_hub/ui_streamlit/app.py
- CLI entry: src/garmin_data_hub/cli_backup_ingest.py
- Data layer: src/garmin_data_hub/db
- Ingest pipeline: src/garmin_data_hub/ingest
- Analytics: src/garmin_data_hub/analytics
- Exports: src/garmin_data_hub/exports
- Build pipeline: packaging/build.ps1

Goals for this task:
1. Implement the requested feature with minimal, focused changes.
2. Keep existing behavior unchanged outside the feature scope.
3. Preserve backward compatibility unless explicitly requested otherwise.

Required workflow:
1. Read all relevant files and summarize current behavior.
2. Propose a short implementation plan (data flow + file changes).
3. Implement incrementally and explain any schema/API changes.
4. Add/update tests for new behavior and edge cases.
5. Run validation commands relevant to changed areas.
6. Return clear changelog and residual risks.

Quality bar:
- Clear naming and small functions.
- Avoid hidden side effects.
- Handle invalid inputs explicitly.
- Prefer maintainable logic over clever shortcuts.

Validation options:
- python -m garmin_data_hub.cli_backup_ingest --help
- streamlit run src/garmin_data_hub/ui_streamlit/app.py
- pytest

Output format:
- Summary
- Files changed (and why)
- Behavior changes
- Validation run
- Risks/follow-ups

Current task:
[REPLACE THIS WITH FEATURE REQUEST]
```
