# Garmin Data Hub: Reusable AI Update Prompt

Use this prompt when asking an AI assistant to make code changes in this repository.

Quick launcher: use `AI_PROMPT_SMART_ROUTER.md` as your default one-file template for feature, bugfix, and release tasks.

## Copy/Paste Prompt

```md
You are updating the Garmin Data Hub project.

Project summary:
- Python project: local-first Garmin analytics and sync tooling.
- Main app: Streamlit UI.
- Main CLI: Garmin backup/sync ingest tool.
- Data store: SQLite (default in %LOCALAPPDATA%\\GarminDataHub\\garmin.db, override via GARMIN_DATA_HUB_DB).
- Primary package: src/garmin_data_hub.

Architecture map:
- src/garmin_data_hub/cli_backup_ingest.py: CLI sync entry point.
- src/garmin_data_hub/ui_streamlit/app.py: Streamlit entry point.
- src/garmin_data_hub/ui_streamlit/pages/: Streamlit pages.
- src/garmin_data_hub/db/: schema, migrations, SQLite helpers, queries.
- src/garmin_data_hub/ingest/: FIT/CSV parsing, fingerprinting, trackpoint ingest, writing.
- src/garmin_data_hub/analytics/: analytics/business logic.
- src/garmin_data_hub/exports/: export logic.
- packaging/build.ps1: builds Streamlit + CLI artifacts into release/<version>/.

Tech constraints:
- Python >= 3.10.
- Dependencies managed in pyproject.toml.
- Keep changes minimal and targeted.
- Preserve existing behavior unless explicitly asked to change it.
- Prefer clear, maintainable code over clever code.

When you work:
1. Read relevant files first and summarize your understanding.
2. Propose a short plan for multi-file changes.
3. Edit only files needed for the request.
4. Keep public interfaces stable unless the task requires interface changes.
5. Add or update tests when behavior changes.
6. Run focused validation (tests/lint/CLI help) and report results.
7. List changed files and explain why each changed.

Validation commands (pick what matches the task):
- python -m garmin_data_hub.cli_backup_ingest --help
- streamlit run src/garmin_data_hub/ui_streamlit/app.py
- pytest

Packaging context:
- Build pipeline: packaging/build.ps1 -Version <x.y.z>
- Outputs should land in release/<version>/ for both GarminDataHub and cli_backup_ingest.

Output format requirements:
- Start with a concise summary of what you changed.
- Then provide:
  - Files changed
  - Key implementation notes
  - Validation performed
  - Any follow-ups or risks
- If blocked, state exactly what is missing and the smallest next step.

Current task:
[REPLACE WITH YOUR SPECIFIC REQUEST]
```

## Example Usage

1. Copy the "Copy/Paste Prompt" block above.
2. Replace "Current task" with your request.
3. Send it to your AI coding assistant.

Example task line:

`Current task: Add a new filter to the Activities page to show only runs above a minimum distance, and include/update tests.`
