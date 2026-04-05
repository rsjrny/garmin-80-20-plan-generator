# Garmin Data Hub AI Prompt: Smart Router

Use this one prompt for all update types. The assistant should classify the request and then follow the matching workflow.

## Copy/Paste Prompt

You are updating Garmin Data Hub.

Project context:
- Python >= 3.10
- Package root: src/garmin_data_hub
- Streamlit app entry: src/garmin_data_hub/ui_streamlit/app.py
- CLI sync entry: src/garmin_data_hub/cli_backup_ingest.py
- DB layer: src/garmin_data_hub/db
- Ingest pipeline: src/garmin_data_hub/ingest
- Analytics: src/garmin_data_hub/analytics
- Exports: src/garmin_data_hub/exports
- Build and release pipeline: packaging/build.ps1
- Version source: pyproject.toml
- Release output target: release/<version>/

Step 1: Route the task to one mode.
- Mode FEATURE when task asks for new functionality, enhancements, UX changes, or additional options.
- Mode BUGFIX when task asks to fix errors, regressions, incorrect results, crashes, or unexpected behavior.
- Mode RELEASE when task asks for version bumps, packaging/build changes, installer updates, or release artifact validation.

Step 2: Follow the matching workflow.

FEATURE workflow:
1. Read relevant files and summarize current behavior.
2. Propose a short implementation plan.
3. Implement with minimal, targeted diffs.
4. Keep existing behavior stable outside feature scope.
5. Add or update tests for new behavior and edge cases.
6. Run focused validation and report results.

BUGFIX workflow:
1. Restate failure scenario and expected behavior.
2. Trace execution path and identify likely fault points.
3. Confirm root cause with concrete evidence.
4. Implement smallest safe fix.
5. Add regression coverage and nearby edge cases.
6. Run focused validation and report results.

RELEASE workflow:
1. Read packaging files and summarize release flow.
2. Verify all version touchpoints.
3. Make deterministic packaging/release edits.
4. Validate expected output structure and artifacts.
5. Report command outcomes and artifact paths.

Global engineering rules:
- Keep changes minimal and maintainable.
- Preserve public interfaces unless the task explicitly requires changes.
- Do not silently swallow errors.
- Explain behavior changes clearly.
- If blocked, state exactly what is missing and the smallest next step.

Validation command options:
- python -m garmin_data_hub.cli_backup_ingest --help
- streamlit run src/garmin_data_hub/ui_streamlit/app.py
- pytest
- powershell -ExecutionPolicy Bypass -File packaging/build.ps1 -Version <x.y.z>

Required output format:
- Chosen mode and why
- Summary of changes
- Files changed and reason for each
- Validation run and results
- Risks or follow-up actions

Current task:
[REPLACE THIS WITH YOUR REQUEST]

## Example task lines
- Add a distance filter on Activities page and tests.
- Fix duplicate trackpoint inserts when re-importing one activity.
- Prepare release 0.2.6 and verify both app and CLI artifacts.
