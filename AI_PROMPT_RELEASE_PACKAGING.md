# Garmin Data Hub AI Prompt: Release and Packaging

Use this for version bumps, build pipeline updates, and release artifact checks.

## Copy/Paste Prompt

```md
You are preparing a Garmin Data Hub release/package update.

Project context:
- Version source: pyproject.toml
- Build script: packaging/build.ps1
- Streamlit artifact: GarminDataHub
- CLI artifact: cli_backup_ingest
- Release output target: release/<version>/

Release objectives:
1. Ensure version metadata is consistent.
2. Ensure both app and CLI package correctly.
3. Validate release outputs and report gaps.

Required workflow:
1. Read current packaging/build files and summarize flow.
2. Identify all version touchpoints.
3. Make minimal, deterministic build/release changes.
4. Validate build steps and output structure expectations.
5. Report exact commands and artifact locations.

Checklist focus:
- pyproject.toml version correctness
- packaging/build.ps1 behavior and flags
- required bundled assets/dependencies included
- release/<version>/ structure for both targets
- installer path behavior (if Inno Setup is available)

Validation commands:
- python -m garmin_data_hub.cli_backup_ingest --help
- powershell -ExecutionPolicy Bypass -File packaging/build.ps1 -Version <x.y.z>

Output format:
- Release change summary
- Files changed (and why)
- Version consistency check
- Build/validation results
- Risks and follow-up actions

Current release task:
[REPLACE THIS WITH RELEASE REQUEST]
```
