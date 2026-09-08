# Packaging Pipeline

This repository uses a **Windows/PowerShell** packaging flow driven by `packaging/build.ps1`.

## 1. Prerequisites

Before packaging, ensure:

- the project and packaging dependencies are installed with `.venv\Scripts\python.exe -m pip install -e ".[dev]"`
- project virtual environment exists at `.venv` (build uses `.venv\\Scripts\\python.exe`)
- `garmin-givemydata` is available on PyPI (build upgrades/installs it in `.venv`)
- optional: `ISCC.exe` is installed if you want the Inno Setup installer built
- optional but recommended for distribution: Windows SDK `signtool.exe` and a trusted code-signing certificate for installer signing

## 2. Build Command

From the repository root:

```powershell
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 1.0.0 -GivemydataPypiSpec "garmin-givemydata==0.1.12"
```

Installer-only signed release:

```powershell
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 1.0.0 -InstallerOnly -SignInstaller -CertificateThumbprint "<cert-sha1-thumbprint>"
```

Signing alternatives:

```powershell
# Certificate selected by subject name from the Windows certificate store
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 1.0.0 -InstallerOnly -SignInstaller -CertificateSubject "Your Publisher Name"

# PFX certificate file
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 1.0.0 -InstallerOnly -SignInstaller -CertificateFile ".\certs\publisher.pfx"
```

Common variants:

```powershell
# Reproducible release build
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 1.0.0 -GivemydataPypiSpec "garmin-givemydata==0.1.12"

# Validate the installed pin without downloading it again
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 1.0.0 -SkipGivemydataUpdate
```

## 2.1 Build Modes Quick Reference

Use these ready-to-run commands for current release `1.0.0`:

```powershell
# Default exact upstream pin
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 1.0.0

# PyPI mode with explicit pin
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 1.0.0 -GivemydataPypiSpec "garmin-givemydata==0.1.12"

# PyPI mode, no auto-update (validate-only)
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 1.0.0 -SkipGivemydataUpdate
```

## 3. What `build.ps1` Does

1. Updates `pyproject.toml` with the requested version
2. Resolves and validates `garmin-givemydata` in `.venv` from PyPI (`-GivemydataPypiSpec`)
3. Cleans and recreates `build/` and `release/<version>/`
4. Builds the NiceGUI desktop app into `build/nicegui_app/dist/GarminDataHub/`
5. Builds the sync CLI into `build/cli_tool/dist/cli_backup_ingest/`
6. Bundles `garmin-givemydata` into the frozen CLI and dispatches it through that executable
7. Copies the final outputs to `release/<version>/`
8. Builds portable and corresponding-source ZIP files
9. Optionally builds the installer from `packaging/installer/GarminDataHub.iss`
10. Optionally signs the installer with Authenticode when `-SignInstaller` is supplied
11. Optionally removes staging/portable artifacts after installer creation when `-InstallerOnly` is supplied
12. Writes `SHA256SUMS.txt`

## 4. Expected Release Outputs

After a successful run, verify:

- `release/<version>/GarminDataHub/GarminDataHub.exe`
- `release/<version>/cli_backup_ingest/cli_backup_ingest.exe`
- `release/<version>/GarminDataHub-<version>-portable.zip`
- `release/<version>/GarminDataHub-<version>-source.zip`
- `release/<version>/GarminDataHub-<version>-installer.exe` when Inno Setup is available
- `release/<version>/SHA256SUMS.txt`

## 5. Validation Checklist

Before publishing a release, confirm:

- the app launches successfully from the packaged GUI build
- the CLI help works: `cli_backup_ingest.exe --help`
- the sync flow can open the browser and finish a run
- the installer upgrades the previous version without creating a second uninstall entry
- `src/garmin_data_hub/db/schema.sql` is included in the packaged artifacts
- version numbers match the intended release
- project and AGPL licenses, third-party notices, and corresponding source are present

## 6. Known Gotchas

- `pyproject.toml` must be written back as **UTF-8 without BOM** or `pytest`/packaging tools may fail to parse it correctly
- `schema.sql` must be included via PyInstaller `--add-data` or the packaged app will fail to initialize the DB schema
- the packaging flow is currently **Windows-first**; the examples here are not intended for Unix shell usage
- the installer is unsigned unless `-SignInstaller` is used with a trusted code-signing certificate
- `garmin-givemydata` is AGPL-3.0-only; public binary releases must retain the included license, notices, and corresponding source
- `pyinstaller-hooks-contrib >= 2026.3` is required for the compiled `charset-normalizer >= 3.4.5` runtime; the `dev` extra pins a tested version

## 7. Related Files

- `packaging/build.ps1`
- `packaging/launcher.py`
- `packaging/installer/GarminDataHub.iss`
- `pyproject.toml`
