"""Static checks for security-sensitive Windows installer behavior."""

from __future__ import annotations

import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_postinstall_app_launch_uses_original_windows_user():
    installer = (
        PROJECT_ROOT / "packaging" / "installer" / "GarminDataHub.iss"
    ).read_text(encoding="utf-8")
    run_section = re.search(
        r"^\[Run\]\s*(.*?)(?=^\[|\Z)",
        installer,
        flags=re.DOTALL | re.MULTILINE,
    )

    assert run_section, "Installer [Run] section was not found"
    app_entry = next(
        (
            line
            for line in run_section.group(1).splitlines()
            if "{#MyAppExeName}" in line
        ),
        None,
    )
    assert app_entry, "Post-install GarminDataHub launch entry was not found"

    flags_match = re.search(r"Flags:\s*([^;]+)", app_entry, flags=re.IGNORECASE)
    assert flags_match, "Post-install app launch has no Flags value"
    flags = set(flags_match.group(1).lower().split())
    assert {"postinstall", "skipifsilent", "runasoriginaluser"} <= flags
    assert "runascurrentuser" not in flags


def test_setup_guide_is_in_release_portable_archive_and_installer():
    build_script = (PROJECT_ROOT / "packaging" / "build.ps1").read_text(
        encoding="utf-8"
    )
    installer = (
        PROJECT_ROOT / "packaging" / "installer" / "GarminDataHub.iss"
    ).read_text(encoding="utf-8")

    assert '$SetupUsageFile = Join-Path $ProjectRoot "SETUP_AND_USAGE.txt"' in build_script
    assert (
        "Copy-Item -LiteralPath $SetupUsageFile "
        "-Destination $ReleaseSetupUsageFile -Force"
    ) in build_script
    portable_line = next(
        line for line in build_script.splitlines() if line.strip().startswith("Compress-Archive")
        and "$PortableArchive" in line
    )
    assert "$ReleaseSetupUsageFile" in portable_line
    assert (
        'Source: "{#SourcePath}\\SETUP_AND_USAGE.txt"; '
        'DestDir: "{app}"; Flags: ignoreversion'
    ) in installer


def test_build_script_supports_signed_installer_only_release():
    build_script = (PROJECT_ROOT / "packaging" / "build.ps1").read_text(
        encoding="utf-8"
    )

    assert "[switch]$InstallerOnly" in build_script
    assert "[switch]$SignInstaller" in build_script
    assert "function Resolve-SignTool" in build_script
    assert "function Invoke-AuthenticodeSign" in build_script
    assert "Signing installer..." in build_script
    assert "Removing installer staging artifacts..." in build_script
    assert 'throw "Inno Setup Compiler (ISCC.exe) is required when -InstallerOnly is used."' in build_script


def test_signed_installer_path_runs_before_checksums():
    build_script = (PROJECT_ROOT / "packaging" / "build.ps1").read_text(
        encoding="utf-8"
    )

    assert (
        build_script.index("Invoke-AuthenticodeSign")
        < build_script.index("# --- CHECKSUMS AND SUMMARY ---")
    )
    assert (
        build_script.index("Removing installer staging artifacts...")
        < build_script.index("# --- CHECKSUMS AND SUMMARY ---")
    )
