"""Validate installer configuration and controlled release sequencing."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest


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


@pytest.mark.parametrize("signed", [True, False], ids=["signed", "unsigned"])
def test_installer_only_release_hashes_the_final_artifact(tmp_path, signed):
    shell = shutil.which("pwsh") or shutil.which("powershell")
    if shell is None:
        pytest.skip("Controlled PowerShell release validation runs in the Windows CI job")
    source = (PROJECT_ROOT / "packaging" / "build.ps1").read_text(encoding="utf-8")
    section = source.split("# --- BUILD INSTALLER ---", 1)[1].split(
        '\nWrite-Host ""\nWrite-Host "###################################################"', 1
    )[0]
    release = str(tmp_path).replace("'", "''")
    harness = RELEASE_HARNESS.replace("__RELEASE__", release).replace(
        "__SIGN__", "$true" if signed else "$false"
    )
    script = tmp_path / "release-validation.ps1"
    script.write_text(harness + "\n" + section + "\n" + RELEASE_RESULT, encoding="utf-8")
    completed = subprocess.run(
        [shell, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(script)],
        capture_output=True, text=True, timeout=30,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    events = result["events"]
    assert events[0] == "compile"
    assert ("sign" in events) is signed
    assert events[-1] == ("hash:signed" if signed else "hash:unsigned")
    if signed:
        assert events.index("sign") < events.index("remove-staging") < len(events) - 1
    artifact = tmp_path / "GarminDataHub-test-installer.exe"
    expected = hashlib.sha256(artifact.read_bytes()).hexdigest()
    assert result["checksums"] == [expected + " *" + artifact.name]


def test_signing_invocation_and_staging_cleanup_precede_checksum_execution():
    script = (PROJECT_ROOT / "packaging" / "build.ps1").read_text(encoding="utf-8")
    invocation = re.search(r"(?m)^\s+Invoke-AuthenticodeSign\s+\x60\s*$", script)
    checksum = re.search(r"(?m)^\s+\$hash = Get-FileHash -LiteralPath \$item -Algorithm SHA256", script)
    cleanup = re.search(r"(?m)^\s+Remove-Item -LiteralPath \$artifact -Recurse -Force", script)
    assert invocation is not None, "Missing actual signing invocation"
    assert checksum is not None, "Missing checksum computation"
    assert cleanup is not None, "Missing installer-only staging cleanup"
    assert invocation.start() < cleanup.start() < checksum.start()


# Only the isolated installer/checksum section is executed. All compiler,
# signing and removal commands are stand-ins; no real artifacts are deleted.
RELEASE_HARNESS = r"""
$ErrorActionPreference = 'Stop'
$ReleaseDir = '__RELEASE__'
$Version = 'test'
$InstallerOnly = $true
$SignInstaller = __SIGN__
$events = [System.Collections.Generic.List[string]]::new()
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$DestinationAppDir = Join-Path $ReleaseDir 'app-staging'
$DestinationCliDir = Join-Path $ReleaseDir 'cli-staging'
$ReleaseLicenseFile = Join-Path $ReleaseDir 'license'
$ReleaseNoticesFile = Join-Path $ReleaseDir 'notices'
$ReleaseSetupUsageFile = Join-Path $ReleaseDir 'setup'
$GivemydataLicenseFile = Join-Path $ReleaseDir 'upstream-license'
$ReleaseSourceDir = Join-Path $ReleaseDir 'source'
$SourceArchive = Join-Path $ReleaseDir 'source.zip'
$SourceInfoFile = Join-Path $ReleaseDir 'source-info'
$IssScriptFile = Join-Path $ReleaseDir 'synthetic.iss'
function Write-Host { }
function Get-Command { return [pscustomobject]@{Source='Invoke-FakeCompiler'} }
function Invoke-FakeCompiler {
    $events.Add('compile')
    [System.IO.File]::WriteAllText((Join-Path $ReleaseDir 'GarminDataHub-test-installer.exe'), 'unsigned')
    $global:LASTEXITCODE = 0
}
function Resolve-SignTool { return 'synthetic-sign-tool' }
function Invoke-AuthenticodeSign {
    param($FilePath,$ToolPath,$Thumbprint,$Subject,$PfxFile,$PfxPassword,$TimestampServer)
    $events.Add('sign')
    [System.IO.File]::WriteAllText($FilePath, 'signed')
}
function Get-AuthenticodeSignature {
    param($LiteralPath)
    return [pscustomobject]@{Status='Valid'}
}
function Test-Path { return $true }
function Remove-Item { $events.Add('remove-staging') }
function Get-FileHash {
    param($LiteralPath,$Algorithm)
    $events.Add('hash:' + [System.IO.File]::ReadAllText($LiteralPath))
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = $sha.ComputeHash([System.IO.File]::ReadAllBytes($LiteralPath))
        return [pscustomobject]@{Hash=([System.BitConverter]::ToString($bytes).Replace('-', ''))}
    } finally { $sha.Dispose() }
}
"""

RELEASE_RESULT = r"""
[pscustomobject]@{events=@($events);checksums=@([System.IO.File]::ReadAllLines($ChecksumFile))} |
    ConvertTo-Json -Compress
"""
