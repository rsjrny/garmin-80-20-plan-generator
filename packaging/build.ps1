# Complete Build Pipeline for GarminDataHub
# Builds both the NiceGUI desktop application and the CLI tool, then organizes them into a release directory.

param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$')]
    [string]$Version,
    [switch]$SkipGivemydataUpdate,
    [string]$GivemydataPypiSpec = "garmin-givemydata==0.1.12"
)

# Set error action preference to stop on errors
$ErrorActionPreference = "Stop"

# --- PATHS ---
# Get script directory and project root
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir

# Source file paths
$CliBackupFile = Join-Path $ProjectRoot "src\garmin_data_hub\cli_backup_ingest.py"
$LauncherFile = Join-Path $ScriptDir "launcher.py"

# Build and release paths
$BuildDir = Join-Path $ProjectRoot "build"
$ReleaseRoot = Join-Path $ProjectRoot "release"
$ReleaseDir = Join-Path $ReleaseRoot $Version
$GuiBuildDir = Join-Path $BuildDir "nicegui_app"
$CliBuildDir = Join-Path $BuildDir "cli_tool"
$GuiDistDir = Join-Path $GuiBuildDir "dist"
$CliDistDir = Join-Path $CliBuildDir "dist"
$PyProjectPath = Join-Path $ProjectRoot "pyproject.toml"
$PackageInitPath = Join-Path $ProjectRoot "src\garmin_data_hub\__init__.py"
$VenvPython = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$IssScriptFile = Join-Path $ScriptDir "installer\GarminDataHub.iss"
$LicenseFile = Join-Path $ProjectRoot "LICENSE"
$ThirdPartyNoticesFile = Join-Path $ProjectRoot "THIRD_PARTY_NOTICES.md"
$SetupUsageFile = Join-Path $ProjectRoot "SETUP_AND_USAGE.txt"

function Update-PyProjectVersion {
    param(
        [string]$FilePath,
        [string]$NewVersion
    )

    if (-not (Test-Path $FilePath)) {
        Write-Host "[WARNING] pyproject.toml not found at $FilePath; skipping version update." -ForegroundColor Yellow
        return
    }

    $lines = Get-Content $FilePath
    $inProject = $false
    $updatedAny = $false

    for ($i = 0; $i -lt $lines.Length; $i++) {
        $line = $lines[$i]
        $normalizedLine = $line.TrimStart([char]0xFEFF)

        if ($normalizedLine -match '^\s*\[project\]\s*$') {
            $inProject = $true
            continue
        }

        if ($inProject -and $normalizedLine -match '^\s*\[.+\]\s*$') {
            $inProject = $false
        }

        if ($inProject -and $normalizedLine -match '^\s*version\s*=\s*"[^"]*"\s*$') {
            $lines[$i] = "version = `"$NewVersion`""
            $updatedAny = $true
            break
        }
    }

    if (-not $updatedAny) {
        Write-Host "[WARNING] Could not find [project] version line in pyproject.toml; no change made." -ForegroundColor Yellow
        return
    }

    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllLines($FilePath, $lines, $utf8NoBom)
    Write-Host "Updated pyproject version to $NewVersion" -ForegroundColor Green
}

function Update-PackageVersion {
    param(
        [string]$FilePath,
        [string]$NewVersion
    )

    if (-not (Test-Path -LiteralPath $FilePath -PathType Leaf)) {
        throw "Package metadata file not found: $FilePath"
    }

    $lines = Get-Content -LiteralPath $FilePath
    $updatedAny = $false
    for ($i = 0; $i -lt $lines.Length; $i++) {
        if ($lines[$i] -match '^__version__\s*=\s*"[^"]*"\s*$') {
            $lines[$i] = "__version__ = `"$NewVersion`""
            $updatedAny = $true
            break
        }
    }
    if (-not $updatedAny) {
        throw "Could not find __version__ in package metadata: $FilePath"
    }

    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllLines($FilePath, $lines, $utf8NoBom)
    Write-Host "Updated package version to $NewVersion" -ForegroundColor Green
}

function Ensure-GivemydataFromPypi {
    param(
        [string]$PythonExe,
        [string]$PackageSpec,
        [switch]$TryAutoUpdate
    )

    if (-not (Test-Path $PythonExe)) {
        Write-Host "[ERROR] Expected venv Python not found: $PythonExe" -ForegroundColor Red
        exit 1
    }

    if ([string]::IsNullOrWhiteSpace($PackageSpec)) {
        Write-Host "[ERROR] Empty package spec provided for PyPI source." -ForegroundColor Red
        exit 1
    }

    if ($TryAutoUpdate) {
        Write-Host "[INFO] Ensuring PyPI package is installed in .venv: $PackageSpec" -ForegroundColor Cyan
        & $PythonExe -m pip install --upgrade "$PackageSpec"
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERROR] Failed to install/upgrade '$PackageSpec' in .venv." -ForegroundColor Red
            exit 1
        }
    }

    $probeLines = & $PythonExe -c "import json, sys
try:
    import garmin_givemydata as g
    from importlib.metadata import version
    print(json.dumps({'ok': True, 'module_file': getattr(g, '__file__', ''), 'version': version('garmin-givemydata')}))
except Exception as e:
    print(json.dumps({'ok': False, 'error': str(e)}))
    sys.exit(1)
" 2>$null

    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Could not import garmin_givemydata from .venv." -ForegroundColor Red
        Write-Host "        Install it with: $PythonExe -m pip install --upgrade `"$PackageSpec`"" -ForegroundColor Yellow
        exit 1
    }

    $probeJson = ($probeLines | Select-Object -Last 1)
    $probe = $probeJson | ConvertFrom-Json
    $moduleFile = [string]$probe.module_file
    $version = [string]$probe.version
    Write-Host "[OK] Using PyPI garmin-givemydata $version from: $moduleFile" -ForegroundColor Green
}

function New-PyInstallerVersionFile {
    param(
        [string]$FilePath,
        [string]$ReleaseVersion,
        [string]$Description,
        [string]$InternalName,
        [string]$OriginalFilename
    )

    $numericVersion = ($ReleaseVersion -split '[-+]')[0]
    $parts = @($numericVersion.Split('.') | ForEach-Object { [int]$_ })
    if ($parts.Count -ne 3) {
        throw "PyInstaller VersionInfo requires a three-part numeric version: $ReleaseVersion"
    }
    $versionTuple = "$($parts[0]), $($parts[1]), $($parts[2]), 0"
    $contents = @"
VSVersionInfo(
  ffi=FixedFileInfo(
    filevers=($versionTuple),
    prodvers=($versionTuple),
    mask=0x3f,
    flags=0x0,
    OS=0x40004,
    fileType=0x1,
    subtype=0x0,
    date=(0, 0)
  ),
  kids=[
    StringFileInfo([
      StringTable(
        u'040904B0',
        [
          StringStruct(u'CompanyName', u'Garmin Data Hub'),
          StringStruct(u'FileDescription', u'$Description'),
          StringStruct(u'FileVersion', u'$ReleaseVersion'),
          StringStruct(u'InternalName', u'$InternalName'),
          StringStruct(u'LegalCopyright', u'Copyright (c) 2024 Garmin Data Hub'),
          StringStruct(u'OriginalFilename', u'$OriginalFilename'),
          StringStruct(u'ProductName', u'GarminDataHub'),
          StringStruct(u'ProductVersion', u'$ReleaseVersion')
        ]
      )
    ]),
    VarFileInfo([VarStruct(u'Translation', [1033, 1200])])
  ]
)
"@
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($FilePath, $contents, $utf8NoBom)
}

function Assert-PackagingEnvironment {
    param([string]$PythonExe)

    Write-Host "[INFO] Validating packaging dependencies..." -ForegroundColor Cyan
    & $PythonExe -c "from importlib.metadata import version
from packaging.version import Version
import PyInstaller, garmin_client, garmin_givemydata, garmin_mcp, nicegui, seleniumbase, webview
hooks_version = Version(version('pyinstaller-hooks-contrib'))
if hooks_version < Version('2026.3'):
    raise SystemExit(f'pyinstaller-hooks-contrib >= 2026.3 is required for charset-normalizer 3.4.5+; found {hooks_version}')
"
    if ($LASTEXITCODE -ne 0) {
        throw "Packaging dependencies are incomplete or incompatible. Run: .venv\Scripts\python.exe -m pip install -e '.[dev]'"
    }
    if (-not (Test-Path -LiteralPath $IssScriptFile -PathType Leaf)) {
        throw "Inno Setup script not found: $IssScriptFile"
    }
    if (-not (Test-Path -LiteralPath $LicenseFile -PathType Leaf)) {
        throw "License file not found: $LicenseFile"
    }
    if (-not (Test-Path -LiteralPath $ThirdPartyNoticesFile -PathType Leaf)) {
        throw "Third-party notices file not found: $ThirdPartyNoticesFile"
    }
    if (-not (Test-Path -LiteralPath $SetupUsageFile -PathType Leaf)) {
        throw "Setup and usage guide not found: $SetupUsageFile"
    }
}

function Invoke-PackagedSmokeTest {
    param(
        [string]$Executable,
        [string[]]$Arguments
    )

    # Native stderr is represented as a PowerShell error record. Judge the
    # smoke test by the process exit code while keeping its output quiet.
    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "SilentlyContinue"
        & $Executable @Arguments *> $null
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }
    if ($exitCode -ne 0) {
        throw "Packaged smoke test failed with exit code $exitCode`: $Executable $($Arguments -join ' ')"
    }
}

# --- SETUP ---
Write-Host "###################################################" -ForegroundColor Magenta
Write-Host "# GarminDataHub Build Pipeline                    #" -ForegroundColor Magenta
Write-Host "###################################################" -ForegroundColor Magenta
Write-Host ""
Write-Host "Building version: $Version" -ForegroundColor Cyan
Write-Host "garmin-givemydata package spec: $GivemydataPypiSpec" -ForegroundColor Cyan
Update-PyProjectVersion -FilePath $PyProjectPath -NewVersion $Version
Update-PackageVersion -FilePath $PackageInitPath -NewVersion $Version
Ensure-GivemydataFromPypi -PythonExe $VenvPython -PackageSpec $GivemydataPypiSpec -TryAutoUpdate:(-not $SkipGivemydataUpdate)
Assert-PackagingEnvironment -PythonExe $VenvPython

# Clean and create directories
if (Test-Path $BuildDir) {
    Write-Host "Cleaning build directory..." -ForegroundColor Yellow
    Remove-Item $BuildDir -Recurse -Force
}
if (Test-Path $ReleaseDir) {
    Write-Host "Cleaning previous release directory..." -ForegroundColor Yellow
    Remove-Item $ReleaseDir -Recurse -Force
}
New-Item -ItemType Directory -Path $ReleaseDir -Force | Out-Null
New-Item -ItemType Directory -Path $GuiBuildDir -Force | Out-Null
New-Item -ItemType Directory -Path $CliBuildDir -Force | Out-Null

$GuiVersionFile = Join-Path $GuiBuildDir "version_info.txt"
$CliVersionFile = Join-Path $CliBuildDir "version_info.txt"
New-PyInstallerVersionFile -FilePath $GuiVersionFile -ReleaseVersion $Version -Description "Garmin Data Hub desktop application" -InternalName "GarminDataHub" -OriginalFilename "GarminDataHub.exe"
New-PyInstallerVersionFile -FilePath $CliVersionFile -ReleaseVersion $Version -Description "Garmin Data Hub sync command-line tool" -InternalName "cli_backup_ingest" -OriginalFilename "cli_backup_ingest.exe"
$numericVersion = ($Version -split '[-+]')[0]
$InstallerNumericVersion = "$numericVersion.0"

# --- BUILD NICEGUI APP ---
Write-Host ""
Write-Host "---------------------------------------------------" -ForegroundColor Cyan
Write-Host "Step 1: Building NiceGUI Desktop Application (as a directory)" -ForegroundColor Cyan
Write-Host "---------------------------------------------------"
Push-Location $ProjectRoot

$GuiAppName = "GarminDataHub"
$pyinstallerArgsGui = @(
    "--windowed",
    "--name", $GuiAppName,
    "--version-file", $GuiVersionFile,
    "--distpath", $GuiDistDir,
    "--workpath", (Join-Path $GuiBuildDir "build"),
    "--specpath", $GuiBuildDir,
    "--add-data", ((Join-Path $ProjectRoot 'src\garmin_data_hub\db\schema.sql') + ";garmin_data_hub/db"),
    "--collect-all", "nicegui",
    "--collect-all", "webview",
    "--collect-all", "garmin_mcp",
    "--hidden-import", "webview.platforms.edgechromium",
    "--hidden-import", "pandas",
    "--hidden-import", "plotly",
    $LauncherFile
)

try {
    Write-Host "Running PyInstaller for NiceGUI app..."
    & $VenvPython -m PyInstaller @pyinstallerArgsGui
    if ($LASTEXITCODE -ne 0) { throw "PyInstaller failed for NiceGUI app." }
    Write-Host "[SUCCESS] NiceGUI app built." -ForegroundColor Green
}
catch {
    Write-Host "[ERROR] Failed to build NiceGUI application." -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

Pop-Location

# --- BUILD CLI TOOLCHAIN ---
Write-Host ""
Write-Host "---------------------------------------------------" -ForegroundColor Cyan
Write-Host "Step 2: Building CLI Tool" -ForegroundColor Cyan
Write-Host "---------------------------------------------------"
Push-Location $ProjectRoot

$CliAppName = "cli_backup_ingest"

# Build the orchestrator and its internally dispatched garmin-givemydata worker (one-dir)
$pyinstallerArgsCli = @(
    "--clean",
    "--console",
    "--name", $CliAppName,
    "--version-file", $CliVersionFile,
    "--distpath", $CliDistDir,
    "--contents-directory", ".",
    "--workpath", (Join-Path $CliBuildDir "build"),
    "--specpath", $CliBuildDir,
    "--add-data", ((Join-Path $ProjectRoot 'src\garmin_data_hub\db\schema.sql') + ";garmin_data_hub/db"),
    "--hidden-import", "garmin_givemydata",
    "--collect-all", "garmin_client",
    "--collect-all", "garmin_mcp",
    "--collect-all", "seleniumbase",
    $CliBackupFile
)

try {
    Write-Host "Running PyInstaller for CLI tool..."
    & $VenvPython -m PyInstaller @pyinstallerArgsCli
    if ($LASTEXITCODE -ne 0) { throw "PyInstaller failed for CLI tool." }
    Write-Host "[SUCCESS] CLI tool built." -ForegroundColor Green
}
catch {
    Write-Host "[ERROR] Failed to build CLI tool." -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

$CliDir = Join-Path $CliDistDir $CliAppName
if (-not (Test-Path $CliDir)) {
    Write-Host "[ERROR] CLI output directory not found at $CliDir" -ForegroundColor Red
    exit 1
}

Pop-Location

# --- ORGANIZE RELEASE ARTIFACTS ---
Write-Host ""
Write-Host "---------------------------------------------------" -ForegroundColor Cyan
Write-Host "Step 3: Organizing Release Artifacts" -ForegroundColor Cyan
Write-Host "---------------------------------------------------"

$DestinationCliDir = Join-Path $ReleaseDir $CliAppName
$DestinationAppDir = Join-Path $ReleaseDir $GuiAppName

# Copy NiceGUI app directory
$GuiAppDir = Join-Path $GuiDistDir $GuiAppName
if (Test-Path $GuiAppDir) {
    Copy-Item -Path $GuiAppDir -Destination $ReleaseDir -Recurse -Force
    Write-Host "  Copied: $GuiAppName (directory)" -ForegroundColor Green
}
else {
    Write-Host "  [ERROR] NiceGUI application directory not found at $GuiAppDir" -ForegroundColor Red
}

# Copy the complete CLI directory; the GUI resolves it as a sibling in portable releases.
if (Test-Path $CliDir) {
    Copy-Item -Path $CliDir -Destination $DestinationCliDir -Recurse -Force
    Write-Host "  Copied: $CliAppName (directory)" -ForegroundColor Green
}
else {
    Write-Host "  [ERROR] CLI directory not found at $CliDir" -ForegroundColor Red
}

# Validate expected release executables exist at final locations
$ExpectedGuiExePath = Join-Path (Join-Path $ReleaseDir $GuiAppName) "$GuiAppName.exe"
$ExpectedCliExePath = Join-Path (Join-Path $ReleaseDir $CliAppName) "$CliAppName.exe"

if (-not (Test-Path $ExpectedGuiExePath)) {
    Write-Host "  [ERROR] Expected GUI executable not found: $ExpectedGuiExePath" -ForegroundColor Red
    exit 1
}

if (-not (Test-Path $ExpectedCliExePath)) {
    Write-Host "  [ERROR] Expected CLI executable not found: $ExpectedCliExePath" -ForegroundColor Red
    exit 1
}

Write-Host "  Verified release executable: $ExpectedGuiExePath" -ForegroundColor Green
Write-Host "  Verified release executable: $ExpectedCliExePath" -ForegroundColor Green

$ReleaseLicenseFile = Join-Path $ReleaseDir "LICENSE.txt"
Copy-Item -LiteralPath $LicenseFile -Destination $ReleaseLicenseFile -Force
Write-Host "  Copied: LICENSE.txt" -ForegroundColor Green

$ReleaseNoticesFile = Join-Path $ReleaseDir "THIRD-PARTY-NOTICES.txt"
Copy-Item -LiteralPath $ThirdPartyNoticesFile -Destination $ReleaseNoticesFile -Force
Write-Host "  Copied: THIRD-PARTY-NOTICES.txt" -ForegroundColor Green

$ReleaseSetupUsageFile = Join-Path $ReleaseDir "SETUP_AND_USAGE.txt"
Copy-Item -LiteralPath $SetupUsageFile -Destination $ReleaseSetupUsageFile -Force
Write-Host "  Copied: SETUP_AND_USAGE.txt" -ForegroundColor Green

$givemydataMetadataLines = & $VenvPython -c "import json
from importlib.metadata import distribution
d = distribution('garmin-givemydata')
license_file = next(f for f in d.files if str(f).replace('\\', '/').endswith('licenses/LICENSE'))
print(json.dumps({'version': d.version, 'license_file': str(d.locate_file(license_file))}))
"
if ($LASTEXITCODE -ne 0) {
    throw "Could not locate the installed garmin-givemydata license."
}
$givemydataMetadata = ($givemydataMetadataLines | Select-Object -Last 1) | ConvertFrom-Json
if ([string]$givemydataMetadata.version -ne "0.1.12") {
    throw "THIRD_PARTY_NOTICES.md is pinned to garmin-givemydata 0.1.12, but the build environment contains $($givemydataMetadata.version)."
}
$GivemydataLicenseFile = Join-Path $ReleaseDir "LICENSE-garmin-givemydata-AGPL-3.0.txt"
Copy-Item -LiteralPath ([string]$givemydataMetadata.license_file) -Destination $GivemydataLicenseFile -Force
Write-Host "  Copied: LICENSE-garmin-givemydata-AGPL-3.0.txt" -ForegroundColor Green

Write-Host "  Creating corresponding-source archives..." -ForegroundColor Cyan
$ReleaseSourceDir = Join-Path $ReleaseDir "source"
New-Item -ItemType Directory -Path $ReleaseSourceDir -Force | Out-Null
& $VenvPython -m pip download --no-deps "--no-binary=:all:" --dest $ReleaseSourceDir $GivemydataPypiSpec
if ($LASTEXITCODE -ne 0) {
    throw "Could not download the garmin-givemydata source distribution."
}

$SourceStageRoot = Join-Path $BuildDir "source_stage"
$SourceSnapshotRoot = Join-Path $SourceStageRoot "GarminDataHub-$Version"
New-Item -ItemType Directory -Path $SourceSnapshotRoot -Force | Out-Null
$sourceFiles = & git -C $ProjectRoot ls-files --cached --others --exclude-standard
if ($LASTEXITCODE -ne 0) {
    throw "Could not enumerate the project source tree with git."
}
foreach ($relativePath in $sourceFiles) {
    $sourcePath = Join-Path $ProjectRoot $relativePath
    if (-not (Test-Path -LiteralPath $sourcePath -PathType Leaf)) {
        continue
    }
    $destinationPath = Join-Path $SourceSnapshotRoot $relativePath
    $destinationParent = Split-Path -Parent $destinationPath
    New-Item -ItemType Directory -Path $destinationParent -Force | Out-Null
    Copy-Item -LiteralPath $sourcePath -Destination $destinationPath -Force
}

$sourceCommit = (& git -C $ProjectRoot rev-parse HEAD).Trim()
$worktreeDirty = if ((& git -C $ProjectRoot status --short --untracked-files=all | Select-Object -First 1)) { "true" } else { "false" }
$sourceInfoLines = @(
    "Version: $Version",
    "Commit: $sourceCommit",
    "Worktree-Dirty-At-Build: $worktreeDirty",
    "Generated-UTC: $([DateTime]::UtcNow.ToString('o'))"
)
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$SourceInfoFile = Join-Path $ReleaseDir "SOURCE-COMMIT.txt"
[System.IO.File]::WriteAllLines($SourceInfoFile, $sourceInfoLines, $utf8NoBom)
[System.IO.File]::WriteAllLines((Join-Path $SourceSnapshotRoot "SOURCE-COMMIT.txt"), $sourceInfoLines, $utf8NoBom)

$SourceArchive = Join-Path $ReleaseDir "GarminDataHub-$Version-source.zip"
Compress-Archive -Path $SourceSnapshotRoot -DestinationPath $SourceArchive -CompressionLevel Optimal -Force
Write-Host "  Project source archive: $SourceArchive" -ForegroundColor Green

Write-Host "  Smoke-testing packaged CLI entry points..." -ForegroundColor Cyan
Invoke-PackagedSmokeTest -Executable $ExpectedCliExePath -Arguments @("--help")
Invoke-PackagedSmokeTest -Executable $ExpectedCliExePath -Arguments @("--_run-bundled-givemydata", "--help")
Write-Host "  Packaged CLI smoke tests passed." -ForegroundColor Green

$PortableArchive = Join-Path $ReleaseDir "GarminDataHub-$Version-portable.zip"
Compress-Archive -Path $DestinationAppDir, $DestinationCliDir, $ReleaseLicenseFile, $ReleaseNoticesFile, $ReleaseSetupUsageFile, $GivemydataLicenseFile, $ReleaseSourceDir, $SourceArchive, $SourceInfoFile -DestinationPath $PortableArchive -CompressionLevel Optimal -Force
Write-Host "  Portable archive: $PortableArchive" -ForegroundColor Green

# --- BUILD INSTALLER ---
Write-Host ""
Write-Host "---------------------------------------------------" -ForegroundColor Cyan
Write-Host "Step 4: Building Installer" -ForegroundColor Cyan
Write-Host "---------------------------------------------------"

$InstallerFile = $null
$iscc = Get-Command ISCC.exe -ErrorAction SilentlyContinue
if ($null -eq $iscc) {
    Write-Host "[WARNING] Inno Setup Compiler (ISCC.exe) not found in PATH; installer build skipped." -ForegroundColor Yellow
}
else {
    Write-Host "Found Inno Setup Compiler: $($iscc.Source)"
    Write-Host "Compiling installer..."

    $isccArgs = @(
        "/Q",
        "/DMyAppVersion=$Version",
        "/DMyAppVersionNumeric=$InstallerNumericVersion",
        "/DSourcePath=`"$ReleaseDir`"",
        "/O`"$ReleaseDir`"",
        $IssScriptFile
    )

    try {
        & $iscc.Source @isccArgs
        if ($LASTEXITCODE -ne 0) { throw "Inno Setup compiler failed." }
        $InstallerFile = Join-Path $ReleaseDir "GarminDataHub-$Version-installer.exe"
        if (-not (Test-Path -LiteralPath $InstallerFile -PathType Leaf)) {
            throw "Expected installer was not created: $InstallerFile"
        }
        Write-Host "[SUCCESS] Installer built: $InstallerFile" -ForegroundColor Green

        $signature = Get-AuthenticodeSignature -LiteralPath $InstallerFile
        if ($signature.Status -ne "Valid") {
            Write-Host "[WARNING] Installer is not Authenticode-signed; Windows SmartScreen may warn recipients." -ForegroundColor Yellow
        }
    }
    catch {
        Write-Host "[ERROR] Failed to build the installer." -ForegroundColor Red
        throw
    }
}

# --- CHECKSUMS AND SUMMARY ---
$ChecksumTargets = @($PortableArchive, $SourceArchive)
if ($null -ne $InstallerFile) {
    $ChecksumTargets += $InstallerFile
}
$ChecksumTargets = $ChecksumTargets | Sort-Object { Split-Path -Leaf $_ }
$ChecksumLines = foreach ($item in $ChecksumTargets) {
    $hash = Get-FileHash -LiteralPath $item -Algorithm SHA256
    "$($hash.Hash.ToLowerInvariant()) *$(Split-Path -Leaf $item)"
}
$ChecksumFile = Join-Path $ReleaseDir "SHA256SUMS.txt"
[System.IO.File]::WriteAllLines($ChecksumFile, $ChecksumLines, $utf8NoBom)

Write-Host ""
Write-Host "###################################################" -ForegroundColor Green
Write-Host "# Build Pipeline Completed!                       #" -ForegroundColor Green
Write-Host "###################################################"
Write-Host ""
Write-Host "garmin-givemydata package: $GivemydataPypiSpec" -ForegroundColor Cyan
Write-Host "Release artifacts are in: $ReleaseDir" -ForegroundColor Cyan
Get-ChildItem -LiteralPath $ReleaseDir | ForEach-Object {
    Write-Host "  - $($_.Name)"
}
Write-Host "SHA-256 checksums: $ChecksumFile" -ForegroundColor Cyan
Write-Host ""
Write-Host "Portable app: extract the complete archive and run '$GuiAppName\$GuiAppName.exe'."
if ($null -ne $InstallerFile) {
    Write-Host "Installer: '$InstallerFile'"
}
