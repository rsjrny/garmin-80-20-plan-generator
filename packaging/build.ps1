# Complete Build Pipeline for GarminDataHub
# Builds both the Streamlit application and the CLI tool, then organizes them into a release directory.

param(
    [string]$Version = "0.0.0", # Default version if not specified
    [switch]$AutoUpdateGivemydata = $true,
    [ValidateSet("local", "pypi")]
    [string]$GivemydataSource = "pypi",
    [string]$GivemydataPypiSpec = "garmin-givemydata"
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
$ReleaseDir = Join-Path $ProjectRoot "release\$Version"
$StreamlitBuildDir = Join-Path $BuildDir "streamlit_app"
$CliBuildDir = Join-Path $BuildDir "cli_tool"
$StreamlitDistDir = Join-Path $StreamlitBuildDir "dist"
$CliDistDir = Join-Path $CliBuildDir "dist"
$PyProjectPath = Join-Path $ProjectRoot "pyproject.toml"
$VenvPython = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$VenvScriptsDir = Join-Path $ProjectRoot ".venv\Scripts"
$VenvGivemydataExe = Join-Path $VenvScriptsDir "garmin-givemydata.exe"
$GivemydataRepo = Join-Path (Split-Path -Parent $ProjectRoot) "garmin-givemydata"
$GivemydataSourceFile = Join-Path $GivemydataRepo "garmin_givemydata.py"

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

function Assert-GivemydataVenvIsCurrent {
    param(
        [string]$PythonExe,
        [string]$SourceFile,
        [switch]$TryAutoUpdate
    )

    if (-not (Test-Path $PythonExe)) {
        Write-Host "[ERROR] Expected venv Python not found: $PythonExe" -ForegroundColor Red
        exit 1
    }

    if (-not (Test-Path $SourceFile)) {
        Write-Host "[ERROR] Expected source file not found: $SourceFile" -ForegroundColor Red
        exit 1
    }

    $probeLines = & $PythonExe -c "import json, sys
try:
    import garmin_givemydata as g
    print(json.dumps({'ok': True, 'module_file': getattr(g, '__file__', '')}))
except Exception as e:
    print(json.dumps({'ok': False, 'error': str(e)}))
    sys.exit(1)
" 2>$null

    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Could not import garmin_givemydata from venv." -ForegroundColor Red
        Write-Host "        Fix with: $PythonExe -m pip install -e `"$GivemydataRepo`"" -ForegroundColor Yellow
        exit 1
    }

    $probeJson = ($probeLines | Select-Object -Last 1)
    $probe = $probeJson | ConvertFrom-Json
    $moduleFile = [System.IO.Path]::GetFullPath([string]$probe.module_file)
    $sourceFullPath = [System.IO.Path]::GetFullPath($SourceFile)
    $sourceDir = Split-Path -Parent $sourceFullPath

    # Editable install points directly into the source repo and is always current.
    if ($moduleFile.StartsWith($sourceDir, [System.StringComparison]::OrdinalIgnoreCase)) {
        Write-Host "[OK] garmin_givemydata is editable-linked to source repo." -ForegroundColor Green
        return
    }

    if (-not (Test-Path $moduleFile)) {
        Write-Host "[ERROR] Installed module path does not exist: $moduleFile" -ForegroundColor Red
        exit 1
    }

    $installedHash = (Get-FileHash -Path $moduleFile -Algorithm SHA256).Hash
    $sourceHash = (Get-FileHash -Path $sourceFullPath -Algorithm SHA256).Hash

    if ($installedHash -ne $sourceHash) {
        if ($TryAutoUpdate) {
            Write-Host "[WARN] .venv garmin_givemydata is stale; reinstalling from local repo..." -ForegroundColor Yellow
            & $PythonExe -m pip install --force-reinstall --no-deps "$GivemydataRepo"
            if ($LASTEXITCODE -ne 0) {
                Write-Host "[ERROR] Auto-update failed while reinstalling garmin-givemydata." -ForegroundColor Red
                exit 1
            }

            $probeLines2 = & $PythonExe -c "import json, sys
try:
    import garmin_givemydata as g
    print(json.dumps({'ok': True, 'module_file': getattr(g, '__file__', '')}))
except Exception as e:
    print(json.dumps({'ok': False, 'error': str(e)}))
    sys.exit(1)
" 2>$null
            if ($LASTEXITCODE -ne 0) {
                Write-Host "[ERROR] Could not import garmin_givemydata after auto-update." -ForegroundColor Red
                exit 1
            }
            $probe2 = ($probeLines2 | Select-Object -Last 1) | ConvertFrom-Json
            $moduleFile2 = [System.IO.Path]::GetFullPath([string]$probe2.module_file)

            if (-not (Test-Path $moduleFile2)) {
                Write-Host "[ERROR] Installed module path missing after auto-update: $moduleFile2" -ForegroundColor Red
                exit 1
            }

            $installedHash2 = (Get-FileHash -Path $moduleFile2 -Algorithm SHA256).Hash
            $sourceHash2 = (Get-FileHash -Path $sourceFullPath -Algorithm SHA256).Hash
            if ($installedHash2 -eq $sourceHash2) {
                Write-Host "[OK] .venv garmin_givemydata refreshed from local source." -ForegroundColor Green
                return
            }
        }

        Write-Host "[ERROR] .venv garmin_givemydata is stale vs local source repo." -ForegroundColor Red
        Write-Host "        Installed: $moduleFile" -ForegroundColor Yellow
        Write-Host "        Source:    $sourceFullPath" -ForegroundColor Yellow
        Write-Host "        Run one of:" -ForegroundColor Yellow
        Write-Host "          $PythonExe -m pip install -e `"$GivemydataRepo`"" -ForegroundColor Yellow
        Write-Host "          $PythonExe -m pip install --force-reinstall --no-deps `"$GivemydataRepo`"" -ForegroundColor Yellow
        exit 1
    }

    Write-Host "[OK] .venv garmin_givemydata matches local source file." -ForegroundColor Green
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
        & $PythonExe -m pip install --upgrade --no-deps "$PackageSpec"
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
        Write-Host "        Install it with: $PythonExe -m pip install --upgrade --no-deps `"$PackageSpec`"" -ForegroundColor Yellow
        exit 1
    }

    $probeJson = ($probeLines | Select-Object -Last 1)
    $probe = $probeJson | ConvertFrom-Json
    $moduleFile = [string]$probe.module_file
    $version = [string]$probe.version
    Write-Host "[OK] Using PyPI garmin-givemydata $version from: $moduleFile" -ForegroundColor Green
}

# --- SETUP ---
Write-Host "###################################################" -ForegroundColor Magenta
Write-Host "# GarminDataHub Build Pipeline                    #" -ForegroundColor Magenta
Write-Host "###################################################" -ForegroundColor Magenta
Write-Host ""
Write-Host "===================================================" -ForegroundColor DarkCyan
Write-Host "garmin-givemydata mode: $($GivemydataSource.ToUpperInvariant())" -ForegroundColor Cyan
if ($GivemydataSource -eq "pypi") {
    Write-Host "garmin-givemydata package: $GivemydataPypiSpec" -ForegroundColor Cyan
}
Write-Host "===================================================" -ForegroundColor DarkCyan
Write-Host ""
Write-Host "Building version: $Version" -ForegroundColor Cyan
Write-Host "garmin-givemydata source mode: $GivemydataSource" -ForegroundColor Cyan
if ($GivemydataSource -eq "pypi") {
    Write-Host "garmin-givemydata package spec: $GivemydataPypiSpec" -ForegroundColor Cyan
}
Update-PyProjectVersion -FilePath $PyProjectPath -NewVersion $Version
if ($GivemydataSource -eq "local") {
    Assert-GivemydataVenvIsCurrent -PythonExe $VenvPython -SourceFile $GivemydataSourceFile -TryAutoUpdate:$AutoUpdateGivemydata
}
else {
    Ensure-GivemydataFromPypi -PythonExe $VenvPython -PackageSpec $GivemydataPypiSpec -TryAutoUpdate:$AutoUpdateGivemydata
}

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
New-Item -ItemType Directory -Path $StreamlitBuildDir -Force | Out-Null
New-Item -ItemType Directory -Path $CliBuildDir -Force | Out-Null

# --- BUILD STREAMLIT APP ---
Write-Host ""
Write-Host "---------------------------------------------------" -ForegroundColor Cyan
Write-Host "Step 1: Building Streamlit Application (as a directory)" -ForegroundColor Cyan
Write-Host "---------------------------------------------------"
Push-Location $ProjectRoot

$StreamlitAppName = "GarminDataHub"
$pyinstallerArgsStreamlit = @(
    "--console",
    "--name", $StreamlitAppName,
    "--distpath", $StreamlitDistDir,
    "--workpath", (Join-Path $StreamlitBuildDir "build"),
    "--specpath", $StreamlitBuildDir,
    "--add-data", ((Join-Path $ProjectRoot 'src\garmin_data_hub\ui_streamlit') + ";garmin_data_hub/ui_streamlit"),
    "--add-data", ((Join-Path $ProjectRoot 'src\garmin_data_hub\db\schema.sql') + ";garmin_data_hub/db"),
    "--collect-all", "streamlit",
    "--collect-all", "garmin_data_hub",
    "--hidden-import", "pandas",
    "--hidden-import", "plotly",
    $LauncherFile
)

try {
    Write-Host "Running PyInstaller for Streamlit app..."
    python -m PyInstaller @pyinstallerArgsStreamlit
    if ($LASTEXITCODE -ne 0) { throw "PyInstaller failed for Streamlit app." }
    Write-Host "[SUCCESS] Streamlit app built." -ForegroundColor Green
}
catch {
    Write-Host "[ERROR] Failed to build Streamlit application." -ForegroundColor Red
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
$GarminSyncExeName = "garmin-givemydata.exe"

# Build orchestrator cli_backup_ingest.exe (one-dir)
$pyinstallerArgsCli = @(
    "--clean",
    "--console",
    "--name", $CliAppName,
    "--distpath", $CliDistDir,
    "--contents-directory", ".",
    "--workpath", (Join-Path $CliBuildDir "build"),
    "--specpath", $CliBuildDir,
    "--add-data", ((Join-Path $ProjectRoot 'src\garmin_data_hub\db\schema.sql') + ";garmin_data_hub/db"),
    $CliBackupFile
)

try {
    Write-Host "Running PyInstaller for CLI tool..."
    python -m PyInstaller @pyinstallerArgsCli
    if ($LASTEXITCODE -ne 0) { throw "PyInstaller failed for CLI tool." }
    Write-Host "[SUCCESS] CLI tool built." -ForegroundColor Green
}
catch {
    Write-Host "[ERROR] Failed to build CLI tool." -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

# Inject standalone helper tools into cli_backup_ingest directory
$CliDir = Join-Path $CliDistDir $CliAppName
if (-not (Test-Path $CliDir)) {
    Write-Host "[ERROR] CLI output directory not found at $CliDir" -ForegroundColor Red
    exit 1
}

# Bundle garmin-givemydata executable next to cli_backup_ingest.exe
$srcGivemydata = $VenvGivemydataExe
if (-not (Test-Path $srcGivemydata)) {
    Write-Host "[ERROR] 'garmin-givemydata.exe' not found in venv at: $srcGivemydata" -ForegroundColor Red
    if ($GivemydataSource -eq "local") {
        Write-Host "        Fix with: $VenvPython -m pip install --force-reinstall --no-deps `"$GivemydataRepo`"" -ForegroundColor Red
    }
    else {
        Write-Host "        Fix with: $VenvPython -m pip install --upgrade --no-deps `"$GivemydataPypiSpec`"" -ForegroundColor Red
    }
    exit 1
}
Copy-Item -Path $srcGivemydata -Destination (Join-Path $CliDir $GarminSyncExeName) -Force
Write-Host "  Copied standalone tool: $GarminSyncExeName -> $CliDir" -ForegroundColor Green

Pop-Location

# --- ORGANIZE RELEASE ARTIFACTS ---
Write-Host ""
Write-Host "---------------------------------------------------" -ForegroundColor Cyan
Write-Host "Step 3: Organizing Release Artifacts" -ForegroundColor Cyan
Write-Host "---------------------------------------------------"

$DestinationCliDir = Join-Path $ReleaseDir $CliAppName
$DestinationAppDir = Join-Path $ReleaseDir $StreamlitAppName

# Copy Streamlit app directory
$StreamlitAppDir = Join-Path $StreamlitDistDir $StreamlitAppName
if (Test-Path $StreamlitAppDir) {
    Copy-Item -Path $StreamlitAppDir -Destination $ReleaseDir -Recurse -Force
    Write-Host "  Copied: $StreamlitAppName (directory)" -ForegroundColor Green
}
else {
    Write-Host "  [ERROR] Streamlit application directory not found at $StreamlitAppDir" -ForegroundColor Red
}

# Also drop garmin-givemydata into the Streamlit app folder for convenience
$DestinationAppDir = Join-Path $ReleaseDir $StreamlitAppName
$GarminSyncExePath = Join-Path $CliDir $GarminSyncExeName
if (Test-Path $GarminSyncExePath) {
    Copy-Item -Path $GarminSyncExePath -Destination $DestinationAppDir -Force
    Write-Host "  Copied: $GarminSyncExeName into $DestinationAppDir" -ForegroundColor Green
}

# Copy CLI executable and its _internal directory
if (Test-Path $CliDir) {
    Copy-Item -Path $CliDir -Destination $DestinationCliDir -Recurse -Force
    Write-Host "  Copied: $CliAppName (directory)" -ForegroundColor Green
}
else {
    Write-Host "  [ERROR] CLI directory not found at $CliDir" -ForegroundColor Red
}

# Validate expected release executables exist at final locations
$ExpectedGuiExePath = Join-Path (Join-Path $ReleaseDir $StreamlitAppName) "$StreamlitAppName.exe"
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

# --- SUMMARY ---
Write-Host ""
Write-Host "###################################################" -ForegroundColor Green
Write-Host "# Build Pipeline Completed!                       #" -ForegroundColor Green
Write-Host "###################################################"
Write-Host ""
Write-Host "Build mode summary:" -ForegroundColor Cyan
Write-Host "  garmin-givemydata source: $GivemydataSource"
if ($GivemydataSource -eq "pypi") {
    Write-Host "  garmin-givemydata package: $GivemydataPypiSpec"
}
Write-Host ""
Write-Host "Release artifacts are in: $ReleaseDir" -ForegroundColor Cyan
Get-ChildItem $ReleaseDir | ForEach-Object {
    Write-Host "  - $($_.Name)"
}
Write-Host ""
Write-Host "To run the application:"
Write-Host "1. Open the '$($ReleaseDir)\$StreamlitAppName' directory."
Write-Host "2. Run '$StreamlitAppName.exe'."
Write-Host ""
Write-Host "To run the CLI tool:"
Write-Host "  '$($ReleaseDir)\$CliAppName\$CliAppName.exe'"
Write-Host ""

# --- BUILD INSTALLER ---
Write-Host ""
Write-Host "---------------------------------------------------" -ForegroundColor Cyan
Write-Host "Step 4: Building Installer" -ForegroundColor Cyan
Write-Host "---------------------------------------------------"

(Get-Content "packaging/installer/GarminDataHub.iss") -replace '#define MyAppVersion \".*\"', "#define MyAppVersion `"$version`"" | Set-Content "packaging/installer/GarminDataHub.iss"
$IssScriptFile = Join-Path $ScriptDir "installer\GarminDataHub.iss"
if (-not (Test-Path $IssScriptFile)) {
    Write-Host "[WARNING] Inno Setup script not found at $IssScriptFile. Skipping installer build." -ForegroundColor Yellow
}
else {
    $iscc = Get-Command ISCC.exe -ErrorAction SilentlyContinue
    if ($null -eq $iscc) {
        Write-Host "[WARNING] Inno Setup Compiler (ISCC.exe) not found in your PATH." -ForegroundColor Yellow
        Write-Host "          Please install Inno Setup from https://jrsoftware.org and add it to your PATH to build the installer." -ForegroundColor Yellow
        Write-Host "          Skipping installer build." -ForegroundColor Yellow
    }
    else {
        Write-Host "Found Inno Setup Compiler: $($iscc.Source)"
        Write-Host "Compiling installer..."
        
        # Define the source path for the installer files
        $InstallerSourcePath = $ReleaseDir
        
        # Arguments for the Inno Setup compiler
        $isccArgs = @(
            "/Q", # Quiet mode
            "/DSourcePath=`"$InstallerSourcePath`"",
            $IssScriptFile
        )
        
        try {
            & $iscc.Source @isccArgs
            if ($LASTEXITCODE -ne 0) { throw "Inno Setup compiler failed." }
            Write-Host "[SUCCESS] Installer built successfully." -ForegroundColor Green
            $InstallerFile = Join-Path $ReleaseDir "GarminDataHub-$Version-installer.exe"
            Write-Host "Installer located at: $InstallerFile" -ForegroundColor Cyan
        }
        catch {
            Write-Host "[ERROR] Failed to build the installer." -ForegroundColor Red
            Write-Host $_.Exception.Message -ForegroundColor Red
        }
    }
}
