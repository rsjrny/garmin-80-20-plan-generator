# Complete Build Pipeline for GarminDataHub
# Builds both the Streamlit application and the CLI tool, then organizes them into a release directory.

param(
    [string]$Version = "0.0.0" # Default version if not specified
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

function Update-PyProjectVersion {
    param(
        [string]$FilePath,
        [string]$NewVersion
    )

    if (-not (Test-Path $FilePath)) {
        Write-Host "[WARNING] pyproject.toml not found at $FilePath; skipping version update." -ForegroundColor Yellow
        return
    }

    $content = Get-Content $FilePath -Raw
    $updated = [regex]::Replace($content, '(?m)^version\s*=\s*"[^"]+"', "version = `"$NewVersion`"", 1)

    if ($updated -eq $content) {
        Write-Host "[WARNING] Could not find [project] version line in pyproject.toml; no change made." -ForegroundColor Yellow
        return
    }

    Set-Content -Path $FilePath -Value $updated -Encoding UTF8
    Write-Host "Updated pyproject version to $NewVersion" -ForegroundColor Green
}

# --- SETUP ---
Write-Host "###################################################" -ForegroundColor Magenta
Write-Host "# GarminDataHub Build Pipeline                    #" -ForegroundColor Magenta
Write-Host "###################################################" -ForegroundColor Magenta
Write-Host ""
Write-Host "Building version: $Version" -ForegroundColor Cyan
Update-PyProjectVersion -FilePath $PyProjectPath -NewVersion $Version

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
$givemydataCmd = Get-Command garmin-givemydata -ErrorAction SilentlyContinue
if ($null -eq $givemydataCmd) {
    Write-Host "[ERROR] 'garmin-givemydata' command not found in PATH." -ForegroundColor Red
    Write-Host "        Install it before packaging: pip install garmin-givemydata" -ForegroundColor Red
    exit 1
}

$srcGivemydata = $givemydataCmd.Source
$candidateExe = [System.IO.Path]::ChangeExtension($srcGivemydata, "exe")
if (([System.IO.Path]::GetExtension($srcGivemydata) -ieq ".exe") -and (Test-Path $srcGivemydata)) {
    Copy-Item -Path $srcGivemydata -Destination (Join-Path $CliDir $GarminSyncExeName) -Force
}
elseif (Test-Path $candidateExe) {
    Copy-Item -Path $candidateExe -Destination (Join-Path $CliDir $GarminSyncExeName) -Force
}
else {
    Write-Host "[ERROR] Could not locate garmin-givemydata executable from '$srcGivemydata'." -ForegroundColor Red
    exit 1
}
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
