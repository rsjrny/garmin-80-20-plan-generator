; GarminDataHub Inno Setup Script
; See https://jrsoftware.org/ishelp/ for documentation

#define MyAppName "GarminDataHub"
#ifndef MyAppVersion
#define MyAppVersion "1.0.0"
#endif
#ifndef MyAppVersionNumeric
#define MyAppVersionNumeric "1.0.0.0"
#endif
#define MyAppPublisher "Garmin Data Hub"
#define MyAppURL "https://github.com/rsjrny/garmin-80-20-plan-generator"
#define MyAppSupportURL "https://github.com/rsjrny/garmin-80-20-plan-generator/issues"
#define MyAppUpdatesURL "https://github.com/rsjrny/garmin-80-20-plan-generator/releases"
#define MyAppExeName "GarminDataHub.exe"
#define MyCliExeName "cli_backup_ingest.exe"

[Setup]
; NOTE: The value of AppId uniquely identifies this application.
; Do not use the same AppId value for other applications.
; Historical releases used this literal value. Keep it stable for upgrade/uninstall continuity.
AppId={{AUTO_GUID}}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppSupportURL}
AppUpdatesURL={#MyAppUpdatesURL}
DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
AllowNoIcons=yes
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
LicenseFile={#SourcePath}\LICENSE.txt
VersionInfoCompany={#MyAppPublisher}
VersionInfoDescription=Local Garmin analytics and training planning desktop app
VersionInfoProductName={#MyAppName}
VersionInfoProductVersion={#MyAppVersion}
VersionInfoVersion={#MyAppVersionNumeric}
VersionInfoCopyright=Copyright (c) 2024 Garmin Data Hub
; The output directory and filename for the compiled installer
OutputDir=..\..\release\{#MyAppVersion}
OutputBaseFilename=GarminDataHub-{#MyAppVersion}-installer
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
UninstallDisplayIcon={app}\{#MyAppExeName}

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
; NOTE: "{#SourcePath}" is a variable that will be passed in from the command line
Source: "{#SourcePath}\GarminDataHub\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "{#SourcePath}\cli_backup_ingest\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "{#SourcePath}\LICENSE.txt"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#SourcePath}\THIRD-PARTY-NOTICES.txt"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#SourcePath}\LICENSE-garmin-givemydata-AGPL-3.0.txt"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#SourcePath}\GarminDataHub-{#MyAppVersion}-source.zip"; DestDir: "{app}\source"; Flags: ignoreversion
Source: "{#SourcePath}\SOURCE-COMMIT.txt"; DestDir: "{app}\source"; Flags: ignoreversion
Source: "{#SourcePath}\source\*"; DestDir: "{app}\source"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{group}\{cm:UninstallProgram,{#MyAppName}}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#StringChange(MyAppName, '&', '&&')}}"; Flags: nowait postinstall skipifsilent

