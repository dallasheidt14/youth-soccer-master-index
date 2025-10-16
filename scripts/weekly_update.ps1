# Weekly Game History Update PowerShell Script
# Run this script every Monday at 6:00 AM

param(
    [string]$ProjectPath = "",
    [string]$LogPath = ""
)

# Determine project path - use parameter, script location, or default
if ([string]::IsNullOrEmpty($ProjectPath)) {
    $ProjectPath = Split-Path -Parent $PSScriptRoot
    Write-Host "Using project path derived from script location: $ProjectPath"
}

# Determine log path - use parameter or default relative to project
if ([string]::IsNullOrEmpty($LogPath)) {
    $LogPath = Join-Path $ProjectPath "data\logs\weekly_update.log"
}

# Ensure log directory exists
$logDir = Split-Path -Parent $LogPath
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
    Write-Host "Created log directory: $logDir"
}

# Function to write timestamped log messages
function Write-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] $Message"
    Write-Host $logMessage
    Add-Content -Path $LogPath -Value $logMessage
}

# Start logging
Write-Log "Starting weekly game history update..."

# Change to project directory
if (Test-Path $ProjectPath) {
    Set-Location $ProjectPath
    Write-Log "Changed to project directory: $ProjectPath"
}
else {
    Write-Log "ERROR: Project directory not found: $ProjectPath"
    exit 1
}

# Check if Python is available
try {
    $pythonVersion = python --version 2>&1
    Write-Log "Python version: $pythonVersion"
}
catch {
    Write-Log "ERROR: Python not found in PATH"
    exit 1
}

# Run the incremental update
Write-Log "Running incremental game history update..."
try {
    $process = Start-Process -FilePath "python" -ArgumentList @(
        "-m", "src.scraper.build_game_history",
        "--providers", "gotsport",
        "--states", "AZ", 
        "--genders", "M,F",
        "--ages", "U10",
        "--incremental"
    ) -Wait -PassThru -RedirectStandardOutput "data/logs/weekly_update_output.log" -RedirectStandardError "data/logs/weekly_update_error.log"
    
    if ($process.ExitCode -eq 0) {
        Write-Log "Weekly update completed successfully!"
    }
    else {
        Write-Log "ERROR: Weekly update failed with exit code $($process.ExitCode)"
        Write-Log "Check error log: data/logs/weekly_update_error.log"
        exit $process.ExitCode
    }
}
catch {
    Write-Log "ERROR: Failed to run update script: $($_.Exception.Message)"
    exit 1
}

Write-Log "Weekly game history update finished successfully!"
# Weekly Game History Update PowerShell Script
# Run this script every Monday at 6:00 AM

param(
    [string]$LogPath = "data/logs/weekly_update.log"
)

# Function to write timestamped log messages
function Write-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] $Message"
    Write-Host $logMessage
    Add-Content -Path $LogPath -Value $logMessage
}

# Start logging
Write-Log "Starting weekly game history update..."

# Change to project directory
$projectPath = "C:\youth-soccer-master-index"
if (Test-Path $projectPath) {
    Set-Location $projectPath
    Write-Log "Changed to project directory: $projectPath"
}
else {
    Write-Log "ERROR: Project directory not found: $projectPath"
    exit 1
}

# Check if Python is available
try {
    $pythonVersion = python --version 2>&1
    Write-Log "Python version: $pythonVersion"
}
catch {
    Write-Log "ERROR: Python not found in PATH"
    exit 1
}

# Run the incremental update
Write-Log "Running incremental game history update..."
try {
    $process = Start-Process -FilePath "python" -ArgumentList @(
        "-m", "src.scraper.build_game_history",
        "--providers", "gotsport",
        "--states", "AZ", 
        "--genders", "M,F",
        "--ages", "U10",
        "--incremental"
    ) -Wait -PassThru -RedirectStandardOutput "data/logs/weekly_update_output.log" -RedirectStandardError "data/logs/weekly_update_error.log"
    
    if ($process.ExitCode -eq 0) {
        Write-Log "Weekly update completed successfully!"
    }
    else {
        Write-Log "ERROR: Weekly update failed with exit code $($process.ExitCode)"
        Write-Log "Check error log: data/logs/weekly_update_error.log"
        exit $process.ExitCode
    }
}
catch {
    Write-Log "ERROR: Failed to run update script: $($_.Exception.Message)"
    exit 1
}

Write-Log "Weekly game history update finished successfully!"

