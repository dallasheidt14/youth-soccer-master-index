@echo off
REM Setup script for Windows Task Scheduler
REM This will create a scheduled task to run the weekly update every Monday at 6:00 AM

echo Setting up weekly game history update task...

REM Check if running as administrator
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: This script must be run as Administrator
    echo Right-click and select "Run as administrator"
    pause
    exit /b 1
)

REM Get the current directory
set "SCRIPT_DIR=%~dp0"
set "PROJECT_DIR=%SCRIPT_DIR%.."

REM Create the scheduled task
schtasks /create /tn "Weekly Game History Update" /tr "powershell.exe -ExecutionPolicy RemoteSigned -File \"%SCRIPT_DIR%weekly_update.ps1\"" /sc weekly /d MON /st 06:00 /f

if %errorlevel% equ 0 (
    echo.
    echo SUCCESS: Weekly task created successfully!
    echo.
    echo Task Details:
    echo - Name: Weekly Game History Update
    echo - Schedule: Every Monday at 6:00 AM
    echo - Command: PowerShell script with incremental update
    echo.
    echo Next run: Next Monday at 6:00 AM
    echo.
    echo To view the task: schtasks /query /tn "Weekly Game History Update"
    echo To delete the task: schtasks /delete /tn "Weekly Game History Update" /f
) else (
    echo ERROR: Failed to create scheduled task
    echo Please check the error message above
)

echo.
pause
