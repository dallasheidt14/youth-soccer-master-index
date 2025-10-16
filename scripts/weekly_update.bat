@echo off
REM Weekly Game History Update Script
REM Run this script every Monday at 6:00 AM

echo Starting weekly game history update...
echo Date: %date%
echo Time: %time%

REM Change to project directory
REM Use PROJECT_ROOT environment variable if set, otherwise use script location
if defined PROJECT_ROOT (
    echo Using PROJECT_ROOT: %PROJECT_ROOT%
    cd /d "%PROJECT_ROOT%"
) else (
    REM Get the directory where this script is located
    set SCRIPT_DIR=%~dp0
    REM Go up one level from scripts directory to project root
    cd /d "%SCRIPT_DIR%.."
)

REM Verify we're in the correct directory
if not exist "src\scraper\build_game_history.py" (
    echo ERROR: Could not find src\scraper\build_game_history.py
    echo Current directory: %CD%
    echo Please set PROJECT_ROOT environment variable or run from the correct location
    exit /b 1
)

REM Activate Python environment (adjust path if using virtual environment)
REM call venv\Scripts\activate.bat

REM Run the incremental update
python -m src.scraper.build_game_history --providers gotsport --states AZ --genders M,F --ages U10 --incremental

REM Check if the command was successful
if %errorlevel% equ 0 (
    echo Weekly update completed successfully!
) else (
    echo Weekly update failed with error code %errorlevel%
    exit /b %errorlevel%
)

echo Weekly game history update finished at %time%
