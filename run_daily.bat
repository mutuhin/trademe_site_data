@echo off
REM ================================================================
REM  Trade Me Motors Scraper - Windows Daily Runner
REM ================================================================
REM  To schedule: use Task Scheduler with this .bat file
REM  Or run manually: double-click this file
REM ================================================================

cd /d "%~dp0"

REM Activate virtual environment
if exist ".venv\Scripts\activate.bat" (
    call .venv\Scripts\activate.bat
) else (
    echo Setting up virtual environment...
    python -m venv .venv
    call .venv\Scripts\activate.bat
    pip install -r requirements.txt
    python -m playwright install chromium
)

REM Run the scraper
python trademe_scraper.py --output output

REM Log completion
echo [%date% %time%] Scraper completed >> logs\run.log

pause
