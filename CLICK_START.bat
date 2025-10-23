@echo off
:: ==========================================================
::  click.bat — Run Selenium Scraper with virtual environment
:: ==========================================================

setlocal

:: --- Project & Environment Setup ---
cd /d "C:\Users\rando\Office Projects\selenium-scrape"

:: Activate the virtual environment
call venv\Scripts\activate.bat

echo.
echo [INFO] Virtual environment activated.
echo [INFO] Starting Scraper...
echo.

:: --- Run the scraper ---
python app\sch_main.py

:: --- Exit handling ---
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Scraper exited with error code %ERRORLEVEL%.
    pause
) else (
    echo [SUCCESS] Scraper completed successfully.
)

:: Deactivate (optional)
call deactivate

endlocal
pause
