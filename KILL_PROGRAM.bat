@echo off
:: ==========================================================
::  kill.bat — Terminate stuck scraper/driver processes
:: ==========================================================

echo.
echo [INFO] Terminating Python and ChromeDriver processes...
echo.

:: Kill Python (scraper)
taskkill /F /IM python.exe /T >nul 2>&1

:: Kill ChromeDriver (if running)
taskkill /F /IM chromedriver.exe /T >nul 2>&1

:: Kill Chrome (headless browser windows)
taskkill /F /IM chrome.exe /T >nul 2>&1

echo [DONE] All related processes terminated.
pause
