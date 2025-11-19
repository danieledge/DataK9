@echo off
REM DataK9 Studio Launcher for Windows
REM This script starts a local web server to avoid CORS issues

echo ========================================
echo   DataK9 Studio Launcher
echo ========================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3 from https://www.python.org/
    pause
    exit /b 1
)

echo Starting local web server on port 8000...
echo.
echo DataK9 Studio will open at:
echo http://localhost:8000/datak9-studio.html
echo.
echo Press Ctrl+C to stop the server
echo ========================================
echo.

REM Start browser after a short delay
start "" timeout /t 2 /nobreak >nul && start http://localhost:8000/datak9-studio.html

REM Start Python HTTP server
python -m http.server 8000
