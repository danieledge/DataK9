# DataK9 Studio Launcher for Windows (PowerShell)
# This script starts a local web server to avoid CORS issues

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  DataK9 Studio Launcher" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if Python is installed
try {
    $pythonVersion = python --version 2>&1
    Write-Host "Found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "ERROR: Python is not installed or not in PATH" -ForegroundColor Red
    Write-Host "Please install Python 3 from https://www.python.org/" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host ""
Write-Host "Starting local web server on port 8000..." -ForegroundColor Yellow
Write-Host ""
Write-Host "DataK9 Studio will open at:" -ForegroundColor Green
Write-Host "http://localhost:8000/datak9-studio.html" -ForegroundColor Cyan
Write-Host ""
Write-Host "Press Ctrl+C to stop the server" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Start browser after a short delay
Start-Sleep -Seconds 2
Start-Process "http://localhost:8000/datak9-studio.html"

# Start Python HTTP server
python -m http.server 8000
