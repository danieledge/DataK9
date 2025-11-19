# DataK9 Studio - Windows Setup Guide

## Why Can't I Just Double-Click the HTML File?

When you open `datak9-studio.html` directly from your file system (double-clicking it), browsers block access to the `validation_definitions.json` file due to **CORS (Cross-Origin Resource Sharing)** security policies. This is a browser security feature that prevents local files from making fetch requests to other local files.

## Solution: Run a Local Web Server

You need to run a simple local web server. Don't worry - it's easy!

### Option 1: Use the Launch Scripts (Easiest)

#### Windows (CMD):
1. Double-click `launch-studio.bat`
2. Your browser will automatically open DataK9 Studio
3. Keep the terminal window open while using Studio
4. Press `Ctrl+C` in the terminal to stop the server when done

#### Windows (PowerShell):
1. Right-click `launch-studio.ps1`
2. Select "Run with PowerShell"
3. If you get an execution policy error, run this command first in PowerShell as Administrator:
   ```powershell
   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
   ```
4. Your browser will automatically open DataK9 Studio

### Option 2: Manual Launch (Alternative)

If you prefer to start the server manually:

1. Open Command Prompt or PowerShell
2. Navigate to the DataK9 directory:
   ```cmd
   cd C:\Users\YourUsername\DataK9-main\data-validation-tool
   ```
3. Start the Python HTTP server:
   ```cmd
   python -m http.server 8000
   ```
4. Open your browser and go to:
   ```
   http://localhost:8000/datak9-studio.html
   ```

### Option 3: Use Node.js (If You Have It)

If you have Node.js installed:

```cmd
npx http-server -p 8000
```

Then open: `http://localhost:8000/datak9-studio.html`

## What Port Should I Use?

The scripts use port `8000` by default. If that port is already in use, you can change it:

**Batch file (launch-studio.bat):**
Change `8000` to another port like `8080` or `3000`

**PowerShell (launch-studio.ps1):**
Change `8000` to another port like `8080` or `3000`

## Troubleshooting

### "Python is not installed or not in PATH"

You need Python 3 installed. Download from: https://www.python.org/downloads/

**Important:** During installation, check the box that says "Add Python to PATH"

### "Address already in use"

Another program is using port 8000. Either:
- Close the other program
- Or change the port number in the launch script

### Browser Opens But Shows Error

Make sure:
1. The terminal window with the server is still running
2. You're accessing `http://localhost:8000/datak9-studio.html` (not `file:///...`)
3. Port 8000 isn't blocked by firewall

### Fallback Mode (Limited Features)

If you see "Loaded 22 validations" in the console, you're in fallback mode. The Studio will work but with a reduced set of validations (22 instead of 34).

To get all 34 validations, you must run the local web server.

## Security Note

The local web server **only** runs on your computer (`localhost`) and is **not accessible** from the internet. It's completely safe to run while working with DataK9 Studio.

## Alternative: Use VSCode Live Server

If you use Visual Studio Code:

1. Install the "Live Server" extension
2. Right-click `datak9-studio.html`
3. Select "Open with Live Server"

This automatically handles the local server for you!
