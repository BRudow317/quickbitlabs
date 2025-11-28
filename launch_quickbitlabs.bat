@echo on
REM Minimal Launcher
REM Sets working directory to the script's location
CD /D "%~dp0"

REM Runs the server using the virtual environment directly
".venv\Scripts\python.exe" run_server.py

REM Launch Cloudflare Tunnel
cd "C:\Program Files (x86)\cloudflared\"
cloudflared.exe tunnel run 6c62ec13-b13d-4f6c-a557-4c4070d675c0
REM Keeps window open if it crashes
PAUSE