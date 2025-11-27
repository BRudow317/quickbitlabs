@echo on
REM Minimal Launcher
REM Sets working directory to the script's location
CD /D "%~dp0"

REM Runs the server using the virtual environment directly
".venv\Scripts\python.exe" run_server.py

REM Keeps window open if it crashes
PAUSE