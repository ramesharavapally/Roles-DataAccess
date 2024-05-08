@echo off
REM Step 1: Activate py_automate venv
call %~dp0venv\Scripts\activate

REM Step 2: Navigate to ERPE2E\tests folder
cd /d "%~dp0src"

REM Step 3: Run pyteest -m jobs command
python main.py

echo Setup complete!

REM Prompt user to press any key to exit
pause >nul