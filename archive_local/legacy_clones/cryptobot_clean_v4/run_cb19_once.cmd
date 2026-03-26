@echo off
set ROOT=%~dp0
cd /d "%ROOT%"

rem --- CB21 once (includes CB20 + EDGE3 snapshot/meta) ---
call "%ROOT%\run_cb21_once.cmd"
if errorlevel 1 exit /b 1

rem --- Bridge lines for monitor/log ---
"%ROOT%\.venv\Scripts\python.exe" "%ROOT%\cb19_cb20_bridge.py"
"%ROOT%\.venv\Scripts\python.exe" "%ROOT%\cb19_edge3_bridge.py"

exit /b 0
