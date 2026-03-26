@echo off
set ROOT=%~dp0
cd /d "%ROOT%"
"%ROOT%\.venv\Scripts\python.exe" "%ROOT%\cb20_regime.py"
