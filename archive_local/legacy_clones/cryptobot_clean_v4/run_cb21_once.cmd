@echo off
set ROOT=%~dp0
cd /d "%ROOT%"

call "%ROOT%\run_cb20.cmd"
if errorlevel 1 exit /b 1

call "%ROOT%\run_cb21_edge3.cmd"
if errorlevel 1 exit /b 1

exit /b 0
