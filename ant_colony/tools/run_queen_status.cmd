@echo off
setlocal

set "ROOT=C:\Users\vikke\OneDrive\bitvavo-bot_clean"
set "SCRIPT=%ROOT%\ant_colony\tools\queen_status.ps1"

powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT%"
exit /b %ERRORLEVEL%