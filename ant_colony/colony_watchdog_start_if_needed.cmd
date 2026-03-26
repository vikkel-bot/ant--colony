@echo off
setlocal
cd /d C:\Users\vikke\OneDrive\bitvavo-bot_clean
"C:\Users\vikke\OneDrive\bitvavo-bot_clean\.venv\Scripts\python.exe" "C:\Users\vikke\OneDrive\bitvavo-bot_clean\ant_colony\colony_watchdog_start_if_needed.py"
exit /b %ERRORLEVEL%