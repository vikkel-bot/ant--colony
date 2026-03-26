@echo off
setlocal EnableExtensions
set ROOT=C:\Users\vikke\OneDrive\bitvavo-bot_clean
set PY=%ROOT%\.venv\Scripts\python.exe
cd /d "C:\Users\vikke\OneDrive\bitvavo-bot_clean\ant_colony\workers\XRP-EUR"

set CB20_MARKET=XRP-EUR
"%PY%" .\cb20_regime.py --interval 4h
exit /b %ERRORLEVEL%
