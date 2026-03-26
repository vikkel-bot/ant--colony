@echo off
setlocal EnableExtensions
set ROOT=C:\Users\vikke\OneDrive\bitvavo-bot_clean
set PY=%ROOT%\.venv\Scripts\python.exe
cd /d "C:\Users\vikke\OneDrive\bitvavo-bot_clean\ant_colony\workers\ETH-EUR"

set CB20_MARKET=ETH-EUR
"%PY%" .\cb20_regime.py --interval 4h
exit /b %ERRORLEVEL%
