@echo off
setlocal EnableExtensions
set ROOT=C:\Users\vikke\OneDrive\bitvavo-bot_clean
set PY=%ROOT%\.venv\Scripts\python.exe
cd /d "C:\Users\vikke\OneDrive\bitvavo-bot_clean\ant_colony\workers\ADA-EUR"
"%PY%" .\cb21_edge3_gate.py --market ADA-EUR --interval 4h
exit /b %ERRORLEVEL%
