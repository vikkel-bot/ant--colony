@echo off
setlocal EnableExtensions
echo === SNAPSHOT_BUILDER START %DATE% %TIME% ===

cd /d "C:\Users\vikke\OneDrive\bitvavo-bot_clean\ant_colony\workers\BTC-EUR"

set ROOT=C:\Users\vikke\OneDrive\bitvavo-bot_clean
set PY=%ROOT%\.venv\Scripts\python.exe

echo --- EDGE3 FAST (heavy) ---
"%PY%" -u .\run_edge3_fast.py --market BTC-EUR --interval 4h --start-iso 2023-01-01 --end-iso 2026-02-25 --initial-equity 1000 --taker-fee 0.0025 --maker-fee 0.0015 --slippage-bps 3 --entry-mode limit_maker --reclaim-limit-offset-bps -10 --fill-prob 0.70 --stop-extra-slip-bps 1 --vol-filter atr_percentile --atr-period 14 --atr-regime-window 200 --atr-regime-percentile 0.50 --position-fraction 0.5

echo === SNAPSHOT_BUILDER END exit=%ERRORLEVEL% ===
exit /b %ERRORLEVEL%
