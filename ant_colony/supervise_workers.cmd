@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Start supervisors for all markets (each minimized)
set "SCRIPTDIR=%~dp0"

for %%M in (BTC-EUR ETH-EUR SOL-EUR XRP-EUR ADA-EUR BNB-EUR) do (
  start "SUPERVISE_%%M" /min "%ComSpec%" /d /c ""%SCRIPTDIR%supervise_one_market.cmd" %%M"
)

echo OK started supervisors.
endlocal
exit /b 0
