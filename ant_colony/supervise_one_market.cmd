@echo off
setlocal
python "%~dp0supervise_one_market.py" %*
exit /b %ERRORLEVEL%