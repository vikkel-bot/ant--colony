@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0\.."

set "ROOT=%CD%"
set "OUTDIR=C:\Trading\ANT_OUT"
set "PYEXE=%ROOT%\.venv\Scripts\python.exe"
set "LOOP_SCRIPT=%ROOT%\ant_colony\colony_cycle_loop_lite.py"

set "LAUNCH_LOG=%OUTDIR%\colony_cycle_loop_launcher.log"
set "STDOUT_LOG=%OUTDIR%\colony_cycle_loop_stdout.log"
set "STDERR_LOG=%OUTDIR%\colony_cycle_loop_stderr.log"

if not exist "%OUTDIR%" mkdir "%OUTDIR%"

echo RUNNING_COLONY_START_LOOP
echo PYEXE=%PYEXE%
echo LOOP_SCRIPT=%LOOP_SCRIPT%
echo OUTDIR=%OUTDIR%

>> "%LAUNCH_LOG%" echo ============================================================
>> "%LAUNCH_LOG%" echo LAUNCH_TS=%DATE% %TIME%
>> "%LAUNCH_LOG%" echo ACTION=START_COLONY_LOOP
>> "%LAUNCH_LOG%" echo ROOT=%ROOT%
>> "%LAUNCH_LOG%" echo PYEXE=%PYEXE%
>> "%LAUNCH_LOG%" echo LOOP_SCRIPT=%LOOP_SCRIPT%

if not exist "%PYEXE%" (
  >> "%LAUNCH_LOG%" echo RESULT=FAILED
  >> "%LAUNCH_LOG%" echo REASON=PYTHON_EXE_NOT_FOUND
  echo PYTHON_EXE_NOT_FOUND
  exit /b 1
)

if not exist "%LOOP_SCRIPT%" (
  >> "%LAUNCH_LOG%" echo RESULT=FAILED
  >> "%LAUNCH_LOG%" echo REASON=LOOP_SCRIPT_NOT_FOUND
  echo LOOP_SCRIPT_NOT_FOUND
  exit /b 1
)

>> "%LAUNCH_LOG%" echo RESULT=STARTING_LOOP_PROCESS

"%PYEXE%" "%LOOP_SCRIPT%" 1>> "%STDOUT_LOG%" 2>> "%STDERR_LOG%"

set "RC=%ERRORLEVEL%"

>> "%LAUNCH_LOG%" echo RESULT=LOOP_PROCESS_EXITED
>> "%LAUNCH_LOG%" echo EXIT_CODE=%RC%

exit /b %RC%