@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%status_copytrade_live_supervised.ps1"
set "EXITCODE=%ERRORLEVEL%"
echo.
pause
if not "%EXITCODE%"=="0" (
  echo Status check failed with exit code %EXITCODE%.
)
exit /b %EXITCODE%
