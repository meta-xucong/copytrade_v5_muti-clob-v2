@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%multi_node_5.ps1" -Action stop
set "EXITCODE=%ERRORLEVEL%"
echo.
pause
exit /b %EXITCODE%
