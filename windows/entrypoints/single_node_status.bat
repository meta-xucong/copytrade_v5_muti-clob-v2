@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%single_node.ps1" -Action status
set "EXITCODE=%ERRORLEVEL%"
echo.
pause
exit /b %EXITCODE%
