@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
set "SELF_PATH=%~f0"

if /i not "%~1"=="__elevated__" (
  powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "$p = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent()); if ($p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { exit 0 } else { exit 1 }"
  if errorlevel 1 (
    powershell -NoProfile -ExecutionPolicy Bypass -Command ^
      "Start-Process -FilePath '%SELF_PATH%' -ArgumentList '__elevated__' -Verb RunAs"
    if errorlevel 1 (
      echo.
      echo Elevation was canceled or failed.
      echo.
      pause
      exit /b 1
    )
    exit /b 0
  )
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%start_copytrade_live_supervised.ps1"
set "EXITCODE=%ERRORLEVEL%"
echo.
if "%EXITCODE%"=="0" (
  echo Start completed successfully.
) else (
  echo Start failed with exit code %EXITCODE%.
)
echo.
pause
exit /b %EXITCODE%
