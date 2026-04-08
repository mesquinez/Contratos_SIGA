@echo off
setlocal

set "PROJECT_ROOT=%~dp0.."
set "POWERSHELL_EXE=powershell.exe"
set "SCRIPT_PATH=%~dp0run_official_contracts_sync.ps1"

%POWERSHELL_EXE% -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_PATH%" -ProjectRoot "%PROJECT_ROOT%"
exit /b %errorlevel%
