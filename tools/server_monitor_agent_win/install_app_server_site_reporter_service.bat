@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul

set "SERVICE_NAME=AppServerSiteReporter"
set "SCRIPT_DIR=%~dp0"
set "PS_EXE=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"
set "SCRIPT_PATH=%SCRIPT_DIR%app_server_site_reporter.ps1"
set "BIN_PATH=\"%PS_EXE%\" -NoProfile -ExecutionPolicy Bypass -File \"%SCRIPT_PATH%\""

echo ===============================================
echo     App Server Site Reporter Service Installer
echo ===============================================
echo.
echo [INFO] Service Name : %SERVICE_NAME%
echo [INFO] Script Path  : %SCRIPT_PATH%
echo.

if not exist "%SCRIPT_PATH%" (
  echo [FAIL] Script not found: %SCRIPT_PATH%
  pause
  exit /b 1
)

sc query "%SERVICE_NAME%" >nul 2>nul
if %errorlevel%==0 (
  echo [WARN] Service exists, try stop/delete first...
  sc stop "%SERVICE_NAME%" >nul 2>nul
  timeout /t 1 >nul
  sc delete "%SERVICE_NAME%" >nul 2>nul
  timeout /t 1 >nul
)

sc create "%SERVICE_NAME%" binPath= %BIN_PATH% start= auto DisplayName= "App Server Site Reporter" 
if not %errorlevel%==0 (
  echo [FAIL] sc create failed.
  pause
  exit /b 1
)

sc description "%SERVICE_NAME%" "Collect IIS sites and report to ProjectStageService."
sc failure "%SERVICE_NAME%" reset= 86400 actions= restart/5000/restart/5000/restart/5000
sc start "%SERVICE_NAME%"

echo.
echo [OK] Service installed and started.
echo.
echo Management:
echo   sc query "%SERVICE_NAME%"
echo   sc stop "%SERVICE_NAME%"
echo   sc start "%SERVICE_NAME%"
echo   sc delete "%SERVICE_NAME%"
echo.
pause

