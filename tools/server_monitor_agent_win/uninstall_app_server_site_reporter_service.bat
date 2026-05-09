@echo off
setlocal
chcp 65001 >nul

set "SERVICE_NAME=AppServerSiteReporter"

echo ===============================================
echo   App Server Site Reporter Service Uninstaller
echo ===============================================
echo.

sc stop "%SERVICE_NAME%" >nul 2>nul
timeout /t 1 >nul
sc delete "%SERVICE_NAME%"

if %errorlevel%==0 (
  echo [OK] Service deleted: %SERVICE_NAME%
) else (
  echo [WARN] Service may not exist or delete failed: %SERVICE_NAME%
)

echo.
pause

