@echo off
chcp 65001 >nul
set PS1=%~dp0watchdog.ps1

if not exist "%PS1%" (
    echo [ERROR] Missing %PS1%
    pause
    exit /b 1
)

reg add "HKCU\Software\Microsoft\Windows\CurrentVersion\Run" /v DingTalkProxyWatchdog /t REG_SZ /d "powershell.exe -WindowStyle Hidden -ExecutionPolicy Bypass -File \"%PS1%\"" /f

echo [OK] Added DingTalkProxy watchdog to startup.
echo It will restart the proxy 5 seconds after any unexpected exit.
pause
