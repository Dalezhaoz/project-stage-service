@echo off
chcp 65001 >nul
set EXE=%~dp0dist\DingTalkProxy.exe
if not exist "%EXE%" (
    echo [错误] 找不到 %EXE%，请先编译。
    pause
    exit /b
)
reg add "HKCU\Software\Microsoft\Windows\CurrentVersion\Run" /v DingTalkProxy /t REG_SZ /d "\"%EXE%\"" /f
echo [完成] 已添加到开机启动（当前用户）。
pause
