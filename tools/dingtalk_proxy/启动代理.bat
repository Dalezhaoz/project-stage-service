@echo off
chcp 65001 >nul
title 钉钉转发代理
cd /d "%~dp0"

python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未检测到 Python，请先安装 Python 3.x
    echo 下载地址: https://www.python.org/downloads/
    pause
    exit /b 1
)

:LOOP
echo.
echo ======================================
echo   钉钉转发代理  %date% %time%
echo ======================================
python dingtalk_proxy.py
echo.
echo [%time%] 代理已停止，5 秒后自动重启...
timeout /t 5 /nobreak >nul
goto LOOP
