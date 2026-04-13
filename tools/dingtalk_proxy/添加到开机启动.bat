@echo off
chcp 65001 >nul
set STARTUP=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup
set PROXY_BAT=%~dp0启动代理.bat
set LINK=%STARTUP%\钉钉代理.bat

(
    echo @echo off
    echo start "" "%PROXY_BAT%"
) > "%LINK%"

echo 已添加到开机自启动。
echo 路径: %LINK%
echo.
echo 下次登录 Windows 后代理将自动启动。
pause
