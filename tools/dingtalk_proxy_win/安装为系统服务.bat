@echo off
chcp 65001 >nul
cd /d "%~dp0"

sc query DingTalkProxy >nul 2>&1
if %errorlevel% == 0 (
    echo [信息] 服务已存在，先停止并删除旧版本...
    sc stop DingTalkProxy >nul 2>&1
    timeout /t 2 /nobreak >nul
    sc delete DingTalkProxy >nul 2>&1
    timeout /t 1 /nobreak >nul
)

echo [信息] 安装 DingTalkProxy 服务...
sc create DingTalkProxy binPath= "\"%~dp0DingTalkProxy.exe\"" start= auto DisplayName= "DingTalk 转发代理"
sc description DingTalkProxy "钉钉消息转发代理，VPN IP 自动探测注册"
sc start DingTalkProxy

echo.
echo [完成] 服务已安装并启动。
echo   查看状态: sc query DingTalkProxy
echo   查看日志: 事件查看器 -> Windows 日志 -> 应用程序
pause
