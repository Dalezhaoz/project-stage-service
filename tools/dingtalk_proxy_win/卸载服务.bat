@echo off
chcp 65001 >nul
sc stop DingTalkProxy >nul 2>&1
timeout /t 2 /nobreak >nul
sc delete DingTalkProxy
echo [完成] 服务已卸载。
pause
