@echo off
chcp 65001 >nul

:: 自动提升管理员权限
net session >nul 2>&1
if %errorlevel% neq 0 (
    powershell -Command "Start-Process '%~f0' -Verb RunAs"
    exit /b
)

set PORT=9100
echo [信息] 注册端口 %PORT% 允许当前用户监听...

:: 先删除可能存在的旧规则
netsh http delete urlacl url=http://+:%PORT%/ >nul 2>&1

:: 给 Everyone 组加上监听权限（不需要再次管理员）
netsh http add urlacl url=http://+:%PORT%/ user=Everyone

if %errorlevel% == 0 (
    echo.
    echo [完成] 已注册端口权限，现在可以直接运行 DingTalkProxy.exe。
) else (
    echo.
    echo [失败] 注册失败，请检查错误信息。
)

pause
