@echo off
chcp 936 >nul
title 安装 MQTT 转发服务

:: 检查管理员权限
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [错误] 需要管理员权限，请以管理员身份运行此脚本。
    pause
    exit /b 1
)

echo ========================================
echo  MQTT 转发服务安装脚本
echo ========================================
echo.

:: 在当前窗口执行 PowerShell 脚本（不弹新窗口）
powershell -ExecutionPolicy Bypass -NoProfile -Command "& '%~dp0install-service.ps1'"

echo.
echo ========================================
if %errorlevel% neq 0 (
    echo [结果] 安装过程出现异常，请查看上方日志。
) else (
    echo [结果] 脚本执行完毕。
)
echo ========================================
pause