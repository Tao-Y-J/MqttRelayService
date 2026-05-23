@echo off
chcp 936 >nul
setlocal
title Run MQTT stress test for 60s

set "ROOT=%~dp0"
set "SCRIPT=%ROOT%stress_mqtt_1883.py"
set "PYTHON_EXE=C:\Users\admin\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"

if not exist "%SCRIPT%" (
    echo [ERROR] Cannot find stress script: %SCRIPT%
    pause
    exit /b 1
)

if not exist "%PYTHON_EXE%" (
    where python >nul 2>&1
    if %errorlevel% neq 0 (
        echo [ERROR] Python runtime not found.
        echo [ERROR] Expected: %PYTHON_EXE%
        pause
        exit /b 1
    )
    set "PYTHON_EXE=python"
)

echo ========================================
echo  MQTT stress test - 60 seconds
echo ========================================
echo [INFO] Python : %PYTHON_EXE%
echo [INFO] Script : %SCRIPT%
echo [INFO] Target : 127.0.0.1:1883
echo.

"%PYTHON_EXE%" "%SCRIPT%" --duration-seconds 60
set "EXITCODE=%errorlevel%"

echo.
echo ========================================
if %EXITCODE% neq 0 (
    echo [RESULT] Stress test failed. ExitCode=%EXITCODE%
) else (
    echo [RESULT] Stress test finished successfully.
)
echo ========================================
pause
exit /b %EXITCODE%
