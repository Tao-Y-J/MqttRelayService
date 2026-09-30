@echo off
chcp 936 >nul
setlocal
title Run MQTT stress test for 60s

set "ROOT=%~dp0"
set "SCRIPT=%ROOT%stress_mqtt_1883.py"

if not exist "%SCRIPT%" (
    echo [ERROR] Cannot find stress script: %SCRIPT%
    pause
    exit /b 1
)

rem Locate Python from PATH instead of a machine-specific absolute path.
set "PYTHON_EXE="
where python >nul 2>&1
if not errorlevel 1 set "PYTHON_EXE=python"
if not defined PYTHON_EXE (
    where py >nul 2>&1
    if not errorlevel 1 set "PYTHON_EXE=py"
)
if not defined PYTHON_EXE (
    echo [ERROR] Python runtime not found in PATH.
    echo [HINT] Install Python 3 or add python.exe to PATH, then run again.
    pause
    exit /b 1
)

echo ========================================
echo  MQTT stress test - 60 seconds
echo ========================================
echo [INFO] Python : %PYTHON_EXE%
echo [INFO] Script : %SCRIPT%
echo [INFO] Target : 127.0.0.1:1883
echo [INFO] Rate   : 1000 msg/s total
echo.

"%PYTHON_EXE%" "%SCRIPT%" --duration-seconds 60 --rate-limit 1000
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
