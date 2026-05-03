#Requires -Version 5.1

[CmdletBinding()]
param()

# 检查管理员权限
$currentPrincipal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $currentPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "[错误] 需要管理员权限，请以管理员身份运行此脚本。" -ForegroundColor Red
    exit 1
}

$ErrorActionPreference = "Stop"

$serviceName = "MqttRelayService"

Write-Host "[信息] 服务名称: $serviceName"
Write-Host ""

# 检查服务是否存在
$service = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
if (-not $service) {
    Write-Warning "服务 '$serviceName' 不存在，无需卸载。"
    exit 0
}

Write-Host "[信息] 服务当前状态: $($service.Status)"
Write-Host ""

# 停止服务
if ($service.Status -ne "Stopped") {
    try {
        Write-Host "[步骤] 正在停止服务 '$serviceName' ..."
        Stop-Service -Name $serviceName -Force
        try {
            $service.WaitForStatus("Stopped", [TimeSpan]::FromSeconds(30))
            Write-Host "[成功] 服务已停止。" -ForegroundColor Green
        } catch {
            Write-Warning "等待服务停止超时，继续卸载..."
        }
    } catch {
        Write-Host "[错误] 停止服务失败: $_" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "[信息] 服务当前未运行，跳过停止步骤。"
}

Write-Host ""

# 删除服务
try {
    Write-Host "[步骤] 正在删除服务 '$serviceName' ..."
    $scPath = Join-Path $env:SystemRoot "System32\sc.exe"
    $deleteOutput = & $scPath delete $serviceName 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw ($deleteOutput -join [Environment]::NewLine)
    }

    for ($i = 0; $i -lt 10; $i++) {
        Start-Sleep -Milliseconds 500
        $deletedService = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
        if (-not $deletedService) {
            break
        }
    }

    $remainingService = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
    if ($remainingService) {
        throw "服务删除请求已提交，但服务仍存在，请关闭服务管理器窗口后重试。"
    }

    Write-Host "[成功] 服务已删除。" -ForegroundColor Green
} catch {
    Write-Host "[错误] 删除服务失败: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "============================================================"
Write-Host " 卸载结果" -ForegroundColor Cyan
Write-Host "============================================================"
Write-Host " 服务名称 : $serviceName"
Write-Host " 操作结果 : 已卸载"
Write-Host "============================================================"
Write-Host " [成功] 卸载完成！" -ForegroundColor Green
Write-Host "============================================================"
