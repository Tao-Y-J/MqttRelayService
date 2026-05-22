#Requires -Version 5.1

[CmdletBinding()]
param(
    [string[]]$ServiceNames = @()
)

# 检查管理员权限
$currentPrincipal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $currentPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "[错误] 需要管理员权限，请以管理员身份运行此脚本。" -ForegroundColor Red
    exit 1
}

$ErrorActionPreference = "Stop"

$defaultServiceName = "MqttRelayService"

# 获取路径
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$baseDir = Split-Path -Parent $scriptDir
$configPath = Join-Path $baseDir "appsettings.json"
$exePath = Join-Path $baseDir "MqttRelayService.exe"

function Get-RelayServiceNamesForCurrentDeployment {
    param(
        [string]$ExpectedServiceName,
        [string]$FallbackServiceName,
        [string]$ExecutablePath
    )

    $candidateNames = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($name in @($ExpectedServiceName, $FallbackServiceName)) {
        if (-not [string]::IsNullOrWhiteSpace($name)) {
            $null = $candidateNames.Add($name)
        }
    }

    try {
        $servicesByPath = Get-CimInstance Win32_Service -ErrorAction Stop | Where-Object {
            $pathName = $_.PathName
            -not [string]::IsNullOrWhiteSpace($pathName) -and
                $pathName.IndexOf($ExecutablePath, [System.StringComparison]::OrdinalIgnoreCase) -ge 0
        }

        foreach ($service in $servicesByPath) {
            $null = $candidateNames.Add($service.Name)
        }
    } catch {
        Write-Warning "按可执行路径探测已安装服务失败: $_"
    }

    return @($candidateNames)
}

# 从 appsettings.json 读取服务名称，失败时回退到默认值
$serviceName = $defaultServiceName
if (Test-Path $configPath) {
    try {
        $config = Get-Content $configPath -Raw | ConvertFrom-Json
        if ($config.Service -and $config.Service.Name) {
            $serviceName = $config.Service.Name
        }
    } catch {
        Write-Warning "读取 Service.Name 失败，使用默认名称 $defaultServiceName"
    }
} else {
    Write-Warning "未找到配置文件，使用默认服务名称 $defaultServiceName"
}

Write-Host "[信息] 服务名称: $serviceName"
Write-Host ""

# 兼容服务改名场景：优先卸载指定名称，否则按当前配置名称、默认名称和当前部署路径匹配
$resolvedServiceNames = @(
    if ($ServiceNames.Count -gt 0) {
        $ServiceNames | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique
    } else {
        Get-RelayServiceNamesForCurrentDeployment `
            -ExpectedServiceName $serviceName `
            -FallbackServiceName $defaultServiceName `
            -ExecutablePath $exePath
    }
)

$services = @(
    foreach ($resolvedServiceName in $resolvedServiceNames) {
        $service = Get-Service -Name $resolvedServiceName -ErrorAction SilentlyContinue
        if ($service) {
            $service
        }
    }
)

if ($services.Count -eq 0) {
    Write-Warning "未找到与当前部署匹配的服务，无需卸载。"
    exit 0
}

Write-Host "[信息] 即将卸载以下服务:"
foreach ($service in $services) {
    Write-Host "         - $($service.Name) [$($service.Status)]"
}
Write-Host ""

# 停止并删除服务
foreach ($service in $services) {
    if ($service.Status -ne "Stopped") {
        try {
            Write-Host "[步骤] 正在停止服务 '$($service.Name)' ..."
            Stop-Service -Name $service.Name -Force
            try {
                $service.WaitForStatus("Stopped", [TimeSpan]::FromSeconds(30))
                Write-Host "[成功] 服务 '$($service.Name)' 已停止。" -ForegroundColor Green
            } catch {
                Write-Warning "等待服务 '$($service.Name)' 停止超时，继续卸载..."
            }
        } catch {
            Write-Host "[错误] 停止服务 '$($service.Name)' 失败: $_" -ForegroundColor Red
            exit 1
        }
    } else {
        Write-Host "[信息] 服务 '$($service.Name)' 当前未运行，跳过停止步骤。"
    }

    Write-Host ""

    try {
        Write-Host "[步骤] 正在删除服务 '$($service.Name)' ..."
        $scPath = Join-Path $env:SystemRoot "System32\sc.exe"
        $deleteOutput = & $scPath delete $service.Name 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw ($deleteOutput -join [Environment]::NewLine)
        }

        for ($i = 0; $i -lt 10; $i++) {
            Start-Sleep -Milliseconds 500
            $deletedService = Get-Service -Name $service.Name -ErrorAction SilentlyContinue
            if (-not $deletedService) {
                break
            }
        }

        $remainingService = Get-Service -Name $service.Name -ErrorAction SilentlyContinue
        if ($remainingService) {
            throw "服务删除请求已提交，但服务仍存在，请关闭服务管理器窗口后重试。"
        }

        Write-Host "[成功] 服务 '$($service.Name)' 已删除。" -ForegroundColor Green
        
        # 清理该服务对应的防火墙端口发布规则
        try {
            Write-Host "[步骤] 正在清理 Windows 防火墙入站规则..."
            $ruleNames = @(
                "$($service.Name)-MQTT",
                "$($service.Name)-Web",
                "$($service.Name)-API",
                "$($service.Name)-Dashboard"
            )
            foreach ($ruleName in $ruleNames) {
                Remove-NetFirewallRule -Name $ruleName -ErrorAction SilentlyContinue | Out-Null
            }
            Write-Host "         - 防火墙入站端口发布规则清理完成。" -ForegroundColor Green
        } catch {
            Write-Warning "清理防火墙规则失败: $_"
        }
    } catch {
        Write-Host "[错误] 删除服务 '$($service.Name)' 失败: $_" -ForegroundColor Red
        exit 1
    }

    Write-Host ""
}

Write-Host ""
Write-Host "============================================================"
Write-Host " 卸载结果" -ForegroundColor Cyan
Write-Host "============================================================"
Write-Host " 服务名称 : $($services.Name -join ', ')"
Write-Host " 操作结果 : 已卸载"
Write-Host "============================================================"
Write-Host " [成功] 卸载完成！" -ForegroundColor Green
Write-Host "============================================================"
