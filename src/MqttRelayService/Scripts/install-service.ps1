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

$defaultServiceName = "MqttRelayService"
$displayName = "MQTT Relay Service"
$description = "内部 MQTT 消息转发服务"

# 获取路径
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$baseDir = Split-Path -Parent $scriptDir
$exePath = Join-Path $baseDir "MqttRelayService.exe"
$configPath = Join-Path $baseDir "appsettings.json"
$binaryPathName = "`"$exePath`""

function Get-RelayServiceNamesForCurrentDeployment {
    param(
        [string]$ExpectedServiceName,
        [string]$FallbackServiceName,
        [string]$ExecutablePath
    )

    $serviceNames = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($name in @($ExpectedServiceName, $FallbackServiceName)) {
        if (-not [string]::IsNullOrWhiteSpace($name)) {
            $null = $serviceNames.Add($name)
        }
    }

    try {
        $servicesByPath = Get-CimInstance Win32_Service -ErrorAction Stop | Where-Object {
            $pathName = $_.PathName
            -not [string]::IsNullOrWhiteSpace($pathName) -and
                $pathName.IndexOf($ExecutablePath, [System.StringComparison]::OrdinalIgnoreCase) -ge 0
        }

        foreach ($service in $servicesByPath) {
            $null = $serviceNames.Add($service.Name)
        }
    } catch {
        Write-Warning "按可执行路径探测已安装服务失败: $_"
    }

    return @($serviceNames)
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

Write-Host "[信息] 脚本目录: $scriptDir"
Write-Host "[信息] 服务目录: $baseDir"
Write-Host "[信息] 服务名称: $serviceName"
Write-Host "[信息] 显示名称: $displayName"

# 检查 exe 文件
if (-not (Test-Path $exePath)) {
    Write-Host "[错误] 未找到服务可执行文件: $exePath" -ForegroundColor Red
    exit 1
}
Write-Host "[信息] 可执行文件: $exePath"

# 读取配置获取端口号
$tcpPort = 1883
if (Test-Path $configPath) {
    try {
        $config = Get-Content $configPath -Raw | ConvertFrom-Json
        $tcpPort = $config.Mqtt.TcpPort
        Write-Host "[信息] 配置文件: $configPath"
        Write-Host "[信息] MQTT 端口: $tcpPort"
    } catch {
        Write-Warning "读取配置文件失败，使用默认端口 1883"
    }
} else {
    Write-Warning "未找到配置文件，使用默认端口 1883"
}

# 获取本机 IP 地址
$ipList = @()
try {
    $ipAddresses = [System.Net.Dns]::GetHostAddresses([System.Net.Dns]::GetHostName()) | 
        Where-Object { $_.AddressFamily -eq 'InterNetwork' }
    $ipList = $ipAddresses | ForEach-Object { $_.IPAddressToString }
} catch {
    Write-Warning "获取本机 IP 失败"
}

if ($ipList.Count -gt 0) {
    Write-Host "[信息] 本机 IP 地址:"
    foreach ($ip in $ipList) {
        Write-Host "         - $ip`:$tcpPort"
    }
} else {
    Write-Host "[信息] 本机 IP 地址: 127.0.0.1:$tcpPort"
}

Write-Host ""

# 检查当前部署目录下是否已存在同一路径或旧名称的服务，先清理避免改名后遗留旧服务
$existingServiceNames = @(Get-RelayServiceNamesForCurrentDeployment `
    -ExpectedServiceName $serviceName `
    -FallbackServiceName $defaultServiceName `
    -ExecutablePath $exePath)
$existingServices = @(
    foreach ($existingServiceName in $existingServiceNames) {
        $existingService = Get-Service -Name $existingServiceName -ErrorAction SilentlyContinue
        if ($existingService) {
            $existingService
        }
    }
)

if ($existingServices.Count -gt 0) {
    Write-Warning "发现当前部署对应的已安装服务，先执行卸载："
    foreach ($existingService in $existingServices) {
        Write-Host "         - $($existingService.Name)"
    }

    & "$scriptDir\uninstall-service.ps1" -ServiceNames $existingServices.Name
    Write-Host ""
}

# 创建服务
try {
    Write-Host "[步骤] 正在创建 Windows Service '$serviceName' ..."
    New-Service -Name $serviceName `
        -BinaryPathName $binaryPathName `
        -DisplayName $displayName `
        -Description $description `
        -StartupType Automatic
    Write-Host "[成功] 服务创建成功。" -ForegroundColor Green
} catch {
    Write-Host "[错误] 创建服务失败: $_" -ForegroundColor Red
    exit 1
}

# 启动服务
try {
    Write-Host "[步骤] 正在启动服务 '$serviceName' ..."
    Start-Service -Name $serviceName
    
    # 等待一小会儿让服务启动
    Start-Sleep -Seconds 2
    
    $service = Get-Service -Name $serviceName
    if ($service.Status -eq 'Running') {
        Write-Host "[成功] 服务已启动，当前状态: $($service.Status)" -ForegroundColor Green
    } else {
        Write-Warning "服务状态: $($service.Status)，可能正在启动中..."
    }
} catch {
    Write-Host "[错误] 启动服务失败: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "============================================================"
Write-Host " 安装结果" -ForegroundColor Cyan
Write-Host "============================================================"
Write-Host " 服务名称 : $serviceName"
Write-Host " 显示名称 : $displayName"
Write-Host " 启动类型 : Automatic"
Write-Host " 当前状态 : $($service.Status)"
Write-Host " MQTT 端口: $tcpPort"
if ($ipList.Count -gt 0) {
    Write-Host " 访问地址 :"
    foreach ($ip in $ipList) {
        Write-Host "            - $ip`:$tcpPort"
    }
}
Write-Host "============================================================"
Write-Host " [成功] 安装完成！" -ForegroundColor Green
Write-Host "============================================================"
