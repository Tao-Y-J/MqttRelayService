using Serilog;
using Serilog.Events;

namespace MqttRelayService.Logging;

/// <summary>
/// Serilog 日志配置集中管理
/// </summary>
public static class SerilogLogging
{
    /// <summary>
    /// 配置 Serilog 日志系统
    /// </summary>
    public static Serilog.ILogger CreateLogger(Microsoft.Extensions.Configuration.IConfiguration configuration, string serviceName)
    {
        var fileNamePrefix = configuration.GetValue("Serilog:FileNamePrefix", "relay");
        var retentionDays = configuration.GetValue("Serilog:RetentionDays", 30);
        var includeCallerInfo = configuration.GetValue("Serilog:IncludeCallerInfo", false);

        var loggerConfig = new LoggerConfiguration()
            .MinimumLevel.Is(GetMinimumLevel(configuration))
            .Enrich.FromLogContext()
            .Enrich.WithProperty("ServiceName", serviceName)
            .WriteTo.Console(
                outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] [{ServiceName}] {Message:lj}{NewLine}{Exception}");

        // 文件日志配置
        var logsPath = Path.Combine(AppContext.BaseDirectory, "Logs");
        loggerConfig.WriteTo.File(
            Path.Combine(logsPath, $"{fileNamePrefix}-.log"),
            rollingInterval: RollingInterval.Hour,
            retainedFileCountLimit: retentionDays * 24,
            outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] [{ServiceName}] {Message:lj}{NewLine}{Exception}");

        if (includeCallerInfo)
        {
            // 调用者信息扩展，简化实现
            loggerConfig = SerilogCallerInfoExtensions.WithCallerInfo(loggerConfig);
        }

        return loggerConfig.CreateLogger();
    }

    private static LogEventLevel GetMinimumLevel(IConfiguration configuration)
    {
        var defaultLevel = configuration.GetValue("Serilog:MinimumLevel:Default", "Information");
        return Enum.TryParse<LogEventLevel>(defaultLevel, out var level) ? level : LogEventLevel.Information;
    }
}

/// <summary>
/// Serilog 调用者信息扩展（简化实现）
/// </summary>
public static class SerilogCallerInfoExtensions
{
    public static LoggerConfiguration WithCallerInfo(this LoggerConfiguration loggerConfiguration)
    {
        // 简化的调用者信息扩展，实际生产环境可使用更完整的实现
        return loggerConfiguration;
    }
}