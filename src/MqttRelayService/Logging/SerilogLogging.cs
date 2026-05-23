using System.Diagnostics;
using System.Reflection;
using Serilog;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;

namespace MqttRelayService.Logging
{
    /// <summary>
    /// Serilog 日志配置集中管理
    /// </summary>
    public static class SerilogLogging
    {
        private const string DefaultConsoleTemplate =
            "[{Timestamp:HH:mm:ss} {Level:u3}] [{ServiceName}] {Message:lj}{NewLine}{Exception}";

        private const string DefaultFileTemplate =
            "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] [{ServiceName}] {Message:lj}{NewLine}{Exception}";

        private const string CallerConsoleTemplate =
            "[{Timestamp:HH:mm:ss} {Level:u3}] [{ServiceName}] [{Caller}] {Message:lj}{NewLine}{Exception}";

        private const string CallerFileTemplate =
            "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] [{ServiceName}] [{Caller}] {Message:lj}{NewLine}{Exception}";

        /// <summary>
        /// 配置 Serilog 日志系统
        /// </summary>
        public static Serilog.ILogger CreateLogger(Microsoft.Extensions.Configuration.IConfiguration configuration, string serviceName)
        {
            var fileNamePrefix = configuration.GetValue("Serilog:FileNamePrefix", "relay");
            var retentionDays = configuration.GetValue("Serilog:RetentionDays", 30);
            var includeCallerInfo = configuration.GetValue("Serilog:IncludeCallerInfo", false);

            var consoleTemplate = includeCallerInfo ? CallerConsoleTemplate : DefaultConsoleTemplate;
            var fileTemplate = includeCallerInfo ? CallerFileTemplate : DefaultFileTemplate;

            var loggerConfig = new LoggerConfiguration()
                .MinimumLevel.Is(GetMinimumLevel(configuration))
                .Enrich.FromLogContext()
                .Enrich.WithProperty("ServiceName", serviceName)
                .WriteTo.Console(outputTemplate: consoleTemplate);

            ApplyMinimumLevelOverrides(loggerConfig, configuration);

            // 文件日志配置
            var logsPath = Path.Combine(AppContext.BaseDirectory, "Logs");
            loggerConfig.WriteTo.File(
                Path.Combine(logsPath, $"{fileNamePrefix}-.log"),
                rollingInterval: RollingInterval.Hour,
                retainedFileCountLimit: retentionDays * 24,
                outputTemplate: fileTemplate);

            if (includeCallerInfo)
            {
                // 启用调用者信息富集器（性能成本：每条日志解析 StackTrace，默认关闭）
                loggerConfig = loggerConfig.Enrich.WithCallerInfo();
            }

            return loggerConfig.CreateLogger();
        }

        private static void ApplyMinimumLevelOverrides(
            LoggerConfiguration loggerConfig,
            Microsoft.Extensions.Configuration.IConfiguration configuration)
        {
            var overrideSection = configuration.GetSection("Serilog:MinimumLevel:Override");
            foreach (var child in overrideSection.GetChildren())
            {
                if (string.IsNullOrWhiteSpace(child.Key) || string.IsNullOrWhiteSpace(child.Value))
                {
                    continue;
                }

                if (!Enum.TryParse<LogEventLevel>(child.Value, out var level))
                {
                    continue;
                }

                loggerConfig.MinimumLevel.Override(child.Key, level);
            }
        }

        private static LogEventLevel GetMinimumLevel(IConfiguration configuration)
        {
            var defaultLevel = configuration.GetValue("Serilog:MinimumLevel:Default", "Information");
            return Enum.TryParse<LogEventLevel>(defaultLevel, out var level) ? level : LogEventLevel.Information;
        }
    }

    /// <summary>
    /// Serilog 调用者信息扩展，通过 StackTrace 解析调用者方法，补充 {Caller} 字段
    /// </summary>
    public static class SerilogCallerInfoExtensions
    {
        /// <summary>
        /// 注册 CallerEnricher 到 LoggerConfiguration
        /// </summary>
        public static LoggerConfiguration WithCallerInfo(this LoggerConfiguration loggerConfiguration)
        {
            return loggerConfiguration.Enrich.With<CallerEnricher>();
        }

        /// <summary>
        /// 注册 CallerEnricher 到 Enrich 配置
        /// </summary>
        public static LoggerConfiguration WithCallerInfo(this LoggerEnrichmentConfiguration enrichmentConfiguration)
        {
            ArgumentNullException.ThrowIfNull(enrichmentConfiguration);
            return enrichmentConfiguration.With<CallerEnricher>();
        }
    }

    /// <summary>
    /// 调用者信息富集器：解析当前栈，跳过日志框架本身的栈帧，记录第一个业务调用者
    /// </summary>
    internal sealed class CallerEnricher : ILogEventEnricher
    {
        private static readonly string[] ExcludedNamespaces =
        {
            "Serilog",
            "Microsoft.Extensions.Logging",
            "Microsoft.Extensions.Hosting",
            "System",
        };

        public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
        {
            ArgumentNullException.ThrowIfNull(logEvent);
            ArgumentNullException.ThrowIfNull(propertyFactory);

            // 跳过 Enrich/Logger 自身，从可能的业务栈帧开始扫描
            var stack = new StackTrace(skipFrames: 3, fNeedFileInfo: false);
            for (var i = 0; i < stack.FrameCount; i++)
            {
                var method = stack.GetFrame(i)?.GetMethod();
                if (method == null)
                {
                    continue;
                }

                var declaringType = method.DeclaringType;
                if (declaringType == null)
                {
                    continue;
                }

                var ns = declaringType.Namespace ?? string.Empty;
                if (IsExcluded(ns))
                {
                    continue;
                }

                var caller = $"{declaringType.FullName}.{method.Name}";
                logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("Caller", caller));
                return;
            }

            // 兜底，避免 outputTemplate 缺字段时显示空白
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("Caller", "(unknown)"));
        }

        private static bool IsExcluded(string ns)
        {
            if (string.IsNullOrEmpty(ns))
            {
                return false;
            }

            foreach (var excluded in ExcludedNamespaces)
            {
                if (ns.StartsWith(excluded, StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        }
    }
}
