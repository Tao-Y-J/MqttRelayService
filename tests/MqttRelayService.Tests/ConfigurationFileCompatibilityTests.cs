using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using MqttRelayService.Options;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// 配置文件兼容性测试。
    /// appsettings*.json 中带有 // 与 /* */ 注释：.NET 配置提供程序会跳过注释，
    /// 但安装/卸载脚本使用 Windows PowerShell 5.1 的 ConvertFrom-Json，它不支持注释，
    /// 因此脚本先剥离注释再解析。这里同时锁住两条契约：
    /// 1）真实配置文件能被 .NET 配置系统加载、绑定，并通过程序自身的启动校验；
    /// 2）剥离注释后必须是严格 JSON（不含尾随逗号），否则安装脚本会静默回退到默认值。
    /// </summary>
    public class ConfigurationFileCompatibilityTests
    {
        /// <summary>
        /// 与 Scripts/*.ps1 中剥离 JSON 注释的正则保持一致：字符串字面量分支优先匹配且原样保留，
        /// 因此引号内的 //（例如 http:// 或路径）不会被误删。
        /// </summary>
        private static readonly Regex JsonCommentPattern = new(
            "(?s)(\"(?:\\\\.|[^\"\\\\])*\")|(//[^\r\n]*)|(/\\*.*?\\*/)",
            RegexOptions.Compiled);

        [Theory]
        [InlineData("appsettings.json")]
        [InlineData("appsettings.Development.json")]
        public void ShippedAppSettings_AfterCommentRemoval_ShouldBeStrictJson(string fileName)
        {
            var text = File.ReadAllText(ResolveRepositoryFile("src", "MqttRelayService", fileName));

            // 注释是本测试的前提：注释被移除后这里会失败，提示该契约已不再被覆盖
            Assert.Contains("//", text, StringComparison.Ordinal);

            // JsonDocument 默认既不接受注释也不接受尾随逗号，能解析即证明剥离注释后是严格 JSON
            using var document = JsonDocument.Parse(StripJsonComments(text));
            Assert.Equal(JsonValueKind.Object, document.RootElement.ValueKind);
        }

        [Fact]
        public void ProductionAppSettings_ShouldBindAllSectionsAndPassStartupValidation()
        {
            var configuration = BuildConfiguration("appsettings.json");

            var service = configuration.GetSection("Service").Get<ServiceOptions>();
            Assert.Equal("MqttRelayService", service?.Name);

            var mqtt = configuration.GetSection("Mqtt").Get<MqttOptions>();
            Assert.Equal(1883, mqtt?.TcpPort);
            Assert.Equal(1, mqtt?.DefaultQos);

            var auth = configuration.GetSection("Auth").Get<AuthOptions>();
            Assert.True(auth?.AllowAnonymous);
            var user = Assert.Single(auth!.Users);
            Assert.Equal("app1", user.Username);
            Assert.Equal("app1", user.ClientIdPrefix);

            var routing = configuration.GetSection("Routing").Get<RoutingOptions>();
            Assert.False(routing?.EchoToSender);

            var reliability = configuration.GetSection("Reliability").Get<ReliabilityOptions>();
            Assert.NotNull(reliability);
            Assert.Equal("AtLeastOnce", reliability!.DeliverySemantics);
            Assert.Equal(10000, reliability.QueueCapacity);
            Assert.Equal(3, reliability.MaxConcurrentHandlers);
            Assert.Equal(3, reliability.MaxRetryCount);
            Assert.True(reliability.EnableDeadLetter);
            Assert.Equal("data/deadletter", reliability.DeadLetterPath);
            Assert.Equal(30, reliability.DeadLetterRetentionDays);
            Assert.False(reliability.DropWhenQueueFull);
            Assert.True(reliability.MaxConcurrencyHardLimit >= reliability.MaxConcurrentHandlers);
            // 停机排空超时小于最大重试退避时，停机阶段最后一批重试会被直接放弃
            Assert.True(reliability.ShutdownDrainTimeoutMs >= reliability.RetryMaxDelayMs);
            // 退避等待中的重试任务上限超过队列容量时，失败洪峰会绕开队列容量上限
            Assert.True(reliability.MaxPendingRetryTasks <= reliability.QueueCapacity);

            var web = configuration.GetSection("Web").Get<WebOptions>();
            Assert.True(web?.Enabled);
            Assert.Equal(5000, web?.Port);
            // 配置绑定会把 JSON 中的 null 折叠成字符串空值，程序按"未配置密钥"处理（鉴权过滤器用 IsNullOrEmpty 判断）
            Assert.True(string.IsNullOrEmpty(web!.ApiKey));

            var audit = configuration.GetSection("AuditStorage").Get<AuditStorageOptions>();
            Assert.NotNull(audit);
            Assert.Equal("Sqlite", audit!.Provider);
            Assert.Equal("Data Source=data/audit.db", audit.ConnectionString);
            Assert.True(audit.AutoInitializeSchema);
            Assert.Equal(30, audit.RetentionDays);
            Assert.Equal(3, audit.CleanupAtHour!.Value);
            Assert.Equal(1440, audit.CleanupIntervalMinutes);
            Assert.True(audit.VacuumAfterCleanup);

            // 生产配置必须通过程序自身的启动校验：注释化过程中漏键或改坏取值都会在这里失败
            Program.ValidateAuditStorageConfiguration(configuration);

            Assert.Equal("relay", configuration.GetValue<string>("Serilog:FileNamePrefix"));
            Assert.Equal(30, configuration.GetValue<int>("Serilog:RetentionDays"));
            Assert.False(configuration.GetValue<bool>("Serilog:IncludeCallerInfo"));
            Assert.Equal("Information", configuration.GetValue<string>("Serilog:MinimumLevel:Default"));
            Assert.Equal("Warning", configuration.GetValue<string>("Serilog:MinimumLevel:Override:Microsoft"));
        }

        [Fact]
        public void DevelopmentAppSettings_ShouldOverrideProductionValues()
        {
            var configuration = BuildConfiguration("appsettings.json", "appsettings.Development.json");

            Assert.Equal(1000, configuration.GetValue<int>("Reliability:QueueCapacity"));
            Assert.Equal(2000, configuration.GetValue<int>("Reliability:EnqueueTimeoutMs"));
            Assert.Equal(1, configuration.GetValue<int>("Reliability:MaxConcurrentHandlers"));
            Assert.Equal("Debug", configuration.GetValue<string>("Serilog:MinimumLevel:Default"));
            Assert.Equal(7, configuration.GetValue<int>("Serilog:RetentionDays"));
            Assert.True(configuration.GetValue<bool>("Serilog:IncludeCallerInfo"));

            // 开发配置同样要通过启动校验，避免开发环境掩盖配置错误
            Program.ValidateAuditStorageConfiguration(configuration);
        }

        [Theory]
        [InlineData("install-service.ps1")]
        [InlineData("uninstall-service.ps1")]
        public void ServiceScripts_ShouldParseAppSettingsThroughCommentTolerantHelper(string fileName)
        {
            var script = File.ReadAllText(ResolveRepositoryFile("src", "MqttRelayService", "Scripts", fileName));

            // Windows PowerShell 5.1 的 ConvertFrom-Json 不支持注释，脚本必须走剥离注释的读取函数
            Assert.Contains("function Read-RelayJsonConfig", script, StringComparison.Ordinal);
            Assert.Contains("Read-RelayJsonConfig -Path $configPath", script, StringComparison.Ordinal);
            Assert.DoesNotContain(
                "Get-Content $configPath -Raw | ConvertFrom-Json",
                script,
                StringComparison.Ordinal);
        }

        private static IConfiguration BuildConfiguration(params string[] fileNames)
        {
            // 基准目录必须是配置文件所在目录（src/MqttRelayService），与程序运行时的 AppContext.BaseDirectory 一致
            var builder = new ConfigurationBuilder()
                .SetBasePath(ResolveRepositoryFile("src", "MqttRelayService"));
            foreach (var fileName in fileNames)
            {
                builder.AddJsonFile(fileName, optional: false, reloadOnChange: false);
            }

            return builder.Build();
        }

        private static string StripJsonComments(string json)
        {
            return JsonCommentPattern.Replace(
                json,
                match => match.Groups[2].Success || match.Groups[3].Success ? string.Empty : match.Value);
        }

        private static string ResolveRepositoryFile(params string[] relativeParts)
        {
            return Path.Combine(new[] { ResolveRepositoryDirectory() }.Concat(relativeParts).ToArray());
        }

        /// <summary>
        /// 从测试输出目录向上定位仓库根目录，避免依赖输出目录层级的硬编码相对路径。
        /// </summary>
        private static string ResolveRepositoryDirectory()
        {
            var directory = new DirectoryInfo(AppContext.BaseDirectory);
            while (directory != null)
            {
                if (Directory.Exists(Path.Combine(directory.FullName, "src", "MqttRelayService")))
                {
                    return directory.FullName;
                }

                directory = directory.Parent;
            }

            throw new DirectoryNotFoundException("未能从测试输出目录向上定位仓库根目录（缺少 src/MqttRelayService）");
        }
    }
}
