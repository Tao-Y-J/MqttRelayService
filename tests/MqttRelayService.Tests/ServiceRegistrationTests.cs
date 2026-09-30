using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MqttRelayService.Services.Abstractions;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// 生产依赖注入图解析测试。
    /// 装饰器注册使用工厂委托，容器无法静态检测循环依赖：一旦形成环，
    /// 真实进程会在启动阶段卡死（此前 MetricsClientRegistry 直接依赖 IMetricsService 与
    /// MetricsService 依赖 IClientRegistry 构成环，导致服务启动后既不监听端口也不报错）。
    /// </summary>
    public class ServiceRegistrationTests
    {
        [Fact]
        public async Task ConfigureCoreServices_WebEnabled_ShouldResolveKeySingletonsWithoutCircularDependency()
        {
            var provider = BuildProvider(enableWeb: true);

            await AssertResolvesAsync<IMetricsService>(provider);
            await AssertResolvesAsync<IClientRegistry>(provider);
            await AssertResolvesAsync<IMessageQueue>(provider);
            await AssertResolvesAsync<IMqttBrokerHost>(provider);
            await AssertResolvesAsync<IMessageDeliveryService>(provider);
            await AssertResolvesAsync<IAuditRepository>(provider);
        }

        [Fact]
        public async Task ConfigureCoreServices_WebDisabled_ShouldResolveKeySingletons()
        {
            var provider = BuildProvider(enableWeb: false);

            await AssertResolvesAsync<IClientRegistry>(provider);
            await AssertResolvesAsync<IMessageQueue>(provider);
            await AssertResolvesAsync<IMqttBrokerHost>(provider);
            await AssertResolvesAsync<IMessageDeliveryService>(provider);

            Assert.Null(provider.GetService<IMetricsService>());
        }

        private static ServiceProvider BuildProvider(bool enableWeb)
        {
            var services = new ServiceCollection();
            services.AddLogging();

            var configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["Service:Name"] = "MqttRelayServiceTests",
                    ["Mqtt:TcpPort"] = "18830",
                    ["Auth:AllowAnonymous"] = "true",
                    ["Routing:EchoToSender"] = "false",
                    ["Reliability:QueueCapacity"] = "10",
                    ["Reliability:MaxConcurrentHandlers"] = "1",
                    ["Web:Enabled"] = enableWeb ? "true" : "false",
                    ["AuditStorage:Provider"] = "Sqlite",
                    ["AuditStorage:ConnectionString"] = "Data Source=data/test-service-registration.db"
                })
                .Build();

            Program.ConfigureCoreServices(services, configuration, enableWeb);
            return services.BuildServiceProvider();
        }

        [Fact]
        public void ConfigureCoreServices_WithCleanupIntervalBeyondOneDay_ShouldThrow()
        {
            // 1441 分钟（超过 24 小时）会让"每天至少清理一次"的保证失效，必须在启动时失败
            Assert.Throws<InvalidOperationException>(() =>
                Program.ValidateAuditStorageConfiguration(BuildAuditStorageConfiguration(
                    ("AuditStorage:CleanupIntervalMinutes", "1441"))));
        }

        [Fact]
        public void ConfigureCoreServices_WithCleanupIntervalBelowOneMinute_ShouldThrow()
        {
            Assert.Throws<InvalidOperationException>(() =>
                Program.ValidateAuditStorageConfiguration(BuildAuditStorageConfiguration(
                    ("AuditStorage:CleanupIntervalMinutes", "0"))));
        }

        [Fact]
        public void ConfigureCoreServices_WithInvalidCleanupAtHour_ShouldThrow()
        {
            Assert.Throws<InvalidOperationException>(() =>
                Program.ValidateAuditStorageConfiguration(BuildAuditStorageConfiguration(
                    ("AuditStorage:CleanupAtHour", "24"))));
        }

        [Fact]
        public void ConfigureCoreServices_WithNegativeRetentionDays_ShouldThrow()
        {
            // 0 是明确的"关闭清理"，负数属于误配
            Assert.Throws<InvalidOperationException>(() =>
                Program.ValidateAuditStorageConfiguration(BuildAuditStorageConfiguration(
                    ("AuditStorage:RetentionDays", "-1"))));
        }

        [Fact]
        public void ConfigureCoreServices_WithDisabledCleanup_ShouldNotThrow()
        {
            Program.ValidateAuditStorageConfiguration(BuildAuditStorageConfiguration(
                ("AuditStorage:RetentionDays", "0")));
        }

        private static IConfiguration BuildAuditStorageConfiguration(params (string Key, string Value)[] overrides)
        {
            var settings = new Dictionary<string, string?>
            {
                ["AuditStorage:Provider"] = "Sqlite",
                ["AuditStorage:ConnectionString"] = "Data Source=data/test-service-registration.db",
                ["AuditStorage:RetentionDays"] = "30",
                ["AuditStorage:CleanupAtHour"] = "3",
                ["AuditStorage:CleanupIntervalMinutes"] = "1440"
            };

            foreach (var (key, value) in overrides)
            {
                settings[key] = value;
            }

            return new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        }

        private static async Task AssertResolvesAsync<TService>(IServiceProvider provider)
            where TService : class
        {
            var resolveTask = Task.Run(() => provider.GetRequiredService<TService>());
            var completed = await Task.WhenAny(resolveTask, Task.Delay(TimeSpan.FromSeconds(10)));

            Assert.True(completed == resolveTask,
                $"解析 {typeof(TService).Name} 超时，依赖注入图中可能存在循环依赖");
            Assert.NotNull(await resolveTask);
        }
    }
}
