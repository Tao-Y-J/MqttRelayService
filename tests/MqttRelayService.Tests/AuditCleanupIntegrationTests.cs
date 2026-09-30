using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;
using MqttRelayService.Services.Implementations;
using MqttRelayService.Workers;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// 审计数据清理集成测试。
    /// 用生产注册路径（Program.ConfigureCoreServices + Program.RegisterHostedServices）构建 DI 图，
    /// 从容器解析真实的 <see cref="AuditCleanupWorker"/> 并启动，验证真实 SQLite 库上的清理结果：
    /// 保留窗口外的数据被删除、窗口内的数据保留。
    /// 该测试同时是"清理任务是否被正确注册"的回归护栏：只注册了 IAuditRepository 而漏注册清理任务时，
    /// 这里找不到 AuditCleanupWorker 会直接失败。
    /// </summary>
    public class AuditCleanupIntegrationTests
    {
        [Fact]
        public async Task RealDiWorker_ShouldCleanExpiredAuditRowsOnStartup()
        {
            var testRoot = Path.Combine(
                Path.GetTempPath(), "MqttRelayServiceTests", "cleanup-integration-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(testRoot);
            var dbFile = Path.Combine(testRoot, "audit.db");

            try
            {
                var dbOptions = new AuditStorageOptions
                {
                    Provider = "Sqlite",
                    ConnectionString = $"Data Source={dbFile}",
                    AutoInitializeSchema = true,
                    RetentionDays = 30,
                    CleanupAtHour = 3,
                    CleanupIntervalMinutes = 1440,
                    VacuumAfterCleanup = true
                };

                var seedRepository = new AuditRepository(dbOptions, NullLogger<AuditRepository>.Instance);
                await seedRepository.InitializeAsync();

                var now = DateTime.Now;
                var expiredAt = now.AddDays(-45);
                var keptAt = now.AddDays(-5);

                await seedRepository.RecordMessageAuditsAsync(new[]
                {
                    new MessageAuditRecord
                    {
                        MessageId = "cleanup-expired",
                        Topic = "cleanup/expired",
                        SourceClientId = "cleanup",
                        Status = "Succeeded",
                        CreatedAt = expiredAt,
                        UpdatedAt = expiredAt
                    },
                    new MessageAuditRecord
                    {
                        MessageId = "cleanup-kept",
                        Topic = "cleanup/kept",
                        SourceClientId = "cleanup",
                        Status = "Succeeded",
                        CreatedAt = keptAt,
                        UpdatedAt = keptAt
                    }
                });

                await seedRepository.RecordClientConnectionHistoriesAsync(new[]
                {
                    new ClientConnectionHistoryRecord
                    {
                        ClientId = "cleanup-expired-client",
                        ConnectionId = "conn-expired",
                        Event = "Connected",
                        Timestamp = expiredAt
                    },
                    new ClientConnectionHistoryRecord
                    {
                        ClientId = "cleanup-kept-client",
                        ConnectionId = "conn-kept",
                        Event = "Connected",
                        Timestamp = keptAt
                    }
                });

                Assert.Equal(2, (await seedRepository.GetPagedMessagesAsync(1, 10)).TotalCount);
                Assert.Equal(2, (await seedRepository.GetPagedClientHistoryAsync(1, 10)).TotalCount);

                var services = new ServiceCollection();
                services.AddLogging(builder => builder.AddSimpleConsole());

                var configuration = new ConfigurationBuilder()
                    .AddInMemoryCollection(new Dictionary<string, string?>
                    {
                        ["Service:Name"] = "AuditCleanupIntegrationTests",
                        ["Mqtt:TcpPort"] = "18831",
                        ["Auth:AllowAnonymous"] = "true",
                        ["Routing:EchoToSender"] = "false",
                        ["Reliability:QueueCapacity"] = "10",
                        ["Reliability:MaxConcurrentHandlers"] = "1",
                        ["Web:Enabled"] = "true",
                        ["Web:Port"] = "15001",
                        ["AuditStorage:Provider"] = "Sqlite",
                        ["AuditStorage:ConnectionString"] = $"Data Source={dbFile}",
                        ["AuditStorage:AutoInitializeSchema"] = "true",
                        ["AuditStorage:RetentionDays"] = "30",
                        ["AuditStorage:CleanupAtHour"] = "3",
                        ["AuditStorage:CleanupIntervalMinutes"] = "1440",
                        ["AuditStorage:VacuumAfterCleanup"] = "true"
                    })
                    .Build();

                Program.ConfigureCoreServices(services, configuration, enableWeb: true);
                Program.RegisterHostedServices(services, enableWeb: true);

                await using var provider = services.BuildServiceProvider();

                var workerType = services
                    .Where(d => d.ServiceType == typeof(IHostedService)
                                && d.ImplementationType == typeof(AuditCleanupWorker))
                    .Select(d => d.ImplementationType!)
                    .Single();

                // 逐个解析 IHostedService 会连带构造参与排空的 BrokerWorker（依赖 Host 生命周期），
                // 这里只构造被验证的清理任务本身。
                var worker = (AuditCleanupWorker)ActivatorUtilities.CreateInstance(provider, workerType);
                var auditRepository = provider.GetRequiredService<IAuditRepository>();
                await auditRepository.InitializeAsync();

                // 真实 ExecuteAsync：启动立即清理一轮，随后等待下一个清理时刻
                await worker.StartAsync(CancellationToken.None);
                await Task.Delay(TimeSpan.FromSeconds(3));
                await worker.StopAsync(CancellationToken.None);

                var messagesAfter = await seedRepository.GetPagedMessagesAsync(1, 10);
                var historyAfter = await seedRepository.GetPagedClientHistoryAsync(1, 10);

                Assert.Equal(1, messagesAfter.TotalCount);
                Assert.Equal("cleanup-kept", Assert.Single(messagesAfter.Items).MessageId);
                Assert.Equal(1, historyAfter.TotalCount);
                Assert.Equal("cleanup-kept-client", Assert.Single(historyAfter.Items).ClientId);
            }
            finally
            {
                try
                {
                    Directory.Delete(testRoot, recursive: true);
                }
                catch
                {
                    // 忽略文件占用导致的清理失败
                }
            }
        }
    }
}
