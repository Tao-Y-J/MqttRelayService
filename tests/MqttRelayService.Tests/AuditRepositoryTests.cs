using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Moq;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Implementations;
using SqlSugar;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// 审计持久化仓储单元测试。
    /// </summary>
    public class AuditRepositoryTests : IDisposable
    {
        private readonly AuditRepository _repository;
        private readonly string _testRoot;
        private readonly string _dbDir;
        private readonly string _dbFile;

        public AuditRepositoryTests()
        {
            _testRoot = Path.Combine(Path.GetTempPath(), "MqttRelayServiceTests", Guid.NewGuid().ToString("N"));
            _dbDir = Path.Combine(_testRoot, "data");
            _dbFile = Path.Combine(_dbDir, "audit.db");

            CleanDatabase();

            var options = new AuditStorageOptions
            {
                Provider = "Sqlite",
                ConnectionString = $"Data Source={_dbFile}",
                AutoInitializeSchema = true,
                MessageRetentionCount = 20000,
                ClientHistoryRetentionCount = 5000
            };

            var loggerMock = new Mock<ILogger<AuditRepository>>();
            _repository = new AuditRepository(options, loggerMock.Object);
        }

        private void CleanDatabase()
        {
            try
            {
                if (Directory.Exists(_testRoot))
                {
                    Directory.Delete(_testRoot, true);
                }
            }
            catch
            {
                // 忽略可能的占用错误
            }
        }

        public void Dispose()
        {
            CleanDatabase();
            GC.SuppressFinalize(this);
        }

        [Fact]
        public async Task InitializeAsync_ShouldCreateDatabaseAndTables()
        {
            await _repository.InitializeAsync();

            Assert.True(Directory.Exists(_dbDir));
            Assert.True(File.Exists(_dbFile));
        }

        [Fact]
        public async Task RecordMessageAuditAsync_ShouldUpsertAndRetrieveCorrectly()
        {
            await _repository.InitializeAsync();
            var record = new MessageAuditRecord
            {
                MessageId = "test_msg_001",
                Topic = "sensor/temp",
                SourceClientId = "device_alpha",
                PayloadSize = 12,
                Payload = "hello world!",
                Qos = 1,
                Retain = false,
                Status = "Queued",
                LatencyMs = 0,
                RetryCount = 0,
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTime.UtcNow
            };

            await _repository.RecordMessageAuditAsync(record);
            var (total1, items1) = await _repository.GetPagedMessagesAsync(1, 10);

            Assert.Equal(1, total1);
            Assert.Single(items1);
            Assert.Equal("test_msg_001", items1[0].MessageId);
            Assert.Equal("Queued", items1[0].Status);

            record.Status = "Succeeded";
            record.LatencyMs = 25.5;
            record.UpdatedAt = DateTime.UtcNow;

            await _repository.RecordMessageAuditAsync(record);
            var (total2, items2) = await _repository.GetPagedMessagesAsync(1, 10);

            Assert.Equal(1, total2);
            Assert.Single(items2);
            Assert.Equal("test_msg_001", items2[0].MessageId);
            Assert.Equal("Succeeded", items2[0].Status);
            Assert.Equal(25.5, items2[0].LatencyMs);
        }

        [Fact]
        public async Task RecordClientConnectionHistoryAsync_ShouldSaveAndFilterCorrectly()
        {
            await _repository.InitializeAsync();
            var record1 = new ClientConnectionHistoryRecord
            {
                ClientId = "client_abc",
                Username = "admin",
                ConnectionId = "conn_x1",
                Event = "Connected",
                Details = "Subscribed to status/#",
                Timestamp = DateTime.UtcNow
            };
            var record2 = new ClientConnectionHistoryRecord
            {
                ClientId = "client_def",
                Username = "user",
                ConnectionId = "conn_x2",
                Event = "Disconnected",
                Details = "Connection lost",
                Timestamp = DateTime.UtcNow.AddSeconds(1)
            };

            await _repository.RecordClientConnectionHistoryAsync(record1);
            await _repository.RecordClientConnectionHistoryAsync(record2);

            var (totalAll, _) = await _repository.GetPagedClientHistoryAsync(1, 10);
            Assert.Equal(2, totalAll);

            var (totalFiltered, filteredItems) = await _repository.GetPagedClientHistoryAsync(1, 10, clientId: "client_abc");
            Assert.Equal(1, totalFiltered);
            Assert.Equal("client_abc", filteredItems[0].ClientId);

            var (totalSearch, searchItems) = await _repository.GetPagedClientHistoryAsync(1, 10, search: "lost");
            Assert.Equal(1, totalSearch);
            Assert.Equal("client_def", searchItems[0].ClientId);
        }

        [Fact]
        public async Task CleanupHistoryAsync_ShouldLimitTableSizesCorrectly()
        {
            await _repository.InitializeAsync();

            for (int i = 0; i < 15; i++)
            {
                await _repository.RecordMessageAuditAsync(new MessageAuditRecord
                {
                    MessageId = $"msg_{i:00}",
                    Topic = "test",
                    SourceClientId = "test_src",
                    PayloadSize = 0,
                    Qos = 0,
                    Retain = false,
                    Status = "Queued",
                    CreatedAt = DateTime.UtcNow.AddMinutes(i),
                    UpdatedAt = DateTime.UtcNow.AddMinutes(i)
                });
            }

            for (int i = 0; i < 10; i++)
            {
                await _repository.RecordClientConnectionHistoryAsync(new ClientConnectionHistoryRecord
                {
                    ClientId = $"client_{i:00}",
                    ConnectionId = $"conn_{i}",
                    Event = "Connected",
                    Timestamp = DateTime.UtcNow.AddMinutes(i)
                });
            }

            await _repository.CleanupHistoryAsync(keepMessagesCount: 5, keepClientHistoryCount: 3);

            var (msgTotal, msgItems) = await _repository.GetPagedMessagesAsync(1, 20);
            Assert.Equal(5, msgTotal);
            Assert.Contains(msgItems, m => m.MessageId == "msg_14");
            Assert.DoesNotContain(msgItems, m => m.MessageId == "msg_00");

            var (clientTotal, clientItems) = await _repository.GetPagedClientHistoryAsync(1, 20);
            Assert.Equal(3, clientTotal);
            Assert.Contains(clientItems, c => c.ClientId == "client_09");
            Assert.DoesNotContain(clientItems, c => c.ClientId == "client_00");
        }

        [Fact]
        public async Task GetDashboardMessageSummaryAsync_ShouldReturnAuditAlignedCountsAndRecentItems()
        {
            await _repository.InitializeAsync();

            var now = DateTime.UtcNow;
            await _repository.RecordMessageAuditAsync(new MessageAuditRecord
            {
                MessageId = "sum_1",
                Topic = "topic/1",
                SourceClientId = "client_1",
                PayloadSize = 1,
                Qos = 0,
                Retain = false,
                Status = "Succeeded",
                CreatedAt = now.AddSeconds(-3),
                UpdatedAt = now.AddSeconds(-3)
            });
            await _repository.RecordMessageAuditAsync(new MessageAuditRecord
            {
                MessageId = "sum_2",
                Topic = "topic/2",
                SourceClientId = "client_2",
                PayloadSize = 1,
                Qos = 0,
                Retain = false,
                Status = "Failed",
                CreatedAt = now.AddSeconds(-2),
                UpdatedAt = now.AddSeconds(-2)
            });
            await _repository.RecordMessageAuditAsync(new MessageAuditRecord
            {
                MessageId = "sum_3",
                Topic = "topic/3",
                SourceClientId = "client_3",
                PayloadSize = 1,
                Qos = 0,
                Retain = false,
                Status = "DeadLetter",
                CreatedAt = now.AddSeconds(-1),
                UpdatedAt = now.AddSeconds(-1)
            });

            var summary = await _repository.GetDashboardMessageSummaryAsync(2);

            Assert.Equal(3, summary.TotalMessages);
            Assert.Equal(1, summary.TotalSucceeded);
            Assert.Equal(1, summary.TotalFailed);
            Assert.Equal(1, summary.TotalDeadLetter);
            Assert.Equal(2, summary.RecentItems.Count);
            Assert.Equal("sum_3", summary.RecentItems[0].MessageId);
            Assert.Equal("sum_2", summary.RecentItems[1].MessageId);
        }

        [Theory]
        [InlineData("Sqlite", DbType.Sqlite)]
        [InlineData("SqlServer", DbType.SqlServer)]
        [InlineData("MySql", DbType.MySql)]
        [InlineData("Dm", DbType.Dm)]
        [InlineData("PostgreSQL", DbType.PostgreSQL)]
        [InlineData("Oracle", DbType.Oracle)]
        public void ParseDbType_ShouldReturnExpectedProvider(string provider, DbType expectedDbType)
        {
            var actual = AuditRepository.ParseDbType(provider);
            Assert.Equal(expectedDbType, actual);
        }
    }
}
