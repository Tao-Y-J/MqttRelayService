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
                MessageArchiveThreshold = 20000,
                ClientHistoryArchiveThreshold = 5000
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
        public async Task GetPagedMessagesAsync_ShouldClampPagingParameters()
        {
            await _repository.InitializeAsync();

            var records = Enumerable.Range(0, 250)
                .Select(i => new MessageAuditRecord
                {
                    MessageId = $"clamp_{i:D4}",
                    Topic = "clamp/topic",
                    SourceClientId = "clamp_client",
                    PayloadSize = 1,
                    Status = "Succeeded",
                    CreatedAt = DateTime.Now,
                    UpdatedAt = DateTime.Now.AddMilliseconds(i)
                })
                .ToList();

            await _repository.RecordMessageAuditsAsync(records);

            // 超大 pageSize 必须被收敛到仓储上限，绝不能一次性物化整张审计表
            var hugePage = await _repository.GetPagedMessagesAsync(1, int.MaxValue);
            Assert.Equal(200, hugePage.Items.Count);

            // 非法小页长必须被抬升到 1，而不是返回 0 条
            var zeroPageSize = await _repository.GetPagedMessagesAsync(1, 0);
            Assert.Single(zeroPageSize.Items);

            // 超大页码必须收敛，不能产生负 offset 或抛异常
            var farPage = await _repository.GetPagedMessagesAsync(int.MaxValue, 10);
            Assert.Empty(farPage.Items);
            Assert.Equal(250, farPage.TotalCount);
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
                CreatedAt = DateTime.Now,
                UpdatedAt = DateTime.Now
            };

            await _repository.RecordMessageAuditAsync(record);
            var (total1, items1) = await _repository.GetPagedMessagesAsync(1, 10);

            Assert.Equal(1, total1);
            Assert.Single(items1);
            Assert.Equal("test_msg_001", items1[0].MessageId);
            Assert.Equal("Queued", items1[0].Status);

            record.Status = "Succeeded";
            record.LatencyMs = 25.5;
            record.UpdatedAt = DateTime.Now;

            await _repository.RecordMessageAuditAsync(record);
            var (total2, items2) = await _repository.GetPagedMessagesAsync(1, 10);

            Assert.Equal(1, total2);
            Assert.Single(items2);
            Assert.Equal("test_msg_001", items2[0].MessageId);
            Assert.Equal("Succeeded", items2[0].Status);
            Assert.Equal(25.5, items2[0].LatencyMs);
        }

        [Fact]
        public async Task RecordMessageAuditsAsync_ShouldBatchInsertAndUpdateWithoutDuplicatingMessageIds()
        {
            await _repository.InitializeAsync();
            var now = DateTime.Now;
            var firstCreatedAt = now.AddSeconds(-2);

            await _repository.RecordMessageAuditsAsync(new[]
            {
                new MessageAuditRecord
                {
                    MessageId = "batch_1",
                    Topic = "topic/batch/1",
                    SourceClientId = "client_1",
                    PayloadSize = 1,
                    Qos = 0,
                    Retain = false,
                    Status = "Queued",
                    CreatedAt = firstCreatedAt,
                    UpdatedAt = firstCreatedAt
                },
                new MessageAuditRecord
                {
                    MessageId = "batch_2",
                    Topic = "topic/batch/2",
                    SourceClientId = "client_2",
                    PayloadSize = 1,
                    Qos = 0,
                    Retain = false,
                    Status = "Queued",
                    CreatedAt = now.AddSeconds(-1),
                    UpdatedAt = now.AddSeconds(-1)
                }
            });

            await _repository.RecordMessageAuditsAsync(new[]
            {
                new MessageAuditRecord
                {
                    MessageId = "batch_1",
                    Topic = "topic/batch/1",
                    SourceClientId = "client_1",
                    PayloadSize = 1,
                    Qos = 0,
                    Retain = false,
                    Status = "Succeeded",
                    LatencyMs = 12.3,
                    CreatedAt = now.AddMinutes(1),
                    UpdatedAt = now
                },
                new MessageAuditRecord
                {
                    MessageId = "batch_3",
                    Topic = "topic/batch/3",
                    SourceClientId = "client_3",
                    PayloadSize = 1,
                    Qos = 0,
                    Retain = false,
                    Status = "Succeeded",
                    LatencyMs = 6.5,
                    CreatedAt = now,
                    UpdatedAt = now
                }
            });

            var (total, items) = await _repository.GetPagedMessagesAsync(1, 10);

            Assert.Equal(3, total);
            Assert.Equal(3, items.Select(x => x.MessageId).Distinct(StringComparer.Ordinal).Count());

            var batch1 = items.Single(x => x.MessageId == "batch_1");
            Assert.Equal("Succeeded", batch1.Status);
            Assert.Equal(12.3, batch1.LatencyMs);
            AssertDateTimeClose(firstCreatedAt, batch1.CreatedAt);
            AssertDateTimeClose(now, batch1.UpdatedAt);

            var batch2 = items.Single(x => x.MessageId == "batch_2");
            Assert.Equal("Queued", batch2.Status);

            var batch3 = items.Single(x => x.MessageId == "batch_3");
            Assert.Equal("Succeeded", batch3.Status);
        }

        [Fact]
        public async Task RecordMessageAuditsAsync_ShouldKeepLatestRecordWhenSameMessageAppearsInOneBatch()
        {
            await _repository.InitializeAsync();
            var now = DateTime.Now;

            await _repository.RecordMessageAuditsAsync(new[]
            {
                new MessageAuditRecord
                {
                    MessageId = "same_batch_1",
                    Topic = "topic/same",
                    SourceClientId = "client_same",
                    PayloadSize = 1,
                    Qos = 0,
                    Retain = false,
                    Status = "Queued",
                    CreatedAt = now.AddSeconds(-1),
                    UpdatedAt = now.AddSeconds(-1)
                },
                new MessageAuditRecord
                {
                    MessageId = "same_batch_1",
                    Topic = "topic/same",
                    SourceClientId = "client_same",
                    PayloadSize = 1,
                    Qos = 0,
                    Retain = false,
                    Status = "Succeeded",
                    IsSubscriberHit = true,
                    LatencyMs = 8.5,
                    CreatedAt = now.AddSeconds(-1),
                    UpdatedAt = now
                }
            });

            var (total, items) = await _repository.GetPagedMessagesAsync(1, 10);

            Assert.Equal(1, total);
            var record = Assert.Single(items);
            Assert.Equal("same_batch_1", record.MessageId);
            Assert.Equal("Succeeded", record.Status);
            Assert.True(record.IsSubscriberHit);
            Assert.Equal(8.5, record.LatencyMs);
        }

        [Fact]
        public async Task GetMessageByIdAsync_ShouldReturnExactMessageOnly()
        {
            await _repository.InitializeAsync();
            var now = DateTime.Now;

            await _repository.RecordMessageAuditsAsync(new[]
            {
                new MessageAuditRecord
                {
                    MessageId = "target-msg",
                    Topic = "topic/exact",
                    SourceClientId = "client_exact",
                    Payload = "exact-payload",
                    Status = "Succeeded",
                    CreatedAt = now.AddSeconds(-3),
                    UpdatedAt = now.AddSeconds(-3)
                },
                new MessageAuditRecord
                {
                    MessageId = "target-msg-extra",
                    Topic = "topic/other",
                    SourceClientId = "client_other",
                    Payload = "wrong-id",
                    Status = "Failed",
                    ErrorMessage = "contains target-msg",
                    CreatedAt = now.AddSeconds(-2),
                    UpdatedAt = now.AddSeconds(-2)
                },
                new MessageAuditRecord
                {
                    MessageId = "another-msg",
                    Topic = "topic/target-msg/search-hit",
                    SourceClientId = "client_topic",
                    Payload = "wrong-topic",
                    Status = "Queued",
                    CreatedAt = now.AddSeconds(-1),
                    UpdatedAt = now.AddSeconds(-1)
                }
            });

            var exact = await _repository.GetMessageByIdAsync("target-msg");
            var missing = await _repository.GetMessageByIdAsync("not-found-msg");

            Assert.NotNull(exact);
            Assert.Equal("target-msg", exact!.MessageId);
            Assert.Equal("exact-payload", exact.Payload);
            Assert.Null(missing);
        }

        [Fact]
        public async Task GetPagedMessagesAsync_ShouldOrderByUpdatedAtDescending()
        {
            await _repository.InitializeAsync();
            var now = DateTime.Now;

            await _repository.RecordMessageAuditsAsync(new[]
            {
                new MessageAuditRecord
                {
                    MessageId = "created_newer_updated_older",
                    Topic = "topic/1",
                    SourceClientId = "client_1",
                    Status = "Succeeded",
                    CreatedAt = now,
                    UpdatedAt = now.AddSeconds(-10)
                },
                new MessageAuditRecord
                {
                    MessageId = "created_older_updated_newer",
                    Topic = "topic/2",
                    SourceClientId = "client_2",
                    Status = "Succeeded",
                    CreatedAt = now.AddMinutes(-1),
                    UpdatedAt = now.AddSeconds(5)
                }
            });

            var (_, items) = await _repository.GetPagedMessagesAsync(1, 10);

            Assert.Equal("created_older_updated_newer", items[0].MessageId);
            Assert.Equal("created_newer_updated_older", items[1].MessageId);
        }

        [Fact]
        public async Task RecordMessageAuditsAsync_ShouldPersistLargeSqliteBatchWithoutDroppingRows()
        {
            await _repository.InitializeAsync();
            var now = DateTime.Now;
            var records = Enumerable.Range(1, 600)
                .Select(i => new MessageAuditRecord
                {
                    MessageId = $"bulk_{i:D4}",
                    Topic = $"topic/{i % 8}",
                    SourceClientId = $"client_{i % 16}",
                    PayloadSize = 64,
                    Payload = "payload",
                    Qos = 0,
                    Retain = false,
                    Status = "Succeeded",
                    LatencyMs = i,
                    RetryCount = 0,
                    CreatedAt = now.AddMilliseconds(i),
                    UpdatedAt = now.AddMilliseconds(i)
                })
                .ToList();

            await _repository.RecordMessageAuditsAsync(records);

            var summary = await _repository.GetDashboardMessageSummaryAsync(10);
            Assert.Equal(600, summary.TotalMessages);
            Assert.Equal(600, summary.TotalSucceeded);
            Assert.Equal(0, summary.TotalPending);
            Assert.Equal(0, summary.TotalFailed);
            Assert.Equal(0, summary.TotalDeadLetter);
        }

        [Fact]
        public async Task RecordMessageAuditsAsync_ShouldUpdateLargeSqliteBatchToLatestState()
        {
            await _repository.InitializeAsync();
            var now = DateTime.Now;
            var queuedRecords = Enumerable.Range(1, 600)
                .Select(i => new MessageAuditRecord
                {
                    MessageId = $"bulk_update_{i:D4}",
                    Topic = $"topic/{i % 8}",
                    SourceClientId = $"client_{i % 16}",
                    PayloadSize = 64,
                    Payload = "payload",
                    Qos = 0,
                    Retain = false,
                    Status = "Queued",
                    LatencyMs = 0,
                    RetryCount = 0,
                    CreatedAt = now.AddMilliseconds(i),
                    UpdatedAt = now.AddMilliseconds(i)
                })
                .ToList();
            var succeededRecords = queuedRecords
                .Select(x => new MessageAuditRecord
                {
                    MessageId = x.MessageId,
                    Topic = x.Topic,
                    SourceClientId = x.SourceClientId,
                    PayloadSize = x.PayloadSize,
                    Payload = x.Payload,
                    Qos = x.Qos,
                    Retain = x.Retain,
                    Status = "Succeeded",
                    IsSubscriberHit = true,
                    LatencyMs = 10.5,
                    RetryCount = 0,
                    CreatedAt = x.CreatedAt.AddMinutes(1),
                    UpdatedAt = x.UpdatedAt.AddMinutes(1)
                })
                .ToList();

            await _repository.RecordMessageAuditsAsync(queuedRecords);
            await _repository.RecordMessageAuditsAsync(succeededRecords);

            var summary = await _repository.GetDashboardMessageSummaryAsync(10);

            Assert.Equal(600, summary.TotalMessages);
            Assert.Equal(600, summary.TotalSucceeded);
            Assert.Equal(0, summary.TotalPending);
            Assert.Equal(0, summary.TotalFailed);
            Assert.Equal(0, summary.TotalDeadLetter);

            var (_, firstPage) = await _repository.GetPagedMessagesAsync(1, 10);
            Assert.All(firstPage, item =>
            {
                Assert.Equal("Succeeded", item.Status);
                Assert.True(item.IsSubscriberHit);
                Assert.Equal(10.5, item.LatencyMs);
            });
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
                Timestamp = DateTime.Now
            };
            var record2 = new ClientConnectionHistoryRecord
            {
                ClientId = "client_def",
                Username = "user",
                ConnectionId = "conn_x2",
                Event = "Disconnected",
                Details = "Connection lost",
                Timestamp = DateTime.Now.AddSeconds(1)
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
        public async Task GetDashboardMessageSummaryAsync_ShouldReturnAuditAlignedCountsAndRecentItems()
        {
            await _repository.InitializeAsync();

            var now = DateTime.Now;
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
            Assert.Equal(0, summary.TotalPending);
            Assert.Equal(1, summary.TotalSucceeded);
            Assert.Equal(1, summary.TotalFailed);
            Assert.Equal(1, summary.TotalDeadLetter);
            Assert.Equal(2, summary.RecentItems.Count);
            Assert.Equal("sum_3", summary.RecentItems[0].MessageId);
            Assert.Equal("sum_2", summary.RecentItems[1].MessageId);
        }

        [Fact]
        public async Task GetDashboardMessageSummaryAsync_ShouldOrderRecentItemsByUpdatedAtDescending()
        {
            await _repository.InitializeAsync();

            var now = DateTime.Now;
            await _repository.RecordMessageAuditAsync(new MessageAuditRecord
            {
                MessageId = "recent_created_newer",
                Topic = "topic/recent/1",
                SourceClientId = "client_1",
                Status = "Succeeded",
                CreatedAt = now,
                UpdatedAt = now.AddSeconds(-10)
            });
            await _repository.RecordMessageAuditAsync(new MessageAuditRecord
            {
                MessageId = "recent_updated_newer",
                Topic = "topic/recent/2",
                SourceClientId = "client_2",
                Status = "Failed",
                CreatedAt = now.AddMinutes(-1),
                UpdatedAt = now.AddSeconds(5)
            });

            var summary = await _repository.GetDashboardMessageSummaryAsync(10);

            Assert.Equal("recent_updated_newer", summary.RecentItems[0].MessageId);
            Assert.Equal("recent_created_newer", summary.RecentItems[1].MessageId);
        }

        [Fact]
        public async Task GetDashboardMessageSummaryAsync_ShouldCountPendingStatusesSeparately()
        {
            await _repository.InitializeAsync();

            var now = DateTime.Now;
            await _repository.RecordMessageAuditAsync(new MessageAuditRecord
            {
                MessageId = "pending_1",
                Topic = "topic/pending/1",
                SourceClientId = "client_1",
                PayloadSize = 1,
                Qos = 0,
                Retain = false,
                Status = "Queued",
                CreatedAt = now.AddSeconds(-3),
                UpdatedAt = now.AddSeconds(-3)
            });
            await _repository.RecordMessageAuditAsync(new MessageAuditRecord
            {
                MessageId = "pending_2",
                Topic = "topic/pending/2",
                SourceClientId = "client_2",
                PayloadSize = 1,
                Qos = 0,
                Retain = false,
                Status = "Forwarding",
                CreatedAt = now.AddSeconds(-2),
                UpdatedAt = now.AddSeconds(-2)
            });
            await _repository.RecordMessageAuditAsync(new MessageAuditRecord
            {
                MessageId = "done_1",
                Topic = "topic/done/1",
                SourceClientId = "client_3",
                PayloadSize = 1,
                Qos = 0,
                Retain = false,
                Status = "Succeeded",
                CreatedAt = now.AddSeconds(-1),
                UpdatedAt = now.AddSeconds(-1)
            });

            var summary = await _repository.GetDashboardMessageSummaryAsync(10);

            Assert.Equal(3, summary.TotalMessages);
            Assert.Equal(2, summary.TotalPending);
            Assert.Equal(1, summary.TotalSucceeded);
            Assert.Equal(0, summary.TotalFailed);
            Assert.Equal(0, summary.TotalDeadLetter);
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

        [Fact]
        public async Task DeleteExpiredMessageAuditsAsync_ShouldDeleteOnlyRowsOlderThanCutoff()
        {
            await _repository.InitializeAsync();

            var cutoff = DateTime.Now.AddDays(-30);
            await _repository.RecordMessageAuditsAsync(new[]
            {
                new MessageAuditRecord
                {
                    MessageId = "expired_msg",
                    Topic = "topic/expired",
                    SourceClientId = "client_1",
                    Status = "Succeeded",
                    CreatedAt = cutoff.AddHours(-1),
                    UpdatedAt = cutoff.AddHours(-1)
                },
                new MessageAuditRecord
                {
                    MessageId = "kept_msg",
                    Topic = "topic/kept",
                    SourceClientId = "client_2",
                    Status = "Succeeded",
                    CreatedAt = cutoff.AddHours(1),
                    UpdatedAt = cutoff.AddHours(1)
                }
            });

            var deleted = await _repository.DeleteExpiredMessageAuditsAsync(cutoff);

            Assert.Equal(1, deleted);
            var (total, items) = await _repository.GetPagedMessagesAsync(1, 10);
            Assert.Equal(1, total);
            Assert.Equal("kept_msg", Assert.Single(items).MessageId);
        }

        [Fact]
        public async Task DeleteExpiredMessageAuditsAsync_ShouldDeleteInBatchesWithoutDroppingRows()
        {
            await _repository.InitializeAsync();

            var cutoff = DateTime.Now.AddDays(-30);
            // 1200 条超过 SQLite 单批删除条数 500，也超过单条 IN 参数上限 900，必须靠批删跑完且不能漏行/死循环
            var records = Enumerable.Range(0, 1200)
                .Select(i => new MessageAuditRecord
                {
                    MessageId = $"expired_batch_{i:D5}",
                    Topic = "topic/batch",
                    SourceClientId = "client_batch",
                    PayloadSize = 1,
                    Status = "Succeeded",
                    CreatedAt = cutoff.AddMinutes(-1).AddMilliseconds(i),
                    UpdatedAt = cutoff.AddMinutes(-1).AddMilliseconds(i)
                })
                .ToList();

            await _repository.RecordMessageAuditsAsync(records);

            var deleted = await _repository.DeleteExpiredMessageAuditsAsync(cutoff);

            Assert.Equal(1200, deleted);
            var (total, items) = await _repository.GetPagedMessagesAsync(1, 10);
            Assert.Equal(0, total);
            Assert.Empty(items);
        }

        [Fact]
        public async Task DeleteExpiredClientHistoriesAsync_ShouldDeleteOnlyRowsOlderThanCutoff()
        {
            await _repository.InitializeAsync();

            var cutoff = DateTime.Now.AddDays(-30);
            await _repository.RecordClientConnectionHistoriesAsync(new[]
            {
                new ClientConnectionHistoryRecord
                {
                    ClientId = "expired_client",
                    ConnectionId = "conn_expired",
                    Event = "Connected",
                    Timestamp = cutoff.AddHours(-1)
                },
                new ClientConnectionHistoryRecord
                {
                    ClientId = "kept_client",
                    ConnectionId = "conn_kept",
                    Event = "Connected",
                    Timestamp = cutoff.AddHours(1)
                }
            });

            var deleted = await _repository.DeleteExpiredClientHistoriesAsync(cutoff);

            Assert.Equal(1, deleted);
            var (total, items) = await _repository.GetPagedClientHistoryAsync(1, 10);
            Assert.Equal(1, total);
            Assert.Equal("kept_client", Assert.Single(items).ClientId);
        }

        [Fact]
        public async Task DeleteExpiredMessageAuditsAsync_WhenSchemaNotInitialized_ShouldSkipWithoutThrowing()
        {
            var options = new AuditStorageOptions
            {
                Provider = "Sqlite",
                ConnectionString = $"Data Source={_dbFile}",
                AutoInitializeSchema = false
            };
            var repository = new AuditRepository(options, new Mock<ILogger<AuditRepository>>().Object);

            // AutoInitializeSchema=false 且从未初始化过表结构：表可能不存在，清理必须跳过而不是抛异常
            var deleted = await repository.DeleteExpiredMessageAuditsAsync(DateTime.Now.AddDays(-30));

            Assert.Equal(0, deleted);
        }

        [Fact]
        public async Task VacuumAsync_ShouldKeepDatabaseUsable()
        {
            await _repository.InitializeAsync();

            var now = DateTime.Now;
            // 待清理记录统一落在 2 分钟前，截止时间取 1 分钟前，保证 500 条全部落在保留窗口之外
            var expiredAt = now.AddMinutes(-2);
            var records = Enumerable.Range(0, 500)
                .Select(i => new MessageAuditRecord
                {
                    MessageId = $"vacuum_{i:D4}",
                    Topic = "topic/vacuum",
                    SourceClientId = "client_vacuum",
                    PayloadSize = 64,
                    Payload = "payload",
                    Status = "Succeeded",
                    CreatedAt = expiredAt.AddMilliseconds(i),
                    UpdatedAt = expiredAt.AddMilliseconds(i)
                })
                .ToList();

            await _repository.RecordMessageAuditsAsync(records);
            var deleted = await _repository.DeleteExpiredMessageAuditsAsync(now.AddMinutes(-1));

            Assert.Equal(500, deleted);

            await _repository.VacuumAsync();

            // VACUUM 之后数据库必须仍然可写可查（不断言文件字节数，SQLite 页级回收不保证字节数下降）
            await _repository.RecordMessageAuditAsync(new MessageAuditRecord
            {
                MessageId = "vacuum_after",
                Topic = "topic/vacuum",
                SourceClientId = "client_vacuum",
                Status = "Succeeded",
                CreatedAt = now,
                UpdatedAt = now
            });

            var (total, items) = await _repository.GetPagedMessagesAsync(1, 10);
            Assert.Equal(1, total);
            Assert.Equal("vacuum_after", Assert.Single(items).MessageId);
        }

        private static void AssertDateTimeClose(DateTime expected, DateTime actual)
        {
            Assert.True(
                Math.Abs((actual - expected).TotalMilliseconds) < 10,
                $"Expected {actual:o} to be within 10ms of {expected:o}.");
        }
    }
}