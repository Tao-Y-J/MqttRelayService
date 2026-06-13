using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using MqttRelayService.Services.Implementations.Decorators;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;
using MqttRelayService.Services.Implementations;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// 针对 Web 管理面板的统计收集服务 (MetricsService) 极其拦截器装饰器的单元测试。
    /// </summary>
    public class DashboardMetricsTests
    {
        private readonly Mock<IClientRegistry> _mockClientRegistry = new();
        private readonly Mock<ILogger<MetricsService>> _mockLogger = new();
        private readonly IOptions<ServiceOptions> _serviceOptions = Microsoft.Extensions.Options.Options.Create(new ServiceOptions { Name = "TestRelay" });
        private readonly IOptions<MqttOptions> _mqttOptions = Microsoft.Extensions.Options.Options.Create(new MqttOptions { TcpPort = 1883 });
        private readonly IOptions<ReliabilityOptions> _reliabilityOptions = Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
        {
            QueueCapacity = 100,
            MaxConcurrentHandlers = 1,
            MaxRetryCount = 3,
            EnableDeadLetter = true,
            DeadLetterPath = "data/test_deadletter"
        });

        private readonly Mock<ILogger<InMemoryMessageQueue>> _mockQueueLogger = new();
        private readonly Mock<IAuditRepository> _mockAuditRepository = new();
        private readonly InMemoryMessageQueue _queue;

        public DashboardMetricsTests()
        {
            // 初始化真实的内存队列，以便 MetricsService 可以获取队列堆积水位信息
            _queue = new InMemoryMessageQueue(_reliabilityOptions, _mockQueueLogger.Object);

            // Mock 客户端会话获取，防止 GetDashboardDataAsync 抛出空引用异常
            _mockClientRegistry.Setup(r => r.GetAllSessionsAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(new List<ClientSessionInfo>());
        }

        [Fact]
        public async Task MetricsService_RecordReceived_ShouldIncrementCounterAndAddLog()
        {
            // Arrange
            using var metricsService = new MetricsService(
                _queue,
                _mockClientRegistry.Object,
                _serviceOptions,
                _mqttOptions,
                _reliabilityOptions,
                _mockLogger.Object);

            var message = new ForwardMessage
            {
                MessageId = "msg_123",
                RouteContext = new RouteContext
                {
                    MessageId = "msg_123",
                    Topic = "test/topic",
                    Payload = new byte[] { 1, 2, 3 },
                    QoS = 1,
                    Retain = false,
                    SourceClientId = "client_1",
                    Timestamp = DateTime.Now
                }
            };

            // Act
            metricsService.RecordReceived(message);
            var result = await metricsService.GetDashboardDataAsync();

            // Assert
            Assert.NotNull(result);
            dynamic data = result;
            Assert.Equal(1L, (long)data.Counters.TotalReceived);

            var logs = (List<object>)data.Logs;
            Assert.Single(logs);
            dynamic log = logs[0];
            Assert.Equal("msg_123", (string)log.MessageId);
            Assert.Equal("test/topic", (string)log.Topic);
            Assert.Equal("client_1", (string)log.SourceClientId);
            Assert.Equal("Queued", (string)log.Status);
        }

        [Fact]
        public async Task MetricsService_RecordForwarded_ShouldIncrementSuccessOrFailureCounters()
        {
            // Arrange
            using var metricsService = new MetricsService(
                _queue,
                _mockClientRegistry.Object,
                _serviceOptions,
                _mqttOptions,
                _reliabilityOptions,
                _mockLogger.Object);

            var context = new RouteContext
            {
                MessageId = "msg_456",
                Topic = "test/forward",
                Payload = new byte[] { 4, 5 },
                QoS = 0,
                Retain = true,
                SourceClientId = "client_2",
                Timestamp = DateTime.Now
            };

            // Act
            metricsService.RecordForwarded(context, success: true, retryCount: 0, latencyMs: 45.5);
            metricsService.RecordForwarded(context, success: false, retryCount: 2, latencyMs: 120.3);
            var result = await metricsService.GetDashboardDataAsync();

            // Assert
            Assert.NotNull(result);
            dynamic data = result;
            Assert.Equal(1L, (long)data.Counters.TotalSucceeded);
            Assert.Equal(1L, (long)data.Counters.TotalFailed);
            Assert.Equal(2L, (long)data.Counters.TotalRetries);

            // 验证在内存日志中，相同 MessageId 仅保留了最终状态（可变就地更新）
            var logs = (List<object>)data.Logs;
            Assert.Single(logs);
            dynamic log = logs[0];
            Assert.Equal("msg_456", (string)log.MessageId);
            Assert.Equal("Failed", (string)log.Status);
            Assert.Equal(120.3, (double)log.LatencyMs);
            Assert.Equal(2, (int)log.RetryCount);
        }

        [Fact]
        public async Task MetricsService_MessageStatusShouldBeMutableAndKeepOnlyFinalState()
        {
            // Arrange
            using var metricsService = new MetricsService(
                _queue,
                _mockClientRegistry.Object,
                _serviceOptions,
                _mqttOptions,
                _reliabilityOptions,
                _mockLogger.Object);

            var message = new ForwardMessage
            {
                MessageId = "msg_mutable",
                RouteContext = new RouteContext
                {
                    MessageId = "msg_mutable",
                    Topic = "test/mutable",
                    Payload = new byte[] { 1, 2, 3 },
                    QoS = 1,
                    Retain = false,
                    SourceClientId = "client_mut",
                    Timestamp = DateTime.Now
                }
            };

            // 1. 记录消息入队 (Queued)
            metricsService.RecordReceived(message);
            var data1 = await metricsService.GetDashboardDataAsync();
            dynamic dynData1 = data1;
            var logs1 = (List<object>)dynData1.Logs;
            Assert.Single(logs1);
            dynamic log1 = logs1[0];
            Assert.Equal("Queued", (string)log1.Status);

            // 2. 记录消息转发成功 (Succeeded) - 应当就地更新状态，保证 Logs 列表无重复项
            metricsService.RecordForwarded(message.RouteContext, success: true, retryCount: 1, latencyMs: 35.2);
            var data2 = await metricsService.GetDashboardDataAsync();
            dynamic dynData2 = data2;
            var logs2 = (List<object>)dynData2.Logs;
            Assert.Single(logs2); // 依然只有唯一一条记录
            dynamic log2 = logs2[0];
            Assert.Equal("Succeeded", (string)log2.Status);
            Assert.Equal(35.2, (double)log2.LatencyMs);
            Assert.Equal(1, (int)log2.RetryCount);
        }

        [Fact]
        public async Task MetricsService_RecordDeadLetter_ShouldIncrementDeadLetterCounter()
        {
            // Arrange
            using var metricsService = new MetricsService(
                _queue,
                _mockClientRegistry.Object,
                _serviceOptions,
                _mqttOptions,
                _reliabilityOptions,
                _mockLogger.Object);

            var record = new DeadLetterRecord
            {
                MessageId = "msg_dlq",
                Topic = "test/dlq",
                PayloadBase64 = "SGVsbG8=",
                FailureReason = "Auth failed",
                FirstReceivedAt = DateTime.Now.AddSeconds(-10),
                LastFailedAt = DateTime.Now,
                RetryCount = 3
            };

            // Act
            metricsService.RecordDeadLetter(record);
            var result = await metricsService.GetDashboardDataAsync();

            // Assert
            Assert.NotNull(result);
            dynamic data = result;
            Assert.Equal(1L, (long)data.Counters.TotalDeadLetter);

            var logs = (List<object>)data.Logs;
            Assert.Single(logs);
            dynamic log = logs[0];
            Assert.Equal("msg_dlq", (string)log.MessageId);
            Assert.Equal("DeadLetter", (string)log.Status);
            Assert.Equal("Auth failed", (string)log.ErrorMessage);
        }

        [Fact]
        public async Task MetricsService_GetDashboardDataAsync_WithAuditRepository_ShouldPreferAuditAlignedCountersAndLogs()
        {
            var now = DateTime.Now;
            _mockAuditRepository
                .Setup(r => r.GetDashboardMessageSummaryAsync(It.IsAny<int>()))
                .ReturnsAsync((
                    TotalMessages: 12,
                    TotalPending: 4,
                    TotalSucceeded: 8,
                    TotalFailed: 3,
                    TotalDeadLetter: 1,
                    RecentItems: (IReadOnlyList<MessageAuditRecord>)new List<MessageAuditRecord>
                    {
                        new()
                        {
                            MessageId = "audit_1",
                            Topic = "audit/topic",
                            SourceClientId = "audit_client",
                            PayloadSize = 16,
                            Payload = "payload",
                            Qos = 1,
                            Retain = false,
                            Status = "Succeeded",
                            LatencyMs = 12.5,
                            RetryCount = 0,
                            CreatedAt = now,
                            UpdatedAt = now
                        }
                    }));

            using var metricsService = new MetricsService(
                _queue,
                _mockClientRegistry.Object,
                _serviceOptions,
                _mqttOptions,
                _reliabilityOptions,
                _mockAuditRepository.Object,
                _mockLogger.Object);

            await metricsService.InitializeDashboardCountersFromAuditAsync();

            var message = new ForwardMessage
            {
                MessageId = "live_msg_1",
                RouteContext = new RouteContext
                {
                    MessageId = "live_msg_1",
                    Topic = "live/topic",
                    Payload = new byte[] { 1, 2, 3 },
                    QoS = 1,
                    Retain = false,
                    SourceClientId = "live_client",
                    Timestamp = now
                }
            };

            metricsService.RecordReceived(message);
            metricsService.RecordForwarded(message.RouteContext, success: true, retryCount: 0, latencyMs: 8.8);
            var result = await metricsService.GetDashboardDataAsync();

            dynamic data = result;
            Assert.Equal(12L, (long)data.Counters.TotalReceived);
            Assert.Equal(8L, (long)data.Counters.TotalSucceeded);
            Assert.Equal(3L, (long)data.Counters.TotalFailed);
            Assert.Equal(1L, (long)data.Counters.TotalDeadLetter);
            Assert.Equal(4L, (long)data.Counters.TotalPending);

            var logs = (List<object>)data.Logs;
            Assert.Single(logs);
            dynamic log = logs[0];
            Assert.Equal("audit_1", (string)log.MessageId);
            Assert.Equal("Succeeded", (string)log.Status);
        }

        [Fact]
        public async Task MetricsMessageQueue_EnqueueAsync_FirstReceipt_ShouldPersistQueuedAuditRecord()
        {
            var persistedQueued = new TaskCompletionSource<MessageAuditRecord>(TaskCreationOptions.RunContinuationsAsynchronously);

            _mockAuditRepository
                .Setup(r => r.RecordMessageAuditsAsync(It.IsAny<IReadOnlyList<MessageAuditRecord>>()))
                .Callback<IReadOnlyList<MessageAuditRecord>>(records =>
                {
                    var record = records.SingleOrDefault(r => r.MessageId == "first_receipt_msg" && r.Status == "Queued");
                    if (record != null)
                    {
                        persistedQueued.TrySetResult(record);
                    }
                })
                .Returns(Task.CompletedTask);

            using var metricsService = new MetricsService(
                _queue,
                _mockClientRegistry.Object,
                _serviceOptions,
                _mqttOptions,
                _reliabilityOptions,
                _mockAuditRepository.Object,
                _mockLogger.Object);

            var queue = new MetricsMessageQueue(_queue, metricsService);
            var message = new ForwardMessage
            {
                MessageId = "first_receipt_msg",
                RouteContext = new RouteContext
                {
                    MessageId = "first_receipt_msg",
                    Topic = "first/topic",
                    Payload = new byte[] { 1, 2, 3 },
                    QoS = 1,
                    Retain = false,
                    SourceClientId = "first_client",
                    Timestamp = DateTime.Now
                }
            };

            Assert.True(await queue.EnqueueAsync(message));

            var completed = await Task.WhenAny(persistedQueued.Task, Task.Delay(TimeSpan.FromSeconds(2)));
            Assert.Same(persistedQueued.Task, completed);

            var persisted = await persistedQueued.Task;
            Assert.Equal("Queued", persisted.Status);
            Assert.Equal("first_receipt_msg", persisted.MessageId);
            Assert.Equal(MessageProcessStatus.Queued, message.Status);
        }

        [Fact]
        public async Task MetricsMessageQueue_EnqueueAsync_FirstReceipt_ShouldRecordQueuedBeforeInnerEnqueueCompletes()
        {
            var persistedQueued = new TaskCompletionSource<MessageAuditRecord>(TaskCreationOptions.RunContinuationsAsynchronously);
            var enqueueGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var mockMetrics = new Mock<IMetricsService>();
            var mockQueue = new Mock<IMessageQueue>();

            _mockAuditRepository
                .Setup(r => r.RecordMessageAuditsAsync(It.IsAny<IReadOnlyList<MessageAuditRecord>>()))
                .Callback<IReadOnlyList<MessageAuditRecord>>(records =>
                {
                    var record = records.SingleOrDefault(r => r.MessageId == "queued_before_enqueue" && r.Status == "Queued");
                    if (record != null)
                    {
                        persistedQueued.TrySetResult(record);
                    }
                })
                .Returns(Task.CompletedTask);

            using var metricsService = new MetricsService(
                _queue,
                _mockClientRegistry.Object,
                _serviceOptions,
                _mqttOptions,
                _reliabilityOptions,
                _mockAuditRepository.Object,
                _mockLogger.Object);

            mockMetrics
                .Setup(m => m.RecordReceived(It.IsAny<ForwardMessage>(), It.IsAny<bool>()))
                .Callback<ForwardMessage, bool>((msg, first) => metricsService.RecordReceived(msg, first));

            var decorator = new MetricsMessageQueue(mockQueue.Object, mockMetrics.Object);
            var message = new ForwardMessage
            {
                MessageId = "queued_before_enqueue",
                RouteContext = new RouteContext
                {
                    MessageId = "queued_before_enqueue",
                    Topic = "race/topic",
                    Payload = new byte[] { 1 },
                    QoS = 0,
                    Retain = false,
                    SourceClientId = "race_client",
                    Timestamp = DateTime.Now
                },
                Status = MessageProcessStatus.Received
            };

            mockQueue
                .Setup(q => q.EnqueueAsync(message, It.IsAny<CancellationToken>()))
                .Returns(async () =>
                {
                    await enqueueGate.Task;
                    return true;
                });

            var enqueueTask = decorator.EnqueueAsync(message);

            var completed = await Task.WhenAny(persistedQueued.Task, Task.Delay(TimeSpan.FromSeconds(2)));
            Assert.Same(persistedQueued.Task, completed);
            mockMetrics.Verify(m => m.RecordReceived(message, true), Times.Once);

            enqueueGate.TrySetResult(true);
            Assert.True(await enqueueTask);
        }

        [Fact]
        public async Task MetricsService_RecordReceived_AfterSucceeded_ShouldNotOverwriteTerminalAuditState()
        {
            var now = DateTime.Now;
            var persistedTerminalState = new TaskCompletionSource<MessageAuditRecord>(TaskCreationOptions.RunContinuationsAsynchronously);

            _mockAuditRepository
                .Setup(r => r.RecordMessageAuditsAsync(It.IsAny<IReadOnlyList<MessageAuditRecord>>()))
                .Callback<IReadOnlyList<MessageAuditRecord>>(records =>
                {
                    var record = records.SingleOrDefault(r => r.MessageId == "terminal_msg_1");
                    if (record != null && record.Status == "Succeeded")
                    {
                        persistedTerminalState.TrySetResult(record);
                    }
                })
                .Returns(Task.CompletedTask);

            using var metricsService = new MetricsService(
                _queue,
                _mockClientRegistry.Object,
                _serviceOptions,
                _mqttOptions,
                _reliabilityOptions,
                _mockAuditRepository.Object,
                _mockLogger.Object);

            var message = new ForwardMessage
            {
                MessageId = "terminal_msg_1",
                RouteContext = new RouteContext
                {
                    MessageId = "terminal_msg_1",
                    Topic = "terminal/topic",
                    Payload = new byte[] { 1, 2, 3 },
                    QoS = 1,
                    Retain = false,
                    SourceClientId = "terminal_client",
                    Timestamp = now
                }
            };

            metricsService.RecordReceived(message);
            metricsService.RecordForwarded(message.RouteContext, success: true, retryCount: 0, latencyMs: 9.6, isSubscriberHit: false);

            message.Status = MessageProcessStatus.Queued;
            metricsService.RecordReceived(message);

            var completed = await Task.WhenAny(persistedTerminalState.Task, Task.Delay(TimeSpan.FromSeconds(2)));
            Assert.Same(persistedTerminalState.Task, completed);

            var persistedRecord = await persistedTerminalState.Task;
            Assert.Equal("Succeeded", persistedRecord.Status);
            Assert.False(persistedRecord.IsSubscriberHit);
        }

        [Fact]
        public async Task MetricsService_RecordForwarded_WithAuditRepository_ShouldPersistLatestStateAsynchronously()
        {
            var now = DateTime.Now;
            var persistedSucceeded = new TaskCompletionSource<MessageAuditRecord>(TaskCreationOptions.RunContinuationsAsynchronously);

            _mockAuditRepository
                .Setup(r => r.RecordMessageAuditAsync(It.IsAny<MessageAuditRecord>()))
                .Returns<MessageAuditRecord>(record =>
                {
                    if (record.MessageId == "async_msg_1" && record.Status == "Succeeded")
                    {
                        persistedSucceeded.TrySetResult(record);
                    }

                    return Task.CompletedTask;
                });

            _mockAuditRepository
                .Setup(r => r.RecordMessageAuditsAsync(It.IsAny<IReadOnlyList<MessageAuditRecord>>()))
                .Returns<IReadOnlyList<MessageAuditRecord>>(records =>
                {
                    foreach (var record in records)
                    {
                        if (record.MessageId == "async_msg_1" && record.Status == "Succeeded")
                        {
                            persistedSucceeded.TrySetResult(record);
                        }
                    }

                    return Task.CompletedTask;
                });

            using var metricsService = new MetricsService(
                _queue,
                _mockClientRegistry.Object,
                _serviceOptions,
                _mqttOptions,
                _reliabilityOptions,
                _mockAuditRepository.Object,
                _mockLogger.Object);

            var message = new ForwardMessage
            {
                MessageId = "async_msg_1",
                RouteContext = new RouteContext
                {
                    MessageId = "async_msg_1",
                    Topic = "async/topic",
                    Payload = new byte[] { 9, 9, 9 },
                    QoS = 1,
                    Retain = false,
                    SourceClientId = "async_client",
                    Timestamp = now
                }
            };

            metricsService.RecordReceived(message);
            metricsService.RecordForwarded(message.RouteContext, success: true, retryCount: 2, latencyMs: 18.6);

            var completed = await Task.WhenAny(persistedSucceeded.Task, Task.Delay(TimeSpan.FromSeconds(2)));
            Assert.Same(persistedSucceeded.Task, completed);

            var persisted = await persistedSucceeded.Task;
            Assert.Equal("Succeeded", persisted.Status);
            Assert.Equal(2, persisted.RetryCount);
            Assert.Equal("async_msg_1", persisted.MessageId);
        }

        [Fact]
        public async Task MetricsService_RecordReceivedAndForwardedInSameFlushWindow_ShouldPersistOnlyFinalState()
        {
            var now = DateTime.Now;
            var firstBatch = new TaskCompletionSource<IReadOnlyList<MessageAuditRecord>>(TaskCreationOptions.RunContinuationsAsynchronously);

            _mockAuditRepository
                .Setup(r => r.RecordMessageAuditsAsync(It.IsAny<IReadOnlyList<MessageAuditRecord>>()))
                .Callback<IReadOnlyList<MessageAuditRecord>>(records =>
                {
                    firstBatch.TrySetResult(records.ToList());
                })
                .Returns(Task.CompletedTask);

            using var metricsService = new MetricsService(
                _queue,
                _mockClientRegistry.Object,
                _serviceOptions,
                _mqttOptions,
                _reliabilityOptions,
                _mockAuditRepository.Object,
                _mockLogger.Object);

            var message = new ForwardMessage
            {
                MessageId = "coalesced_final_msg",
                RouteContext = new RouteContext
                {
                    MessageId = "coalesced_final_msg",
                    Topic = "coalesced/topic",
                    Payload = new byte[] { 1, 2, 3 },
                    QoS = 1,
                    Retain = false,
                    SourceClientId = "coalesced_client",
                    Timestamp = now
                }
            };

            metricsService.RecordReceived(message);
            metricsService.RecordForwarded(message.RouteContext, success: true, retryCount: 0, latencyMs: 5.5, isSubscriberHit: true);

            var completed = await Task.WhenAny(firstBatch.Task, Task.Delay(TimeSpan.FromSeconds(2)));
            Assert.Same(firstBatch.Task, completed);

            var records = await firstBatch.Task;
            var record = Assert.Single(records);
            Assert.Equal("coalesced_final_msg", record.MessageId);
            Assert.Equal("Succeeded", record.Status);
            Assert.True(record.IsSubscriberHit);
            Assert.Equal(5.5, record.LatencyMs);

            await Task.Delay(TimeSpan.FromMilliseconds(150));
            _mockAuditRepository.Verify(r => r.RecordMessageAuditsAsync(It.IsAny<IReadOnlyList<MessageAuditRecord>>()), Times.Once);
        }

        [Fact]
        public async Task MetricsService_RecordForwarded_WhenAuditBatchFails_ShouldRetryPendingAudit()
        {
            var now = DateTime.Now;
            var persistedAfterRetry = new TaskCompletionSource<MessageAuditRecord>(TaskCreationOptions.RunContinuationsAsynchronously);
            var callCount = 0;

            _mockAuditRepository
                .Setup(r => r.RecordMessageAuditsAsync(It.IsAny<IReadOnlyList<MessageAuditRecord>>()))
                .Returns<IReadOnlyList<MessageAuditRecord>>(records =>
                {
                    var currentCall = Interlocked.Increment(ref callCount);
                    if (currentCall == 1)
                    {
                        throw new InvalidOperationException("temporary audit store failure");
                    }

                    var record = records.SingleOrDefault(r => r.MessageId == "retry_audit_msg" && r.Status == "Succeeded");
                    if (record != null)
                    {
                        persistedAfterRetry.TrySetResult(record);
                    }

                    return Task.CompletedTask;
                });

            using var metricsService = new MetricsService(
                _queue,
                _mockClientRegistry.Object,
                _serviceOptions,
                _mqttOptions,
                _reliabilityOptions,
                _mockAuditRepository.Object,
                _mockLogger.Object);

            var message = new ForwardMessage
            {
                MessageId = "retry_audit_msg",
                RouteContext = new RouteContext
                {
                    MessageId = "retry_audit_msg",
                    Topic = "retry/audit",
                    Payload = new byte[] { 7, 7, 7 },
                    QoS = 1,
                    Retain = false,
                    SourceClientId = "retry_client",
                    Timestamp = now
                }
            };

            metricsService.RecordReceived(message);
            metricsService.RecordForwarded(message.RouteContext, success: true, retryCount: 0, latencyMs: 6.4);

            var completed = await Task.WhenAny(persistedAfterRetry.Task, Task.Delay(TimeSpan.FromSeconds(2)));
            Assert.Same(persistedAfterRetry.Task, completed);
            Assert.True(Volatile.Read(ref callCount) >= 2);

            var persisted = await persistedAfterRetry.Task;
            Assert.Equal("Succeeded", persisted.Status);
            Assert.Equal("retry_audit_msg", persisted.MessageId);
        }

        [Fact]
        public async Task MetricsMessageQueue_EnqueueAsync_BinaryPayload_ShouldPersistSqliteSafePayloadText()
        {
            var persistedQueued = new TaskCompletionSource<MessageAuditRecord>(TaskCreationOptions.RunContinuationsAsynchronously);

            _mockAuditRepository
                .Setup(r => r.RecordMessageAuditsAsync(It.IsAny<IReadOnlyList<MessageAuditRecord>>()))
                .Callback<IReadOnlyList<MessageAuditRecord>>(records =>
                {
                    var record = records.SingleOrDefault(r => r.MessageId == "binary_payload_msg" && r.Status == "Queued");
                    if (record != null)
                    {
                        persistedQueued.TrySetResult(record);
                    }
                })
                .Returns(Task.CompletedTask);

            using var metricsService = new MetricsService(
                _queue,
                _mockClientRegistry.Object,
                _serviceOptions,
                _mqttOptions,
                _reliabilityOptions,
                _mockAuditRepository.Object,
                _mockLogger.Object);

            var queue = new MetricsMessageQueue(_queue, metricsService);
            var message = new ForwardMessage
            {
                MessageId = "binary_payload_msg",
                RouteContext = new RouteContext
                {
                    MessageId = "binary_payload_msg",
                    Topic = "binary/topic",
                    Payload = new byte[] { 0x18, 0xFF, 0x1F, 0x00, 0x41, 0x42, 0x43 },
                    QoS = 0,
                    Retain = false,
                    SourceClientId = "binary_client",
                    Timestamp = DateTime.Now
                }
            };

            Assert.True(await queue.EnqueueAsync(message));

            var completed = await Task.WhenAny(persistedQueued.Task, Task.Delay(TimeSpan.FromSeconds(2)));
            Assert.Same(persistedQueued.Task, completed);

            var persisted = await persistedQueued.Task;
            Assert.StartsWith("[二进制载荷，大小:", persisted.Payload);
            Assert.Contains("HEX:", persisted.Payload);
        }

        [Fact]
        public async Task MetricsMessageQueue_EnqueueAsync_InvalidUtf8Payload_ShouldPersistBinaryHexPreview()
        {
            var persistedQueued = new TaskCompletionSource<MessageAuditRecord>(TaskCreationOptions.RunContinuationsAsynchronously);

            _mockAuditRepository
                .Setup(r => r.RecordMessageAuditsAsync(It.IsAny<IReadOnlyList<MessageAuditRecord>>()))
                .Callback<IReadOnlyList<MessageAuditRecord>>(records =>
                {
                    var record = records.SingleOrDefault(r => r.MessageId == "invalid_utf8_payload_msg" && r.Status == "Queued");
                    if (record != null)
                    {
                        persistedQueued.TrySetResult(record);
                    }
                })
                .Returns(Task.CompletedTask);

            using var metricsService = new MetricsService(
                _queue,
                _mockClientRegistry.Object,
                _serviceOptions,
                _mqttOptions,
                _reliabilityOptions,
                _mockAuditRepository.Object,
                _mockLogger.Object);

            var queue = new MetricsMessageQueue(_queue, metricsService);
            var message = new ForwardMessage
            {
                MessageId = "invalid_utf8_payload_msg",
                RouteContext = new RouteContext
                {
                    MessageId = "invalid_utf8_payload_msg",
                    Topic = "binary/invalid-utf8",
                    Payload = new byte[] { 0xC3, 0x28, 0x41, 0x42 },
                    QoS = 0,
                    Retain = false,
                    SourceClientId = "binary_client",
                    Timestamp = DateTime.Now
                }
            };

            Assert.True(await queue.EnqueueAsync(message));

            var completed = await Task.WhenAny(persistedQueued.Task, Task.Delay(TimeSpan.FromSeconds(2)));
            Assert.Same(persistedQueued.Task, completed);

            var persisted = await persistedQueued.Task;
            Assert.StartsWith("[二进制载荷，大小:", persisted.Payload);
            Assert.Contains("HEX: C3284142", persisted.Payload);
            Assert.DoesNotContain("\uFFFD", persisted.Payload);
        }

        [Fact]
        public async Task MetricsService_WhenPendingAuditQueueIsFull_ShouldStillUpdateExistingMessageToTerminalState()
        {
            var writeStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var allowWriteToFinish = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            _mockAuditRepository
                .Setup(r => r.RecordMessageAuditsAsync(It.IsAny<IReadOnlyList<MessageAuditRecord>>()))
                .Returns<IReadOnlyList<MessageAuditRecord>>(async records =>
                {
                    if (records.Any(r => r.MessageId == "full_queue_existing_msg"))
                    {
                        writeStarted.TrySetResult(true);
                        await allowWriteToFinish.Task;
                    }
                });

            using var metricsService = new MetricsService(
                _queue,
                _mockClientRegistry.Object,
                _serviceOptions,
                _mqttOptions,
                _reliabilityOptions,
                _mockAuditRepository.Object,
                _mockLogger.Object);

            var pending = GetPendingAudits(metricsService);
            var createdAt = DateTime.Now.AddSeconds(-5);
            pending["full_queue_existing_msg"] = new MessageAuditRecord
            {
                MessageId = "full_queue_existing_msg",
                Topic = "queued/topic",
                SourceClientId = "queued_client",
                Payload = "queued-payload",
                Status = "Queued",
                CreatedAt = createdAt,
                UpdatedAt = createdAt
            };

            for (int i = 0; i < 49999; i++)
            {
                pending.TryAdd(
                    $"prefill_{i:D5}",
                    new MessageAuditRecord
                    {
                        MessageId = $"prefill_{i:D5}",
                        Topic = "prefill/topic",
                        SourceClientId = "prefill_client",
                        Status = "Queued",
                        CreatedAt = DateTime.Now,
                        UpdatedAt = DateTime.Now
                    });
            }

            Assert.Equal(50000, pending.Count);

            var existingContext = new RouteContext
            {
                MessageId = "full_queue_existing_msg",
                Topic = "updated/topic",
                Payload = new byte[] { 1, 2, 3 },
                QoS = 1,
                Retain = false,
                SourceClientId = "updated_client",
                Timestamp = createdAt
            };

            metricsService.RecordForwarded(existingContext, success: true, retryCount: 2, latencyMs: 12.5, isSubscriberHit: true);
            Assert.True(pending.TryGetValue("full_queue_existing_msg", out var updatedExisting));
            Assert.NotNull(updatedExisting);
            Assert.Equal("Succeeded", updatedExisting!.Status);
            Assert.Equal("updated/topic", updatedExisting.Topic);
            Assert.Equal("updated_client", updatedExisting.SourceClientId);
            Assert.Equal(2, updatedExisting.RetryCount);
            Assert.True(updatedExisting.IsSubscriberHit);

            var newContext = new RouteContext
            {
                MessageId = "full_queue_new_msg",
                Topic = "new/topic",
                Payload = new byte[] { 4, 5, 6 },
                QoS = 0,
                Retain = false,
                SourceClientId = "new_client",
                Timestamp = DateTime.Now
            };

            metricsService.RecordForwarded(newContext, success: false, retryCount: 0, latencyMs: 8.1);

            Assert.False(pending.ContainsKey("full_queue_new_msg"));

            var started = await Task.WhenAny(writeStarted.Task, Task.Delay(TimeSpan.FromSeconds(2)));
            Assert.Same(writeStarted.Task, started);
            allowWriteToFinish.TrySetResult(true);
        }

        [Fact]
        public async Task MetricsService_Dispose_ShouldWaitForAuditWriterToFlushPendingAudits()
        {
            var firstWriteStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var allowWriteToFinish = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var persistedBeforeDisposeReturns = false;

            _mockAuditRepository
                .Setup(r => r.RecordMessageAuditsAsync(It.IsAny<IReadOnlyList<MessageAuditRecord>>()))
                .Returns<IReadOnlyList<MessageAuditRecord>>(async records =>
                {
                    if (records.Any(r => r.MessageId == "dispose_flush_msg"))
                    {
                        firstWriteStarted.TrySetResult(true);
                        await allowWriteToFinish.Task;
                        persistedBeforeDisposeReturns = true;
                    }

                    return;
                });

            var metricsService = new MetricsService(
                _queue,
                _mockClientRegistry.Object,
                _serviceOptions,
                _mqttOptions,
                _reliabilityOptions,
                _mockAuditRepository.Object,
                _mockLogger.Object);

            var message = new ForwardMessage
            {
                MessageId = "dispose_flush_msg",
                RouteContext = new RouteContext
                {
                    MessageId = "dispose_flush_msg",
                    Topic = "dispose/topic",
                    Payload = new byte[] { 7, 8, 9 },
                    QoS = 1,
                    Retain = false,
                    SourceClientId = "dispose_client",
                    Timestamp = DateTime.Now
                }
            };

            metricsService.RecordReceived(message);

            var started = await Task.WhenAny(firstWriteStarted.Task, Task.Delay(TimeSpan.FromSeconds(2)));
            Assert.Same(firstWriteStarted.Task, started);

            var disposeTask = Task.Run(() => metricsService.Dispose());
            await Task.Delay(100);
            Assert.False(disposeTask.IsCompleted);

            allowWriteToFinish.TrySetResult(true);
            await disposeTask;

            Assert.True(persistedBeforeDisposeReturns);
            _mockAuditRepository.Verify(r => r.RecordMessageAuditsAsync(
                It.Is<IReadOnlyList<MessageAuditRecord>>(records => records.Any(x => x.MessageId == "dispose_flush_msg"))),
                Times.AtLeastOnce);
        }

        [Fact]
        public async Task MetricsMessageQueue_EnqueueAsync_ShouldInterceptAndRecordMetrics()
        {
            // Arrange
            var mockMetrics = new Mock<IMetricsService>();
            var mockQueue = new Mock<IMessageQueue>();
            var decorator = new MetricsMessageQueue(mockQueue.Object, mockMetrics.Object);
            var message = new ForwardMessage
            {
                MessageId = "msg_1",
                RouteContext = new RouteContext { MessageId = "msg_1" }
            };

            mockQueue.Setup(q => q.EnqueueAsync(message, It.IsAny<CancellationToken>())).ReturnsAsync(true);

            // Act
            var success = await decorator.EnqueueAsync(message);

            // Assert
            Assert.True(success);
            mockQueue.Verify(q => q.EnqueueAsync(message, It.IsAny<CancellationToken>()), Times.Once);
            mockMetrics.Verify(m => m.RecordReceived(message, true), Times.Once);
        }

        [Fact]
        public async Task MetricsMqttBrokerHost_PublishAsync_ShouldInterceptAndRecordMetrics()
        {
            // Arrange
            var mockMetrics = new Mock<IMetricsService>();
            var mockBroker = new Mock<IMqttBrokerHost>();
            var decorator = new MetricsMqttBrokerHost(mockBroker.Object, mockMetrics.Object);
            var payload = new byte[] { 9, 8 };

            mockBroker.Setup(b => b.PublishAsync(
                "topic", payload, 1, "client_x", true, It.IsAny<CancellationToken>()
            )).ReturnsAsync(true);

            // Act
            var success = await decorator.PublishAsync("topic", payload, 1, "client_x", true);

            // Assert
            Assert.True(success);
            mockBroker.Verify(b => b.PublishAsync("topic", payload, 1, "client_x", true, It.IsAny<CancellationToken>()), Times.Once);
            mockMetrics.Verify(m => m.RecordForwarded(
                It.Is<RouteContext>(ctx => ctx.Topic == "topic" && ctx.SourceClientId == "client_x" && ctx.Retain),
                true,
                0,
                It.IsAny<double>(),
                It.IsAny<bool>()
            ), Times.Once);
        }

        [Fact]
        public async Task MetricsMqttBrokerHost_PublishAsync_WithCurrentMessageIdInContext_ShouldSkipDuplicateMetrics()
        {
            // Arrange
            var mockMetrics = new Mock<IMetricsService>();
            var mockBroker = new Mock<IMqttBrokerHost>();
            var decorator = new MetricsMqttBrokerHost(mockBroker.Object, mockMetrics.Object);
            var payload = new byte[] { 1, 2, 3 };
            var originalMessageId = "test-original-msg-id-12345";

            mockBroker.Setup(b => b.PublishAsync(
                "topic_abc", payload, 1, "client_abc", false, It.IsAny<CancellationToken>()
            )).ReturnsAsync(true);

            // Set context MessageId
            MessageContext.CurrentMessageId.Value = originalMessageId;

            try
            {
                // Act
                var success = await decorator.PublishAsync("topic_abc", payload, 1, "client_abc", false);

                // Assert
                Assert.True(success);
                mockMetrics.Verify(m => m.RecordForwarded(
                    It.IsAny<RouteContext>(),
                    It.IsAny<bool>(),
                    It.IsAny<int>(),
                    It.IsAny<double>(),
                    It.IsAny<bool>()
                ), Times.Never);
            }
            finally
            {
                MessageContext.CurrentMessageId.Value = null;
            }
        }

        [Fact]
        public async Task MetricsDeadLetterService_WriteAsync_ShouldInterceptAndRecordMetrics()
        {
            // Arrange
            var mockMetrics = new Mock<IMetricsService>();
            var mockDeadLetter = new Mock<IDeadLetterService>();
            var decorator = new MetricsDeadLetterService(mockDeadLetter.Object, mockMetrics.Object);
            var record = new DeadLetterRecord { MessageId = "dlq_msg" };

            // Act
            await decorator.WriteAsync(record);

            // Assert
            mockDeadLetter.Verify(d => d.WriteAsync(record, It.IsAny<CancellationToken>()), Times.Once);
            mockMetrics.Verify(m => m.RecordDeadLetter(record), Times.Once);
        }

        private static ConcurrentDictionary<string, MessageAuditRecord> GetPendingAudits(MetricsService metricsService)
        {
            var pendingField = typeof(MetricsService).GetField("_pendingMessageAudits", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(pendingField);
            return Assert.IsType<ConcurrentDictionary<string, MessageAuditRecord>>(pendingField!.GetValue(metricsService));
        }
    }
}
