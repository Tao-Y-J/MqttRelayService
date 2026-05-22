using System;
using System.Collections.Generic;
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
        private readonly Mock<ISqliteAuditRepository> _mockAuditRepository = new();
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
                    Timestamp = DateTime.UtcNow
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
                Timestamp = DateTime.UtcNow
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
                    Timestamp = DateTime.UtcNow
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
                FirstReceivedAt = DateTime.UtcNow.AddSeconds(-10),
                LastFailedAt = DateTime.UtcNow,
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
        public async Task MetricsService_GetDashboardDataAsync_WithAuditRepository_ShouldPreferAuditAlignedSummary()
        {
            var now = DateTime.UtcNow;
            _mockAuditRepository
                .Setup(r => r.GetDashboardMessageSummaryAsync(100))
                .ReturnsAsync((
                    TotalMessages: 12,
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

            var result = await metricsService.GetDashboardDataAsync();

            dynamic data = result;
            Assert.Equal(12L, (long)data.Counters.TotalReceived);
            Assert.Equal(8L, (long)data.Counters.TotalSucceeded);
            Assert.Equal(3L, (long)data.Counters.TotalFailed);
            Assert.Equal(1L, (long)data.Counters.TotalDeadLetter);

            var logs = (List<object>)data.Logs;
            Assert.Single(logs);
            dynamic log = logs[0];
            Assert.Equal("audit_1", (string)log.MessageId);
            Assert.Equal("Succeeded", (string)log.Status);
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
            mockMetrics.Verify(m => m.RecordReceived(message), Times.Once);
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
                It.IsAny<double>()
            ), Times.Once);
        }

        [Fact]
        public async Task MetricsMqttBrokerHost_PublishAsync_WithCurrentMessageIdInContext_ShouldReuseOriginalMessageId()
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
                    It.Is<RouteContext>(ctx => ctx.MessageId == originalMessageId && ctx.Topic == "topic_abc" && ctx.SourceClientId == "client_abc"),
                    true,
                    0,
                    It.IsAny<double>()
                ), Times.Once);
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
    }
}
