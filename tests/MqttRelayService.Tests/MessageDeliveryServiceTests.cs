using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;
using MqttRelayService.Services.Implementations;
using MqttRelayService.Utilities;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// MessageDeliveryService 单元测试
    /// </summary>
    public class MessageDeliveryServiceTests
    {
        private readonly Mock<IMessageQueue> _queueMock;
        private readonly Mock<IMessageRouter> _routerMock;
        private readonly Mock<IMqttBrokerHost> _brokerHostMock;
        private readonly Mock<IDeadLetterService> _deadLetterMock;
        private readonly Mock<IRetryPolicyProvider> _retryPolicyMock;
        private readonly Mock<IMetricsService> _metricsMock;
        private readonly MessageDeliveryService _service;

        public MessageDeliveryServiceTests()
        {
            _queueMock = new Mock<IMessageQueue>();
            _routerMock = new Mock<IMessageRouter>();
            _brokerHostMock = new Mock<IMqttBrokerHost>();
            _deadLetterMock = new Mock<IDeadLetterService>();
            _retryPolicyMock = new Mock<IRetryPolicyProvider>();
            _metricsMock = new Mock<IMetricsService>();

            var options = Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
            {
                QueueCapacity = 100,
                EnqueueTimeoutMs = 1000,
                MaxRetryCount = 3,
                RetryBaseDelayMs = 100,
                RetryMaxDelayMs = 1000,
                ForwardTimeoutMs = 5000,
                ShutdownDrainTimeoutMs = 10000,
                DropWhenQueueFull = false
            });

            var loggerMock = new Mock<ILogger<MessageDeliveryService>>();

            _service = new MessageDeliveryService(
                _queueMock.Object,
                _routerMock.Object,
                _brokerHostMock.Object,
                _deadLetterMock.Object,
                _retryPolicyMock.Object,
                options,
                new ThroughputController(),
                _metricsMock.Object,
                loggerMock.Object);
        }

        [Fact]
        public async Task ProcessMessageAsync_SingleTarget_CallsPublishOnce()
        {
            var message = CreateTestMessage();
            var targets = new List<ForwardResult>
            {
                new() { TargetClientId = "client-2", Success = true }
            };

            _routerMock.Setup(r => r.RouteAsync(It.IsAny<RouteContext>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(targets);
            _brokerHostMock.Setup(b => b.PublishAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            // 使用反射调用私有方法
            var method = typeof(MessageDeliveryService).GetMethod("ProcessMessageAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            await (Task)method!.Invoke(_service, new object[] { message, CancellationToken.None })!;

            // PublishAsync 应只调用一次，且 sourceClientId 等于 RouteContext.SourceClientId
            _brokerHostMock.Verify(b => b.PublishAsync(
                It.Is<string>(t => t == "test/topic"),
                It.IsAny<byte[]>(),
                It.IsAny<int>(),
                It.Is<string>(s => s == "client-1"),
                It.Is<bool>(retain => !retain),
                It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task ProcessMessageAsync_Success_RecordsProcessingLatencyFromReceiveToBrokerPublish()
        {
            var message = CreateTestMessage();
            message.RouteContext.Timestamp = DateTime.Now.AddMilliseconds(-800);
            var targets = new List<ForwardResult>
            {
                new() { TargetClientId = "client-2", Success = true }
            };

            _routerMock.Setup(r => r.RouteAsync(It.IsAny<RouteContext>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(targets);
            _brokerHostMock.Setup(b => b.PublishAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            var method = typeof(MessageDeliveryService).GetMethod("ProcessMessageAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            await (Task)method!.Invoke(_service, new object[] { message, CancellationToken.None })!;

            _metricsMock.Verify(m => m.RecordForwarded(
                It.Is<RouteContext>(ctx => ctx.MessageId == message.RouteContext.MessageId),
                true,
                0,
                It.Is<double>(latency => latency >= 500),
                true),
                Times.Once);
        }

        [Fact]
        public async Task ProcessMessageAsync_MultipleTargets_CallsPublishOnce()
        {
            var message = CreateTestMessage();
            var targets = new List<ForwardResult>
            {
                new() { TargetClientId = "client-2", Success = true },
                new() { TargetClientId = "client-3", Success = true },
                new() { TargetClientId = "client-4", Success = true }
            };

            _routerMock.Setup(r => r.RouteAsync(It.IsAny<RouteContext>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(targets);
            _brokerHostMock.Setup(b => b.PublishAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            var method = typeof(MessageDeliveryService).GetMethod("ProcessMessageAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            await (Task)method!.Invoke(_service, new object[] { message, CancellationToken.None })!;

            // 即使有多个目标，PublishAsync 也应只调用一次
            _brokerHostMock.Verify(b => b.PublishAsync(
                It.Is<string>(t => t == "test/topic"),
                It.IsAny<byte[]>(),
                It.IsAny<int>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task ProcessMessageAsync_PublishFails_TriggersRetry()
        {
            var message = CreateTestMessage();
            var targets = new List<ForwardResult>
            {
                new() { TargetClientId = "client-2", Success = true }
            };

            _routerMock.Setup(r => r.RouteAsync(It.IsAny<RouteContext>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(targets);
            _brokerHostMock.Setup(b => b.PublishAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(false);
            _retryPolicyMock.Setup(rp => rp.GetDelayAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(TimeSpan.FromMilliseconds(50));
            _queueMock.Setup(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            var method = typeof(MessageDeliveryService).GetMethod("ProcessMessageAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            await (Task)method!.Invoke(_service, new object[] { message, CancellationToken.None })!;

            // 重试调度为非阻塞，等待后台任务收敛
            await _service.WaitForPendingRetriesAsync();

            // 应触发重试入队
            _queueMock.Verify(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()), Times.Once);
            Assert.Equal(1, message.RetryCount);
        }

        [Fact]
        public async Task ProcessMessageAsync_RetryExhausted_MovesToDeadLetter()
        {
            var message = CreateTestMessage();
            message.RetryCount = 3; // 已达到最大重试次数
            var targets = new List<ForwardResult>
            {
                new() { TargetClientId = "client-2", Success = true }
            };

            _routerMock.Setup(r => r.RouteAsync(It.IsAny<RouteContext>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(targets);
            _brokerHostMock.Setup(b => b.PublishAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(false);
            _deadLetterMock.Setup(d => d.WriteAsync(It.IsAny<DeadLetterRecord>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var method = typeof(MessageDeliveryService).GetMethod("ProcessMessageAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            await (Task)method!.Invoke(_service, new object[] { message, CancellationToken.None })!;

            // 应写入死信
            _deadLetterMock.Verify(d => d.WriteAsync(It.IsAny<DeadLetterRecord>(), It.IsAny<CancellationToken>()), Times.Once);
            Assert.Equal(MessageProcessStatus.DeadLetter, message.Status);
        }

        [Fact]
        public async Task ProcessMessageAsync_DeadLetterWriteFails_ReEnqueuesMessage()
        {
            var message = CreateTestMessage();
            message.RetryCount = 3;
            var targets = new List<ForwardResult>
            {
                new() { TargetClientId = "client-2", Success = true }
            };

            _routerMock.Setup(r => r.RouteAsync(It.IsAny<RouteContext>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(targets);
            _brokerHostMock.Setup(b => b.PublishAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(false);
            _deadLetterMock.Setup(d => d.WriteAsync(It.IsAny<DeadLetterRecord>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new IOException("dead letter path unavailable"));
            _retryPolicyMock.Setup(rp => rp.GetDelayAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(TimeSpan.Zero);
            _queueMock.Setup(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            var method = typeof(MessageDeliveryService).GetMethod("ProcessMessageAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            await (Task)method!.Invoke(_service, new object[] { message, CancellationToken.None })!;

            _deadLetterMock.Verify(d => d.WriteAsync(
                It.Is<DeadLetterRecord>(record => record.MessageId == "msg-1"),
                It.IsAny<CancellationToken>()),
                Times.Once);
            _queueMock.Verify(q => q.EnqueueAsync(
                It.Is<ForwardMessage>(m => m.MessageId == "msg-1"),
                It.Is<CancellationToken>(token => !token.CanBeCanceled)),
                Times.Once);
            Assert.Equal(MessageProcessStatus.Failed, message.Status);
            Assert.Equal(1, message.DeadLetterFailureCount);
        }

        [Fact]
        public async Task ProcessMessageAsync_DeadLetterWriteKeepsFailing_StopsReEnqueueAfterLimit()
        {
            var message = CreateTestMessage();
            message.RetryCount = 3;
            message.DeadLetterFailureCount = 3;
            var targets = new List<ForwardResult>
            {
                new() { TargetClientId = "client-2", Success = true }
            };

            _routerMock.Setup(r => r.RouteAsync(It.IsAny<RouteContext>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(targets);
            _brokerHostMock.Setup(b => b.PublishAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(false);
            _deadLetterMock.Setup(d => d.WriteAsync(It.IsAny<DeadLetterRecord>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new IOException("dead letter path unavailable"));

            var method = typeof(MessageDeliveryService).GetMethod("ProcessMessageAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                (Task)method!.Invoke(_service, new object[] { message, CancellationToken.None })!);

            _queueMock.Verify(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()), Times.Never);
            Assert.Equal(MessageProcessStatus.DeadLetter, message.Status);
            Assert.Equal(3, message.DeadLetterFailureCount);
        }

        [Fact]
        public async Task ProcessMessageAsync_RouterException_TriggersRetry()
        {
            var message = CreateTestMessage();

            _routerMock.Setup(r => r.RouteAsync(It.IsAny<RouteContext>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new InvalidOperationException("Registry failure"));
            _retryPolicyMock.Setup(rp => rp.GetDelayAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(TimeSpan.FromMilliseconds(50));
            _queueMock.Setup(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            var method = typeof(MessageDeliveryService).GetMethod("ProcessMessageAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            await (Task)method!.Invoke(_service, new object[] { message, CancellationToken.None })!;

            // 重试调度为非阻塞，等待后台任务收敛
            await _service.WaitForPendingRetriesAsync();

            // 路由异常应触发重试
            _queueMock.Verify(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task HandleFailureAsync_WaitsDelayBeforeReEnqueue()
        {
            var message = CreateTestMessage();
            var delay = TimeSpan.FromMilliseconds(100);

            _retryPolicyMock.Setup(rp => rp.GetDelayAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(delay);
            _queueMock.Setup(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            var method = typeof(MessageDeliveryService).GetMethod("HandleFailureAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            // HandleFailureAsync 应快速返回（只调度后台任务，不阻塞退避延迟）
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            await (Task)method!.Invoke(_service, new object[] { message, "test failure", CancellationToken.None })!;
            stopwatch.Stop();

            Assert.True(stopwatch.ElapsedMilliseconds < 80,
                $"期望非阻塞返回（<80ms），实际 {stopwatch.ElapsedMilliseconds}ms");

            // 等待后台重试任务收敛（实际退避延迟在后台完成）
            await _service.WaitForPendingRetriesAsync();

            // 重试入队应在退避延迟后被调用一次
            _queueMock.Verify(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()), Times.Once);
            Assert.Equal(1, message.RetryCount);
        }

        [Fact]
        public async Task HandleFailureAsync_PendingRetryLimitExceeded_MovesToDeadLetter()
        {
            var firstMessage = CreateTestMessage();
            var secondMessage = CreateTestMessage();
            secondMessage.MessageId = "msg-2";
            secondMessage.RouteContext.MessageId = "msg-2";

            var retryPolicyMock = new Mock<IRetryPolicyProvider>();
            var queueMock = new Mock<IMessageQueue>();
            var deadLetterMock = new Mock<IDeadLetterService>();
            var logMessages = new List<string>();
            var loggerMock = new Mock<ILogger<MessageDeliveryService>>();

            retryPolicyMock.Setup(rp => rp.GetDelayAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(TimeSpan.FromSeconds(30));
            queueMock.Setup(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);
            deadLetterMock.Setup(d => d.WriteAsync(It.IsAny<DeadLetterRecord>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            loggerMock.Setup(x => x.Log(
                    It.IsAny<LogLevel>(),
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((o, t) => true),
                    It.IsAny<Exception?>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()))
                .Callback(new InvocationAction(i =>
                {
                    var formatter = i.Arguments[4] as Delegate;
                    var msg = formatter?.DynamicInvoke(i.Arguments[2], i.Arguments[3])?.ToString();
                    if (msg != null)
                    {
                        logMessages.Add(msg);
                    }
                }));

            var service = new MessageDeliveryService(
                queueMock.Object,
                new Mock<IMessageRouter>().Object,
                new Mock<IMqttBrokerHost>().Object,
                deadLetterMock.Object,
                retryPolicyMock.Object,
                Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
                {
                    QueueCapacity = 10,
                    MaxRetryCount = 3,
                    MaxPendingRetryTasks = 1,
                    RetryBaseDelayMs = 10,
                    RetryMaxDelayMs = 100,
                    ForwardTimeoutMs = 5000,
                    ShutdownDrainTimeoutMs = 2000,
                    DropWhenQueueFull = false
                }),
                loggerMock.Object);

            var method = typeof(MessageDeliveryService).GetMethod("HandleFailureAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            using var cts = new CancellationTokenSource();
            await (Task)method!.Invoke(service, new object[] { firstMessage, "first failure", cts.Token })!;
            await (Task)method.Invoke(service, new object[] { secondMessage, "second failure", cts.Token })!;

            deadLetterMock.Verify(d => d.WriteAsync(
                It.Is<DeadLetterRecord>(record => record.MessageId == "msg-2"),
                It.IsAny<CancellationToken>()),
                Times.Once);
            Assert.Equal(MessageProcessStatus.DeadLetter, secondMessage.Status);
            Assert.Contains(logMessages, msg =>
                msg.Contains("重试调度超出上限", StringComparison.Ordinal) &&
                msg.Contains("1", StringComparison.Ordinal));

            cts.Cancel();
            await service.WaitForPendingRetriesAsync();
        }

        [Fact]
        public async Task HandleFailureAsync_FailureBurstHonorsPendingRetryLimit()
        {
            var messages = Enumerable.Range(1, 4)
                .Select(index =>
                {
                    var message = CreateTestMessage();
                    message.MessageId = $"msg-{index}";
                    message.RouteContext.MessageId = $"msg-{index}";
                    return message;
                })
                .ToArray();

            var retryPolicyMock = new Mock<IRetryPolicyProvider>();
            var queueMock = new Mock<IMessageQueue>();
            var deadLetterMock = new Mock<IDeadLetterService>();

            retryPolicyMock.Setup(rp => rp.GetDelayAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(TimeSpan.FromSeconds(30));
            queueMock.Setup(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);
            deadLetterMock.Setup(d => d.WriteAsync(It.IsAny<DeadLetterRecord>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var service = new MessageDeliveryService(
                queueMock.Object,
                new Mock<IMessageRouter>().Object,
                new Mock<IMqttBrokerHost>().Object,
                deadLetterMock.Object,
                retryPolicyMock.Object,
                Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
                {
                    QueueCapacity = 10,
                    MaxRetryCount = 3,
                    MaxPendingRetryTasks = 2,
                    RetryBaseDelayMs = 10,
                    RetryMaxDelayMs = 100,
                    ForwardTimeoutMs = 5000,
                    ShutdownDrainTimeoutMs = 2000,
                    DropWhenQueueFull = false
                }),
                new Mock<ILogger<MessageDeliveryService>>().Object);

            var method = typeof(MessageDeliveryService).GetMethod("HandleFailureAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            using var cts = new CancellationTokenSource();
            foreach (var message in messages)
            {
                await (Task)method!.Invoke(service, new object[] { message, $"failure-{message.MessageId}", cts.Token })!;
            }

            deadLetterMock.Verify(d => d.WriteAsync(
                It.Is<DeadLetterRecord>(record => record.MessageId == "msg-3"),
                It.IsAny<CancellationToken>()),
                Times.Once);
            deadLetterMock.Verify(d => d.WriteAsync(
                It.Is<DeadLetterRecord>(record => record.MessageId == "msg-4"),
                It.IsAny<CancellationToken>()),
                Times.Once);
            Assert.Equal(MessageProcessStatus.DeadLetter, messages[2].Status);
            Assert.Equal(MessageProcessStatus.DeadLetter, messages[3].Status);
            Assert.NotEqual(MessageProcessStatus.DeadLetter, messages[0].Status);
            Assert.NotEqual(MessageProcessStatus.DeadLetter, messages[1].Status);

            cts.Cancel();
            await service.WaitForPendingRetriesAsync();

            queueMock.Verify(q => q.EnqueueAsync(
                It.Is<ForwardMessage>(message => message.MessageId == "msg-1" || message.MessageId == "msg-2"),
                It.IsAny<CancellationToken>()),
                Times.Exactly(2));
        }

        [Fact]
        public async Task StopAsync_DrainsRemainingMessages()
        {
            var message = CreateTestMessage();
            var queueMock = new Mock<IMessageQueue>();
            var routerMock = new Mock<IMessageRouter>();
            var recordingBroker = new RecordingBrokerHost();

            // ReadAllAsync 返回空枚举，消费循环不处理任何消息
            queueMock.Setup(q => q.ReadAllAsync(It.IsAny<CancellationToken>()))
                .Returns(GetEmptyAsyncEnumerable());

            // TryDequeueAsync 在 drain 阶段返回消息，然后返回 null
            queueMock.SetupSequence(q => q.TryDequeueAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(message)
                .ReturnsAsync((ForwardMessage?)null);

            routerMock.Setup(r => r.RouteAsync(It.IsAny<RouteContext>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new List<ForwardResult> { new() { TargetClientId = "client-2" } });

            var service = new MessageDeliveryService(
                queueMock.Object,
                routerMock.Object,
                recordingBroker,
                new Mock<IDeadLetterService>().Object,
                new Mock<IRetryPolicyProvider>().Object,
                Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
                {
                    QueueCapacity = 10,
                    MaxRetryCount = 3,
                    RetryBaseDelayMs = 10,
                    RetryMaxDelayMs = 100,
                    ForwardTimeoutMs = 5000,
                    ShutdownDrainTimeoutMs = 2000,
                    DropWhenQueueFull = false
                }),
                new Mock<ILogger<MessageDeliveryService>>().Object);

            await service.StartAsync(CancellationToken.None);
            await Task.Delay(200); // 确保消费循环已启动
            await service.StopAsync(CancellationToken.None);

            // 验证 drain 阶段调用了 TryDequeueAsync
            queueMock.Verify(q => q.TryDequeueAsync(It.IsAny<CancellationToken>()), Times.AtLeastOnce);

            // 验证消息被处理（状态从 Queued 变为 Succeeded）
            Assert.Equal(MessageProcessStatus.Succeeded, message.Status);

            // 验证 drain 阶段调用了 PublishAsync，且 sourceClientId 正确传递
            Assert.True(recordingBroker.PublishCalled, "drain 阶段应调用 PublishAsync 处理消息");
            Assert.Equal("client-1", recordingBroker.LastSourceClientId);
        }

        [Fact]
        public async Task StopAsync_CancelledInFlightMessage_ReEnqueuesAndDrains()
        {
            var message = CreateTestMessage();
            var queue = new InMemoryMessageQueue(
                Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
                {
                    QueueCapacity = 10,
                    EnqueueTimeoutMs = 1000,
                    MaxRetryCount = 3,
                    RetryBaseDelayMs = 10,
                    RetryMaxDelayMs = 100,
                    ForwardTimeoutMs = 5000,
                    ShutdownDrainTimeoutMs = 2000,
                    DropWhenQueueFull = false,
                    MaxConcurrentHandlers = 1
                }),
                new Mock<ILogger<InMemoryMessageQueue>>().Object);
            var routerMock = new Mock<IMessageRouter>();
            var brokerHostMock = new Mock<IMqttBrokerHost>();
            var deadLetterMock = new Mock<IDeadLetterService>();
            var retryPolicyMock = new Mock<IRetryPolicyProvider>();
            var firstPublishStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var publishCallCount = 0;

            routerMock.Setup(r => r.RouteAsync(It.IsAny<RouteContext>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new List<ForwardResult> { new() { TargetClientId = "client-2", Success = true } });
            brokerHostMock.Setup(b => b.PublishAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .Returns<string, byte[], int, string?, bool, CancellationToken>(async (_, _, _, _, _, token) =>
                {
                    var currentCall = Interlocked.Increment(ref publishCallCount);
                    if (currentCall == 1)
                    {
                        firstPublishStarted.TrySetResult();
                        await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    }

                    return true;
                });

            var service = new MessageDeliveryService(
                queue,
                routerMock.Object,
                brokerHostMock.Object,
                deadLetterMock.Object,
                retryPolicyMock.Object,
                Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
                {
                    QueueCapacity = 10,
                    EnqueueTimeoutMs = 1000,
                    MaxRetryCount = 3,
                    RetryBaseDelayMs = 10,
                    RetryMaxDelayMs = 100,
                    ForwardTimeoutMs = 5000,
                    ShutdownDrainTimeoutMs = 2000,
                    DropWhenQueueFull = false,
                    MaxConcurrentHandlers = 1
                }),
                new Mock<ILogger<MessageDeliveryService>>().Object);

            await service.StartAsync(CancellationToken.None);
            Assert.True(await queue.EnqueueAsync(message, CancellationToken.None));
            await firstPublishStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));

            await service.StopAsync(CancellationToken.None);

            Assert.Equal(2, publishCallCount);
            Assert.Equal(MessageProcessStatus.Succeeded, message.Status);
            Assert.Equal(0, queue.Count);
            deadLetterMock.Verify(d => d.WriteAsync(It.IsAny<DeadLetterRecord>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task StopAsync_DrainRetryWaitsForReEnqueueAndProcessesMessage()
        {
            var message = CreateTestMessage();
            var queueMock = new Mock<IMessageQueue>();
            var routerMock = new Mock<IMessageRouter>();
            var brokerHostMock = new Mock<IMqttBrokerHost>();
            var deadLetterMock = new Mock<IDeadLetterService>();
            var retryPolicyMock = new Mock<IRetryPolicyProvider>();
            var syncRoot = new object();
            var pendingMessages = new Queue<ForwardMessage>();
            pendingMessages.Enqueue(message);
            var publishCallCount = 0;

            queueMock.Setup(q => q.ReadAllAsync(It.IsAny<CancellationToken>()))
                .Returns(GetEmptyAsyncEnumerable());
            queueMock.Setup(q => q.TryDequeueAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(() =>
                {
                    lock (syncRoot)
                    {
                        return pendingMessages.Count > 0 ? pendingMessages.Dequeue() : null;
                    }
                });
            queueMock.Setup(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((ForwardMessage queuedMessage, CancellationToken _) =>
                {
                    lock (syncRoot)
                    {
                        pendingMessages.Enqueue(queuedMessage);
                    }

                    return true;
                });
            queueMock.SetupGet(q => q.Count)
                .Returns(() =>
                {
                    lock (syncRoot)
                    {
                        return pendingMessages.Count;
                    }
                });

            routerMock.Setup(r => r.RouteAsync(It.IsAny<RouteContext>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new List<ForwardResult> { new() { TargetClientId = "client-2", Success = true } });
            brokerHostMock.Setup(b => b.PublishAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => Interlocked.Increment(ref publishCallCount) >= 2);
            retryPolicyMock.Setup(rp => rp.GetDelayAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(TimeSpan.FromMilliseconds(50));

            var service = new MessageDeliveryService(
                queueMock.Object,
                routerMock.Object,
                brokerHostMock.Object,
                deadLetterMock.Object,
                retryPolicyMock.Object,
                Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
                {
                    QueueCapacity = 10,
                    EnqueueTimeoutMs = 1000,
                    MaxRetryCount = 3,
                    RetryBaseDelayMs = 10,
                    RetryMaxDelayMs = 100,
                    ForwardTimeoutMs = 5000,
                    ShutdownDrainTimeoutMs = 1000,
                    DropWhenQueueFull = false
                }),
                new Mock<ILogger<MessageDeliveryService>>().Object);

            await service.StartAsync(CancellationToken.None);
            await Task.Delay(100);

            await service.StopAsync(CancellationToken.None);

            Assert.Equal(2, publishCallCount);
            Assert.Equal(1, message.RetryCount);
            Assert.Equal(MessageProcessStatus.Succeeded, message.Status);

            lock (syncRoot)
            {
                Assert.Empty(pendingMessages);
            }

            deadLetterMock.Verify(d => d.WriteAsync(It.IsAny<DeadLetterRecord>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task StartAsync_ShutdownDrainTimeoutLessThanRetryMaxDelay_LogsWarning()
        {
            var queueMock = new Mock<IMessageQueue>();
            var routerMock = new Mock<IMessageRouter>();
            var brokerHostMock = new Mock<IMqttBrokerHost>();
            var deadLetterMock = new Mock<IDeadLetterService>();
            var retryPolicyMock = new Mock<IRetryPolicyProvider>();
            var logMessages = new List<string>();
            var loggerMock = new Mock<ILogger<MessageDeliveryService>>();

            loggerMock.Setup(x => x.Log(
                    It.IsAny<LogLevel>(),
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((o, t) => true),
                    It.IsAny<Exception?>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()))
                .Callback(new InvocationAction(i =>
                {
                    var formatter = i.Arguments[4] as Delegate;
                    var msg = formatter?.DynamicInvoke(i.Arguments[2], i.Arguments[3])?.ToString();
                    if (msg != null)
                    {
                        logMessages.Add(msg);
                    }
                }));

            queueMock.Setup(q => q.ReadAllAsync(It.IsAny<CancellationToken>()))
                .Returns(GetEmptyAsyncEnumerable());

            var service = new MessageDeliveryService(
                queueMock.Object,
                routerMock.Object,
                brokerHostMock.Object,
                deadLetterMock.Object,
                retryPolicyMock.Object,
                Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
                {
                    QueueCapacity = 10,
                    EnqueueTimeoutMs = 1000,
                    MaxRetryCount = 3,
                    RetryBaseDelayMs = 10,
                    RetryMaxDelayMs = 3000,
                    ForwardTimeoutMs = 5000,
                    ShutdownDrainTimeoutMs = 1000,
                    DropWhenQueueFull = false
                }),
                loggerMock.Object);

            await service.StartAsync(CancellationToken.None);
            await Task.Delay(100);
            await service.StopAsync(CancellationToken.None);

            Assert.Contains(logMessages, msg =>
                msg.Contains("ShutdownDrainTimeoutMs", StringComparison.Ordinal) &&
                msg.Contains("RetryMaxDelayMs", StringComparison.Ordinal));
        }

        [Fact]
        public async Task StartAsync_DefaultConfiguration_DoesNotLogDrainTimeoutWarning()
        {
            var queueMock = new Mock<IMessageQueue>();
            var logMessages = new List<string>();
            var loggerMock = new Mock<ILogger<MessageDeliveryService>>();

            loggerMock.Setup(x => x.Log(
                    It.IsAny<LogLevel>(),
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((o, t) => true),
                    It.IsAny<Exception?>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()))
                .Callback(new InvocationAction(i =>
                {
                    var formatter = i.Arguments[4] as Delegate;
                    var msg = formatter?.DynamicInvoke(i.Arguments[2], i.Arguments[3])?.ToString();
                    if (msg != null)
                    {
                        logMessages.Add(msg);
                    }
                }));

            queueMock.Setup(q => q.ReadAllAsync(It.IsAny<CancellationToken>()))
                .Returns(GetEmptyAsyncEnumerable());

            var service = new MessageDeliveryService(
                queueMock.Object,
                new Mock<IMessageRouter>().Object,
                new Mock<IMqttBrokerHost>().Object,
                new Mock<IDeadLetterService>().Object,
                new Mock<IRetryPolicyProvider>().Object,
                Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions()),
                loggerMock.Object);

            await service.StartAsync(CancellationToken.None);
            await Task.Delay(100);
            await service.StopAsync(CancellationToken.None);

            Assert.DoesNotContain(logMessages, msg =>
                msg.Contains("ShutdownDrainTimeoutMs", StringComparison.Ordinal) &&
                msg.Contains("RetryMaxDelayMs", StringComparison.Ordinal));
        }

        #region 取消语义测试

        [Fact]
        public async Task ProcessMessageAsync_CancelledToken_RethrowsOperationCanceledException()
        {
            var message = CreateTestMessage();
            var targets = new List<ForwardResult>
            {
                new() { TargetClientId = "client-2", Success = true }
            };

            _routerMock.Setup(r => r.RouteAsync(It.IsAny<RouteContext>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(targets);
            _brokerHostMock.Setup(b => b.PublishAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(false);

            var method = typeof(MessageDeliveryService).GetMethod("ProcessMessageAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            var cancelledToken = new CancellationToken(canceled: true);

            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await (Task)method!.Invoke(_service, new object[] { message, cancelledToken })!);

            Assert.Equal(0, message.RetryCount);
            _queueMock.Verify(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()), Times.Never);
            _deadLetterMock.Verify(d => d.WriteAsync(It.IsAny<DeadLetterRecord>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task TryForwardAsync_CancelledToken_RethrowsOperationCanceledException()
        {
            _brokerHostMock.Setup(b => b.PublishAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            var method = typeof(MessageDeliveryService).GetMethod("TryForwardAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            var cancelledToken = new CancellationToken(canceled: true);

            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await (Task<bool>)method!.Invoke(_service, new object[] { CreateTestMessage(), true, cancelledToken })!);
        }

        [Fact]
        public async Task HandleFailureAsync_CancelledToken_DoesNotIncrementRetryCount()
        {
            var message = CreateTestMessage();
            var delay = TimeSpan.FromMilliseconds(100);

            _retryPolicyMock.Setup(rp => rp.GetDelayAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(delay);

            var method = typeof(MessageDeliveryService).GetMethod("HandleFailureAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            var cancelledToken = new CancellationToken(canceled: true);

            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await (Task)method!.Invoke(_service, new object[] { message, "test failure", cancelledToken })!);

            Assert.Equal(0, message.RetryCount);
            _queueMock.Verify(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()), Times.Never);
            _deadLetterMock.Verify(d => d.WriteAsync(It.IsAny<DeadLetterRecord>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task HandleFailureAsync_CancelledDuringDelay_PreservesMessageWithoutIncrementingRetry()
        {
            var message = CreateTestMessage();
            var delay = TimeSpan.FromSeconds(2);

            _retryPolicyMock.Setup(rp => rp.GetDelayAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(delay);

            // 取消时后台任务以 CancellationToken.None 重新入队保留消息
            _queueMock.Setup(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            var method = typeof(MessageDeliveryService).GetMethod("HandleFailureAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            using var cts = new CancellationTokenSource();
            var token = cts.Token;

            // HandleFailureAsync 仅调度后台任务，本身应快速返回，不抛 OCE
            await (Task)method!.Invoke(_service, new object[] { message, "test failure", token })!;

            // 在退避延迟期间取消 token，触发后台任务的取消保留分支
            cts.Cancel();

            // 等待后台重试任务收敛（取消路径应快速结束）
            await _service.WaitForPendingRetriesAsync();

            // 取消期间延迟未完成，RetryCount 不应被推进
            Assert.Equal(0, message.RetryCount);

            // 后台任务应尝试以 CancellationToken.None 保留消息（再次入队）
            _queueMock.Verify(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()), Times.Once);

            // 保留成功时不应写死信
            _deadLetterMock.Verify(d => d.WriteAsync(It.IsAny<DeadLetterRecord>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task HandleFailureAsync_CancelAfterDelayBeforeEnqueue_DoesNotDeadLetter()
        {
            var message = CreateTestMessage();
            var enqueueStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var allowEnqueueToFinish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            _retryPolicyMock.Setup(rp => rp.GetDelayAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(TimeSpan.Zero);
            _queueMock.Setup(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()))
                .Returns<ForwardMessage, CancellationToken>(async (_, token) =>
                {
                    enqueueStarted.TrySetResult();
                    await allowEnqueueToFinish.Task;
                    return !token.IsCancellationRequested;
                });
            _deadLetterMock.Setup(d => d.WriteAsync(It.IsAny<DeadLetterRecord>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var method = typeof(MessageDeliveryService).GetMethod("HandleFailureAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            using var cts = new CancellationTokenSource();
            await (Task)method!.Invoke(_service, new object[] { message, "test failure", cts.Token })!;

            await enqueueStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));
            cts.Cancel();
            allowEnqueueToFinish.TrySetResult();

            await _service.WaitForPendingRetriesAsync();

            Assert.Equal(1, message.RetryCount);
            _queueMock.Verify(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()), Times.Once);
            _deadLetterMock.Verify(d => d.WriteAsync(It.IsAny<DeadLetterRecord>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        #endregion

        #region Drain 超时测试

        [Fact]
        public async Task StopAsync_DrainTimeout_LogsRemainingCount()
        {
            var message = CreateTestMessage();
            var queueMock = new Mock<IMessageQueue>();
            var routerMock = new Mock<IMessageRouter>();
            var brokerHost = new RecordingBrokerHost();
            var logMessages = new List<string>();
            var loggerMock = new Mock<ILogger<MessageDeliveryService>>();

            loggerMock.Setup(x => x.Log(
                It.IsAny<LogLevel>(),
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => true),
                It.IsAny<Exception?>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()))
                .Callback(new InvocationAction(i =>
                {
                    var formatter = i.Arguments[4] as Delegate;
                    var msg = formatter?.DynamicInvoke(i.Arguments[2], i.Arguments[3])?.ToString();
                    if (msg != null) logMessages.Add(msg);
                }));

            queueMock.Setup(q => q.ReadAllAsync(It.IsAny<CancellationToken>()))
                .Returns(GetEmptyAsyncEnumerable());

            queueMock.SetupSequence(q => q.TryDequeueAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(message)
                .ReturnsAsync((ForwardMessage?)null);

            queueMock.SetupGet(q => q.Count).Returns(1);

            routerMock.Setup(r => r.RouteAsync(It.IsAny<RouteContext>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new List<ForwardResult> { new() { TargetClientId = "client-2" } });

            var service = new MessageDeliveryService(
                queueMock.Object,
                routerMock.Object,
                brokerHost,
                new Mock<IDeadLetterService>().Object,
                new Mock<IRetryPolicyProvider>().Object,
                Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
                {
                    QueueCapacity = 10,
                    MaxRetryCount = 3,
                    RetryBaseDelayMs = 10,
                    RetryMaxDelayMs = 100,
                    ForwardTimeoutMs = 5000,
                    ShutdownDrainTimeoutMs = 2000,
                    DropWhenQueueFull = false
                }),
                loggerMock.Object);

            await service.StartAsync(CancellationToken.None);
            await Task.Delay(200);
            await service.StopAsync(CancellationToken.None);

            // 验证警告日志中包含 "已排空" 和 "剩余"
            var warningLog = logMessages.FirstOrDefault(m => m.Contains("已排空") && m.Contains("剩余"));
            Assert.NotNull(warningLog);
        }

        #endregion

        #region 并发消费者测试

        [Fact]
        public async Task StartAsync_MaxConcurrentHandlers3_StartsThreeConsumers()
        {
            var queueMock = new Mock<IMessageQueue>();
            var routerMock = new Mock<IMessageRouter>();
            var brokerHost = new RecordingBrokerHost();

            var channel = Channel.CreateUnbounded<ForwardMessage>();
            queueMock.Setup(q => q.ReadAllAsync(It.IsAny<CancellationToken>()))
                .Returns(channel.Reader.ReadAllAsync());

            var startTimes = new ConcurrentBag<long>();
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            routerMock.Setup(r => r.RouteAsync(It.IsAny<RouteContext>(), It.IsAny<CancellationToken>()))
                .Returns(async (RouteContext ctx, CancellationToken ct) =>
                {
                    startTimes.Add(stopwatch.ElapsedMilliseconds);
                    await Task.Delay(30, ct);
                    return new List<ForwardResult> { new() { TargetClientId = "client-2", Success = true } };
                });

            var service = new MessageDeliveryService(
                queueMock.Object,
                routerMock.Object,
                brokerHost,
                new Mock<IDeadLetterService>().Object,
                new Mock<IRetryPolicyProvider>().Object,
                Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
                {
                    QueueCapacity = 10,
                    MaxRetryCount = 3,
                    RetryBaseDelayMs = 10,
                    RetryMaxDelayMs = 100,
                    ForwardTimeoutMs = 5000,
                    ShutdownDrainTimeoutMs = 2000,
                    DropWhenQueueFull = false,
                    MaxConcurrentHandlers = 3
                }),
                new Mock<ILogger<MessageDeliveryService>>().Object);

            await service.StartAsync(CancellationToken.None);
            await Task.Delay(100);

            channel.Writer.TryWrite(CreateTestMessage());
            channel.Writer.TryWrite(CreateTestMessage());
            channel.Writer.TryWrite(CreateTestMessage());
            channel.Writer.Complete();

            await Task.Delay(200);
            await service.StopAsync(CancellationToken.None);

            var times = startTimes.ToArray();
            Assert.True(times.Length >= 3, $"期望处理 3 条消息，实际处理了 {times.Length} 条");

            var maxDiff = times.Max() - times.Min();
            Assert.True(maxDiff < 50, $"期望 3 条消息并发开始处理，实际最大时间差 {maxDiff}ms");
        }

        [Fact]
        public async Task StartAsync_MaxConcurrentHandlers0_FallsBackToOne()
        {
            var queueMock = new Mock<IMessageQueue>();
            var routerMock = new Mock<IMessageRouter>();
            var brokerHost = new RecordingBrokerHost();

            var channel = Channel.CreateUnbounded<ForwardMessage>();
            queueMock.Setup(q => q.ReadAllAsync(It.IsAny<CancellationToken>()))
                .Returns(channel.Reader.ReadAllAsync());

            var startTimes = new ConcurrentBag<long>();
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            routerMock.Setup(r => r.RouteAsync(It.IsAny<RouteContext>(), It.IsAny<CancellationToken>()))
                .Returns(async (RouteContext ctx, CancellationToken ct) =>
                {
                    startTimes.Add(stopwatch.ElapsedMilliseconds);
                    await Task.Delay(30, ct);
                    return new List<ForwardResult> { new() { TargetClientId = "client-2", Success = true } };
                });

            var service = new MessageDeliveryService(
                queueMock.Object,
                routerMock.Object,
                brokerHost,
                new Mock<IDeadLetterService>().Object,
                new Mock<IRetryPolicyProvider>().Object,
                Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
                {
                    QueueCapacity = 10,
                    MaxRetryCount = 3,
                    RetryBaseDelayMs = 10,
                    RetryMaxDelayMs = 100,
                    ForwardTimeoutMs = 5000,
                    ShutdownDrainTimeoutMs = 2000,
                    DropWhenQueueFull = false,
                    MaxConcurrentHandlers = 0
                }),
                new Mock<ILogger<MessageDeliveryService>>().Object);

            await service.StartAsync(CancellationToken.None);
            await Task.Delay(100);

            channel.Writer.TryWrite(CreateTestMessage());
            channel.Writer.TryWrite(CreateTestMessage());
            channel.Writer.TryWrite(CreateTestMessage());
            channel.Writer.Complete();

            await Task.Delay(200);
            await service.StopAsync(CancellationToken.None);

            var times = startTimes.ToArray();
            Assert.True(times.Length >= 3, $"期望处理 3 条消息，实际处理了 {times.Length} 条");

            // MaxConcurrentHandlers=0 时应回退到单消费者，顺序处理时间差应 >= 50ms
            var maxDiff = times.Max() - times.Min();
            Assert.True(maxDiff >= 50, $"期望单消费者顺序处理，实际最大时间差 {maxDiff}ms");
        }

        [Fact]
        public async Task StartAsync_AfterStop_StartsConsumersAgain()
        {
            var queueMock = new Mock<IMessageQueue>();
            var routerMock = new Mock<IMessageRouter>();
            var brokerHost = new RecordingBrokerHost();

            queueMock.Setup(q => q.ReadAllAsync(It.IsAny<CancellationToken>()))
                .Returns(GetEmptyAsyncEnumerable());

            var service = new MessageDeliveryService(
                queueMock.Object,
                routerMock.Object,
                brokerHost,
                new Mock<IDeadLetterService>().Object,
                new Mock<IRetryPolicyProvider>().Object,
                Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
                {
                    QueueCapacity = 10,
                    MaxRetryCount = 3,
                    RetryBaseDelayMs = 10,
                    RetryMaxDelayMs = 100,
                    ForwardTimeoutMs = 5000,
                    ShutdownDrainTimeoutMs = 2000,
                    DropWhenQueueFull = false,
                    MaxConcurrentHandlers = 1
                }),
                new Mock<ILogger<MessageDeliveryService>>().Object);

            await service.StartAsync(CancellationToken.None);
            await Task.Delay(100);
            await service.StopAsync(CancellationToken.None);

            await service.StartAsync(CancellationToken.None);
            await Task.Delay(100);
            await service.StopAsync(CancellationToken.None);

            queueMock.Verify(q => q.ReadAllAsync(It.IsAny<CancellationToken>()), Times.Exactly(2));
        }

        [Fact]
        public async Task StartAsync_WhenPreviousConsumerStillRunning_ThrowsInvalidOperationException()
        {
            var message = CreateTestMessage();
            var queue = new InMemoryMessageQueue(
                Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
                {
                    QueueCapacity = 10,
                    EnqueueTimeoutMs = 1000,
                    MaxRetryCount = 3,
                    RetryBaseDelayMs = 10,
                    RetryMaxDelayMs = 100,
                    ForwardTimeoutMs = 5000,
                    ShutdownDrainTimeoutMs = 2000,
                    DropWhenQueueFull = false,
                    MaxConcurrentHandlers = 1
                }),
                new Mock<ILogger<InMemoryMessageQueue>>().Object);
            var routerMock = new Mock<IMessageRouter>();
            var brokerHostMock = new Mock<IMqttBrokerHost>();
            var loggerMock = new Mock<ILogger<MessageDeliveryService>>();
            var publishStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var allowPublishToFinish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            routerMock.Setup(r => r.RouteAsync(It.IsAny<RouteContext>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new List<ForwardResult> { new() { TargetClientId = "client-2", Success = true } });
            brokerHostMock.Setup(b => b.PublishAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .Returns<string, byte[], int, string?, bool, CancellationToken>(async (_, _, _, _, _, _) =>
                {
                    publishStarted.TrySetResult();
                    await allowPublishToFinish.Task;
                    return true;
                });

            var service = new MessageDeliveryService(
                queue,
                routerMock.Object,
                brokerHostMock.Object,
                new Mock<IDeadLetterService>().Object,
                new Mock<IRetryPolicyProvider>().Object,
                Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
                {
                    QueueCapacity = 10,
                    EnqueueTimeoutMs = 1000,
                    MaxRetryCount = 3,
                    RetryBaseDelayMs = 10,
                    RetryMaxDelayMs = 100,
                    ForwardTimeoutMs = 5000,
                    ShutdownDrainTimeoutMs = 2000,
                    DropWhenQueueFull = false,
                    MaxConcurrentHandlers = 1
                }),
                loggerMock.Object);

            await service.StartAsync(CancellationToken.None);
            Assert.True(await queue.EnqueueAsync(message, CancellationToken.None));
            await publishStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));

            await service.StopAsync(CancellationToken.None);
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => service.StartAsync(CancellationToken.None));

            Assert.Contains("拒绝重新启动", exception.Message, StringComparison.Ordinal);

            allowPublishToFinish.TrySetResult();
            await Task.Delay(100);
            await service.StopAsync(CancellationToken.None);
        }

        [Fact]
        public async Task StartAsync_WhenPreviousRetrySchedulerStillRunning_ThrowsInvalidOperationException()
        {
            var message = CreateTestMessage();

            _retryPolicyMock.Setup(rp => rp.GetDelayAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(TimeSpan.FromSeconds(30));
            _queueMock.Setup(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            var handleFailureMethod = typeof(MessageDeliveryService).GetMethod("HandleFailureAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.NotNull(handleFailureMethod);

            using var cts = new CancellationTokenSource();
            await (Task)handleFailureMethod!.Invoke(_service, new object[] { message, "test failure", cts.Token })!;

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => _service.StartAsync(CancellationToken.None));

            Assert.Contains("重试调度未结束", exception.Message, StringComparison.Ordinal);

            cts.Cancel();
            await _service.WaitForPendingRetriesAsync();
        }

        #endregion

        private class RecordingBrokerHost : IMqttBrokerHost
        {
            public bool PublishCalled { get; private set; }
            public string? LastSourceClientId { get; private set; }
            public int PublishCallCount { get; private set; }

            public bool IsRunning => true;

            public Task StartAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
            public Task StopAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

            public Task<bool> PublishAsync(string topic, byte[] payload, int qos, string? sourceClientId = null, bool retain = false, CancellationToken cancellationToken = default)
            {
                PublishCalled = true;
                PublishCallCount++;
                LastSourceClientId = sourceClientId;
                return Task.FromResult(true);
            }
        }

        private static async IAsyncEnumerable<ForwardMessage> GetEmptyAsyncEnumerable()
        {
            await Task.CompletedTask;
            yield break;
        }

        private static ForwardMessage CreateTestMessage()
        {
            return new ForwardMessage
            {
                MessageId = "msg-1",
                RouteContext = new RouteContext
                {
                    MessageId = "msg-1",
                    Topic = "test/topic",
                    Payload = new byte[] { 1, 2, 3 },
                    QoS = 1,
                    SourceClientId = "client-1"
                },
                Status = MessageProcessStatus.Queued,
                RetryCount = 0
            };
        }
    }
}
