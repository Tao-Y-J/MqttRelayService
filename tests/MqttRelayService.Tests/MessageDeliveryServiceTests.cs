using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;
using MqttRelayService.Services.Implementations;
using Xunit;

namespace MqttRelayService.Tests;

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
    private readonly MessageDeliveryService _service;

    public MessageDeliveryServiceTests()
    {
        _queueMock = new Mock<IMessageQueue>();
        _routerMock = new Mock<IMessageRouter>();
        _brokerHostMock = new Mock<IMqttBrokerHost>();
        _deadLetterMock = new Mock<IDeadLetterService>();
        _retryPolicyMock = new Mock<IRetryPolicyProvider>();

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
        _brokerHostMock.Setup(b => b.PublishAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        // 使用反射调用私有方法
        var method = typeof(MessageDeliveryService).GetMethod("ProcessMessageAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        await (Task)method!.Invoke(_service, new object[] { message, CancellationToken.None })!;

        // PublishAsync 应只调用一次
        _brokerHostMock.Verify(b => b.PublishAsync(
            It.Is<string>(t => t == "test/topic"),
            It.IsAny<byte[]>(),
            It.IsAny<int>(),
            It.IsAny<CancellationToken>()),
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
        _brokerHostMock.Setup(b => b.PublishAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var method = typeof(MessageDeliveryService).GetMethod("ProcessMessageAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        await (Task)method!.Invoke(_service, new object[] { message, CancellationToken.None })!;

        // 即使有多个目标，PublishAsync 也应只调用一次
        _brokerHostMock.Verify(b => b.PublishAsync(
            It.Is<string>(t => t == "test/topic"),
            It.IsAny<byte[]>(),
            It.IsAny<int>(),
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
        _brokerHostMock.Setup(b => b.PublishAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        _retryPolicyMock.Setup(rp => rp.GetDelayAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(TimeSpan.FromMilliseconds(50));
        _queueMock.Setup(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var method = typeof(MessageDeliveryService).GetMethod("ProcessMessageAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        await (Task)method!.Invoke(_service, new object[] { message, CancellationToken.None })!;

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
        _brokerHostMock.Setup(b => b.PublishAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
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

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await (Task)method!.Invoke(_service, new object[] { message, "test failure", CancellationToken.None })!;
        stopwatch.Stop();

        // 验证实际等待了退避时间
        Assert.True(stopwatch.ElapsedMilliseconds >= 80, $"Expected delay ~100ms, actual {stopwatch.ElapsedMilliseconds}ms");
        _queueMock.Verify(q => q.EnqueueAsync(It.IsAny<ForwardMessage>(), It.IsAny<CancellationToken>()), Times.Once);
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
