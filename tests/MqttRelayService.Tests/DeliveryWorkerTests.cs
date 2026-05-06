using Microsoft.Extensions.Logging;
using Moq;
using MqttRelayService.Services.Abstractions;
using MqttRelayService.Workers;
using Xunit;

namespace MqttRelayService.Tests;

/// <summary>
/// DeliveryWorker 单元测试
/// </summary>
public class DeliveryWorkerTests
{
    private readonly Mock<IMessageDeliveryService> _deliveryServiceMock;
    private readonly DeliveryWorker _worker;

    public DeliveryWorkerTests()
    {
        _deliveryServiceMock = new Mock<IMessageDeliveryService>();
        var loggerMock = new Mock<ILogger<DeliveryWorker>>();
        _worker = new DeliveryWorker(_deliveryServiceMock.Object, loggerMock.Object);
    }

    [Fact]
    public async Task ExecuteAsync_CallsDeliveryServiceStartAsync()
    {
        using var cts = new CancellationTokenSource();
        
        _deliveryServiceMock.Setup(d => d.StartAsync(It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        // 启动 worker 然后立即取消
        var executeTask = _worker.StartAsync(cts.Token);
        cts.CancelAfter(100);
        
        try
        {
            await executeTask;
        }
        catch (OperationCanceledException)
        {
            // 预期异常
        }

        _deliveryServiceMock.Verify(d => d.StartAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StopAsync_CallsDeliveryServiceStopAsync()
    {
        _deliveryServiceMock.Setup(d => d.StopAsync(It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        await _worker.StopAsync(CancellationToken.None);

        _deliveryServiceMock.Verify(d => d.StopAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StartAsync_WhenDeliveryServiceStartFails_PropagatesException()
    {
        _deliveryServiceMock.Setup(d => d.StartAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("startup blocked"));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => _worker.StartAsync(CancellationToken.None));

        Assert.Equal("startup blocked", exception.Message);
    }
}
