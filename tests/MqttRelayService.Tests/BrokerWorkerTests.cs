using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using MqttRelayService.Services.Abstractions;
using MqttRelayService.Workers;
using Xunit;

namespace MqttRelayService.Tests;

/// <summary>
/// BrokerWorker 单元测试
/// </summary>
public class BrokerWorkerTests
{
    [Fact]
    public async Task StartAsync_WhenBrokerStartFails_RequestsHostStop()
    {
        var brokerHostMock = new Mock<IMqttBrokerHost>();
        brokerHostMock.Setup(b => b.StartAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("start failed"));

        var lifetime = new RecordingHostApplicationLifetime();
        var worker = new BrokerWorker(
            brokerHostMock.Object,
            lifetime,
            new Mock<ILogger<BrokerWorker>>().Object);

        await worker.StartAsync(CancellationToken.None);

        Assert.True(lifetime.StopRequested);
        brokerHostMock.Verify(b => b.StartAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    private class RecordingHostApplicationLifetime : IHostApplicationLifetime
    {
        private readonly CancellationTokenSource _stoppingCts = new();

        public bool StopRequested { get; private set; }

        public CancellationToken ApplicationStarted => CancellationToken.None;

        public CancellationToken ApplicationStopping => _stoppingCts.Token;

        public CancellationToken ApplicationStopped => CancellationToken.None;

        public void StopApplication()
        {
            StopRequested = true;
            _stoppingCts.Cancel();
        }
    }
}
