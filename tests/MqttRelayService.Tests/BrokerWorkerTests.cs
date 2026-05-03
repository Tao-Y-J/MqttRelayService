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

    [Fact]
    public async Task ExecuteAsync_WhenRestartExhausted_RequestsHostStop()
    {
        var brokerHostMock = new Mock<IMqttBrokerHost>();
        var startCallCount = 0;

        brokerHostMock.Setup(b => b.StartAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(_ =>
            {
                startCallCount++;
                return startCallCount == 1
                    ? Task.CompletedTask
                    : Task.FromException(new InvalidOperationException("restart failed"));
            });
        brokerHostMock.SetupGet(b => b.IsRunning).Returns(false);

        var lifetime = new RecordingHostApplicationLifetime();
        var worker = new BrokerWorker(
            brokerHostMock.Object,
            lifetime,
            new Mock<ILogger<BrokerWorker>>().Object,
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMilliseconds(10),
            maxConsecutiveFailures: 1);

        await worker.StartAsync(CancellationToken.None);
        await lifetime.WaitForStopRequestAsync(TimeSpan.FromSeconds(1));
        await worker.StopAsync(CancellationToken.None);

        Assert.True(lifetime.StopRequested);
        brokerHostMock.Verify(b => b.StartAsync(It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    private class RecordingHostApplicationLifetime : IHostApplicationLifetime
    {
        private readonly CancellationTokenSource _stoppingCts = new();
        private readonly TaskCompletionSource _stopRequestedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool StopRequested { get; private set; }

        public CancellationToken ApplicationStarted => CancellationToken.None;

        public CancellationToken ApplicationStopping => _stoppingCts.Token;

        public CancellationToken ApplicationStopped => CancellationToken.None;

        public void StopApplication()
        {
            StopRequested = true;
            _stoppingCts.Cancel();
            _stopRequestedTcs.TrySetResult();
        }

        public async Task WaitForStopRequestAsync(TimeSpan timeout)
        {
            var completedTask = await Task.WhenAny(_stopRequestedTcs.Task, Task.Delay(timeout));
            Assert.Equal(_stopRequestedTcs.Task, completedTask);
        }
    }
}
