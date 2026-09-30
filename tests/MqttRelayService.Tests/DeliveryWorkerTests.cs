using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using MqttRelayService.Services.Abstractions;
using MqttRelayService.Workers;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// DeliveryWorker 单元测试
    /// </summary>
    public class DeliveryWorkerTests
    {
        private readonly Mock<IMessageDeliveryService> _deliveryServiceMock;
        private readonly Mock<IHostApplicationLifetime> _lifetimeMock;
        private readonly DeliveryWorker _worker;

        public DeliveryWorkerTests()
        {
            _deliveryServiceMock = new Mock<IMessageDeliveryService>();
            _lifetimeMock = new Mock<IHostApplicationLifetime>();
            var loggerMock = new Mock<ILogger<DeliveryWorker>>();
            _worker = new DeliveryWorker(_deliveryServiceMock.Object, _lifetimeMock.Object, loggerMock.Object);
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
            _lifetimeMock.Verify(l => l.StopApplication(), Times.Never);
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
        public async Task ExecuteAsync_WhenDeliveryServiceStartFails_RequestsHostStop()
        {
            _deliveryServiceMock.Setup(d => d.StartAsync(It.IsAny<CancellationToken>()))
                .ThrowsAsync(new InvalidOperationException("startup blocked"));

            // 后台入口必须自行兜底：Host 配置了 BackgroundServiceExceptionBehavior.Ignore，
            // 若不主动请求停止，进程会带着「Broker 继续接收消息、消费者已死」的状态假存活。
            await _worker.StartAsync(CancellationToken.None);

            _lifetimeMock.Verify(l => l.StopApplication(), Times.Once);
        }
    }
}
