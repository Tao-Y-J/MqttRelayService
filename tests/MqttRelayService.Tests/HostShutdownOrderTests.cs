using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace MqttRelayService.Tests;

/// <summary>
/// Host 停机顺序集成测试，验证后台服务注册顺序与停止顺序的对应关系。
/// </summary>
public class HostShutdownOrderTests
{
    [Fact]
    public async Task HostShutdown_BrokerWorkerStopAsync_PrecedesDeliveryWorkerStopAsync()
    {
        var stopOrder = new List<string>();
        var builder = new HostBuilder();

        builder.ConfigureServices(services =>
        {
            // 按 Program.cs 实际注册顺序注册跟踪服务：
            // QueueMetricsWorker -> DeliveryWorker -> BrokerWorker
            // HostedService 的 StopAsync 按注册逆序执行，
            // 因此停止顺序应为 BrokerWorker -> DeliveryWorker -> QueueMetricsWorker。
            // 使用 AddTransient<IHostedService> 绕过 TryAddEnumerable 去重限制，
            // 确保三个独立实例都被注册。
            services.AddTransient<IHostedService>(_ => new OrderRecordingService("QueueMetricsWorker", stopOrder));
            services.AddTransient<IHostedService>(_ => new OrderRecordingService("DeliveryWorker", stopOrder));
            services.AddTransient<IHostedService>(_ => new OrderRecordingService("BrokerWorker", stopOrder));
        });

        var host = builder.Build();

        await host.StartAsync();
        await Task.Delay(50);
        await host.StopAsync();

        var brokerIndex = stopOrder.IndexOf("BrokerWorker");
        var deliveryIndex = stopOrder.IndexOf("DeliveryWorker");

        Assert.True(brokerIndex >= 0, "BrokerWorker 未触发 StopAsync");
        Assert.True(deliveryIndex >= 0, "DeliveryWorker 未触发 StopAsync");
        Assert.True(brokerIndex < deliveryIndex,
            $"BrokerWorker.StopAsync 应在 DeliveryWorker.StopAsync 之前调用。实际停止顺序: [{string.Join(", ", stopOrder)}]");
    }

    private class OrderRecordingService : IHostedService
    {
        private readonly string _name;
        private readonly List<string> _stopOrder;

        public OrderRecordingService(string name, List<string> stopOrder)
        {
            _name = name;
            _stopOrder = stopOrder;
        }

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _stopOrder.Add(_name);
            return Task.CompletedTask;
        }
    }
}
