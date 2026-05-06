using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MqttRelayService.Workers;
using Xunit;

namespace MqttRelayService.Tests;

/// <summary>
/// Host 停机顺序测试，验证 Program.cs 的真实后台服务注册顺序。
/// </summary>
public class HostShutdownOrderTests
{
    [Fact]
    public void RegisterHostedServices_RegistersShutdownSafeOrder()
    {
        var services = new ServiceCollection();

        Program.RegisterHostedServices(services);

        var hostedServiceTypes = services
            .Where(descriptor => descriptor.ServiceType == typeof(IHostedService))
            .Select(descriptor => descriptor.ImplementationType)
            .OfType<Type>()
            .ToArray();

        Assert.Equal(
            new[]
            {
                typeof(QueueMetricsWorker),
                typeof(DeliveryWorker),
                typeof(BrokerWorker)
            },
            hostedServiceTypes);
    }
}
