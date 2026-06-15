using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MqttRelayService.Workers;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// Host 停机顺序测试，验证 Program.cs 的真实后台服务注册顺序。
    /// 注册顺序必须满足停机不变式：先阻断 Broker 入口 → DeliveryWorker 排空队列 → QueueMetricsWorker 最后停止。
    /// 注意：本测试仅验证注册顺序，未触发 IHost 的真实 StopAsync 流程，真正的停机排空语义应由集成测试覆盖。
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

            var stopOrder = hostedServiceTypes.Reverse().ToArray();
            Assert.True(
                Array.IndexOf(stopOrder, typeof(BrokerWorker)) < Array.IndexOf(stopOrder, typeof(DeliveryWorker)),
                "BrokerWorker 必须先停止以阻断新的 MQTT 发布入口，DeliveryWorker 随后排空队列。");
            Assert.True(
                Array.IndexOf(stopOrder, typeof(DeliveryWorker)) < Array.IndexOf(stopOrder, typeof(QueueMetricsWorker)),
                "QueueMetricsWorker 应最后停止，以便停机排空期间仍可观察队列状态。");
        }
    }
}
