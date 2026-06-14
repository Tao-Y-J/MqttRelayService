using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MqttRelayService.Workers;
using Xunit;

namespace MqttRelayService.Tests
{
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

            var stopOrder = hostedServiceTypes.Reverse().ToArray();
            Assert.True(
                Array.IndexOf(stopOrder, typeof(BrokerWorker)) < Array.IndexOf(stopOrder, typeof(DeliveryWorker)),
                "BrokerWorker 必须先停止以阻断新的 MQTT 发布入口，DeliveryWorker 随后排空队列。");
            Assert.True(
                Array.IndexOf(stopOrder, typeof(DeliveryWorker)) < Array.IndexOf(stopOrder, typeof(QueueMetricsWorker)),
                "QueueMetricsWorker 应最后停止，以便停机排空期间仍可观察队列状态。");
        }

        [Fact]
        public void RegisterHostedServices_StopOrder_ShouldBlockIngressBeforeDeliveryDrain()
        {
            var services = new ServiceCollection();

            Program.RegisterHostedServices(services);

            var stopOrder = services
                .Where(descriptor => descriptor.ServiceType == typeof(IHostedService))
                .Select(descriptor => descriptor.ImplementationType)
                .OfType<Type>()
                .Reverse()
                .ToArray();

            var brokerAcceptingPublishes = true;
            var pendingMessages = 0;

            foreach (var serviceType in stopOrder)
            {
                if (serviceType == typeof(BrokerWorker))
                {
                    brokerAcceptingPublishes = false;
                }
                else if (serviceType == typeof(DeliveryWorker))
                {
                    pendingMessages = 0;

                    if (brokerAcceptingPublishes)
                    {
                        pendingMessages++;
                    }
                }
            }

            Assert.False(brokerAcceptingPublishes);
            Assert.Equal(0, pendingMessages);
        }
    }
}
