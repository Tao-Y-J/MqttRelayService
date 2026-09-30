using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// 真实 Host 生命周期测试：使用生产注册代码启动一个真实 Host（Web 面关闭），
    /// 验证依赖注入图可解析、Broker 真实绑定端口、消息能经过队列转发，并且优雅停机不会卡死。
    /// 该测试覆盖的是生产注册路径，装饰器或 Options 注册形成构造期循环依赖时会直接超时失败。
    /// </summary>
    public class HostLifecycleTests
    {
        [Fact]
        public async Task Host_StartPublishAndStop_ShouldRunRealPipelineAndShutdownGracefully()
        {
            var port = GetAvailablePort();
            var deadLetterPath = Path.Combine(Path.GetTempPath(), "mqtt-relay-host-tests", Guid.NewGuid().ToString("N"));

            var configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["Service:Name"] = "MqttRelayServiceHostTests",
                    ["Mqtt:TcpPort"] = port.ToString(),
                    ["Auth:AllowAnonymous"] = "true",
                    ["Routing:EchoToSender"] = "false",
                    ["Reliability:QueueCapacity"] = "100",
                    ["Reliability:EnqueueTimeoutMs"] = "1000",
                    ["Reliability:MaxConcurrentHandlers"] = "1",
                    ["Reliability:RetryBaseDelayMs"] = "50",
                    ["Reliability:RetryMaxDelayMs"] = "500",
                    ["Reliability:ForwardTimeoutMs"] = "5000",
                    ["Reliability:ShutdownDrainTimeoutMs"] = "5000",
                    ["Reliability:EnableDeadLetter"] = "true",
                    ["Reliability:DeadLetterPath"] = deadLetterPath,
                    ["Web:Enabled"] = "false",
                    ["AuditStorage:Provider"] = "Sqlite"
                })
                .Build();

            var builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders();

            Program.ConfigureCoreServices(builder.Services, configuration, enableWeb: false);
            Program.RegisterHostedServices(builder.Services, enableWeb: false);

            using var host = builder.Build();

            try
            {
                await host.StartAsync();

                var factory = new MqttClientFactory();
                var subscriber = factory.CreateMqttClient();
                var publisher = factory.CreateMqttClient();
                var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

                subscriber.ApplicationMessageReceivedAsync += e =>
                {
                    received.TrySetResult(e.ApplicationMessage.ConvertPayloadToString());
                    return Task.CompletedTask;
                };

                try
                {
                    await subscriber.ConnectAsync(new MqttClientOptionsBuilder()
                        .WithTcpServer("127.0.0.1", port)
                        .WithClientId("host-lifecycle-subscriber")
                        .WithProtocolVersion(MqttProtocolVersion.V500)
                        .Build());

                    await subscriber.SubscribeAsync(new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter("host/lifecycle")
                        .Build());

                    await publisher.ConnectAsync(new MqttClientOptionsBuilder()
                        .WithTcpServer("127.0.0.1", port)
                        .WithClientId("host-lifecycle-publisher")
                        .WithProtocolVersion(MqttProtocolVersion.V500)
                        .Build());

                    // 真实管线：客户端发布 → 发布拦截入队 → 后台消费者路由并注入 → 订阅者收到
                    await publisher.PublishAsync(new MqttApplicationMessageBuilder()
                        .WithTopic("host/lifecycle")
                        .WithPayload(Encoding.UTF8.GetBytes("host-lifecycle-payload"))
                        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build());

                    var delivered = await Task.WhenAny(received.Task, Task.Delay(TimeSpan.FromSeconds(10)));
                    Assert.True(delivered == received.Task,
                        "Host 启动后消息必须能经过内部队列完成转发");
                    Assert.Equal("host-lifecycle-payload", await received.Task);
                }
                finally
                {
                    await subscriber.DisconnectAsync();
                    subscriber.Dispose();
                    await publisher.DisconnectAsync();
                    publisher.Dispose();
                }

                // 真实优雅停机：DeliveryWorker 先停止（封堵新发布 + 排空，此时 Broker 仍在运行），随后才是 BrokerWorker
                var stopTask = host.StopAsync();
                var stopped = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(30)));
                Assert.True(stopped == stopTask, "Host 优雅停机必须在 30 秒内完成");
                await stopTask;

                Assert.False(host.Services.GetService(typeof(IHostApplicationLifetime)) is null);
            }
            finally
            {
                try
                {
                    if (Directory.Exists(deadLetterPath))
                    {
                        Directory.Delete(deadLetterPath, recursive: true);
                    }
                }
                catch
                {
                    // 清理失败不影响测试结论
                }
            }
        }

        private static int GetAvailablePort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }
    }
}
