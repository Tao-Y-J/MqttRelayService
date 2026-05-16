using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Packets;
using MQTTnet.Protocol;
using MqttRelayService.Options;
using MqttRelayService.Models;
using MqttRelayService.Services.Abstractions;
using MqttRelayService.Services.Implementations;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// MQTT Broker 集成测试，使用真实 MQTTnet Broker 和客户端验证端到端消息流
    /// </summary>
    public class MqttBrokerIntegrationTests
    {
        /// <summary>
        /// 获取一个当前可用的随机 TCP 端口
        /// </summary>
        private static int GetAvailablePort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }

        /// <summary>
        /// 创建测试所需的全部真实服务实例
        /// </summary>
        private static (MqttBrokerHost Broker, InMemoryMessageQueue Queue, ClientRegistry Registry, MessageDeliveryService Delivery, int Port) CreateServices(bool echoToSender = false)
        {
            var port = GetAvailablePort();

            var brokerLogger = Mock.Of<ILogger<MqttBrokerHost>>();
            var queueLogger = Mock.Of<ILogger<InMemoryMessageQueue>>();
            var registryLogger = Mock.Of<ILogger<ClientRegistry>>();
            var routerLogger = Mock.Of<ILogger<MessageRouter>>();
            var deliveryLogger = Mock.Of<ILogger<MessageDeliveryService>>();
            var authLogger = Mock.Of<ILogger<AuthService>>();
            var deadLetterLogger = Mock.Of<ILogger<DeadLetterService>>();
            var retryLogger = Mock.Of<ILogger<RetryPolicyProvider>>();

            var mqttOptions = Microsoft.Extensions.Options.Options.Create(new MqttOptions { TcpPort = port });
            var routingOptions = Microsoft.Extensions.Options.Options.Create(new RoutingOptions { EchoToSender = echoToSender });
            var reliabilityOptions = Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
            {
                QueueCapacity = 1000,
                EnqueueTimeoutMs = 2000,
                MaxRetryCount = 3,
                RetryBaseDelayMs = 100,
                RetryMaxDelayMs = 1000,
                ForwardTimeoutMs = 5000,
                ShutdownDrainTimeoutMs = 2000,
                EnableDeadLetter = false,
                DropWhenQueueFull = false
            });
            var authOptions = Microsoft.Extensions.Options.Options.Create(new AuthOptions { AllowAnonymous = true });

            var authService = new AuthService(authOptions, authLogger);
            var registry = new ClientRegistry(registryLogger);
            var queue = new InMemoryMessageQueue(reliabilityOptions, queueLogger);
            var broker = new MqttBrokerHost(authService, registry, queue, mqttOptions, routingOptions, brokerLogger);
            var router = new MessageRouter(registry, routingOptions, routerLogger);
            var deadLetterService = new DeadLetterService(reliabilityOptions, deadLetterLogger);
            var retryPolicy = new RetryPolicyProvider(reliabilityOptions, retryLogger);
            var delivery = new MessageDeliveryService(queue, router, broker, deadLetterService, retryPolicy, reliabilityOptions, deliveryLogger);

            return (broker, queue, registry, delivery, port);
        }

        /// <summary>
        /// EchoToSender=false 时，发布方不应收到自己发送的消息，但其他订阅者应收到
        /// </summary>
        [Fact]
        public async Task EchoToSenderFalse_PublisherDoesNotReceiveOwnMessage()
        {
            var (broker, queue, registry, delivery, port) = CreateServices(echoToSender: false);

            try
            {
                await broker.StartAsync();
                await delivery.StartAsync();

                var factory = new MqttClientFactory();
                var clientA = factory.CreateMqttClient();
                var clientB = factory.CreateMqttClient();

                try
                {
                    var optionsA = new MqttClientOptionsBuilder()
                        .WithTcpServer("127.0.0.1", port)
                        .WithClientId("client-a")
                        .WithProtocolVersion(MqttProtocolVersion.V500)
                        .Build();
                    var optionsB = new MqttClientOptionsBuilder()
                        .WithTcpServer("127.0.0.1", port)
                        .WithClientId("client-b")
                        .WithProtocolVersion(MqttProtocolVersion.V500)
                        .Build();

                    await clientA.ConnectAsync(optionsA);
                    await clientB.ConnectAsync(optionsB);

                    var topic = "test/echo-false";
                    var subscribeOptions = new MqttClientSubscribeOptions
                    {
                        TopicFilters = [new MqttTopicFilter { Topic = topic, QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce }]
                    };
                    await clientA.SubscribeAsync(subscribeOptions);
                    await clientB.SubscribeAsync(subscribeOptions);

                    var tcsA = new TaskCompletionSource<string>();
                    var tcsB = new TaskCompletionSource<string>();

                    clientA.ApplicationMessageReceivedAsync += e =>
                    {
                        tcsA.TrySetResult(Encoding.UTF8.GetString(e.ApplicationMessage.Payload));
                        return Task.CompletedTask;
                    };
                    clientB.ApplicationMessageReceivedAsync += e =>
                    {
                        tcsB.TrySetResult(Encoding.UTF8.GetString(e.ApplicationMessage.Payload));
                        return Task.CompletedTask;
                    };

                    var payload = Encoding.UTF8.GetBytes("hello-echo-false");
                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic(topic)
                        .WithPayload(payload)
                        .Build();
                    await clientA.PublishAsync(message);

                    // 等待 B 收到消息（最多 5 秒）
                    var timeout = Task.Delay(TimeSpan.FromSeconds(5));
                    var completed = await Task.WhenAny(tcsB.Task, timeout);
                    Assert.Equal(tcsB.Task, completed);
                    Assert.Equal("hello-echo-false", await tcsB.Task);

                    // 给 A 一个短暂窗口，确认 A 没有收到自己的消息
                    var timeoutA = Task.Delay(TimeSpan.FromSeconds(1));
                    var completedA = await Task.WhenAny(tcsA.Task, timeoutA);
                    Assert.Equal(timeoutA, completedA);
                    Assert.False(tcsA.Task.IsCompleted);
                }
                finally
                {
                    await clientA.DisconnectAsync();
                    await clientB.DisconnectAsync();
                    clientA.Dispose();
                    clientB.Dispose();
                }
            }
            finally
            {
                await delivery.StopAsync();
                await broker.StopAsync();
                broker.Dispose();
            }
        }

        /// <summary>
        /// EchoToSender=true 时，发布方和其他订阅者都应收到消息
        /// </summary>
        [Fact]
        public async Task EchoToSenderTrue_PublisherReceivesOwnMessage()
        {
            var (broker, queue, registry, delivery, port) = CreateServices(echoToSender: true);

            try
            {
                await broker.StartAsync();
                await delivery.StartAsync();

                var factory = new MqttClientFactory();
                var clientA = factory.CreateMqttClient();
                var clientB = factory.CreateMqttClient();

                try
                {
                    var optionsA = new MqttClientOptionsBuilder()
                        .WithTcpServer("127.0.0.1", port)
                        .WithClientId("client-a-echo")
                        .WithProtocolVersion(MqttProtocolVersion.V500)
                        .Build();
                    var optionsB = new MqttClientOptionsBuilder()
                        .WithTcpServer("127.0.0.1", port)
                        .WithClientId("client-b-echo")
                        .WithProtocolVersion(MqttProtocolVersion.V500)
                        .Build();

                    await clientA.ConnectAsync(optionsA);
                    await clientB.ConnectAsync(optionsB);

                    var topic = "test/echo-true";
                    var subscribeOptions = new MqttClientSubscribeOptions
                    {
                        TopicFilters = [new MqttTopicFilter { Topic = topic, QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce }]
                    };
                    await clientA.SubscribeAsync(subscribeOptions);
                    await clientB.SubscribeAsync(subscribeOptions);

                    var tcsA = new TaskCompletionSource<string>();
                    var tcsB = new TaskCompletionSource<string>();

                    clientA.ApplicationMessageReceivedAsync += e =>
                    {
                        tcsA.TrySetResult(Encoding.UTF8.GetString(e.ApplicationMessage.Payload));
                        return Task.CompletedTask;
                    };
                    clientB.ApplicationMessageReceivedAsync += e =>
                    {
                        tcsB.TrySetResult(Encoding.UTF8.GetString(e.ApplicationMessage.Payload));
                        return Task.CompletedTask;
                    };

                    var payload = Encoding.UTF8.GetBytes("hello-echo-true");
                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic(topic)
                        .WithPayload(payload)
                        .Build();
                    await clientA.PublishAsync(message);

                    // 等待 A 和 B 都收到消息（最多 5 秒）
                    var timeout = Task.Delay(TimeSpan.FromSeconds(5));
                    var allReceived = Task.WhenAll(tcsA.Task, tcsB.Task);
                    var completed = await Task.WhenAny(allReceived, timeout);
                    Assert.Equal(allReceived, completed);

                    Assert.Equal("hello-echo-true", await tcsA.Task);
                    Assert.Equal("hello-echo-true", await tcsB.Task);
                }
                finally
                {
                    await clientA.DisconnectAsync();
                    await clientB.DisconnectAsync();
                    clientA.Dispose();
                    clientB.Dispose();
                }
            }
            finally
            {
                await delivery.StopAsync();
                await broker.StopAsync();
                broker.Dispose();
            }
        }

        /// <summary>
        /// 订阅后注册表包含 Topic，取消订阅后移除，后续发布不再收到
        /// </summary>
        [Fact]
        public async Task Subscribe_Unsubscribe_UpdatesRegistry()
        {
            var (broker, queue, registry, delivery, port) = CreateServices(echoToSender: false);

            try
            {
                await broker.StartAsync();
                await delivery.StartAsync();

                var factory = new MqttClientFactory();
                var clientA = factory.CreateMqttClient();
                var clientB = factory.CreateMqttClient();

                try
                {
                    var optionsA = new MqttClientOptionsBuilder()
                        .WithTcpServer("127.0.0.1", port)
                        .WithClientId("client-a-sub")
                        .WithProtocolVersion(MqttProtocolVersion.V500)
                        .Build();
                    var optionsB = new MqttClientOptionsBuilder()
                        .WithTcpServer("127.0.0.1", port)
                        .WithClientId("client-b-sub")
                        .WithProtocolVersion(MqttProtocolVersion.V500)
                        .Build();

                    await clientA.ConnectAsync(optionsA);
                    await clientB.ConnectAsync(optionsB);

                    var topic = "test/sub-unsub";
                    var subscribeOptions = new MqttClientSubscribeOptions
                    {
                        TopicFilters = [new MqttTopicFilter { Topic = topic, QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce }]
                    };
                    await clientA.SubscribeAsync(subscribeOptions);

                    // 验证注册表已记录订阅
                    var sessionAfterSub = await registry.GetSessionAsync("client-a-sub");
                    Assert.NotNull(sessionAfterSub);
                    Assert.Contains(topic, sessionAfterSub.Subscriptions);

                    // 取消订阅
                    var unsubscribeOptions = new MqttClientUnsubscribeOptions
                    {
                        TopicFilters = [topic]
                    };
                    await clientA.UnsubscribeAsync(unsubscribeOptions);

                    // 验证注册表已移除订阅
                    var sessionAfterUnsub = await registry.GetSessionAsync("client-a-sub");
                    Assert.NotNull(sessionAfterUnsub);
                    Assert.DoesNotContain(topic, sessionAfterUnsub.Subscriptions);

                    // B 订阅并发布消息
                    await clientB.SubscribeAsync(subscribeOptions);

                    var tcsA = new TaskCompletionSource<string>();
                    clientA.ApplicationMessageReceivedAsync += e =>
                    {
                        tcsA.TrySetResult(Encoding.UTF8.GetString(e.ApplicationMessage.Payload));
                        return Task.CompletedTask;
                    };

                    var payload = Encoding.UTF8.GetBytes("after-unsub");
                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic(topic)
                        .WithPayload(payload)
                        .Build();
                    await clientB.PublishAsync(message);

                    // 确认 A 没有收到消息
                    var timeout = Task.Delay(TimeSpan.FromSeconds(2));
                    var completed = await Task.WhenAny(tcsA.Task, timeout);
                    Assert.Equal(timeout, completed);
                    Assert.False(tcsA.Task.IsCompleted);
                }
                finally
                {
                    await clientA.DisconnectAsync();
                    await clientB.DisconnectAsync();
                    clientA.Dispose();
                    clientB.Dispose();
                }
            }
            finally
            {
                await delivery.StopAsync();
                await broker.StopAsync();
                broker.Dispose();
            }
        }

        /// <summary>
        /// 客户端发布后，消息应进入 IMessageQueue（Count 大于 0）
        /// </summary>
        [Fact]
        public async Task ClientPublish_MessageEntersQueue()
        {
            var (broker, queue, registry, delivery, port) = CreateServices(echoToSender: false);

            try
            {
                // 不启动 DeliveryService，确保消息留在队列中
                await broker.StartAsync();

                var factory = new MqttClientFactory();
                var client = factory.CreateMqttClient();

                try
                {
                    var options = new MqttClientOptionsBuilder()
                        .WithTcpServer("127.0.0.1", port)
                        .WithClientId("client-queue")
                        .WithProtocolVersion(MqttProtocolVersion.V500)
                        .Build();

                    await client.ConnectAsync(options);

                    var topic = "test/queue";
                    var payload = Encoding.UTF8.GetBytes("queue-test");
                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic(topic)
                        .WithPayload(payload)
                        .Build();
                    var publishResult = await client.PublishAsync(message);

                    // 等待消息被 Broker 拦截并入队（最多 2 秒轮询）
                    var queued = false;
                    for (int i = 0; i < 20; i++)
                    {
                        if (queue.Count > 0)
                        {
                            queued = true;
                            break;
                        }
                        await Task.Delay(100);
                    }

                    Assert.True(queued, $"消息发布后队列 Count 应大于 0。PublishResult: {publishResult.ReasonCode}");
                }
                finally
                {
                    await client.DisconnectAsync();
                    client.Dispose();
                }
            }
            finally
            {
                await broker.StopAsync();
                broker.Dispose();
            }
        }

        /// <summary>
        /// Retained 消息经过内部队列转发后，迟到订阅者仍应收到 Broker 保存的最后一条消息。
        /// </summary>
        [Fact]
        public async Task RetainedPublish_LateSubscriberReceivesRetainedMessage()
        {
            var (broker, queue, registry, delivery, port) = CreateServices(echoToSender: false);

            try
            {
                await broker.StartAsync();
                await delivery.StartAsync();

                var factory = new MqttClientFactory();
                var publisher = factory.CreateMqttClient();
                var subscriber = factory.CreateMqttClient();

                try
                {
                    var publisherOptions = new MqttClientOptionsBuilder()
                        .WithTcpServer("127.0.0.1", port)
                        .WithClientId("client-retain-pub")
                        .WithProtocolVersion(MqttProtocolVersion.V500)
                        .Build();
                    var subscriberOptions = new MqttClientOptionsBuilder()
                        .WithTcpServer("127.0.0.1", port)
                        .WithClientId("client-retain-sub")
                        .WithProtocolVersion(MqttProtocolVersion.V500)
                        .Build();

                    await publisher.ConnectAsync(publisherOptions);

                    var topic = "test/retain-late-subscriber";
                    var payload = Encoding.UTF8.GetBytes("retained-value");
                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic(topic)
                        .WithPayload(payload)
                        .WithRetainFlag(true)
                        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build();

                    await publisher.PublishAsync(message);
                    await Task.Delay(500);

                    var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
                    subscriber.ApplicationMessageReceivedAsync += e =>
                    {
                        received.TrySetResult(Encoding.UTF8.GetString(e.ApplicationMessage.Payload));
                        return Task.CompletedTask;
                    };

                    await subscriber.ConnectAsync(subscriberOptions);
                    await subscriber.SubscribeAsync(new MqttClientSubscribeOptions
                    {
                        TopicFilters = [new MqttTopicFilter { Topic = topic, QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce }]
                    });

                    var timeout = Task.Delay(TimeSpan.FromSeconds(5));
                    var completed = await Task.WhenAny(received.Task, timeout);
                    Assert.Equal(received.Task, completed);
                    Assert.Equal("retained-value", await received.Task);
                }
                finally
                {
                    await publisher.DisconnectAsync();
                    await subscriber.DisconnectAsync();
                    publisher.Dispose();
                    subscriber.Dispose();
                }
            }
            finally
            {
                await delivery.StopAsync();
                await broker.StopAsync();
                broker.Dispose();
            }
        }

        /// <summary>
        /// 客户端伪造内部转发标记时，仍应进入内部队列，不能绕过发布拦截路径
        /// </summary>
        [Fact]
        public async Task ClientPublish_WithSourceUserPropertyStillEntersQueue()
        {
            var (broker, queue, registry, delivery, port) = CreateServices(echoToSender: false);

            try
            {
                await broker.StartAsync();

                var factory = new MqttClientFactory();
                var client = factory.CreateMqttClient();

                try
                {
                    var options = new MqttClientOptionsBuilder()
                        .WithTcpServer("127.0.0.1", port)
                        .WithClientId("client-spoof")
                        .WithProtocolVersion(MqttProtocolVersion.V500)
                        .Build();

                    await client.ConnectAsync(options);

                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic("test/spoofed-user-property")
                        .WithPayload(Encoding.UTF8.GetBytes("spoof-test"))
                        .WithUserProperty("x-source-client-id", Encoding.UTF8.GetBytes("client-a"))
                        .Build();

                    var publishResult = await client.PublishAsync(message);

                    var queued = false;
                    for (int i = 0; i < 20; i++)
                    {
                        if (queue.Count > 0)
                        {
                            queued = true;
                            break;
                        }

                        await Task.Delay(100);
                    }

                    Assert.True(queued, $"客户端伪造 x-source-client-id 时仍应进入内部队列。PublishResult: {publishResult.ReasonCode}");
                }
                finally
                {
                    await client.DisconnectAsync();
                    client.Dispose();
                }
            }
            finally
            {
                await broker.StopAsync();
                broker.Dispose();
            }
        }

        /// <summary>
        /// 发布拦截器内部异常时，客户端原始消息不能回落到 Broker 默认分发路径。
        /// </summary>
        [Fact]
        public async Task ClientPublish_WhenInterceptingThrows_DoesNotUseDefaultBrokerDistribution()
        {
            var port = GetAvailablePort();

            var reliabilityOptions = Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
            {
                QueueCapacity = 1000,
                EnqueueTimeoutMs = 2000,
                MaxRetryCount = 3,
                RetryBaseDelayMs = 100,
                RetryMaxDelayMs = 1000,
                ForwardTimeoutMs = 5000,
                ShutdownDrainTimeoutMs = 2000,
                EnableDeadLetter = false,
                DropWhenQueueFull = false
            });

            var broker = new MqttBrokerHost(
                new AuthService(
                    Microsoft.Extensions.Options.Options.Create(new AuthOptions { AllowAnonymous = true }),
                    Mock.Of<ILogger<AuthService>>()),
                new ThrowingActivityClientRegistry(),
                new InMemoryMessageQueue(reliabilityOptions, Mock.Of<ILogger<InMemoryMessageQueue>>()),
                Microsoft.Extensions.Options.Options.Create(new MqttOptions { TcpPort = port }),
                Microsoft.Extensions.Options.Options.Create(new RoutingOptions { EchoToSender = false }),
                Mock.Of<ILogger<MqttBrokerHost>>());

            try
            {
                await broker.StartAsync();

                var factory = new MqttClientFactory();
                var publisher = factory.CreateMqttClient();
                var subscriber = factory.CreateMqttClient();

                try
                {
                    var publisherOptions = new MqttClientOptionsBuilder()
                        .WithTcpServer("127.0.0.1", port)
                        .WithClientId("client-intercept-ex-pub")
                        .WithProtocolVersion(MqttProtocolVersion.V500)
                        .Build();
                    var subscriberOptions = new MqttClientOptionsBuilder()
                        .WithTcpServer("127.0.0.1", port)
                        .WithClientId("client-intercept-ex-sub")
                        .WithProtocolVersion(MqttProtocolVersion.V500)
                        .Build();

                    await publisher.ConnectAsync(publisherOptions);
                    await subscriber.ConnectAsync(subscriberOptions);

                    var topic = "test/intercept-exception";
                    await subscriber.SubscribeAsync(new MqttClientSubscribeOptions
                    {
                        TopicFilters = [new MqttTopicFilter { Topic = topic, QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce }]
                    });

                    var received = new TaskCompletionSource<string>();
                    subscriber.ApplicationMessageReceivedAsync += e =>
                    {
                        received.TrySetResult(Encoding.UTF8.GetString(e.ApplicationMessage.Payload));
                        return Task.CompletedTask;
                    };

                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic(topic)
                        .WithPayload(Encoding.UTF8.GetBytes("should-not-bypass-queue"))
                        .Build();

                    await publisher.PublishAsync(message);

                    var timeout = Task.Delay(TimeSpan.FromSeconds(1));
                    var completed = await Task.WhenAny(received.Task, timeout);
                    Assert.Equal(timeout, completed);
                    Assert.False(received.Task.IsCompleted);
                }
                finally
                {
                    await publisher.DisconnectAsync();
                    await subscriber.DisconnectAsync();
                    publisher.Dispose();
                    subscriber.Dispose();
                }
            }
            finally
            {
                await broker.StopAsync();
                broker.Dispose();
            }
        }

        private class ThrowingActivityClientRegistry : IClientRegistry
        {
            public int Count => 0;

            public Task RegisterAsync(ClientSessionInfo session, CancellationToken cancellationToken = default) => Task.CompletedTask;

            public Task UnregisterAsync(string clientId, string? connectionId = null, CancellationToken cancellationToken = default) => Task.CompletedTask;

            public Task<ClientSessionInfo?> GetSessionAsync(string clientId, CancellationToken cancellationToken = default) => Task.FromResult<ClientSessionInfo?>(null);

            public Task<IReadOnlyCollection<ClientSessionInfo>> GetAllSessionsAsync(CancellationToken cancellationToken = default)
            {
                return Task.FromResult<IReadOnlyCollection<ClientSessionInfo>>(Array.Empty<ClientSessionInfo>());
            }

            public Task UpdateSubscriptionAsync(string clientId, string topic, bool isSubscribed, CancellationToken cancellationToken = default)
                => Task.CompletedTask;

            public Task UpdateActivityAsync(string clientId, CancellationToken cancellationToken = default)
                => throw new InvalidOperationException("activity update failed");
        }
    }
}
