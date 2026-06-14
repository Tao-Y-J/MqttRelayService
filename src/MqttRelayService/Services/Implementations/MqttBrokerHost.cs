using System.Buffers;
using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Packets;
using MQTTnet.Protocol;
using MQTTnet.Server;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Services.Implementations
{
    /// <summary>
    /// MQTT Broker 宿主实现，负责 MQTT Server 的创建、事件监听和消息入队
    /// </summary>
    public class MqttBrokerHost : IMqttBrokerHost, IDisposable
    {
        private const string SourceClientIdUserPropertyName = "x-source-client-id";
        private const string RelayMessageIdUserPropertyName = "x-relay-message-id";
        private const string ConnectionIdSessionItemKey = "mqtt-relay-connection-id";

        /// <summary>
        /// 注入消息 ID 跟踪字典的容量上限，防止异常情况下无界增长导致内存泄漏。
        /// </summary>
        private const int MaxInjectedMessageIdTracking = 50000;

        private readonly IAuthService _authService;
        private readonly IClientRegistry _clientRegistry;
        private readonly IMessageQueue _messageQueue;
        private readonly MqttOptions _options;
        private readonly RoutingOptions _routingOptions;
        private readonly ILogger<MqttBrokerHost> _logger;
        private readonly ConcurrentDictionary<string, byte> _injectedMessageIds = new();

        private MqttServer? _mqttServer;
        private bool _disposed;
        private int _injectedMessageIdOverflowLogged;

        public MqttBrokerHost(
            IAuthService authService,
            IClientRegistry clientRegistry,
            IMessageQueue messageQueue,
            IOptions<MqttOptions> options,
            IOptions<RoutingOptions> routingOptions,
            ILogger<MqttBrokerHost> logger)
        {
            _authService = authService;
            _clientRegistry = clientRegistry;
            _messageQueue = messageQueue;
            _options = options.Value;
            _routingOptions = routingOptions.Value;
            _logger = logger;
        }

        /// <summary>
        /// 当前是否正在运行
        /// </summary>
        public bool IsRunning => _mqttServer?.IsStarted ?? false;

        /// <summary>
        /// 启动 MQTT Server
        /// </summary>
        public async Task StartAsync(CancellationToken cancellationToken = default)
        {
            if (_mqttServer?.IsStarted == true)
            {
                _logger.LogWarning("MQTT Server 已经处于运行状态");
                return;
            }

            try
            {
                var factory = new MqttServerFactory();
                var serverOptions = new MqttServerOptionsBuilder()
                    .WithDefaultEndpoint()
                    .WithDefaultEndpointPort(_options.TcpPort)
                    .Build();

                _mqttServer = factory.CreateMqttServer(serverOptions);

                // 注册事件处理器（必须在 StartAsync 之前）
                _mqttServer.ValidatingConnectionAsync += OnValidatingConnectionAsync;
                _mqttServer.InterceptingPublishAsync += OnInterceptingPublishAsync;
                _mqttServer.InterceptingSubscriptionAsync += OnInterceptingSubscriptionAsync;
                _mqttServer.InterceptingUnsubscriptionAsync += OnInterceptingUnsubscriptionAsync;
                _mqttServer.InterceptingOutboundPacketAsync += OnInterceptingOutboundPacketAsync;
                _mqttServer.ClientConnectedAsync += OnClientConnectedAsync;
                _mqttServer.ClientDisconnectedAsync += OnClientDisconnectedAsync;

                await _mqttServer.StartAsync();

                _logger.LogInformation("MQTT Server 已启动，监听端口 {Port}", _options.TcpPort);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "启动 MQTT Server 失败");
                throw;
            }
        }

        /// <summary>
        /// 停止 MQTT Server
        /// </summary>
        public async Task StopAsync(CancellationToken cancellationToken = default)
        {
            if (_mqttServer == null || !_mqttServer.IsStarted)
            {
                return;
            }

            try
            {
                // 取消事件订阅
                _mqttServer.ValidatingConnectionAsync -= OnValidatingConnectionAsync;
                _mqttServer.InterceptingPublishAsync -= OnInterceptingPublishAsync;
                _mqttServer.InterceptingSubscriptionAsync -= OnInterceptingSubscriptionAsync;
                _mqttServer.InterceptingUnsubscriptionAsync -= OnInterceptingUnsubscriptionAsync;
                _mqttServer.InterceptingOutboundPacketAsync -= OnInterceptingOutboundPacketAsync;
                _mqttServer.ClientConnectedAsync -= OnClientConnectedAsync;
                _mqttServer.ClientDisconnectedAsync -= OnClientDisconnectedAsync;

                await _mqttServer.StopAsync();
                _logger.LogInformation("MQTT Server 已停止");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "停止 MQTT Server 时发生异常");
            }
        }

        /// <summary>
        /// 向指定 Topic 注入应用消息，由 Broker 自动分发给匹配的订阅者
        /// </summary>
        public async Task<bool> PublishAsync(
            string topic,
            byte[] payload,
            int qos,
            string? sourceClientId = null,
            bool retain = false,
            CancellationToken cancellationToken = default)
        {
            if (_mqttServer == null || !_mqttServer.IsStarted)
            {
                _logger.LogWarning("MQTT Server 未运行，无法发布消息");
                return false;
            }

            try
            {
                var messageBuilder = new MqttApplicationMessageBuilder()
                    .WithTopic(topic)
                    .WithPayload(payload)
                    .WithQualityOfServiceLevel((MqttQualityOfServiceLevel)qos)
                    .WithRetainFlag(retain);

                string? relayMessageId = null;

                // 如果指定了源客户端 ID，附加到 UserProperties 以便出站拦截器识别
                if (!string.IsNullOrEmpty(sourceClientId))
                {
                    relayMessageId = Guid.NewGuid().ToString("N");

                    // 防止注入消息 ID 跟踪字典无界增长：超限时触发一次性清理，避免内存泄漏
                    if (!TryTrackInjectedMessageId(relayMessageId))
                    {
                        // 跟踪失败意味着字典已满且无法清理旧条目——继续执行转发但无法 EchoToSender 过滤
                        _logger.LogError("注入消息 ID 跟踪字典溢出，消息 {RelayMessageId} 无法被 EchoToSender 出站拦截识别",
                            relayMessageId);
                    }

                    messageBuilder.WithUserProperty(SourceClientIdUserPropertyName, Encoding.UTF8.GetBytes(sourceClientId));
                    messageBuilder.WithUserProperty(RelayMessageIdUserPropertyName, Encoding.UTF8.GetBytes(relayMessageId));
                }

                var message = messageBuilder.Build();
                var injectedMessage = new InjectedMqttApplicationMessage(message);

                try
                {
                    await _mqttServer.InjectApplicationMessage(injectedMessage, cancellationToken);
                }
                catch
                {
                    if (relayMessageId != null)
                    {
                        _injectedMessageIds.TryRemove(relayMessageId, out _);
                    }

                    throw;
                }

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "注入消息到 Topic {Topic} 失败", topic);
                return false;
            }
        }

        /// <summary>
        /// 连接认证验证
        /// </summary>
        private async Task OnValidatingConnectionAsync(ValidatingConnectionEventArgs e)
        {
            try
            {
                var authRequest = new AuthRequest
                {
                    ClientId = e.ClientId,
                    Username = e.UserName ?? string.Empty,
                    Password = e.Password ?? string.Empty
                };

                var result = await _authService.AuthenticateAsync(authRequest);

                if (result.Success)
                {
                    e.ReasonCode = MqttConnectReasonCode.Success;
                }
                else
                {
                    e.ReasonCode = MqttConnectReasonCode.BadUserNameOrPassword;
                    _logger.LogWarning("客户端 {ClientId} 认证失败：{Reason}", e.ClientId, result.ErrorMessage);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "验证客户端 {ClientId} 连接时发生异常", e.ClientId);
                e.ReasonCode = MqttConnectReasonCode.UnspecifiedError;
            }
        }

        /// <summary>
        /// 消息发布拦截：将消息入队并阻止默认分发
        /// </summary>
        private async Task OnInterceptingPublishAsync(InterceptingPublishEventArgs e)
        {
            if (IsRelayInjectedMessage(e.ApplicationMessage))
            {
                // 服务端注入的转发消息需要继续交给 Broker 分发，避免再次进入内部队列形成循环。
                return;
            }

            // 客户端原始发布默认必须由内部队列接管；即使后续入队失败或发生异常，也不能落回 Broker 默认分发路径。
            e.ProcessPublish = false;

            try
            {
                // 更新客户端活动时间
                await _clientRegistry.UpdateActivityAsync(e.ClientId);

                var context = new MqttRelayService.Models.RouteContext
                {
                    MessageId = Guid.NewGuid().ToString("N"),
                    Topic = e.ApplicationMessage.Topic,
                    Payload = e.ApplicationMessage.Payload.IsEmpty
                        ? Array.Empty<byte>()
                        : BuffersExtensions.ToArray(e.ApplicationMessage.Payload),
                    QoS = (int)e.ApplicationMessage.QualityOfServiceLevel,
                    Retain = e.ApplicationMessage.Retain,
                    SourceClientId = e.ClientId,
                    Timestamp = DateTime.UtcNow.ToLocalTime()
                };

                var forwardMessage = new ForwardMessage
                {
                    MessageId = context.MessageId,
                    RouteContext = context,
                    Status = Models.MessageProcessStatus.Received
                };

                var enqueued = await _messageQueue.EnqueueAsync(forwardMessage);

                if (enqueued)
                {
                    _logger.LogInformation("消息 {MessageId} 已入队，主题 {Topic}，来源 {ClientId}",
                        context.MessageId, context.Topic, e.ClientId);
                }
                else
                {
                    _logger.LogError("消息 {MessageId} 入队失败，主题 {Topic}", context.MessageId, context.Topic);
                }
            }
            catch (Exception ex)
            {
                // 保持 ProcessPublish = false，宁可丢消息也不让消息绕过内部管道（审计/重试/死信）
                _logger.LogError(ex, "拦截客户端 {ClientId} 发布消息时发生异常，消息已丢弃", e.ClientId);
            }
        }

        /// <summary>
        /// 订阅拦截
        /// </summary>
        private async Task OnInterceptingSubscriptionAsync(InterceptingSubscriptionEventArgs e)
        {
            try
            {
                await _clientRegistry.UpdateSubscriptionAsync(
                    e.ClientId,
                    e.TopicFilter.Topic,
                    true);

                _logger.LogInformation("客户端 {ClientId} 订阅主题 {Topic}", e.ClientId, e.TopicFilter.Topic);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "处理客户端 {ClientId} 订阅时发生异常", e.ClientId);
            }
        }

        /// <summary>
        /// 出站数据包拦截：EchoToSender=false 时阻止消息回发给发送方自身
        /// </summary>
        private Task OnInterceptingOutboundPacketAsync(InterceptingPacketEventArgs e)
        {
            if (e.Packet is MqttPublishPacket publishPacket
                && ShouldBlockEchoToSender(_routingOptions.EchoToSender, e.ClientId, publishPacket))
            {
                e.ProcessPacket = false;
                _logger.LogDebug("阻止消息回发给发送方 {ClientId}，Topic={Topic}", e.ClientId, publishPacket.Topic);
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// 判断是否应该阻止消息回发给发送方
        /// </summary>
        internal static bool ShouldBlockEchoToSender(bool echoToSender, string targetClientId, MqttPublishPacket publishPacket)
        {
            if (echoToSender) return false;

            var sourceClientId = publishPacket.UserProperties
                ?.FirstOrDefault(p => p.Name == SourceClientIdUserPropertyName)
                ?.ReadValueAsString();

            return !string.IsNullOrEmpty(sourceClientId) && sourceClientId == targetClientId;
        }

        /// <summary>
        /// 判断消息是否为投递服务通过 Broker 注入的转发消息。
        /// </summary>
        private bool IsRelayInjectedMessage(MqttApplicationMessage message)
        {
            var relayMessageId = message.UserProperties
                ?.FirstOrDefault(p => p.Name == RelayMessageIdUserPropertyName)
                ?.ReadValueAsString();

            if (string.IsNullOrEmpty(relayMessageId))
            {
                return false;
            }

            var removed = _injectedMessageIds.TryRemove(relayMessageId, out _);

            // 正常清理后重置溢出日志计数器，允许下一次溢出时再次记录
            if (removed && Interlocked.Exchange(ref _injectedMessageIdOverflowLogged, 0) > 0)
            {
                _logger.LogInformation("注入消息 ID 跟踪字典已恢复正常清理");
            }

            return removed;
        }

        /// <summary>
        /// 尝试将注入消息 ID 加入跟踪字典，超限时触发一次性清理并记录错误。
        /// </summary>
        private bool TryTrackInjectedMessageId(string relayMessageId)
        {
            if (_injectedMessageIds.Count < MaxInjectedMessageIdTracking)
            {
                _injectedMessageIds.TryAdd(relayMessageId, 0);
                return true;
            }

            // 超限：一次性清空字典以恢复服务，避免内存泄漏；后续转发消息将无法通过 EchoToSender 出站拦截识别
            if (Interlocked.CompareExchange(ref _injectedMessageIdOverflowLogged, 1, 0) == 0)
            {
                _logger.LogError(
                    "注入消息 ID 跟踪字典已超出容量上限 {MaxCapacity}，将清空字典以恢复服务。后续转发消息在本次生命周期内将无法被 EchoToSender 出站拦截识别，可能导致发送方收到自己发布的消息。",
                    MaxInjectedMessageIdTracking);

                _injectedMessageIds.Clear();
            }

            // 清空后重试一次
            if (_injectedMessageIds.Count < MaxInjectedMessageIdTracking)
            {
                _injectedMessageIds.TryAdd(relayMessageId, 0);
                return true;
            }

            return false;
        }

        /// <summary>
        /// 取消订阅拦截
        /// </summary>
        private async Task OnInterceptingUnsubscriptionAsync(InterceptingUnsubscriptionEventArgs e)
        {
            try
            {
                await _clientRegistry.UpdateSubscriptionAsync(
                    e.ClientId,
                    e.Topic,
                    false);

                _logger.LogInformation("客户端 {ClientId} 取消订阅主题 {Topic}", e.ClientId, e.Topic);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "处理客户端 {ClientId} 取消订阅时发生异常", e.ClientId);
            }
        }

        /// <summary>
        /// 客户端连接事件
        /// </summary>
        private async Task OnClientConnectedAsync(ClientConnectedEventArgs e)
        {
            try
            {
                var connectionId = Guid.NewGuid().ToString("N");
                e.SessionItems[ConnectionIdSessionItemKey] = connectionId;

                var session = new ClientSessionInfo
                {
                    ClientId = e.ClientId,
                    ConnectionId = connectionId,
                    Username = e.UserName ?? string.Empty,
                    ConnectedAt = DateTime.Now,
                    LastActivityAt = DateTime.Now,
                    Status = ConnectionStatus.Connected
                };

                await _clientRegistry.RegisterAsync(session);
                _logger.LogInformation("客户端 {ClientId} 已连接，当前在线数 {Count}",
                    e.ClientId, _clientRegistry.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "处理客户端 {ClientId} 连接事件时发生异常", e.ClientId);
            }
        }

        /// <summary>
        /// 客户端断开事件
        /// </summary>
        private async Task OnClientDisconnectedAsync(ClientDisconnectedEventArgs e)
        {
            try
            {
                var connectionId = e.SessionItems.Contains(ConnectionIdSessionItemKey)
                    ? e.SessionItems[ConnectionIdSessionItemKey] as string
                    : null;

                await _clientRegistry.UnregisterAsync(e.ClientId, connectionId);
                _logger.LogInformation("客户端 {ClientId} 已断开，原因 {Reason}，当前在线数 {Count}",
                    e.ClientId, e.DisconnectType, _clientRegistry.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "处理客户端 {ClientId} 断开事件时发生异常", e.ClientId);
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _mqttServer?.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
