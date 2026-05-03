using System.Buffers;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Protocol;
using MQTTnet.Server;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Services.Implementations;

/// <summary>
/// MQTT Broker 宿主实现，负责 MQTT Server 的创建、事件监听和消息入队
/// </summary>
public class MqttBrokerHost : IMqttBrokerHost, IDisposable
{
    private readonly IAuthService _authService;
    private readonly IClientRegistry _clientRegistry;
    private readonly IMessageQueue _messageQueue;
    private readonly MqttOptions _options;
    private readonly ILogger<MqttBrokerHost> _logger;

    private MqttServer? _mqttServer;
    private bool _disposed;

    public MqttBrokerHost(
        IAuthService authService,
        IClientRegistry clientRegistry,
        IMessageQueue messageQueue,
        IOptions<MqttOptions> options,
        ILogger<MqttBrokerHost> logger)
    {
        _authService = authService;
        _clientRegistry = clientRegistry;
        _messageQueue = messageQueue;
        _options = options.Value;
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
    /// 向指定 Topic 注入应用消息
    /// </summary>
    public async Task<bool> PublishAsync(string topic, byte[] payload, int qos, CancellationToken cancellationToken = default)
    {
        if (_mqttServer == null || !_mqttServer.IsStarted)
        {
            _logger.LogWarning("MQTT Server 未运行，无法发布消息");
            return false;
        }

        try
        {
            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payload)
                .WithQualityOfServiceLevel((MqttQualityOfServiceLevel)qos)
                .Build();

            var injectedMessage = new InjectedMqttApplicationMessage(message);
            await _mqttServer.InjectApplicationMessage(injectedMessage, cancellationToken);

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
        try
        {
            // 更新客户端活动时间
            await _clientRegistry.UpdateActivityAsync(e.ClientId);

            var context = new RouteContext
            {
                MessageId = Guid.NewGuid().ToString("N"),
                Topic = e.ApplicationMessage.Topic,
                Payload = e.ApplicationMessage.Payload.IsEmpty 
                    ? Array.Empty<byte>() 
                    : BuffersExtensions.ToArray(e.ApplicationMessage.Payload),
                QoS = (int)e.ApplicationMessage.QualityOfServiceLevel,
                SourceClientId = e.ClientId,
                Timestamp = DateTime.UtcNow
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

            // 阻止 Broker 默认分发，由投递服务控制转发
            e.ProcessPublish = false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "拦截客户端 {ClientId} 发布消息时发生异常", e.ClientId);
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
            var session = new ClientSessionInfo
            {
                ClientId = e.ClientId,
                Username = e.UserName ?? string.Empty,
                ConnectedAt = DateTime.UtcNow,
                LastActivityAt = DateTime.UtcNow,
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
            await _clientRegistry.UnregisterAsync(e.ClientId);
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
