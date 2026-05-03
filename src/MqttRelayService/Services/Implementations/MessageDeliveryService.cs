using Microsoft.Extensions.Options;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;
using MqttRelayService.Utilities;

namespace MqttRelayService.Services.Implementations;

/// <summary>
/// 消息投递服务实现，负责消费队列、执行转发、重试和死信处理
/// </summary>
public class MessageDeliveryService : IMessageDeliveryService
{
    private readonly IMessageQueue _queue;
    private readonly IMessageRouter _router;
    private readonly IMqttBrokerHost _brokerHost;
    private readonly IDeadLetterService _deadLetterService;
    private readonly IRetryPolicyProvider _retryPolicy;
    private readonly ReliabilityOptions _options;
    private readonly ILogger<MessageDeliveryService> _logger;

    private Task? _consumerTask;
    private CancellationTokenSource? _cts;

    public MessageDeliveryService(
        IMessageQueue queue,
        IMessageRouter router,
        IMqttBrokerHost brokerHost,
        IDeadLetterService deadLetterService,
        IRetryPolicyProvider retryPolicy,
        IOptions<ReliabilityOptions> options,
        ILogger<MessageDeliveryService> logger)
    {
        _queue = queue;
        _router = router;
        _brokerHost = brokerHost;
        _deadLetterService = deadLetterService;
        _retryPolicy = retryPolicy;
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>
    /// 启动投递服务，开启后台消费循环
    /// </summary>
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _consumerTask = Task.Run(() => ConsumeLoopAsync(_cts.Token), _cts.Token);

        _logger.LogInformation("消息投递服务已启动");
        return Task.CompletedTask;
    }

    /// <summary>
    /// 停止投递服务
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("消息投递服务正在停止...");

        _cts?.Cancel();

        if (_consumerTask != null)
        {
            try
            {
                var drainTimeout = TimeSpan.FromMilliseconds(_options.ShutdownDrainTimeoutMs);
                using var timeoutCts = new CancellationTokenSource(drainTimeout);

                // 等待消费循环结束或超时
                await Task.WhenAny(_consumerTask, Task.Delay(drainTimeout, cancellationToken));

                if (!_consumerTask.IsCompleted)
                {
                    _logger.LogWarning("投递服务停止超时，剩余未处理消息可能丢失");
                }
                else
                {
                    _logger.LogInformation("投递服务已停止");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "停止投递服务时发生异常");
            }
        }
    }

    /// <summary>
    /// 后台消费循环（使用 ChannelReader.ReadAllAsync 异步挂起等待）
    /// </summary>
    private async Task ConsumeLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in _queue.ReadAllAsync(cancellationToken))
            {
                try
                {
                    // 处理单条消息，异常隔离
                    await ProcessMessageAsync(message, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "处理消息 {MessageId} 时发生未处理异常，继续执行",
                        message.RouteContext.MessageId);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // 正常取消，退出循环
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "消费循环中发生未处理异常");
        }
    }

    /// <summary>
    /// 处理单条消息：路由 → 转发 → 确认或重试
    /// </summary>
    private async Task ProcessMessageAsync(ForwardMessage message, CancellationToken cancellationToken)
    {
        var context = message.RouteContext;

        try
        {
            _logger.LogInformation("开始处理消息 {MessageId}，主题 {Topic}，来源 {ClientId}",
                context.MessageId, context.Topic, context.SourceClientId);

            // 路由阶段
            message.Status = MessageProcessStatus.Routing;
            var targets = await _router.RouteAsync(context, cancellationToken);

            if (targets.Count == 0)
            {
                _logger.LogWarning("消息 {MessageId} 没有找到匹配的目标客户端", context.MessageId);
                message.Status = MessageProcessStatus.Succeeded; // 没有目标也算成功
                return;
            }

            // 转发阶段：向 Topic 单次注入，由 Broker 分发给所有匹配订阅者
            message.Status = MessageProcessStatus.Forwarding;
            var success = await TryForwardAsync(context, cancellationToken);

            if (success)
            {
                message.Status = MessageProcessStatus.Succeeded;
                _logger.LogInformation("消息 {MessageId} 注入成功，匹配目标数 {TargetCount}",
                    context.MessageId, targets.Count);
            }
            else
            {
                // 注入失败，进入重试
                await HandleFailureAsync(message, "消息注入失败", cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "处理消息 {MessageId} 时发生异常", context.MessageId);
            await HandleFailureAsync(message, ex.Message, cancellationToken);
        }
    }

    /// <summary>
    /// 尝试向 Topic 注入消息（由 Broker 分发给匹配订阅者）
    /// </summary>
    private async Task<bool> TryForwardAsync(RouteContext context, CancellationToken cancellationToken)
    {
        try
        {
            using var timeoutCts = new CancellationTokenSource(_options.ForwardTimeoutMs);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

            var success = await _brokerHost.PublishAsync(
                context.Topic,
                context.Payload,
                context.QoS,
                linkedCts.Token);

            if (success)
            {
                _logger.LogDebug("消息 {MessageId} 注入 Topic {Topic} 成功",
                    context.MessageId, context.Topic);
            }
            else
            {
                _logger.LogWarning("消息 {MessageId} 注入 Topic {Topic} 失败",
                    context.MessageId, context.Topic);
            }

            return success;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning("消息 {MessageId} 注入 Topic {Topic} 超时",
                context.MessageId, context.Topic);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "消息 {MessageId} 注入 Topic {Topic} 时发生异常",
                context.MessageId, context.Topic);
            return false;
        }
    }

    /// <summary>
    /// 处理转发失败：重试或进入死信
    /// </summary>
    private async Task HandleFailureAsync(ForwardMessage message, string reason, CancellationToken cancellationToken)
    {
        message.RetryCount++;

        if (message.RetryCount <= _options.MaxRetryCount)
        {
            // 计算重试延迟
            var delay = await _retryPolicy.GetDelayAsync(message.RetryCount, cancellationToken);
            message.NextRetryAt = DateTime.UtcNow.Add(delay);

            _logger.LogWarning("消息 {MessageId} 第 {RetryCount} 次转发失败，{DelayMs}ms 后重试，原因：{Reason}",
                message.RouteContext.MessageId, message.RetryCount, delay.TotalMilliseconds, reason);

            // 等待退避延迟（当前单线程消费者，阻塞是可接受的简化方案）
            await Task.Delay(delay, cancellationToken);

            // 重新入队等待重试
            var enqueued = await _queue.EnqueueAsync(message, cancellationToken);
            if (!enqueued)
            {
                _logger.LogError("消息 {MessageId} 重试入队失败，直接进入死信",
                    message.RouteContext.MessageId);
                await MoveToDeadLetterAsync(message, "重试入队失败：" + reason, cancellationToken);
            }
        }
        else
        {
            _logger.LogError("消息 {MessageId} 超过最大重试次数 {MaxRetryCount}，进入死信",
                message.RouteContext.MessageId, _options.MaxRetryCount);
            await MoveToDeadLetterAsync(message, reason, cancellationToken);
        }
    }

    /// <summary>
    /// 将消息移入死信
    /// </summary>
    private async Task MoveToDeadLetterAsync(ForwardMessage message, string reason, CancellationToken cancellationToken)
    {
        message.Status = MessageProcessStatus.DeadLetter;

        var record = new DeadLetterRecord
        {
            MessageId = message.RouteContext.MessageId,
            Topic = message.RouteContext.Topic,
            SourceClientId = message.RouteContext.SourceClientId,
            PayloadBase64 = MessagePayloadFormatter.ToBase64(message.RouteContext.Payload),
            FirstReceivedAt = message.CreatedAt,
            LastFailedAt = DateTime.UtcNow,
            FailureReason = reason,
            RetryCount = message.RetryCount
        };

        await _deadLetterService.WriteAsync(record, cancellationToken);
    }
}