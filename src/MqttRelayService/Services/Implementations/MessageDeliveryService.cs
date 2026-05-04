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
    private const int ConsumerShutdownWaitTimeoutMs = 2000;
    private const int RetrySettlementWaitTimeoutMs = 2000;

    private readonly IMessageQueue _queue;
    private readonly IMessageRouter _router;
    private readonly IMqttBrokerHost _brokerHost;
    private readonly IDeadLetterService _deadLetterService;
    private readonly IRetryPolicyProvider _retryPolicy;
    private readonly ReliabilityOptions _options;
    private readonly ILogger<MessageDeliveryService> _logger;

    private readonly object _lifecycleLock = new();
    private readonly List<Task> _consumerTasks = new();
    private readonly object _retryTasksLock = new();
    private readonly HashSet<Task> _retryTasks = new();
    private volatile bool _isStopping;
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
        CancellationTokenSource runCts;
        var handlerCount = 0;
        var shouldLogDrainTimeoutWarning = false;

        lock (_lifecycleLock)
        {
            _consumerTasks.RemoveAll(static task => task.IsCompleted);

            if (_cts != null)
            {
                _logger.LogWarning("消息投递服务已处于启动状态，忽略重复启动请求");
                return Task.CompletedTask;
            }

            if (_consumerTasks.Count > 0)
            {
                _logger.LogError("消息投递服务上一次停止后仍有 {ConsumerCount} 个消费者未退出，拒绝重新启动",
                    _consumerTasks.Count);
                return Task.CompletedTask;
            }

            _isStopping = false;
            shouldLogDrainTimeoutWarning = ShouldLogDrainTimeoutConfigurationWarning();
            runCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _cts = runCts;
            handlerCount = Math.Max(1, _options.MaxConcurrentHandlers);

            for (int i = 0; i < handlerCount; i++)
            {
                _consumerTasks.Add(Task.Run(() => ConsumeLoopAsync(runCts.Token), runCts.Token));
            }
        }

        if (shouldLogDrainTimeoutWarning)
        {
            LogDrainTimeoutConfigurationWarning();
        }

        _logger.LogInformation("消息投递服务已启动，消费者数量 {HandlerCount}", handlerCount);
        return Task.CompletedTask;
    }

    /// <summary>
    /// 停止投递服务，先取消消费循环，等待重试调度收敛，再在超时内尽量排空队列中剩余消息
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("消息投递服务正在停止……");
        CancellationTokenSource? ctsToCancel;
        Task[] consumerSnapshot;

        lock (_lifecycleLock)
        {
            _consumerTasks.RemoveAll(static task => task.IsCompleted);

            if (_cts == null && _consumerTasks.Count == 0)
            {
                _isStopping = false;
                return;
            }

            _isStopping = true;
            ctsToCancel = _cts;
            consumerSnapshot = _consumerTasks.ToArray();
        }

        try
        {
            // 阶段 1：取消消费循环的 ReadAllAsync，停止接收新消息
            ctsToCancel?.Cancel();

            if (consumerSnapshot.Length > 0)
            {
                // 等待所有消费者退出
                var allConsumersTask = Task.WhenAll(consumerSnapshot);
                await Task.WhenAny(allConsumersTask, Task.Delay(ConsumerShutdownWaitTimeoutMs, cancellationToken));
            }

            // 阶段 1.5：等待所有后台重试调度任务收敛
            // 取消信号下，这些任务会快速结束（进入"保留或死信"分支），无需等待真实的退避延迟
            await WaitForPendingRetriesInternalAsync(TimeSpan.FromMilliseconds(RetrySettlementWaitTimeoutMs), cancellationToken);

            // 阶段 2：排空队列中剩余的消息
            var drainTimeout = TimeSpan.FromMilliseconds(_options.ShutdownDrainTimeoutMs);
            using var drainCts = new CancellationTokenSource(drainTimeout);

            var drainedCount = 0;
            while (!drainCts.Token.IsCancellationRequested)
            {
                ForwardMessage? message;
                try
                {
                    message = await _queue.TryDequeueAsync(drainCts.Token);
                }
                catch (OperationCanceledException) when (drainCts.Token.IsCancellationRequested)
                {
                    break; // drain 超时
                }

                if (message == null)
                {
                    break; // 队列为空，排空完成
                }

                try
                {
                    await ProcessMessageAsync(message, drainCts.Token);
                    drainedCount++;
                }
                catch (OperationCanceledException) when (drainCts.Token.IsCancellationRequested)
                {
                    // drain 超时取消，停止排空
                    _logger.LogWarning("排空阶段因超时取消，已排空 {DrainedCount} 条消息", drainedCount);
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "排空阶段处理消息 {MessageId} 时发生异常",
                        message.RouteContext.MessageId);
                }
            }

            var remainingCount = _queue.Count;
            if (remainingCount > 0)
            {
                _logger.LogWarning("投递服务停止，已排空 {DrainedCount} 条消息，剩余 {RemainingCount} 条未处理",
                    drainedCount, remainingCount);
            }
            else
            {
                _logger.LogInformation("投递服务已停止，排空 {DrainedCount} 条消息，剩余 0 条", drainedCount);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "停止投递服务时发生异常");
        }
        finally
        {
            int remainingConsumerCount;
            lock (_lifecycleLock)
            {
                remainingConsumerCount = ResetLifecycleState();
            }

            if (remainingConsumerCount > 0)
            {
                _logger.LogError("投递服务停止时仍有 {ConsumerCount} 个消费者未退出，已保留引用并拒绝后续重启，直到这些消费者自行结束",
                    remainingConsumerCount);
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
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    await PreserveInFlightMessageAsync(message);
                    break;
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
    /// 服务停止取消消费时，尽力保留已取出但尚未完成的在途消息。
    /// 优先重新入队，让 StopAsync 的排空阶段继续处理；回队失败则记录死信。
    /// </summary>
    private async Task PreserveInFlightMessageAsync(ForwardMessage message)
    {
        try
        {
            var preserved = await _queue.EnqueueAsync(message);
            if (preserved)
            {
                _logger.LogInformation("服务停止时已保留在途消息 {MessageId}，等待排空阶段继续处理",
                    message.RouteContext.MessageId);
                return;
            }

            _logger.LogError("服务停止时无法重新入队在途消息 {MessageId}，转入死信",
                message.RouteContext.MessageId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "服务停止时重新入队在途消息 {MessageId} 失败，转入死信",
                message.RouteContext.MessageId);
        }

        await MoveToDeadLetterAsync(message, "服务停止时无法保留在途消息", CancellationToken.None);
    }

    /// <summary>
    /// 处理单条消息：路由 → 转发 → 确认或重试
    /// </summary>
    private async Task ProcessMessageAsync(ForwardMessage message, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

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
                // 注入失败，进入重试调度
                await HandleFailureAsync(message, "消息注入失败", cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // 正常取消，向上传播，不视为业务失败
            throw;
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
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            using var timeoutCts = new CancellationTokenSource(_options.ForwardTimeoutMs);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

            var success = await _brokerHost.PublishAsync(
                context.Topic,
                context.Payload,
                context.QoS,
                context.SourceClientId,
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
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // 主机取消信号，重新抛出，不触发重试
            throw;
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
    /// 处理转发失败：超过重试次数则进入死信，否则非阻塞地调度延迟重新入队，
    /// 让消费者立即返回处理下一条消息（不会被退避延迟阻塞）。
    /// </summary>
    private async Task HandleFailureAsync(ForwardMessage message, string reason, CancellationToken cancellationToken)
    {
        // 取消时不增加业务失败语义，不入队，不写死信
        if (cancellationToken.IsCancellationRequested)
        {
            _logger.LogInformation("消息 {MessageId} 因取消而停止重试处理", message.RouteContext.MessageId);
            cancellationToken.ThrowIfCancellationRequested();
        }

        var nextRetryCount = message.RetryCount + 1;

        if (nextRetryCount <= _options.MaxRetryCount)
        {
            // 计算重试延迟
            var delay = await _retryPolicy.GetDelayAsync(nextRetryCount, cancellationToken);
            message.NextRetryAt = DateTime.UtcNow.Add(delay);

            _logger.LogWarning("消息 {MessageId} 第 {RetryCount} 次转发失败，{DelayMs}ms 后重试，原因：{Reason}",
                message.RouteContext.MessageId, nextRetryCount, delay.TotalMilliseconds, reason);

            if (_isStopping)
            {
                // 停机 drain 阶段需要等待当前消息的重试重新入队完成，
                // 避免 drain 误把“已安排延迟重试”当成“已处理完成”而提前退出。
                await DelayAndRequeueDuringStopAsync(message, nextRetryCount, delay, reason, cancellationToken);
            }
            else
            {
                // 运行期保持非阻塞调度：消费者立即返回处理下一条消息；
                // 实际的退避延迟和重新入队在后台任务中执行。
                ScheduleRetryEnqueue(message, nextRetryCount, delay, reason, cancellationToken);
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
    /// 后台延迟+入队任务的调度，非阻塞地完成退避重试，
    /// 取消时尝试将消息保留到队列（让 drain 阶段处理），失败则转入死信。
    /// </summary>
    private void ScheduleRetryEnqueue(
        ForwardMessage message,
        int retryCountAfterDelay,
        TimeSpan delay,
        string reason,
        CancellationToken cancellationToken)
    {
        var task = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(delay, cancellationToken);

                // 延迟正常完成后才正式增加重试计数，保证取消场景下计数不被错误推进
                message.RetryCount = retryCountAfterDelay;

                // 延迟已经完成后，即使服务开始停机，也要尝试把消息保留回队列给 drain 阶段处理，
                // 不能再把已取消的调用方 token 传给 EnqueueAsync 导致误入死信。
                var enqueued = await _queue.EnqueueAsync(message, CancellationToken.None);
                if (!enqueued)
                {
                    _logger.LogError("消息 {MessageId} 重试入队失败，直接进入死信",
                        message.RouteContext.MessageId);
                    await MoveToDeadLetterAsync(message, "重试入队失败：" + reason, CancellationToken.None);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // 服务停止取消重试调度；尝试将消息保留到队列让 drain 处理，失败则转入死信
                _logger.LogInformation("消息 {MessageId} 重试调度因服务停止而中止，尝试保留",
                    message.RouteContext.MessageId);
                await PreserveRetryMessageAsync(message, "服务停止时取消重试调度：" + reason);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "消息 {MessageId} 重试调度任务发生异常，转入死信",
                    message.RouteContext.MessageId);
                try
                {
                    await MoveToDeadLetterAsync(
                        message,
                        "重试调度异常：" + reason,
                        CancellationToken.None);
                }
                catch (Exception writeEx)
                {
                    _logger.LogError(writeEx, "消息 {MessageId} 写入死信失败", message.RouteContext.MessageId);
                }
            }
        });

        TrackRetryTask(task);
    }

    /// <summary>
    /// 停机 drain 阶段的重试：同步等待当前退避时间，再尝试重新入队，
    /// 保证 drain 不会在消息重新出现之前提前结束。
    /// </summary>
    private async Task DelayAndRequeueDuringStopAsync(
        ForwardMessage message,
        int retryCountAfterDelay,
        TimeSpan delay,
        string reason,
        CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, cancellationToken);

            message.RetryCount = retryCountAfterDelay;

            var enqueued = await _queue.EnqueueAsync(message, CancellationToken.None);
            if (!enqueued)
            {
                _logger.LogError("消息 {MessageId} 在停止排空阶段重试入队失败，直接进入死信",
                    message.RouteContext.MessageId);
                await MoveToDeadLetterAsync(message, "停止排空阶段重试入队失败：" + reason, CancellationToken.None);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogInformation("消息 {MessageId} 在停止排空阶段取消重试等待，尝试保留",
                message.RouteContext.MessageId);
            await PreserveRetryMessageAsync(message, "停止排空阶段取消重试等待：" + reason);
        }
    }

    /// <summary>
    /// 停止过程中尽量把待重试消息保留回队列，失败再写入死信。
    /// </summary>
    private async Task PreserveRetryMessageAsync(ForwardMessage message, string reason)
    {
        try
        {
            var preserved = await _queue.EnqueueAsync(message, CancellationToken.None);
            if (preserved)
            {
                return;
            }

            await MoveToDeadLetterAsync(message, reason, CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "消息 {MessageId} 在停止过程中保留失败，转入死信",
                message.RouteContext.MessageId);
            try
            {
                await MoveToDeadLetterAsync(message, "服务停止时保留消息异常：" + reason, CancellationToken.None);
            }
            catch (Exception writeEx)
            {
                _logger.LogError(writeEx, "消息 {MessageId} 写入死信也失败", message.RouteContext.MessageId);
            }
        }
    }

    /// <summary>
    /// 跟踪后台重试调度任务，任务完成后自动从集合中移除，避免长期运行场景下的引用堆积。
    /// </summary>
    private void TrackRetryTask(Task task)
    {
        lock (_retryTasksLock)
        {
            _retryTasks.Add(task);
        }

        // 任务完成时清理引用（在线程池上调度，避免与外层 lock 形成同步死锁）
        _ = task.ContinueWith(t =>
        {
            lock (_retryTasksLock)
            {
                _retryTasks.Remove(t);
            }
        }, TaskScheduler.Default);
    }

    /// <summary>
    /// 等待当前所有后台重试调度任务收敛（测试辅助和 StopAsync 复用）。
    /// </summary>
    internal Task WaitForPendingRetriesAsync()
    {
        Task[] snapshot;
        lock (_retryTasksLock)
        {
            snapshot = _retryTasks.ToArray();
        }
        return snapshot.Length == 0 ? Task.CompletedTask : Task.WhenAll(snapshot);
    }

    /// <summary>
    /// 等待重试调度收敛，带超时上限。
    /// </summary>
    private async Task WaitForPendingRetriesInternalAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        Task[] snapshot;
        lock (_retryTasksLock)
        {
            snapshot = _retryTasks.ToArray();
        }

        if (snapshot.Length == 0)
        {
            return;
        }

        try
        {
            // 这里只等待当前快照中的任务；若在取快照后又有任务加入，
            // 会由停机取消信号和后续 drain 阶段的保留逻辑继续收敛。
            await Task.WhenAny(Task.WhenAll(snapshot), Task.Delay(timeout, cancellationToken));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // 调用方取消，直接返回让 StopAsync 继续后续阶段
        }
    }

    /// <summary>
    /// 启动时检查停机排空配置是否覆盖最大退避时间，避免停机时第一条失败消息还未等到下一次注入尝试就触发排空超时。
    /// </summary>
    private bool ShouldLogDrainTimeoutConfigurationWarning()
    {
        return _options.ShutdownDrainTimeoutMs < _options.RetryMaxDelayMs;
    }

    /// <summary>
    /// 启动时记录停机排空配置提示。
    /// </summary>
    private void LogDrainTimeoutConfigurationWarning()
    {
        _logger.LogWarning(
            "可靠性配置提示：ShutdownDrainTimeoutMs({ShutdownDrainTimeoutMs}ms) 小于 RetryMaxDelayMs({RetryMaxDelayMs}ms)。停机排空阶段若遇到失败消息，超时可能先于单次最大退避结束，消息会转入保留回队列或死信收敛，而不会完成当次下一次注入尝试。",
            _options.ShutdownDrainTimeoutMs,
            _options.RetryMaxDelayMs);
    }

    /// <summary>
    /// 停止完成后清理生命周期状态，确保同一实例可以再次启动。
    /// </summary>
    private int ResetLifecycleState()
    {
        _cts?.Dispose();
        _cts = null;
        _consumerTasks.RemoveAll(static task => task.IsCompleted);

        lock (_retryTasksLock)
        {
            _retryTasks.RemoveWhere(static task => task.IsCompleted);
        }

        _isStopping = false;
        return _consumerTasks.Count;
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
