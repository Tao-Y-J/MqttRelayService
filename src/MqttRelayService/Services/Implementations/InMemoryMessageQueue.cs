using System.Threading.Channels;
using Microsoft.Extensions.Options;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Services.Implementations;

/// <summary>
/// 基于 System.Threading.Channels 的内存消息队列实现
/// </summary>
public class InMemoryMessageQueue : IMessageQueue
{
    private readonly Channel<ForwardMessage> _channel;
    private readonly ReliabilityOptions _options;
    private readonly ILogger<InMemoryMessageQueue> _logger;
    private int _peakCount;

    public InMemoryMessageQueue(IOptions<ReliabilityOptions> options, ILogger<InMemoryMessageQueue> logger)
    {
        _options = options.Value;
        _logger = logger;

        // 统一使用 Wait 模式，满载丢弃通过容量预检实现
        // 避免 BoundedChannelFullMode.DropWrite 导致 TryWrite 返回 true 但消息被静默丢弃
        var channelOptions = new BoundedChannelOptions(_options.QueueCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait
        };

        _channel = Channel.CreateBounded<ForwardMessage>(channelOptions);
        _logger.LogInformation("消息队列已初始化，容量 {Capacity}，满载策略 {FullMode}",
            _options.QueueCapacity, channelOptions.FullMode);
    }

    /// <summary>
    /// 当前队列长度
    /// </summary>
    public int Count => _channel.Reader.Count;

    /// <summary>
    /// 队列容量上限
    /// </summary>
    public int Capacity => _options.QueueCapacity;

    /// <summary>
    /// 历史峰值长度
    /// </summary>
    public int PeakCount => _peakCount;

    /// <summary>
    /// 将消息入队
    /// </summary>
    public async Task<bool> EnqueueAsync(ForwardMessage message, CancellationToken cancellationToken = default)
    {
        try
        {
            message.Status = MessageProcessStatus.Queued;

            if (_options.DropWhenQueueFull)
            {
                // Wait 模式下使用 TryWrite 同步判断：满载立即丢弃
                // 不使用 BoundedChannelFullMode.DropWrite，因为它会静默吞消息并返回 true
                if (!_channel.Writer.TryWrite(message))
                {
                    _logger.LogWarning("队列已满，消息 {MessageId} 被丢弃", message.MessageId);
                    return false;
                }
            }
            else
            {
                // 等待写入，带超时
                using var timeoutCts = new CancellationTokenSource(_options.EnqueueTimeoutMs);
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

                await _channel.Writer.WriteAsync(message, linkedCts.Token);
            }

            // 更新峰值
            var currentCount = Count;
            if (currentCount > _peakCount)
            {
                Interlocked.Exchange(ref _peakCount, currentCount);
            }

            _logger.LogDebug("消息 {MessageId} 已入队，当前队列长度 {Count}", message.MessageId, currentCount);
            return true;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // 入队超时
            _logger.LogWarning("消息 {MessageId} 入队超时（{TimeoutMs}ms），队列可能已满",
                message.MessageId, _options.EnqueueTimeoutMs);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "消息 {MessageId} 入队时发生异常", message.MessageId);
            return false;
        }
    }

    /// <summary>
    /// 尝试从队列取出消息（非阻塞）
    /// </summary>
    public Task<ForwardMessage?> TryDequeueAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            if (_channel.Reader.TryRead(out var message))
            {
                return Task.FromResult<ForwardMessage?>(message);
            }

            return Task.FromResult<ForwardMessage?>(null);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "从队列取出消息时发生异常");
            return Task.FromResult<ForwardMessage?>(null);
        }
    }

    /// <summary>
    /// 异步读取队列中所有消息（挂起等待，非轮询）
    /// </summary>
    public IAsyncEnumerable<ForwardMessage> ReadAllAsync(CancellationToken cancellationToken = default)
    {
        return _channel.Reader.ReadAllAsync(cancellationToken);
    }
}