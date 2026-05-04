using MqttRelayService.Models;

namespace MqttRelayService.Services.Abstractions;

/// <summary>
/// 消息队列接口，解耦接入层与处理层
/// </summary>
public interface IMessageQueue
{
    /// <summary>
    /// 当前队列长度
    /// </summary>
    int Count { get; }

    /// <summary>
    /// 队列容量上限
    /// </summary>
    int Capacity { get; }

    /// <summary>
    /// 历史峰值长度
    /// </summary>
    int PeakCount { get; }

    /// <summary>
    /// 将消息入队
    /// </summary>
    Task<bool> EnqueueAsync(ForwardMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// 尝试从队列取出消息
    /// </summary>
    Task<ForwardMessage?> TryDequeueAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// 异步读取队列中所有消息（挂起等待，非轮询）
    /// </summary>
    IAsyncEnumerable<ForwardMessage> ReadAllAsync(CancellationToken cancellationToken = default);
}