using MqttRelayService.Models;

namespace MqttRelayService.Services.Abstractions
{
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

        /// <summary>
        /// 以同步阻塞方式取出消息，供专用消费线程使用。
        /// 与 <see cref="ReadAllAsync"/> 的关键区别：等待发生在调用方自己的线程上，不产生线程池续体，
        /// 因此取件唤醒延迟不受线程池繁忙程度影响（线程池被审计查询等阻塞型工作占满时，
        /// 基于续体的唤醒会排队数秒，实测可达 3.5 秒）。
        /// 只允许在专用线程（如 LongRunning 任务线程）上调用，禁止在 MQTT 事件回调或线程池线程上调用。
        /// </summary>
        /// <param name="message">取出的消息；返回 false 时为 null</param>
        /// <param name="cancellationToken">取消令牌，取消时抛出 OperationCanceledException</param>
        /// <returns>取到消息返回 true；队列已关闭且已取空返回 false</returns>
        bool TryDequeueBlocking(out ForwardMessage? message, CancellationToken cancellationToken = default);
    }
}