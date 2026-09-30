using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MqttRelayService.Models;
using MqttRelayService.Services.Abstractions;
using MqttRelayService.Utilities;

namespace MqttRelayService.Services.Implementations.Decorators
{
    /// <summary>
    /// 消息队列指标拦截装饰器，包裹原生的 IMessageQueue，无侵入地统计入队和队列溢出事件。
    /// 指标服务以延迟方式解析：指标服务自身依赖被装饰的队列，构造期直接注入会形成循环依赖。
    /// </summary>
    public class MetricsMessageQueue : IMessageQueue
    {
        private readonly IMessageQueue _inner;
        private readonly LazyService<IMetricsService> _metrics;

        /// <summary>
        /// 构造消息队列指标拦截装饰器
        /// </summary>
        public MetricsMessageQueue(IMessageQueue inner, LazyService<IMetricsService> metrics)
        {
            _inner = inner;
            _metrics = metrics;
        }

        /// <summary>
        /// 直接注入指标服务实例的构造重载，供单元测试与不依赖容器的场景使用。
        /// </summary>
        public MetricsMessageQueue(IMessageQueue inner, IMetricsService metrics)
            : this(inner, LazyService<IMetricsService>.From(metrics))
        {
        }

        public int Count => _inner.Count;

        public int Capacity => _inner.Capacity;

        public int PeakCount => _inner.PeakCount;

        public async Task<bool> EnqueueAsync(ForwardMessage message, CancellationToken cancellationToken = default)
        {
            var isFirstReceipt = message.Status == MessageProcessStatus.Received;
            if (isFirstReceipt)
            {
                // 首次接收的 Queued 审计必须先于真实入队完成，避免高并发下消费者已写入终态后，
                // 迟到的首次入队指标再把同一 MessageId 重新覆盖回 Queued。
                SafeRecord(() => _metrics.Value.RecordReceived(message, isFirstReceipt: true), message.MessageId);
            }

            var success = await _inner.EnqueueAsync(message, cancellationToken);
            if (success)
            {
                if (!isFirstReceipt)
                {
                    SafeRecord(() => _metrics.Value.RecordReceived(message, isFirstReceipt: false), message.MessageId);
                }
            }
            else
            {
                // 入队失败必须写入终态审计：只累加拒绝计数会让审计表永久残留 Queued 行，
                // 使 Dashboard 的待处理数与总消息数长期虚高。
                SafeRecord(() => _metrics.Value.RecordRejectedMessage(message), message.MessageId);
            }

            return success;
        }

        /// <summary>
        /// 指标记录只允许影响观测结果，绝不能改变被装饰队列的入队结果或向上抛出异常。
        /// </summary>
        private static void SafeRecord(Action recordAction, string messageId)
        {
            try
            {
                recordAction();
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"记录消息 {messageId} 的队列指标失败: {ex}");
            }
        }

        public Task<ForwardMessage?> TryDequeueAsync(CancellationToken cancellationToken = default)
        {
            return _inner.TryDequeueAsync(cancellationToken);
        }

        public IAsyncEnumerable<ForwardMessage> ReadAllAsync(CancellationToken cancellationToken = default)
        {
            return _inner.ReadAllAsync(cancellationToken);
        }
    }
}
