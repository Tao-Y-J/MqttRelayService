using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MqttRelayService.Models;
using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Services.Implementations.Decorators
{
    /// <summary>
    /// 消息队列指标拦截装饰器，包裹原生的 IMessageQueue，无侵入地统计入队和队列溢出事件。
    /// </summary>
    public class MetricsMessageQueue : IMessageQueue
    {
        private readonly IMessageQueue _inner;
        private readonly IMetricsService _metrics;

        /// <summary>
        /// 构造消息队列指标拦截装饰器
        /// </summary>
        public MetricsMessageQueue(IMessageQueue inner, IMetricsService metrics)
        {
            _inner = inner;
            _metrics = metrics;
        }

        public int Count => _inner.Count;

        public int Capacity => _inner.Capacity;

        public int PeakCount => _inner.PeakCount;

        public async Task<bool> EnqueueAsync(ForwardMessage message, CancellationToken cancellationToken = default)
        {
            var isFirstReceipt = message.Status == MessageProcessStatus.Received;
            var success = await _inner.EnqueueAsync(message, cancellationToken);
            if (success)
            {
                _metrics.RecordReceived(message, isFirstReceipt);
            }
            else
            {
                _metrics.RecordRejected();
            }
            return success;
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
