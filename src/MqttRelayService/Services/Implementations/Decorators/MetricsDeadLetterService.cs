using System.Threading;
using System.Threading.Tasks;
using MqttRelayService.Models;
using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Services.Implementations.Decorators
{
    /// <summary>
    /// 死信写入指标拦截装饰器，包裹原生的 IDeadLetterService，无侵入地统计进入 DLQ 的事件。
    /// </summary>
    public class MetricsDeadLetterService : IDeadLetterService
    {
        private readonly IDeadLetterService _inner;
        private readonly IMetricsService _metrics;

        /// <summary>
        /// 构造死信写入指标拦截装饰器
        /// </summary>
        public MetricsDeadLetterService(IDeadLetterService inner, IMetricsService metrics)
        {
            _inner = inner;
            _metrics = metrics;
        }

        public async Task WriteAsync(DeadLetterRecord record, CancellationToken cancellationToken = default)
        {
            await _inner.WriteAsync(record, cancellationToken);
            _metrics.RecordDeadLetter(record);
        }
    }
}
