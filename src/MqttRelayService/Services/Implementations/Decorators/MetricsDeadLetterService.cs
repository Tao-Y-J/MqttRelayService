using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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
        private readonly ILogger<MetricsDeadLetterService>? _logger;

        /// <summary>
        /// 构造死信写入指标拦截装饰器
        /// </summary>
        /// <param name="inner">被装饰的死信服务</param>
        /// <param name="metrics">指标服务</param>
        /// <param name="logger">可选日志记录器，仅用于记录指标本身的记录失败</param>
        public MetricsDeadLetterService(IDeadLetterService inner, IMetricsService metrics, ILogger<MetricsDeadLetterService>? logger = null)
        {
            _inner = inner;
            _metrics = metrics;
            _logger = logger;
        }

        public async Task WriteAsync(DeadLetterRecord record, CancellationToken cancellationToken = default)
        {
            await _inner.WriteAsync(record, cancellationToken);

            // 死信文件已经落盘，指标记录失败绝不能抛出：
            // 否则调用方会把已成功的死信写入当成失败，重新入队并造成重复投递。
            try
            {
                _metrics.RecordDeadLetter(record);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "记录消息 {MessageId} 的死信指标失败", record.MessageId);
            }
        }
    }
}
