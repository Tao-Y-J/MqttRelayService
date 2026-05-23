using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using MqttRelayService.Models;
using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Services.Implementations.Decorators
{
    /// <summary>
    /// MQTT 宿主指标拦截装饰器，包裹原生的 IMqttBrokerHost，无侵入地统计转发成功率与注入耗时。
    /// </summary>
    public class MetricsMqttBrokerHost : IMqttBrokerHost
    {
        private readonly IMqttBrokerHost _inner;
        private readonly IMetricsService _metrics;

        /// <summary>
        /// 构造MQTT宿主指标拦截装饰器
        /// </summary>
        public MetricsMqttBrokerHost(IMqttBrokerHost inner, IMetricsService metrics)
        {
            _inner = inner;
            _metrics = metrics;
        }

        public bool IsRunning => _inner.IsRunning;

        public Task StartAsync(CancellationToken cancellationToken = default)
        {
            return _inner.StartAsync(cancellationToken);
        }

        public Task StopAsync(CancellationToken cancellationToken = default)
        {
            return _inner.StopAsync(cancellationToken);
        }

        public async Task<bool> PublishAsync(
            string topic,
            byte[] payload,
            int qos,
            string? sourceClientId = null,
            bool retain = false,
            CancellationToken cancellationToken = default)
        {
            // 在 await 发生之前，立即读取当前上下文的消息 ID、首次接收时间、已重试次数与是否命中订阅者，防止因 await 后线程切换导致丢失
            var currentId = MessageContext.CurrentMessageId.Value;
            var firstReceivedAt = MessageContext.CurrentFirstReceivedAt.Value;
            var retryCount = MessageContext.CurrentRetryCount.Value ?? 0;
            var isSubscriberHit = MessageContext.CurrentIsSubscriberHit.Value ?? false;

            // 通过获取当前正在运行的堆栈与转发属性，度量单次注入的真实延迟
            var stopwatch = Stopwatch.StartNew();
            var success = false;

            try
            {
                success = await _inner.PublishAsync(topic, payload, qos, sourceClientId, retain, cancellationToken);
                return success;
            }
            finally
            {
                stopwatch.Stop();

                Console.WriteLine($"[DEBUG-MSG] MetricsMqttBrokerHost.PublishAsync: CurrentMessageId = '{currentId}', Topic = '{topic}', SourceClientId = '{sourceClientId}', FirstReceivedAt = '{firstReceivedAt}', RetryCount = {retryCount}, IsSubscriberHit = {isSubscriberHit}");

                // 构建路由上下文传递给指标器，以便记录详细的审计日志
                var context = new MqttRelayService.Models.RouteContext
                {
                    MessageId = currentId ?? Guid.NewGuid().ToString("N"),
                    Topic = topic,
                    Payload = payload,
                    QoS = qos,
                    Retain = retain,
                    SourceClientId = sourceClientId ?? string.Empty,
                    Timestamp = firstReceivedAt ?? DateTime.UtcNow.ToLocalTime()
                };

                // 度量延迟：若存在首次接收时间，则计算端到端总耗时；否则使用注入调用的物理耗时
                double elapsedMs = firstReceivedAt.HasValue
                    ? (DateTime.UtcNow.ToLocalTime() - firstReceivedAt.Value).TotalMilliseconds
                    : stopwatch.Elapsed.TotalMilliseconds;

                // 记录转发成功或失败的性能指标
                _metrics.RecordForwarded(context, success, retryCount, elapsedMs, isSubscriberHit);
            }
        }
    }
}
