using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using MqttRelayService.Models;
using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Services.Implementations.Decorators
{
    /// <summary>
    /// MQTT 宿主指标拦截装饰器，包装原生的 IMqttBrokerHost，并为直接 PublishAsync 调用提供回退指标。
    /// </summary>
    public class MetricsMqttBrokerHost : IMqttBrokerHost
    {
        private readonly IMqttBrokerHost _inner;
        private readonly IMetricsService _metrics;
        private readonly ILogger<MetricsMqttBrokerHost>? _logger;

        /// <summary>
        /// 构造 MQTT 宿主指标拦截装饰器。
        /// </summary>
        /// <param name="inner">被装饰的 Broker 宿主</param>
        /// <param name="metrics">指标服务</param>
        /// <param name="logger">可选的日志记录器，仅用于记录指标本身的记录失败</param>
        public MetricsMqttBrokerHost(IMqttBrokerHost inner, IMetricsService metrics, ILogger<MetricsMqttBrokerHost>? logger = null)
        {
            _inner = inner;
            _metrics = metrics;
            _logger = logger;
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

        /// <summary>
        /// 封堵客户端新发布入口，直接透传给被装饰的 Broker 宿主。
        /// </summary>
        public void StopAcceptingClientPublishes()
        {
            _inner.StopAcceptingClientPublishes();
        }

        public async Task<bool> PublishAsync(
            string topic,
            byte[] payload,
            int qos,
            string? sourceClientId = null,
            bool retain = false,
            CancellationToken cancellationToken = default)
        {
            var currentId = MessageContext.CurrentMessageId.Value;
            var firstReceivedAt = MessageContext.CurrentFirstReceivedAt.Value;
            var retryCount = MessageContext.CurrentRetryCount.Value ?? 0;
            var isSubscriberHit = MessageContext.CurrentIsSubscriberHit.Value ?? false;
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

                // 投递链路中的消息由 MessageDeliveryService 负责写入真实处理耗时，避免重复统计。
                if (string.IsNullOrEmpty(currentId))
                {
                    var context = new MqttRelayService.Models.RouteContext
                    {
                        MessageId = Guid.NewGuid().ToString("N"),
                        Topic = topic,
                        Payload = payload,
                        QoS = qos,
                        Retain = retain,
                        SourceClientId = sourceClientId ?? string.Empty,
                        Timestamp = firstReceivedAt ?? DateTime.UtcNow.ToLocalTime()
                    };

                    // 指标记录只做观测，绝不能改变被装饰对象的投递结果。
                    try
                    {
                        _metrics.RecordForwarded(context, success, retryCount, stopwatch.Elapsed.TotalMilliseconds, isSubscriberHit);
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "记录直接注入消息 {MessageId} 的转发指标失败", context.MessageId);
                    }
                }
            }
        }
    }
}
