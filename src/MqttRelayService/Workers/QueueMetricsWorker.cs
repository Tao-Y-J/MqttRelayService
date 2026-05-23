using System.Text.Json;
using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Workers
{
    /// <summary>
    /// 队列指标后台服务，周期性输出队列长度和峰值快照
    /// </summary>
    public class QueueMetricsWorker : BackgroundService
    {
        private readonly IMessageQueue _queue;
        private readonly ILogger<QueueMetricsWorker> _logger;

        private int _lastCount = -1;
        private int _lastPeak = -1;

        public QueueMetricsWorker(IMessageQueue queue, ILogger<QueueMetricsWorker> logger)
        {
            _queue = queue;
            _logger = logger;
        }

        /// <summary>
        /// 执行后台指标采集循环
        /// </summary>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var metricsDir = Path.Combine(AppContext.BaseDirectory, "data", "metrics");
            var metricsFile = Path.Combine(metricsDir, "queue-metrics.json");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

                    var currentCount = _queue.Count;
                    var currentPeak = _queue.PeakCount;

                    // 仅在变化时输出
                    if (currentCount == _lastCount && currentPeak == _lastPeak)
                    {
                        continue;
                    }

                    _lastCount = currentCount;
                    _lastPeak = currentPeak;

                    var metrics = new
                    {
                        Timestamp = DateTime.Now.ToString("O"),
                        QueueLength = currentCount,
                        PeakLength = currentPeak,
                        Capacity = _queue.Capacity
                    };

                    // 写入指标文件
                    try
                    {
                        if (!Directory.Exists(metricsDir))
                        {
                            Directory.CreateDirectory(metricsDir);
                        }

                        var json = JsonSerializer.Serialize(metrics, new JsonSerializerOptions
                        {
                            WriteIndented = true
                        });

                        await File.WriteAllTextAsync(metricsFile, json, stoppingToken);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "写入队列指标文件失败");
                    }

                    // 输出日志
                    if (currentCount > _queue.Capacity * 0.8)
                    {
                        _logger.LogWarning("队列积压告警：当前 {Current}/{Capacity}，峰值 {Peak}",
                            currentCount, _queue.Capacity, currentPeak);
                    }
                    else
                    {
                        _logger.LogDebug("队列指标：当前 {Current}/{Capacity}，峰值 {Peak}",
                            currentCount, _queue.Capacity, currentPeak);
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "队列指标采集循环中发生异常，继续执行");
                }
            }
        }
    }
}