using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Workers;

/// <summary>
/// Broker 后台服务，负责 MQTT Broker 的启停和异常自动重启
/// </summary>
public class BrokerWorker : BackgroundService
{
    private readonly IMqttBrokerHost _brokerHost;
    private readonly IHostApplicationLifetime _applicationLifetime;
    private readonly ILogger<BrokerWorker> _logger;

    public BrokerWorker(
        IMqttBrokerHost brokerHost,
        IHostApplicationLifetime applicationLifetime,
        ILogger<BrokerWorker> logger)
    {
        _brokerHost = brokerHost;
        _applicationLifetime = applicationLifetime;
        _logger = logger;
    }

    /// <summary>
    /// 执行后台服务主循环
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            // 启动 Broker
            await _brokerHost.StartAsync(stoppingToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Broker 启动失败");
            // 启动失败是致命错误，主动请求 Host 停止，避免 Windows Service 假存活。
            _applicationLifetime.StopApplication();
            return;
        }

        // 监控循环：检查 Broker 状态，异常停止时自动重启
        var restartDelay = TimeSpan.FromSeconds(5);
        var consecutiveFailures = 0;
        const int maxConsecutiveFailures = 10;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

                if (!_brokerHost.IsRunning)
                {
                    consecutiveFailures++;

                    if (consecutiveFailures > maxConsecutiveFailures)
                    {
                        _logger.LogError("Broker 连续重启失败 {Count} 次，停止重试", consecutiveFailures);
                        break;
                    }

                    _logger.LogWarning("Broker 异常停止，第 {Count} 次尝试重启...", consecutiveFailures);

                    try
                    {
                        await _brokerHost.StartAsync(stoppingToken);
                        consecutiveFailures = 0;
                        _logger.LogInformation("Broker 重启成功");
                    }
                    catch (Exception restartEx)
                    {
                        _logger.LogError(restartEx, "Broker 重启失败，{Delay}s 后再次尝试", restartDelay.TotalSeconds);
                        await Task.Delay(restartDelay, stoppingToken);
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Broker 监控循环中发生异常，继续执行");
            }
        }
    }

    /// <summary>
    /// 服务停止时优雅关闭 Broker
    /// </summary>
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("BrokerWorker 正在停止...");
        await base.StopAsync(cancellationToken);
        await _brokerHost.StopAsync(cancellationToken);
        _logger.LogInformation("BrokerWorker 已停止");
    }
}
