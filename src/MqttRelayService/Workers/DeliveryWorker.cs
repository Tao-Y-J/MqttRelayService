using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Workers
{
    /// <summary>
    /// 投递后台服务，负责管理 IMessageDeliveryService 的生命周期
    /// </summary>
    public class DeliveryWorker : BackgroundService
    {
        private readonly IMessageDeliveryService _deliveryService;
        private readonly IHostApplicationLifetime _applicationLifetime;
        private readonly ILogger<DeliveryWorker> _logger;

        public DeliveryWorker(
            IMessageDeliveryService deliveryService,
            IHostApplicationLifetime applicationLifetime,
            ILogger<DeliveryWorker> logger)
        {
            _deliveryService = deliveryService;
            _applicationLifetime = applicationLifetime;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                _logger.LogInformation("投递后台服务正在启动...");
                await _deliveryService.StartAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                // 投递链路启动失败是致命错误：Host 配置了 BackgroundServiceExceptionBehavior.Ignore，
                // 若不主动请求停止，进程会带着「Broker 继续接收消息、消费者已死」的状态假存活。
                _logger.LogError(ex, "投递后台服务启动失败，请求 Host 停止");
                _applicationLifetime.StopApplication();
                return;
            }

            try
            {
                // 保持运行直到停止信号
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                await using (stoppingToken.Register(() => tcs.TrySetResult()))
                {
                    await tcs.Task;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "投递后台服务运行期间发生未处理异常");
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("投递后台服务正在停止...");
            await _deliveryService.StopAsync(cancellationToken);
            await base.StopAsync(cancellationToken);
        }
    }
}
