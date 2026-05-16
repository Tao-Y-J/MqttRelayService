using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Workers
{
    /// <summary>
    /// 投递后台服务，负责管理 IMessageDeliveryService 的生命周期
    /// </summary>
    public class DeliveryWorker : BackgroundService
    {
        private readonly IMessageDeliveryService _deliveryService;
        private readonly ILogger<DeliveryWorker> _logger;

        public DeliveryWorker(IMessageDeliveryService deliveryService, ILogger<DeliveryWorker> logger)
        {
            _deliveryService = deliveryService;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("投递后台服务正在启动...");
            await _deliveryService.StartAsync(stoppingToken);

            // 保持运行直到停止信号
            var tcs = new TaskCompletionSource();
            await using (stoppingToken.Register(() => tcs.TrySetResult()))
            {
                await tcs.Task;
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
