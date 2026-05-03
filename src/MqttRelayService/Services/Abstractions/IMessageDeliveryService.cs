namespace MqttRelayService.Services.Abstractions;

/// <summary>
/// 消息投递服务接口，负责消费队列并执行转发
/// </summary>
public interface IMessageDeliveryService
{
    /// <summary>
    /// 启动投递服务
    /// </summary>
    Task StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// 停止投递服务
    /// </summary>
    Task StopAsync(CancellationToken cancellationToken = default);
}