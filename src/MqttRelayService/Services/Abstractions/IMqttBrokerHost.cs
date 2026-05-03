namespace MqttRelayService.Services.Abstractions;

/// <summary>
/// MQTT Broker 宿主接口，负责 MQTT Server 的启停管理
/// </summary>
public interface IMqttBrokerHost
{
    /// <summary>
    /// 当前是否正在运行
    /// </summary>
    bool IsRunning { get; }

    /// <summary>
    /// 启动 MQTT Server
    /// </summary>
    Task StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// 停止 MQTT Server
    /// </summary>
    Task StopAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// 向指定 Topic 注入应用消息，由 Broker 自动分发给匹配的订阅者
    /// </summary>
    Task<bool> PublishAsync(string topic, byte[] payload, int qos, CancellationToken cancellationToken = default);
}