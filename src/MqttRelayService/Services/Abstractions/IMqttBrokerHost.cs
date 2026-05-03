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
    /// <param name="topic">目标主题</param>
    /// <param name="payload">消息负载</param>
    /// <param name="qos">QoS 等级</param>
    /// <param name="sourceClientId">源客户端 ID（用于 EchoToSender=false 时阻止回发）</param>
    /// <param name="cancellationToken">取消令牌</param>
    Task<bool> PublishAsync(string topic, byte[] payload, int qos, string? sourceClientId = null, CancellationToken cancellationToken = default);
}