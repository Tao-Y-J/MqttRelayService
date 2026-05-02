namespace MqttRelayService.Options;

/// <summary>
/// 消息路由配置选项
/// </summary>
public class RoutingOptions
{
    /// <summary>
    /// 是否将消息回发给发送方自身
    /// </summary>
    public bool EchoToSender { get; set; } = false;
}