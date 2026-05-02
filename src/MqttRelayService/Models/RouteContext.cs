namespace MqttRelayService.Models;

/// <summary>
/// 路由上下文，包含消息的基本信息
/// </summary>
public class RouteContext
{
    /// <summary>
    /// 消息唯一标识
    /// </summary>
    public string MessageId { get; set; } = string.Empty;

    /// <summary>
    /// 消息主题
    /// </summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>
    /// 消息负载
    /// </summary>
    public byte[] Payload { get; set; } = Array.Empty<byte>();

    /// <summary>
    /// 服务质量等级
    /// </summary>
    public int QoS { get; set; }

    /// <summary>
    /// 来源客户端标识
    /// </summary>
    public string SourceClientId { get; set; } = string.Empty;

    /// <summary>
    /// 消息时间戳
    /// </summary>
    public DateTime Timestamp { get; set; }
}