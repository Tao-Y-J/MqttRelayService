namespace MqttRelayService.Models;

/// <summary>
/// 内部转发消息
/// </summary>
public class ForwardMessage
{
    /// <summary>
    /// 消息唯一标识
    /// </summary>
    public string MessageId { get; set; } = Guid.NewGuid().ToString("N");

    /// <summary>
    /// 路由上下文
    /// </summary>
    public RouteContext RouteContext { get; set; } = new();

    /// <summary>
    /// 当前处理状态
    /// </summary>
    public MessageProcessStatus Status { get; set; } = MessageProcessStatus.Received;

    /// <summary>
    /// 已重试次数
    /// </summary>
    public int RetryCount { get; set; }

    /// <summary>
    /// 创建时间
    /// </summary>
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// 下次重试时间
    /// </summary>
    public DateTime? NextRetryAt { get; set; }
}
