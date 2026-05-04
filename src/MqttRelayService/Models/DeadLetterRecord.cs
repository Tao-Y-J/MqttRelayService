namespace MqttRelayService.Models;

/// <summary>
/// 死信记录
/// </summary>
public class DeadLetterRecord
{
    /// <summary>
    /// 消息唯一标识
    /// </summary>
    public string MessageId { get; set; } = string.Empty;

    /// <summary>
    /// 原始主题
    /// </summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>
    /// 来源客户端标识
    /// </summary>
    public string SourceClientId { get; set; } = string.Empty;

    /// <summary>
    /// 目标客户端标识或目标规则
    /// </summary>
    public string? TargetClientId { get; set; }

    /// <summary>
    /// Payload Base64 编码
    /// </summary>
    public string PayloadBase64 { get; set; } = string.Empty;

    /// <summary>
    /// 首次接收时间
    /// </summary>
    public DateTime FirstReceivedAt { get; set; }

    /// <summary>
    /// 最后失败时间
    /// </summary>
    public DateTime LastFailedAt { get; set; }

    /// <summary>
    /// 失败原因
    /// </summary>
    public string FailureReason { get; set; } = string.Empty;

    /// <summary>
    /// 已重试次数
    /// </summary>
    public int RetryCount { get; set; }
}