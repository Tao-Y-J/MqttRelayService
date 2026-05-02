namespace MqttRelayService.Models;

/// <summary>
/// 转发结果
/// </summary>
public class ForwardResult
{
    /// <summary>
    /// 是否成功
    /// </summary>
    public bool Success { get; set; }

    /// <summary>
    /// 目标客户端标识
    /// </summary>
    public string TargetClientId { get; set; } = string.Empty;

    /// <summary>
    /// 错误信息
    /// </summary>
    public string? Error { get; set; }

    /// <summary>
    /// 转发时间戳
    /// </summary>
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}