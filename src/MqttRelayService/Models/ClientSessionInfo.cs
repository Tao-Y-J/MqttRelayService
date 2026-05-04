namespace MqttRelayService.Models;

/// <summary>
/// 客户端会话信息
/// </summary>
public class ClientSessionInfo
{
    /// <summary>
    /// 客户端标识
    /// </summary>
    public string ClientId { get; set; } = string.Empty;

    /// <summary>
    /// 用户名
    /// </summary>
    public string Username { get; set; } = string.Empty;

    /// <summary>
    /// 连接时间
    /// </summary>
    public DateTime ConnectedAt { get; set; }

    /// <summary>
    /// 最近活动时间
    /// </summary>
    public DateTime LastActivityAt { get; set; }

    /// <summary>
    /// 连接状态
    /// </summary>
    public ConnectionStatus Status { get; set; }

    /// <summary>
    /// 订阅的 Topic 列表
    /// </summary>
    public HashSet<string> Subscriptions { get; set; } = new();
}
