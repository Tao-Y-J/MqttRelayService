using MqttRelayService.Models;

namespace MqttRelayService.Services.Abstractions;

/// <summary>
/// 客户端注册表接口，管理在线客户端信息
/// </summary>
public interface IClientRegistry
{
    /// <summary>
    /// 注册客户端连接
    /// </summary>
    Task RegisterAsync(ClientSessionInfo session, CancellationToken cancellationToken = default);

    /// <summary>
    /// 注销客户端连接
    /// </summary>
    Task UnregisterAsync(string clientId, string? connectionId = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// 获取指定客户端会话
    /// </summary>
    Task<ClientSessionInfo?> GetSessionAsync(string clientId, CancellationToken cancellationToken = default);

    /// <summary>
    /// 获取所有在线客户端会话
    /// </summary>
    Task<IReadOnlyCollection<ClientSessionInfo>> GetAllSessionsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// 更新客户端订阅信息
    /// </summary>
    Task UpdateSubscriptionAsync(string clientId, string topic, bool isSubscribed, CancellationToken cancellationToken = default);

    /// <summary>
    /// 更新客户端最近活动时间
    /// </summary>
    Task UpdateActivityAsync(string clientId, CancellationToken cancellationToken = default);

    /// <summary>
    /// 获取当前在线客户端数量
    /// </summary>
    int Count { get; }
}
