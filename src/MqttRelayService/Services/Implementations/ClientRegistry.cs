using System.Collections.Concurrent;
using MqttRelayService.Models;
using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Services.Implementations;

/// <summary>
/// 客户端注册表实现，使用线程安全的 ConcurrentDictionary 管理在线客户端
/// </summary>
public class ClientRegistry : IClientRegistry
{
    private readonly ConcurrentDictionary<string, ClientSessionInfo> _sessions = new();
    private readonly ILogger<ClientRegistry> _logger;

    public ClientRegistry(ILogger<ClientRegistry> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// 当前在线客户端数量
    /// </summary>
    public int Count => _sessions.Count;

    /// <summary>
    /// 注册客户端连接
    /// </summary>
    public Task RegisterAsync(ClientSessionInfo session, CancellationToken cancellationToken = default)
    {
        try
        {
            _sessions[session.ClientId] = session;
            _logger.LogInformation("客户端 {ClientId} 已注册到在线客户端表，当前在线数 {Count}",
                session.ClientId, Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "注册客户端 {ClientId} 时发生异常", session.ClientId);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// 注销客户端连接
    /// </summary>
    public Task UnregisterAsync(string clientId, CancellationToken cancellationToken = default)
    {
        try
        {
            if (_sessions.TryRemove(clientId, out _))
            {
                _logger.LogInformation("客户端 {ClientId} 已从在线客户端表移除，当前在线数 {Count}",
                    clientId, Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "注销客户端 {ClientId} 时发生异常", clientId);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// 获取指定客户端会话
    /// </summary>
    public Task<ClientSessionInfo?> GetSessionAsync(string clientId, CancellationToken cancellationToken = default)
    {
        _sessions.TryGetValue(clientId, out var session);
        return Task.FromResult(session);
    }

    /// <summary>
    /// 获取所有在线客户端会话
    /// </summary>
    public Task<IReadOnlyCollection<ClientSessionInfo>> GetAllSessionsAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult<IReadOnlyCollection<ClientSessionInfo>>(_sessions.Values.ToList());
    }

    /// <summary>
    /// 更新客户端订阅信息
    /// </summary>
    public Task UpdateSubscriptionAsync(string clientId, string topic, bool isSubscribed, CancellationToken cancellationToken = default)
    {
        try
        {
            if (_sessions.TryGetValue(clientId, out var session))
            {
                if (isSubscribed)
                {
                    session.Subscriptions.Add(topic);
                    _logger.LogDebug("客户端 {ClientId} 订阅主题 {Topic}", clientId, topic);
                }
                else
                {
                    session.Subscriptions.Remove(topic);
                    _logger.LogDebug("客户端 {ClientId} 取消订阅主题 {Topic}", clientId, topic);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "更新客户端 {ClientId} 订阅信息时发生异常", clientId);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// 更新客户端最近活动时间
    /// </summary>
    public Task UpdateActivityAsync(string clientId, CancellationToken cancellationToken = default)
    {
        if (_sessions.TryGetValue(clientId, out var session))
        {
            session.LastActivityAt = DateTime.UtcNow;
        }

        return Task.CompletedTask;
    }
}