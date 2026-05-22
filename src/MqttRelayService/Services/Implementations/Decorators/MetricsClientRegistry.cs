using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MqttRelayService.Models;
using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Services.Implementations.Decorators
{
    /// <summary>
    /// 客户端注册表指标拦截装饰器，用于无侵入拦截连接、断开以及订阅主题事件，并记录到 SQLite 物理审计表
    /// </summary>
    public class MetricsClientRegistry : IClientRegistry
    {
        private readonly IClientRegistry _inner;
        private readonly ISqliteAuditRepository _auditRepository;

        public MetricsClientRegistry(IClientRegistry inner, ISqliteAuditRepository auditRepository)
        {
            _inner = inner;
            _auditRepository = auditRepository;
        }

        public int Count => _inner.Count;

        /// <summary>
        /// 拦截客户端注册连接事件
        /// </summary>
        public async Task RegisterAsync(ClientSessionInfo session, CancellationToken cancellationToken = default)
        {
            // 1. 调用内置的在线列表更新
            await _inner.RegisterAsync(session, cancellationToken);

            // 2. 写入物理连接日志
            var record = new ClientConnectionHistoryRecord
            {
                ClientId = session.ClientId,
                Username = session.Username,
                ConnectionId = session.ConnectionId,
                Event = "Connected",
                Details = $"客户端成功建立连接，会话状态为: {session.Status}",
                Timestamp = DateTime.UtcNow
            };

            await _auditRepository.RecordClientConnectionHistoryAsync(record);
        }

        /// <summary>
        /// 拦截客户端注销断开事件
        /// </summary>
        public async Task UnregisterAsync(string clientId, string? connectionId = null, CancellationToken cancellationToken = default)
        {
            // 1. 先尝试从内存注册表中获取当前的会话信息，以便获取 ConnectionId 和 Username
            var session = await _inner.GetSessionAsync(clientId, cancellationToken);
            var username = session?.Username;
            var actualConnId = connectionId ?? session?.ConnectionId ?? "Unknown";

            // 2. 调用内置的在线列表移除
            await _inner.UnregisterAsync(clientId, connectionId, cancellationToken);

            // 3. 写入物理断开日志
            var record = new ClientConnectionHistoryRecord
            {
                ClientId = clientId,
                Username = username,
                ConnectionId = actualConnId,
                Event = "Disconnected",
                Details = $"连接已断开并注销会话",
                Timestamp = DateTime.UtcNow
            };

            await _auditRepository.RecordClientConnectionHistoryAsync(record);
        }

        public Task<ClientSessionInfo?> GetSessionAsync(string clientId, CancellationToken cancellationToken = default)
        {
            return _inner.GetSessionAsync(clientId, cancellationToken);
        }

        public Task<IReadOnlyCollection<ClientSessionInfo>> GetAllSessionsAsync(CancellationToken cancellationToken = default)
        {
            return _inner.GetAllSessionsAsync(cancellationToken);
        }

        /// <summary>
        /// 拦截客户端订阅和取消订阅事件
        /// </summary>
        public async Task UpdateSubscriptionAsync(string clientId, string topic, bool isSubscribed, CancellationToken cancellationToken = default)
        {
            // 1. 获取客户端会话信息以完善日志数据
            var session = await _inner.GetSessionAsync(clientId, cancellationToken);
            var username = session?.Username;
            var connectionId = session?.ConnectionId ?? "Unknown";

            // 2. 调用内置的注册表进行订阅更新
            await _inner.UpdateSubscriptionAsync(clientId, topic, isSubscribed, cancellationToken);

            // 3. 写入物理订阅变化日志
            var record = new ClientConnectionHistoryRecord
            {
                ClientId = clientId,
                Username = username,
                ConnectionId = connectionId,
                Event = isSubscribed ? "Subscribed" : "Unsubscribed",
                Details = $"主题: {topic}",
                Timestamp = DateTime.UtcNow
            };

            await _auditRepository.RecordClientConnectionHistoryAsync(record);
        }

        public Task UpdateActivityAsync(string clientId, CancellationToken cancellationToken = default)
        {
            return _inner.UpdateActivityAsync(clientId, cancellationToken);
        }
    }
}
