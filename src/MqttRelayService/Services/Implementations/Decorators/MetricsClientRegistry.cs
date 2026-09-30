using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MqttRelayService.Models;
using MqttRelayService.Services.Abstractions;
using MqttRelayService.Utilities;

namespace MqttRelayService.Services.Implementations.Decorators
{
    /// <summary>
    /// 客户端注册表指标拦截装饰器，用于无侵入记录连接、断开与订阅历史。
    /// 历史记录统一投递到指标服务的后台有界队列，绝不在 MQTT 事件回调线程上直接访问数据库。
    /// 指标服务以延迟方式解析：指标服务自身依赖被装饰的注册表，构造期直接注入会形成循环依赖。
    /// </summary>
    public class MetricsClientRegistry : IClientRegistry
    {
        private readonly IClientRegistry _inner;
        private readonly LazyService<IMetricsService> _metrics;

        public MetricsClientRegistry(IClientRegistry inner, LazyService<IMetricsService> metrics)
        {
            _inner = inner;
            _metrics = metrics;
        }

        /// <summary>
        /// 直接注入指标服务实例的构造重载，供单元测试与不依赖容器的场景使用。
        /// </summary>
        public MetricsClientRegistry(IClientRegistry inner, IMetricsService metrics)
            : this(inner, LazyService<IMetricsService>.From(metrics))
        {
        }

        public int Count => _inner.Count;

        /// <summary>
        /// 拦截客户端注册连接事件。
        /// </summary>
        public async Task RegisterAsync(ClientSessionInfo session, CancellationToken cancellationToken = default)
        {
            await _inner.RegisterAsync(session, cancellationToken);

            RecordHistory(new ClientConnectionHistoryRecord
            {
                ClientId = session.ClientId,
                Username = session.Username,
                ConnectionId = session.ConnectionId,
                Event = "Connected",
                Details = $"客户端成功建立连接，会话状态为: {session.Status}",
                Timestamp = DateTime.Now
            });
        }

        /// <summary>
        /// 拦截客户端注销断开事件。
        /// 断开后重新读取会话：若同 ClientId 仍在线且 ConnectionId 不同，说明这是上一连接的延迟断开事件，
        /// 必须与真实断开封成不同事件，避免客户端历史出现"连接中新连接被标记为已断开"的错误序列。
        /// </summary>
        public async Task UnregisterAsync(string clientId, string? connectionId = null, CancellationToken cancellationToken = default)
        {
            var session = await _inner.GetSessionAsync(clientId, cancellationToken);
            var username = session?.Username;
            var actualConnId = connectionId ?? session?.ConnectionId ?? "Unknown";

            await _inner.UnregisterAsync(clientId, connectionId, cancellationToken);

            var sessionAfter = await _inner.GetSessionAsync(clientId, cancellationToken);
            var isStaleEvent = sessionAfter != null
                && !string.IsNullOrEmpty(connectionId)
                && !string.Equals(sessionAfter.ConnectionId, actualConnId, StringComparison.Ordinal);

            RecordHistory(new ClientConnectionHistoryRecord
            {
                ClientId = clientId,
                Username = sessionAfter?.Username ?? username,
                ConnectionId = actualConnId,
                Event = isStaleEvent ? "DisconnectedStale" : "Disconnected",
                Details = isStaleEvent
                    ? "收到旧连接的延迟断开事件，当前同 ClientId 会话仍在线"
                    : "连接已断开并注销会话",
                Timestamp = DateTime.Now
            });
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
        /// 拦截客户端订阅和取消订阅事件。
        /// </summary>
        public async Task UpdateSubscriptionAsync(string clientId, string topic, bool isSubscribed, CancellationToken cancellationToken = default)
        {
            var session = await _inner.GetSessionAsync(clientId, cancellationToken);
            var username = session?.Username;
            var connectionId = session?.ConnectionId ?? "Unknown";

            await _inner.UpdateSubscriptionAsync(clientId, topic, isSubscribed, cancellationToken);

            RecordHistory(new ClientConnectionHistoryRecord
            {
                ClientId = clientId,
                Username = username,
                ConnectionId = connectionId,
                Event = isSubscribed ? "Subscribed" : "Unsubscribed",
                Details = $"主题: {topic}",
                Timestamp = DateTime.Now
            });
        }

        public Task UpdateActivityAsync(string clientId, CancellationToken cancellationToken = default)
        {
            return _inner.UpdateActivityAsync(clientId, cancellationToken);
        }

        /// <summary>
        /// 历史记录只允许影响观测结果，绝不能改变被装饰注册表的行为或向上抛出异常。
        /// </summary>
        private void RecordHistory(ClientConnectionHistoryRecord record)
        {
            try
            {
                _metrics.Value.RecordClientHistory(record);
            }
            catch
            {
                // 客户端历史仅为观测数据，记录失败不得影响连接、断开与订阅主链路。
            }
        }
    }
}
