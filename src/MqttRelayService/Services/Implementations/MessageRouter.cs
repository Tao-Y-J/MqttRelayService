using Microsoft.Extensions.Options;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Services.Implementations;

/// <summary>
/// 消息路由器实现，根据 Topic 匹配订阅并决定消息去向
/// </summary>
public class MessageRouter : IMessageRouter
{
    private readonly IClientRegistry _clientRegistry;
    private readonly RoutingOptions _options;
    private readonly ILogger<MessageRouter> _logger;

    public MessageRouter(
        IClientRegistry clientRegistry,
        IOptions<RoutingOptions> options,
        ILogger<MessageRouter> logger)
    {
        _clientRegistry = clientRegistry;
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>
    /// 执行消息路由
    /// </summary>
    public async Task<IReadOnlyList<ForwardResult>> RouteAsync(RouteContext context, CancellationToken cancellationToken = default)
    {
        var results = new List<ForwardResult>();

        var sessions = await _clientRegistry.GetAllSessionsAsync(cancellationToken);

        foreach (var session in sessions)
        {
            // 默认不回发给发送方（除非配置允许）
            if (session.ClientId == context.SourceClientId && !_options.EchoToSender)
            {
                continue;
            }

            // 检查客户端是否订阅了匹配的 Topic
            if (IsTopicMatch(context.Topic, session.Subscriptions))
            {
                results.Add(new ForwardResult
                {
                    Success = true,
                    TargetClientId = session.ClientId
                });
            }
        }

        _logger.LogDebug("消息 {MessageId} 路由完成，目标客户端数 {TargetCount}",
            context.MessageId, results.Count);

        return results;
    }

    /// <summary>
    /// 判断 Topic 是否匹配客户端的订阅列表
    /// </summary>
    private static bool IsTopicMatch(string topic, HashSet<string> subscriptions)
    {
        foreach (var subscription in subscriptions)
        {
            if (TopicMatches(topic, subscription))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// MQTT Topic 匹配逻辑（支持通配符 # 和 +）
    /// </summary>
    private static bool TopicMatches(string topic, string subscription)
    {
        // 精确匹配
        if (topic == subscription)
        {
            return true;
        }

        var topicParts = topic.Split('/');
        var subParts = subscription.Split('/');

        for (int i = 0; i < subParts.Length; i++)
        {
            // # 通配符匹配所有剩余层级
            if (subParts[i] == "#")
            {
                return true;
            }

            // + 通配符匹配单层
            if (subParts[i] == "+")
            {
                continue;
            }

            // 超出 topic 层级则不匹配
            if (i >= topicParts.Length)
            {
                return false;
            }

            // 层级不匹配
            if (subParts[i] != topicParts[i])
            {
                return false;
            }
        }

        // 订阅层级和 topic 层级必须完全一致（除非已经遇到 #）
        return topicParts.Length == subParts.Length;
    }
}
