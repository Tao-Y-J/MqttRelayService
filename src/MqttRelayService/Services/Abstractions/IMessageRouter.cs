using MqttRelayService.Models;

namespace MqttRelayService.Services.Abstractions;

/// <summary>
/// 消息路由器接口，负责根据 Topic 规则决定消息去向
/// </summary>
public interface IMessageRouter
{
    /// <summary>
    /// 执行消息路由
    /// </summary>
    Task<IReadOnlyList<ForwardResult>> RouteAsync(RouteContext context, CancellationToken cancellationToken = default);
}