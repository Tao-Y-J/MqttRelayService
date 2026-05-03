using MqttRelayService.Models;

namespace MqttRelayService.Services.Abstractions;

/// <summary>
/// 死信服务接口，负责记录无法成功转发的消息
/// </summary>
public interface IDeadLetterService
{
    /// <summary>
    /// 写入死信记录
    /// </summary>
    Task WriteAsync(DeadLetterRecord record, CancellationToken cancellationToken = default);
}