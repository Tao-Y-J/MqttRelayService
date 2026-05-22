using System.Threading;

namespace MqttRelayService.Models
{
    /// <summary>
    /// 消息处理上下文，提供基于 AsyncLocal 的异步调用链上下文数据传递能力。
    /// </summary>
    public static class MessageContext
    {
        /// <summary>
        /// 当前正在转发消息的原始 MessageId。
        /// 用于在 MessageDeliveryService 转发注入时，将消息 ID 无侵入地透传给 MetricsMqttBrokerHost，以便度量与审计日志按 ID 进行就地状态更新。
        /// </summary>
        public static readonly AsyncLocal<string?> CurrentMessageId = new();
    }
}
