using System;
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

        /// <summary>
        /// 当前正在转发消息的首次接收时间。
        /// 用于在 MetricsMqttBrokerHost 中计算端到端的转发延迟。
        /// </summary>
        public static readonly AsyncLocal<DateTime?> CurrentFirstReceivedAt = new();

        /// <summary>
        /// 当前正在转发消息的已重试次数。
        /// </summary>
        public static readonly AsyncLocal<int?> CurrentRetryCount = new();

        /// <summary>
        /// 当前正在转发消息的是否命中订阅者。
        /// </summary>
        public static readonly AsyncLocal<bool?> CurrentIsSubscriberHit = new();
    }
}
