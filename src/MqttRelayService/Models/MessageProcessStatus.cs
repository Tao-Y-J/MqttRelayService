namespace MqttRelayService.Models
{
    /// <summary>
    /// 消息处理状态枚举
    /// </summary>
    public enum MessageProcessStatus
    {
        /// <summary>
        /// 已接收
        /// </summary>
        Received,

        /// <summary>
        /// 已入队
        /// </summary>
        Queued,

        /// <summary>
        /// 路由中
        /// </summary>
        Routing,

        /// <summary>
        /// 转发中
        /// </summary>
        Forwarding,

        /// <summary>
        /// 转发成功
        /// </summary>
        Succeeded,

        /// <summary>
        /// 转发失败
        /// </summary>
        Failed,

        /// <summary>
        /// 已进入死信
        /// </summary>
        DeadLetter
    }
}
