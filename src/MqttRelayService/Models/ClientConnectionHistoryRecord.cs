using System;

namespace MqttRelayService.Models
{
    /// <summary>
    /// 客户端设备连接与订阅历史记录，用于 SQLite 物理持久化存储
    /// </summary>
    public class ClientConnectionHistoryRecord
    {
        /// <summary>
        /// 自增唯一标识
        /// </summary>
        public long Id { get; set; }

        /// <summary>
        /// 客户端唯一 ID (ClientId)
        /// </summary>
        public string ClientId { get; set; } = string.Empty;

        /// <summary>
        /// 客户端连接用户名 (Username)
        /// </summary>
        public string? Username { get; set; }

        /// <summary>
        /// 单次 TCP 连接会话唯一 ID (ConnectionId)
        /// </summary>
        public string ConnectionId { get; set; } = string.Empty;

        /// <summary>
        /// 事件类型 (Connected, Disconnected, Subscribed, Unsubscribed)
        /// </summary>
        public string Event { get; set; } = string.Empty;

        /// <summary>
        /// 附加详细信息 (如订阅的主题、断开连接的原因等)
        /// </summary>
        public string? Details { get; set; }

        /// <summary>
        /// 事件记录时间戳
        /// </summary>
        public DateTime Timestamp { get; set; }
    }
}
