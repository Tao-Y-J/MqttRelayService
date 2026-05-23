using System;
using SqlSugar;

namespace MqttRelayService.Models
{
    /// <summary>
    /// 客户端连接与订阅历史持久化模型。
    /// </summary>
    [SugarTable("client_connection_history")]
    public class ClientConnectionHistoryRecord
    {
        /// <summary>
        /// 自增唯一标识。
        /// </summary>
        [SugarColumn(IsPrimaryKey = true, IsIdentity = true)]
        public int Id { get; set; }

        /// <summary>
        /// 客户端唯一 ID。
        /// </summary>
        public string ClientId { get; set; } = string.Empty;

        /// <summary>
        /// 客户端连接用户名。
        /// </summary>
        [SugarColumn(IsNullable = true)]
        public string? Username { get; set; }

        /// <summary>
        /// 单次连接会话唯一 ID。
        /// </summary>
        public string ConnectionId { get; set; } = string.Empty;

        /// <summary>
        /// 事件类型。
        /// </summary>
        public string Event { get; set; } = string.Empty;

        /// <summary>
        /// 附加说明信息。
        /// </summary>
        [SugarColumn(IsNullable = true)]
        public string? Details { get; set; }

        /// <summary>
        /// 事件记录时间。
        /// </summary>
        [SugarColumn(IndexGroupNameList = new string[] { "idx_timestamp" })]
        public DateTime Timestamp { get; set; }
    }
}
