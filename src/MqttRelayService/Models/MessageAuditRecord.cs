using System;
using SqlSugar;

namespace MqttRelayService.Models
{
    /// <summary>
    /// 消息审计记录，用于 SQLite 物理持久化存储
    /// </summary>
    [SugarTable("message_audit")]
    public class MessageAuditRecord
    {
        /// <summary>
        /// 消息唯一 ID
        /// </summary>
        [SugarColumn(IsPrimaryKey = true)]
        public string MessageId { get; set; } = string.Empty;

        /// <summary>
        /// 消息主题 (Topic)
        /// </summary>
        public string Topic { get; set; } = string.Empty;

        /// <summary>
        /// 发送客户端 ID
        /// </summary>
        public string SourceClientId { get; set; } = string.Empty;

        /// <summary>
        /// 载荷大小（字节）
        /// </summary>
        public int PayloadSize { get; set; }

        /// <summary>
        /// 格式化后的载荷文本（限额截断保存）
        /// </summary>
        public string? Payload { get; set; }

        /// <summary>
        /// 服务质量等级 (QoS)
        /// </summary>
        public int Qos { get; set; }

        /// <summary>
        /// 是否保留消息 (Retain)
        /// </summary>
        public bool Retain { get; set; }

        /// <summary>
        /// 当前处理状态 (Queued, Routing, Forwarding, Succeeded, Failed, DeadLetter)
        /// </summary>
        public string Status { get; set; } = string.Empty;

        /// <summary>
        /// 处理耗时（毫秒）
        /// </summary>
        public double LatencyMs { get; set; }

        /// <summary>
        /// 当前已重试次数
        /// </summary>
        public int RetryCount { get; set; }

        /// <summary>
        /// 首次接收时间
        /// </summary>
        public DateTime CreatedAt { get; set; }

        /// <summary>
        /// 最后更新时间
        /// </summary>
        public DateTime UpdatedAt { get; set; }

        /// <summary>
        /// 异常错误信息
        /// </summary>
        public string? ErrorMessage { get; set; }
    }
}
