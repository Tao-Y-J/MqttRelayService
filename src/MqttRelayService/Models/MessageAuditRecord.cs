using System;
using SqlSugar;

namespace MqttRelayService.Models
{
    /// <summary>
    /// 消息审计记录持久化模型。
    /// </summary>
    [SugarTable("message_audit")]
    public class MessageAuditRecord
    {
        /// <summary>
        /// 消息唯一 ID。
        /// </summary>
        [SugarColumn(IsPrimaryKey = true)]
        public string MessageId { get; set; } = string.Empty;

        /// <summary>
        /// 消息主题。
        /// </summary>
        public string Topic { get; set; } = string.Empty;

        /// <summary>
        /// 源客户端 ID。
        /// </summary>
        public string SourceClientId { get; set; } = string.Empty;

        /// <summary>
        /// 载荷大小（字节）。
        /// </summary>
        public int PayloadSize { get; set; }

        /// <summary>
        /// 格式化后的载荷内容。
        /// </summary>
        [SugarColumn(IsNullable = true)]
        public string? Payload { get; set; }

        /// <summary>
        /// 消息 QoS。
        /// </summary>
        public int Qos { get; set; }

        /// <summary>
        /// 是否保留消息。
        /// </summary>
        public bool Retain { get; set; }

        /// <summary>
        /// 当前处理状态。
        /// </summary>
        public string Status { get; set; } = string.Empty;

        /// <summary>
        /// 是否命中订阅者。
        /// </summary>
        public bool IsSubscriberHit { get; set; }

        /// <summary>
        /// 处理耗时（毫秒）。
        /// </summary>
        public double LatencyMs { get; set; }

        /// <summary>
        /// 已重试次数。
        /// </summary>
        public int RetryCount { get; set; }

        /// <summary>
        /// 首次接收时间。
        /// </summary>
        public DateTime CreatedAt { get; set; }

        /// <summary>
        /// 最后更新时间。
        /// </summary>
        public DateTime UpdatedAt { get; set; }

        /// <summary>
        /// 异常错误信息。
        /// </summary>
        [SugarColumn(IsNullable = true)]
        public string? ErrorMessage { get; set; }
    }
}
