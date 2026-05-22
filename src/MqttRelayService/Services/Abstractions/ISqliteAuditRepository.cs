using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MqttRelayService.Models;

namespace MqttRelayService.Services.Abstractions
{
    /// <summary>
    /// SQLite 物理持久化审计仓储接口
    /// </summary>
    public interface ISqliteAuditRepository
    {
        /// <summary>
        /// 初始化数据库表结构及索引
        /// </summary>
        Task InitializeAsync();

        /// <summary>
        /// 记录或更新一条消息的审计状态
        /// </summary>
        Task RecordMessageAuditAsync(MessageAuditRecord record);

        /// <summary>
        /// 记录一条设备连接或订阅变动历史
        /// </summary>
        Task RecordClientConnectionHistoryAsync(ClientConnectionHistoryRecord record);

        /// <summary>
        /// 获取分页的消息审计日志列表
        /// </summary>
        Task<(int TotalCount, IReadOnlyList<MessageAuditRecord> Items)> GetPagedMessagesAsync(
            int page,
            int pageSize,
            string? status = null,
            string? topic = null,
            string? sourceClientId = null,
            string? search = null,
            DateTime? startDate = null,
            DateTime? endDate = null);

        /// <summary>
        /// 获取分页的客户端连接与订阅历史记录列表
        /// </summary>
        Task<(int TotalCount, IReadOnlyList<ClientConnectionHistoryRecord> Items)> GetPagedClientHistoryAsync(
            int page,
            int pageSize,
            string? clientId = null,
            string? eventType = null,
            string? search = null);

        Task<(
            int TotalMessages,
            int TotalSucceeded,
            int TotalFailed,
            int TotalDeadLetter,
            IReadOnlyList<MessageAuditRecord> RecentItems)> GetDashboardMessageSummaryAsync(int recentCount);

        /// <summary>
        /// 清理历史数据，保持数据库在健康大小区间
        /// </summary>
        /// <param name="keepMessagesCount">保留的消息审计记录最大条数</param>
        /// <param name="keepClientHistoryCount">保留的客户端历史记录最大条数</param>
        Task CleanupHistoryAsync(int keepMessagesCount, int keepClientHistoryCount);
    }
}
