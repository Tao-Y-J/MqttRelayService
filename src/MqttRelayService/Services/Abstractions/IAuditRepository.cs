using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MqttRelayService.Models;

namespace MqttRelayService.Services.Abstractions
{
    /// <summary>
    /// 审计持久化仓储接口。
    /// </summary>
    public interface IAuditRepository
    {
        /// <summary>
        /// 初始化数据库表结构。
        /// </summary>
        Task InitializeAsync();

        /// <summary>
        /// 记录或更新一条消息审计状态。
        /// </summary>
        Task RecordMessageAuditAsync(MessageAuditRecord record);

        /// <summary>
        /// 批量记录或更新多条消息审计状态。
        /// </summary>
        Task RecordMessageAuditsAsync(IReadOnlyList<MessageAuditRecord> records);

        /// <summary>
        /// 记录一条客户端连接或订阅历史。
        /// </summary>
        Task RecordClientConnectionHistoryAsync(ClientConnectionHistoryRecord record);

        /// <summary>
        /// 获取分页消息审计列表。
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
        /// 获取分页客户端历史列表。
        /// </summary>
        Task<(int TotalCount, IReadOnlyList<ClientConnectionHistoryRecord> Items)> GetPagedClientHistoryAsync(
            int page,
            int pageSize,
            string? clientId = null,
            string? eventType = null,
            string? search = null);

        /// <summary>
        /// 获取 Dashboard 汇总数据。
        /// </summary>
        Task<(
            int TotalMessages,
            int TotalPending,
            int TotalSucceeded,
            int TotalFailed,
            int TotalDeadLetter,
            IReadOnlyList<MessageAuditRecord> RecentItems)> GetDashboardMessageSummaryAsync(int recentCount);

        /// <summary>
        /// 清理历史数据，保持表规模可控。
        /// </summary>
    }
}
