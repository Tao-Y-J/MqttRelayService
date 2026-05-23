using System.Threading.Tasks;
using MqttRelayService.Models;

namespace MqttRelayService.Services.Abstractions
{
    /// <summary>
    /// 指标统计与收集服务接口，为 Web 管理面板提供实时监控指标支持。
    /// </summary>
    public interface IMetricsService
    {
        /// <summary>
        /// 记录一条消息成功入队
        /// </summary>
        /// <param name="message">入队的消息实例</param>
        void RecordReceived(ForwardMessage message, bool isFirstReceipt = true);

        /// <summary>
        /// 记录由于队列满载导致的消息被丢弃事件
        /// </summary>
        void RecordRejected();

        /// <summary>
        /// 记录消息转发结果
        /// </summary>
        /// <param name="context">路由上下文</param>
        /// <param name="success">是否转发成功</param>
        /// <param name="retryCount">当前重试次数</param>
        /// <param name="latencyMs">转发耗时（毫秒）</param>
        /// <param name="isSubscriberHit">是否命中订阅者</param>
        void RecordForwarded(MqttRelayService.Models.RouteContext context, bool success, int retryCount, double latencyMs, bool isSubscriberHit = false);

        /// <summary>
        /// 记录一条消息进入死信队列事件
        /// </summary>
        /// <param name="record">死信记录模型</param>
        void RecordDeadLetter(DeadLetterRecord record);

        /// <summary>
        /// 获取当前所有统计指标的聚合快照，用于 Dashboard 前端展示
        /// </summary>
        /// <returns>包含各项统计与历史趋势的 Dashboard 数据包</returns>
        Task<object> GetDashboardDataAsync();

        /// <summary>
        /// 根据消息 ID 获取最近缓存的格式化载荷内容
        /// </summary>
        /// <param name="messageId">消息唯一ID</param>
        /// <returns>载荷文本内容，若不存在则返回 null</returns>
        string? GetPayload(string messageId);
    }
}
