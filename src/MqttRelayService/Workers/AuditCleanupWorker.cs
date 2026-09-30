using Microsoft.Extensions.Options;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;
using MqttRelayService.Services.Implementations;

namespace MqttRelayService.Workers
{
    /// <summary>
    /// 审计数据保留清理后台服务。
    /// 启动后立即清理一次，随后保证每天至少清理一次：配置了 <see cref="AuditStorageOptions.CleanupAtHour"/>
    /// 时每天在该整点执行，未配置时按不超过 24 小时的间隔等间隔执行。
    /// 清理只删除行，删除后由仓储层执行 VACUUM 回收文件空间。
    /// </summary>
    public class AuditCleanupWorker : BackgroundService
    {
        private readonly IAuditRepository _auditRepository;
        private readonly AuditStorageOptions _options;
        private readonly ILogger<AuditCleanupWorker> _logger;

        public AuditCleanupWorker(
            IAuditRepository auditRepository,
            IOptions<AuditStorageOptions> options,
            ILogger<AuditCleanupWorker> logger)
        {
            _auditRepository = auditRepository;
            _options = options.Value;
            _logger = logger;
        }

        /// <summary>
        /// 计算下一轮清理时间。
        /// 配置了清理整点时取"今天的该整点，已过则取明天"，因此每 24 小时恰好覆盖一天一次；
        /// 未配置整点时取 now + 间隔，间隔由配置校验收敛到不超过 1440 分钟。
        /// </summary>
        internal static DateTime ComputeNextRunAt(DateTime now, int? cleanupAtHour, int intervalMinutes)
        {
            if (cleanupAtHour.HasValue)
            {
                var todayTarget = now.Date.AddHours(cleanupAtHour.Value);
                return todayTarget > now ? todayTarget : todayTarget.AddDays(1);
            }

            return now.AddMinutes(intervalMinutes);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // 顶层捕获：Host 配置了 BackgroundServiceExceptionBehavior.Ignore，
            // 清理循环一旦异常退出就会静默停止，审计库随运行时间无界增长且无人可见。
            try
            {
                if (_options.RetentionDays <= 0)
                {
                    _logger.LogInformation(
                        "审计数据清理已关闭（AuditStorage:RetentionDays={RetentionDays}<=0），历史数据需由运维手工维护",
                        _options.RetentionDays);
                    return;
                }

                _logger.LogInformation(
                    "审计数据清理已启用：保留 {RetentionDays} 天，清理时刻 {CleanupAtHour} 时（留空则按 {IntervalMinutes} 分钟间隔），清理后 VACUUM {Vacuum}",
                    _options.RetentionDays,
                    _options.CleanupAtHour?.ToString() ?? "未配置",
                    _options.CleanupIntervalMinutes,
                    _options.VacuumAfterCleanup);

                while (!stoppingToken.IsCancellationRequested)
                {
                    // 启动先清理一次：服务长时间停机期间堆积的超期数据必须马上收敛，
                    // 不能等到下一个清理时刻。
                    await RunCleanupOnceAsync(stoppingToken);

                    var nextRunAt = ComputeNextRunAt(DateTime.Now, _options.CleanupAtHour, _options.CleanupIntervalMinutes);
                    var delay = nextRunAt - DateTime.Now;
                    if (delay < TimeSpan.Zero)
                    {
                        delay = TimeSpan.Zero;
                    }

                    try
                    {
                        await Task.Delay(delay, stoppingToken);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // 正常停机
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "审计数据清理后台任务异常终止，历史数据将保留到下次进程启动");
            }
        }

        /// <summary>
        /// 执行一轮清理：按保留天数计算截止时间，先删消息审计再删客户端历史，最后按配置回收文件空间。
        /// 单轮失败只记录日志并跳过本轮的 VACUUM，不向上抛出，避免一次失败终止后续所有轮次。
        /// </summary>
        internal async Task RunCleanupOnceAsync(CancellationToken cancellationToken)
        {
            if (_options.RetentionDays <= 0)
            {
                return;
            }

            // 与审计写入使用同一时间基准（DateTime.Now），保证跨时区/夏令时配置下判据一致。
            var cutoff = DateTime.Now.AddDays(-_options.RetentionDays);

            // 两张表互相隔离：任一张表持续失败（例如单表被长时间锁住）不得让另一张表的清理也被饿死，
            // 否则该表仍会无界增长，违背"默认只保留 30 天"的目标。
            var messageDeleted = await TryDeleteAsync(
                () => _auditRepository.DeleteExpiredMessageAuditsAsync(cutoff),
                "消息审计", cutoff, cancellationToken);

            var historyDeleted = await TryDeleteAsync(
                () => _auditRepository.DeleteExpiredClientHistoriesAsync(cutoff),
                "客户端历史", cutoff, cancellationToken);

            if (messageDeleted > 0 || historyDeleted > 0)
            {
                _logger.LogInformation(
                    "审计数据清理完成：保留 {RetentionDays} 天，截止时间 {Cutoff:o}，已删除消息审计 {MessageDeleted} 条、客户端历史 {HistoryDeleted} 条",
                    _options.RetentionDays, cutoff, messageDeleted, historyDeleted);
            }
            else
            {
                _logger.LogDebug(
                    "审计数据清理完成：保留 {RetentionDays} 天，截止时间 {Cutoff:o}，没有超过保留窗口的记录",
                    _options.RetentionDays, cutoff);
            }

            if (!_options.VacuumAfterCleanup)
            {
                return;
            }

            // VACUUM 是 SQLite 专有语句且需要独占数据库，因此只在真实实现上调用；
            // 失败由仓储层降级为 Warning，不改变本轮清理结果。
            if (_auditRepository is AuditRepository sqliteAuditRepository)
            {
                await sqliteAuditRepository.VacuumAsync();
            }
        }

        /// <summary>
        /// 执行单张表的清理，返回删除条数。
        /// 失败时记录 Error 并返回 0：清理异常不能让后台任务终止，也不能影响另一张表的清理。
        /// </summary>
        private async Task<int> TryDeleteAsync(Func<Task<int>> deleteAction, string tableName, DateTime cutoff, CancellationToken cancellationToken)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                return await deleteAction();
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "审计数据清理失败：{TableName} 表保留 {RetentionDays} 天，截止时间 {Cutoff:o}，将在下一轮重试",
                    tableName, _options.RetentionDays, cutoff);
                return 0;
            }
        }
    }
}
