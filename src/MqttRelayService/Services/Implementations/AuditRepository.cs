using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;
using SqlSugar;

namespace MqttRelayService.Services.Implementations
{
    /// <summary>
    /// 通用审计持久化仓储实现。
    /// </summary>
    public class AuditRepository : IAuditRepository
    {
        private const int SqliteExistsQueryBatchSize = 2000;
        private const int SqliteWriteBatchSize = 500;
        private const int DefaultExistsQueryBatchSize = 2000;
        private const int DefaultWriteBatchSize = 200;

        /// <summary>
        /// 单条 SQL 里 IN 条件允许承载的最大参数个数。
        /// SQLite 默认 SQLITE_MAX_VARIABLE_NUMBER = 999，清理批删必须留出余量，
        /// 否则大批量删除会以 "too many SQL variables" 失败。
        /// </summary>
        private const int SqliteQueryParameterBatchSize = 900;

        /// <summary>
        /// 单页最大条数。分页参数由仓储层统一收敛，任何调用方都无法请求超大页长。
        /// </summary>
        private const int MaxPageSize = 200;

        /// <summary>
        /// 最大页码，与 MaxPageSize 配合保证 Skip 偏移量不溢出 int。
        /// </summary>
        private const int MaxPageNumber = 1000000;

        /// <summary>
        /// 收敛分页参数：页码不小于 1、页长限制在 [1, MaxPageSize]。
        /// </summary>
        private static void NormalizePaging(ref int page, ref int pageSize)
        {
            page = Math.Clamp(page, 1, MaxPageNumber);
            pageSize = Math.Clamp(pageSize, 1, MaxPageSize);
        }
        private readonly AuditStorageOptions _options;
        private readonly ILogger<AuditRepository> _logger;
        private readonly SemaphoreSlim _writeLock = new(1, 1);
        private readonly SqlSugarScope _db;
        private readonly string? _sqliteDataSourcePath;

        // 双检锁标志位：读路径（GetPagedMessagesAsync 等不持 _writeLock 的查询）也会读取此标志，
        // 必须用 volatile 保证跨线程可见性，否则读线程可能看到陈旧的 false 而重复进入 InitTables。
        private volatile bool _schemaEnsured;

        public AuditRepository(IOptions<AuditStorageOptions> options, ILogger<AuditRepository> logger)
            : this(options.Value, logger)
        {
        }

        internal AuditRepository(AuditStorageOptions options, ILogger<AuditRepository> logger)
        {
            _options = options;
            _logger = logger;
            _sqliteDataSourcePath = ResolveSqliteDataSourcePath(options);
            _db = new SqlSugarScope(new ConnectionConfig
            {
                DbType = ParseDbType(options.Provider),
                ConnectionString = BuildConnectionString(options, _sqliteDataSourcePath),
                IsAutoCloseConnection = true
            });
        }

        internal static DbType ParseDbType(string? provider)
        {
            if (string.IsNullOrWhiteSpace(provider))
            {
                return DbType.Sqlite;
            }

            if (Enum.TryParse<DbType>(provider, ignoreCase: true, out var dbType))
            {
                return dbType;
            }

            throw new ArgumentException($"不支持的审计数据库提供程序: {provider}", nameof(provider));
        }

        /// <summary>
        /// 从连接串中提取 <c>Data Source</c> 的原始配置值（不做路径展开）。
        /// Web 管理面展示审计文件路径时复用该方法，只回传路径本身，避免把可能含账号密码的完整连接串回传给浏览器。
        /// </summary>
        internal static string? ExtractSqliteDataSource(string? connectionString)
        {
            var parts = (connectionString ?? string.Empty)
                .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            foreach (var part in parts)
            {
                var kv = part.Split('=', 2, StringSplitOptions.TrimEntries);
                if (kv.Length == 2 && kv[0].Equals("Data Source", StringComparison.OrdinalIgnoreCase))
                {
                    return kv[1];
                }
            }

            return null;
        }

        private static string? ResolveSqliteDataSourcePath(AuditStorageOptions options)
        {
            var dbType = ParseDbType(options.Provider);
            if (dbType != DbType.Sqlite)
            {
                return null;
            }

            var path = ExtractSqliteDataSource(options.ConnectionString);
            if (path is null)
            {
                return null;
            }

            return Path.IsPathRooted(path)
                ? path
                : Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, path));
        }

        private static string BuildConnectionString(AuditStorageOptions options, string? sqliteDataSourcePath)
        {
            if (ParseDbType(options.Provider) != DbType.Sqlite || string.IsNullOrWhiteSpace(sqliteDataSourcePath))
            {
                return options.ConnectionString;
            }

            return $"Data Source={sqliteDataSourcePath}";
        }

        private async Task EnsureSchemaAsync()
        {
            if (!_options.AutoInitializeSchema || _schemaEnsured)
            {
                return;
            }

            if (ParseDbType(_options.Provider) == DbType.Sqlite && !string.IsNullOrWhiteSpace(_sqliteDataSourcePath))
            {
                var directory = Path.GetDirectoryName(_sqliteDataSourcePath);
                if (!string.IsNullOrWhiteSpace(directory) && !Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }
            }

            _db.CodeFirst.InitTables<MessageAuditRecord, ClientConnectionHistoryRecord>();
            _schemaEnsured = true;
            await Task.CompletedTask;
        }

        /// <summary>
        /// 初始化数据库表结构，并收敛上次非正常关闭残留的在途状态。
        /// </summary>
        public async Task InitializeAsync()
        {
            await _writeLock.WaitAsync();
            try
            {
                await EnsureSchemaAsync();

                // 启动时自动收敛：修复因上次服务异常关闭或重启导致的在途未决消息状态悬挂
                var suspendedCount = await _db.Updateable<MessageAuditRecord>()
                    .SetColumns(x => x.Status == "Failed")
                    .SetColumns(x => x.ErrorMessage == "服务非正常关闭，未决在途消息已在内存队列中丢失")
                    .SetColumns(x => x.UpdatedAt == DateTime.Now)
                    .Where(x => x.Status == "Queued" || x.Status == "Routing" || x.Status == "Forwarding")
                    .ExecuteCommandAsync();

                if (suspendedCount > 0)
                {
                    _logger.LogWarning("系统启动自愈：已自动收敛 {Count} 条因非正常关闭残留的在途未决消息状态为 [Failed]", suspendedCount);
                }

                _logger.LogInformation("审计持久化初始化成功");

                await WarnWhenArchiveThresholdExceededAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "初始化审计持久化时发生致命异常");
                throw;
            }
            finally
            {
                _writeLock.Release();
            }
        }

        /// <summary>
        /// 写入或更新单条消息审计记录。
        /// </summary>
        /// <summary>
        /// 启动时按配置阈值给出历史数据规模提示。
        /// 阈值只用于提示，不会因为达到阈值而删除数据；按保留天数的清理由 AuditCleanupWorker 独立负责。
        /// </summary>
        private async Task WarnWhenArchiveThresholdExceededAsync()
        {
            try
            {
                if (_options.MessageArchiveThreshold > 0)
                {
                    var messageCount = await _db.Queryable<MessageAuditRecord>().CountAsync();
                    if (messageCount >= _options.MessageArchiveThreshold)
                    {
                        _logger.LogWarning(
                            "消息审计表已有 {MessageCount} 条记录，达到迁移阈值提示 {Threshold}，请安排历史数据迁移",
                            messageCount, _options.MessageArchiveThreshold);
                    }
                }

                if (_options.ClientHistoryArchiveThreshold > 0)
                {
                    var historyCount = await _db.Queryable<ClientConnectionHistoryRecord>().CountAsync();
                    if (historyCount >= _options.ClientHistoryArchiveThreshold)
                    {
                        _logger.LogWarning(
                            "客户端历史表已有 {HistoryCount} 条记录，达到迁移阈值提示 {Threshold}，请安排历史数据迁移",
                            historyCount, _options.ClientHistoryArchiveThreshold);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "统计审计数据规模失败，已跳过迁移阈值提示");
            }
        }

        public async Task RecordMessageAuditAsync(MessageAuditRecord record)
        {
            await _writeLock.WaitAsync();
            try
            {
                await EnsureSchemaAsync();
                await UpsertMessageAuditsInternalAsync(new[] { record });

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "写入消息 {MessageId} 审计记录发生异常", record.MessageId);
                throw;
            }
            finally
            {
                _writeLock.Release();
            }
        }

        /// <summary>
        /// 批量写入或更新消息审计记录。
        /// </summary>
        public async Task RecordMessageAuditsAsync(IReadOnlyList<MessageAuditRecord> records)
        {
            if (records == null || records.Count == 0) return;

            await _writeLock.WaitAsync();
            try
            {
                await EnsureSchemaAsync();
                await UpsertMessageAuditsInternalAsync(records);

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "批量写入 {Count} 条消息审计记录发生异常", records.Count);
                throw;
            }
            finally
            {
                _writeLock.Release();
            }
        }

        private async Task UpsertMessageAuditsInternalAsync(IReadOnlyList<MessageAuditRecord> records)
        {
            if (records.Count == 0)
            {
                return;
            }

            var latestRecords = records
                .GroupBy(x => x.MessageId, StringComparer.Ordinal)
                .Select(g => g.Last())
                .ToList();

            var messageIds = latestRecords
                .Select(x => x.MessageId)
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Distinct(StringComparer.Ordinal)
                .ToList();

            if (messageIds.Count == 0)
            {
                return;
            }

            var existingIds = new HashSet<string>(StringComparer.Ordinal);
            foreach (var idBatch in Chunk(messageIds, GetExistsQueryBatchSize()))
            {
                var batchIds = idBatch.ToList();
                var foundIds = await _db.Queryable<MessageAuditRecord>()
                    .Where(x => batchIds.Contains(x.MessageId))
                    .Select(x => x.MessageId)
                    .ToListAsync();

                foreach (var existingId in foundIds)
                {
                    existingIds.Add(existingId);
                }
            }

            var toInsert = latestRecords
                .Where(x => !existingIds.Contains(x.MessageId))
                .ToList();

            var toUpdate = latestRecords
                .Where(x => existingIds.Contains(x.MessageId))
                .ToList();

            var writeBatchSize = GetWriteBatchSize();
            var tranResult = await _db.UseTranAsync(async () =>
            {
                foreach (var insertBatch in Chunk(toInsert, writeBatchSize))
                {
                    var batchItems = insertBatch.ToList();
                    if (batchItems.Count == 0)
                    {
                        continue;
                    }

                    await _db.Insertable(batchItems).ExecuteCommandAsync();
                }

                foreach (var updateBatch in Chunk(toUpdate, writeBatchSize))
                {
                    var batchItems = updateBatch.ToList();
                    if (batchItems.Count == 0)
                    {
                        continue;
                    }

                    await _db.Updateable(batchItems)
                        .IgnoreColumns(x => new { x.CreatedAt })
                        .ExecuteCommandAsync();
                }
            });

            if (!tranResult.IsSuccess)
            {
                throw tranResult.ErrorException ?? new InvalidOperationException("批量 Upsert 消息审计记录事务失败");
            }
        }

        private int GetExistsQueryBatchSize()
        {
            return ParseDbType(_options.Provider) == DbType.Sqlite
                ? SqliteExistsQueryBatchSize
                : DefaultExistsQueryBatchSize;
        }

        private int GetWriteBatchSize()
        {
            return ParseDbType(_options.Provider) == DbType.Sqlite
                ? SqliteWriteBatchSize
                : DefaultWriteBatchSize;
        }

        private static IEnumerable<List<T>> Chunk<T>(IReadOnlyList<T> source, int batchSize)
        {
            if (source.Count == 0)
            {
                yield break;
            }

            if (batchSize <= 0)
            {
                batchSize = source.Count;
            }

            for (var i = 0; i < source.Count; i += batchSize)
            {
                var count = Math.Min(batchSize, source.Count - i);
                var batch = new List<T>(count);
                for (var j = 0; j < count; j++)
                {
                    batch.Add(source[i + j]);
                }

                yield return batch;
            }
        }

        /// <summary>
        /// 记录一条客户端连接或订阅历史；写入失败只记日志，不向上抛出。
        /// </summary>
        public async Task RecordClientConnectionHistoryAsync(ClientConnectionHistoryRecord record)
        {
            await _writeLock.WaitAsync();
            try
            {
                await EnsureSchemaAsync();
                await _db.Insertable(record).ExecuteCommandAsync();

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "写入客户端 {ClientId} 历史记录发生异常", record.ClientId);
            }
            finally
            {
                _writeLock.Release();
            }
        }

        /// <summary>
        /// 批量记录客户端连接或订阅历史；写入失败只记日志，由调用方决定是否降级。
        /// </summary>
        public async Task RecordClientConnectionHistoriesAsync(IReadOnlyList<ClientConnectionHistoryRecord> records)
        {
            if (records == null || records.Count == 0)
            {
                return;
            }

            await _writeLock.WaitAsync();
            try
            {
                await EnsureSchemaAsync();
                await _db.Insertable(records.ToList()).ExecuteCommandAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "批量写入 {Count} 条客户端历史记录发生异常", records.Count);
                throw;
            }
            finally
            {
                _writeLock.Release();
            }
        }

        /// <summary>
        /// 按消息 ID 精确查询单条消息审计记录。
        /// </summary>
        public async Task<MessageAuditRecord?> GetMessageByIdAsync(string messageId)
        {
            if (string.IsNullOrWhiteSpace(messageId))
            {
                return null;
            }

            try
            {
                await EnsureSchemaAsync();
                return await _db.Queryable<MessageAuditRecord>()
                    .Where(x => x.MessageId == messageId)
                    .SingleAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "按消息 ID {MessageId} 精确查询消息审计记录失败", messageId);
                return null;
            }
        }

        /// <summary>
        /// 分页查询消息审计记录；分页参数在仓储层统一收敛，避免调用方传入超大页长一次性物化全表。
        /// </summary>
        public async Task<(int TotalCount, IReadOnlyList<MessageAuditRecord> Items)> GetPagedMessagesAsync(
            int page,
            int pageSize,
            string? status = null,
            string? topic = null,
            string? sourceClientId = null,
            string? search = null,
            DateTime? startDate = null,
            DateTime? endDate = null)
        {
            NormalizePaging(ref page, ref pageSize);

            try
            {
                await EnsureSchemaAsync();

                var query = _db.Queryable<MessageAuditRecord>()
                    .WhereIF(!string.IsNullOrEmpty(status), x => x.Status == status)
                    .WhereIF(!string.IsNullOrEmpty(topic), x => x.Topic == topic)
                    .WhereIF(!string.IsNullOrEmpty(sourceClientId), x => x.SourceClientId == sourceClientId)
                    .WhereIF(!string.IsNullOrEmpty(search),
                        x => x.MessageId.Contains(search!) ||
                             x.Topic.Contains(search!) ||
                             x.SourceClientId.Contains(search!) ||
                             (x.ErrorMessage != null && x.ErrorMessage.Contains(search!)))
                    .WhereIF(startDate.HasValue, x => x.CreatedAt >= startDate!.Value)
                    .WhereIF(endDate.HasValue, x => x.CreatedAt <= endDate!.Value);

                var totalCount = await query.CountAsync();
                var items = await query
                    .OrderBy(x => x.UpdatedAt, OrderByType.Desc)
                    .Skip((page - 1) * pageSize)
                    .Take(pageSize)
                    .ToListAsync();

                return (totalCount, items);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "分页查询消息审计记录失败");
                return (0, Array.Empty<MessageAuditRecord>());
            }
        }

        /// <summary>
        /// 分页查询客户端连接与订阅历史；分页参数同样在仓储层统一收敛。
        /// </summary>
        public async Task<(int TotalCount, IReadOnlyList<ClientConnectionHistoryRecord> Items)> GetPagedClientHistoryAsync(
            int page,
            int pageSize,
            string? clientId = null,
            string? eventType = null,
            string? search = null)
        {
            NormalizePaging(ref page, ref pageSize);

            try
            {
                await EnsureSchemaAsync();

                var query = _db.Queryable<ClientConnectionHistoryRecord>()
                    .WhereIF(!string.IsNullOrEmpty(clientId), x => x.ClientId == clientId)
                    .WhereIF(!string.IsNullOrEmpty(eventType), x => x.Event == eventType)
                    .WhereIF(!string.IsNullOrEmpty(search),
                        x => x.ClientId.Contains(search!) ||
                             (x.Username != null && x.Username.Contains(search!)) ||
                             x.ConnectionId.Contains(search!) ||
                             (x.Details != null && x.Details.Contains(search!)));

                var totalCount = await query.CountAsync();
                var items = await query
                    .OrderBy(x => x.Timestamp, OrderByType.Desc)
                    .Skip((page - 1) * pageSize)
                    .Take(pageSize)
                    .ToListAsync();

                return (totalCount, items);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "分页查询客户端历史记录失败");
                return (0, Array.Empty<ClientConnectionHistoryRecord>());
            }
        }

        /// <summary>
        /// 获取 Dashboard 汇总数据（总消息数、各状态计数与最近记录）。
        /// </summary>
        public async Task<(
            int TotalMessages,
            int TotalPending,
            int TotalSucceeded,
            int TotalFailed,
            int TotalDeadLetter,
            IReadOnlyList<MessageAuditRecord> RecentItems)> GetDashboardMessageSummaryAsync(int recentCount)
        {
            recentCount = Math.Clamp(recentCount, 1, MaxPageSize);

            try
            {
                await EnsureSchemaAsync();

                var query = _db.Queryable<MessageAuditRecord>();
                var totalMessages = await query.Clone().CountAsync();
                var totalPending = await query.Clone()
                    .Where(x => x.Status == "Queued" || x.Status == "Routing" || x.Status == "Forwarding")
                    .CountAsync();
                var totalSucceeded = await query.Clone().Where(x => x.Status == "Succeeded").CountAsync();
                var totalFailed = await query.Clone().Where(x => x.Status == "Failed").CountAsync();
                var totalDeadLetter = await query.Clone().Where(x => x.Status == "DeadLetter").CountAsync();
                var recentItems = await query.Clone()
                    .OrderBy(x => x.UpdatedAt, OrderByType.Desc)
                    .Take(recentCount)
                    .ToListAsync();

                return (totalMessages, totalPending, totalSucceeded, totalFailed, totalDeadLetter, recentItems);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "获取 Dashboard 审计摘要失败");
                return (0, 0, 0, 0, 0, Array.Empty<MessageAuditRecord>());
            }
        }

        /// <summary>
        /// 删除早于指定时间点的消息审计记录，返回删除条数。
        /// 删除全程持 <see cref="_writeLock"/>，与审计写入互斥。
        /// </summary>
        public async Task<int> DeleteExpiredMessageAuditsAsync(DateTime cutoff)
        {
            await _writeLock.WaitAsync();
            try
            {
                await EnsureSchemaAsync();

                if (!_schemaEnsured)
                {
                    // AutoInitializeSchema=false 且从未初始化过表结构：表可能不存在，清理必须跳过而不是抛异常。
                    return 0;
                }

                var deletedCount = 0;
                var batchSize = GetDeleteBatchSize();

                while (true)
                {
                    // 先查主键再按主键删除：不用 Deleteable().Take(n)，因为 SQLite 的 DELETE ... LIMIT
                    // 依赖编译选项 SQLITE_ENABLE_UPDATE_DELETE_LIMIT，并非所有构建都启用。
                    var expiredIds = await _db.Queryable<MessageAuditRecord>()
                        .Where(x => x.CreatedAt < cutoff)
                        .OrderBy(x => x.CreatedAt)
                        .Select(x => x.MessageId)
                        .Take(batchSize)
                        .ToListAsync();

                    if (expiredIds.Count == 0)
                    {
                        break;
                    }

                    var batchDeleted = 0;
                    foreach (var idChunk in Chunk(expiredIds.Distinct(StringComparer.Ordinal).ToList(), SqliteQueryParameterBatchSize))
                    {
                        batchDeleted += await _db.Deleteable<MessageAuditRecord>()
                            .Where(x => idChunk.Contains(x.MessageId))
                            .ExecuteCommandAsync();
                    }

                    deletedCount += batchDeleted;

                    // 没有任何行被删除说明选中的行已不再满足删除条件（例如并发写入后 CreatedAt 已被更新），
                    // 继续循环只会反复选中同一批主键，必须立即退出。
                    if (batchDeleted == 0)
                    {
                        break;
                    }

                    if (expiredIds.Count < batchSize)
                    {
                        break;
                    }
                }

                return deletedCount;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "删除过期消息审计记录失败（截止时间 {Cutoff:o}）", cutoff);
                throw;
            }
            finally
            {
                _writeLock.Release();
            }
        }

        /// <summary>
        /// 删除早于指定时间点的客户端连接历史记录，返回删除条数。
        /// 删除全程持 <see cref="_writeLock"/>，与历史写入互斥。
        /// </summary>
        public async Task<int> DeleteExpiredClientHistoriesAsync(DateTime cutoff)
        {
            await _writeLock.WaitAsync();
            try
            {
                await EnsureSchemaAsync();

                if (!_schemaEnsured)
                {
                    return 0;
                }

                var deletedCount = 0;
                var batchSize = GetDeleteBatchSize();

                while (true)
                {
                    var expiredIds = await _db.Queryable<ClientConnectionHistoryRecord>()
                        .Where(x => x.Timestamp < cutoff)
                        .OrderBy(x => x.Timestamp)
                        .Select(x => x.Id)
                        .Take(batchSize)
                        .ToListAsync();

                    if (expiredIds.Count == 0)
                    {
                        break;
                    }

                    var batchDeleted = 0;
                    foreach (var idChunk in Chunk(expiredIds.Distinct().ToList(), SqliteQueryParameterBatchSize))
                    {
                        batchDeleted += await _db.Deleteable<ClientConnectionHistoryRecord>()
                            .Where(x => idChunk.Contains(x.Id))
                            .ExecuteCommandAsync();
                    }

                    deletedCount += batchDeleted;

                    // 未删除任何行时立即退出，避免反复选中同一批主键形成死循环。
                    if (batchDeleted == 0)
                    {
                        break;
                    }

                    if (expiredIds.Count < batchSize)
                    {
                        break;
                    }
                }

                return deletedCount;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "删除过期客户端历史记录失败（截止时间 {Cutoff:o}）", cutoff);
                throw;
            }
            finally
            {
                _writeLock.Release();
            }
        }

        /// <summary>
        /// 对 SQLite 执行 VACUUM 回收数据库文件空间。
        /// DELETE 不会缩小 .db 文件，不回收时磁盘占用会停留在历史峰值。
        /// VACUUM 需要独占连接且在部分数据库上耗时可观，因此刻意在 <see cref="_writeLock"/> 之外执行：
        /// 持锁会把审计写入队列堵到内存上限，代价高于让 VACUUM 偶发遇到 SQLite 忙锁后下一轮重试。
        /// 非 SQLite 提供程序没有 VACUUM 语句，直接跳过。
        /// </summary>
        public async Task VacuumAsync()
        {
            if (ParseDbType(_options.Provider) != DbType.Sqlite)
            {
                return;
            }

            try
            {
                await _db.Ado.ExecuteCommandAsync("VACUUM");
                _logger.LogInformation("审计数据库 VACUUM 完成，已回收删除记录占用的文件空间");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "审计数据库 VACUUM 失败，本次未回收文件空间，将在下一次清理后重试");
            }
        }

        private int GetDeleteBatchSize()
        {
            return ParseDbType(_options.Provider) == DbType.Sqlite
                ? SqliteWriteBatchSize
                : SqliteExistsQueryBatchSize;
        }

    }
}
