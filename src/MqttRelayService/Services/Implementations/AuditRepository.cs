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
        private const int SqliteExistsQueryBatchSize = 500;
        private const int SqliteWriteBatchSize = 50;
        private const int DefaultExistsQueryBatchSize = 2000;
        private const int DefaultWriteBatchSize = 200;
        private readonly AuditStorageOptions _options;
        private readonly ILogger<AuditRepository> _logger;
        private readonly SemaphoreSlim _writeLock = new(1, 1);
        private readonly SqlSugarScope _db;
        private readonly string? _sqliteDataSourcePath;

        private bool _schemaEnsured;

        /// <summary>
        /// 正在执行清理的历史状态标志，防止多线程清理任务堆积排队造成线程池饥饿。
        /// </summary>

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

        private static string? ResolveSqliteDataSourcePath(AuditStorageOptions options)
        {
            var dbType = ParseDbType(options.Provider);
            if (dbType != DbType.Sqlite)
            {
                return null;
            }

            var connectionString = options.ConnectionString ?? string.Empty;
            var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            foreach (var part in parts)
            {
                var kv = part.Split('=', 2, StringSplitOptions.TrimEntries);
                if (kv.Length == 2 && kv[0].Equals("Data Source", StringComparison.OrdinalIgnoreCase))
                {
                    var path = kv[1];
                    if (Path.IsPathRooted(path))
                    {
                        return path;
                    }

                    return Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, path));
                }
            }

            return null;
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
                    .OrderBy(x => x.CreatedAt, OrderByType.Desc)
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

        public async Task<(int TotalCount, IReadOnlyList<ClientConnectionHistoryRecord> Items)> GetPagedClientHistoryAsync(
            int page,
            int pageSize,
            string? clientId = null,
            string? eventType = null,
            string? search = null)
        {
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

        public async Task<(
            int TotalMessages,
            int TotalPending,
            int TotalSucceeded,
            int TotalFailed,
            int TotalDeadLetter,
            IReadOnlyList<MessageAuditRecord> RecentItems)> GetDashboardMessageSummaryAsync(int recentCount)
        {
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
                    .OrderBy(x => x.CreatedAt, OrderByType.Desc)
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

    }
}
