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
        private readonly AuditStorageOptions _options;
        private readonly ILogger<AuditRepository> _logger;
        private readonly SemaphoreSlim _writeLock = new(1, 1);
        private readonly SqlSugarScope _db;
        private readonly string? _sqliteDataSourcePath;

        private long _messageInsertCounter;
        private long _clientInsertCounter;
        private bool _schemaEnsured;

        /// <summary>
        /// 正在执行清理的历史状态标志，防止多线程清理任务堆积排队造成线程池饥饿。
        /// </summary>
        private int _isCleaningUp = 0;

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

                var exists = await _db.Queryable<MessageAuditRecord>().AnyAsync(x => x.MessageId == record.MessageId);
                if (exists)
                {
                    await _db.Updateable(record)
                        .IgnoreColumns(x => new { x.CreatedAt })
                        .ExecuteCommandAsync();
                }
                else
                {
                    await _db.Insertable(record).ExecuteCommandAsync();
                }

                var val = Interlocked.Increment(ref _messageInsertCounter);
                if (val % 5000 == 0)
                {
                    _ = Task.Run(() => CleanupHistoryAsync(_options.MessageRetentionCount, _options.ClientHistoryRetentionCount));
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "写入消息 {MessageId} 审计记录发生异常", record.MessageId);
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

                // 使用事务进行高性能批量写入
                await _db.UseTranAsync(async () =>
                {
                    foreach (var record in records)
                    {
                        var exists = await _db.Queryable<MessageAuditRecord>().AnyAsync(x => x.MessageId == record.MessageId);
                        if (exists)
                        {
                            await _db.Updateable(record)
                                .IgnoreColumns(x => new { x.CreatedAt })
                                .ExecuteCommandAsync();
                        }
                        else
                        {
                            await _db.Insertable(record).ExecuteCommandAsync();
                        }
                    }
                });

                var val = Interlocked.Add(ref _messageInsertCounter, records.Count);
                if (val / 5000 > (val - records.Count) / 5000)
                {
                    _ = Task.Run(() => CleanupHistoryAsync(_options.MessageRetentionCount, _options.ClientHistoryRetentionCount));
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "批量写入 {Count} 条消息审计记录发生异常", records.Count);
            }
            finally
            {
                _writeLock.Release();
            }
        }

        public async Task RecordClientConnectionHistoryAsync(ClientConnectionHistoryRecord record)
        {
            await _writeLock.WaitAsync();
            try
            {
                await EnsureSchemaAsync();
                await _db.Insertable(record).ExecuteCommandAsync();

                var val = Interlocked.Increment(ref _clientInsertCounter);
                if (val % 5000 == 0)
                {
                    _ = Task.Run(() => CleanupHistoryAsync(_options.MessageRetentionCount, _options.ClientHistoryRetentionCount));
                }
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
                var totalSucceeded = await query.Clone().Where(x => x.Status == "Succeeded").CountAsync();
                var totalFailed = await query.Clone().Where(x => x.Status == "Failed").CountAsync();
                var totalDeadLetter = await query.Clone().Where(x => x.Status == "DeadLetter").CountAsync();
                var recentItems = await query.Clone()
                    .OrderBy(x => x.CreatedAt, OrderByType.Desc)
                    .Take(recentCount)
                    .ToListAsync();

                return (totalMessages, totalSucceeded, totalFailed, totalDeadLetter, recentItems);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "获取 Dashboard 审计摘要失败");
                return (0, 0, 0, 0, Array.Empty<MessageAuditRecord>());
            }
        }

        public async Task CleanupHistoryAsync(int keepMessagesCount, int keepClientHistoryCount)
        {
            if (Interlocked.Exchange(ref _isCleaningUp, 1) == 1)
            {
                // 已经有清理任务在运行，直接丢弃本次触发，防止并发堆积造成线程池饥饿
                return;
            }

            await _writeLock.WaitAsync();
            try
            {
                await EnsureSchemaAsync();

                var deletedMessages = 0;
                var deletedClients = 0;

                // 使用 ORM 方法获取最新的消息审计边界时间
                var boundaryMsg = await _db.Queryable<MessageAuditRecord>()
                    .OrderBy(x => x.CreatedAt, OrderByType.Desc)
                    .Skip(keepMessagesCount - 1)
                    .Take(1)
                    .FirstAsync();

                // 使用 ORM 方法获取最新的客户端历史边界时间
                var boundaryClient = await _db.Queryable<ClientConnectionHistoryRecord>()
                    .OrderBy(x => x.Timestamp, OrderByType.Desc)
                    .Skip(keepClientHistoryCount - 1)
                    .Take(1)
                    .FirstAsync();

                await _db.UseTranAsync(async () =>
                {
                    if (boundaryMsg != null)
                    {
                        deletedMessages = await _db.Deleteable<MessageAuditRecord>()
                            .Where(x => x.CreatedAt < boundaryMsg.CreatedAt)
                            .ExecuteCommandAsync();
                    }

                    if (boundaryClient != null)
                    {
                        deletedClients = await _db.Deleteable<ClientConnectionHistoryRecord>()
                            .Where(x => x.Timestamp < boundaryClient.Timestamp)
                            .ExecuteCommandAsync();
                    }
                });

                if (deletedMessages > 0 || deletedClients > 0)
                {
                    _logger.LogInformation(
                        "审计历史自动清理完成，已清理 {MsgCount} 条消息记录，{ClientCount} 条客户端历史",
                        deletedMessages,
                        deletedClients);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "清理审计历史数据发生异常");
            }
            finally
            {
                _writeLock.Release();
                Interlocked.Exchange(ref _isCleaningUp, 0);
            }
        }
    }
}
