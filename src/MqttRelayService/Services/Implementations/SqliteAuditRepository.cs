using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MqttRelayService.Models;
using MqttRelayService.Services.Abstractions;
using SqlSugar;

namespace MqttRelayService.Services.Implementations
{
    /// <summary>
    /// SQLite 物理持久化审计仓储实现类。
    /// 当前通过 SqlSugar 统一数据库访问层，便于后续接入正式数据库。
    /// </summary>
    public class SqliteAuditRepository : ISqliteAuditRepository
    {
        private const string DefaultDbRelativePath = "data/audit.db";
        private const int SqliteDefaultTimeoutSeconds = 5;

        private readonly string _dbDirectory;
        private readonly string _dbFilePath;
        private readonly ILogger<SqliteAuditRepository> _logger;
        private readonly SemaphoreSlim _writeLock = new(1, 1);
        private readonly SqlSugarScope _db;

        private long _messageInsertCounter;
        private long _clientInsertCounter;

        public SqliteAuditRepository(ILogger<SqliteAuditRepository> logger)
            : this(logger, DefaultDbRelativePath)
        {
        }

        internal SqliteAuditRepository(ILogger<SqliteAuditRepository> logger, string dbFilePath)
        {
            _logger = logger;
            _dbFilePath = dbFilePath;
            _dbDirectory = Path.GetDirectoryName(_dbFilePath) ?? "data";
            _db = new SqlSugarScope(new ConnectionConfig
            {
                DbType = DbType.Sqlite,
                ConnectionString = $"Data Source={_dbFilePath};Mode=ReadWriteCreate;Cache=Shared;Pooling=True;Default Timeout={SqliteDefaultTimeoutSeconds};",
                IsAutoCloseConnection = true
            });
        }

        private async Task EnsurePragmasAsync()
        {
            await _db.Ado.ExecuteCommandAsync("PRAGMA busy_timeout = 5000;");
            await _db.Ado.ExecuteCommandAsync("PRAGMA journal_mode = WAL;");
            await _db.Ado.ExecuteCommandAsync("PRAGMA synchronous = NORMAL;");
        }

        public async Task InitializeAsync()
        {
            await _writeLock.WaitAsync();
            try
            {
                if (!Directory.Exists(_dbDirectory))
                {
                    Directory.CreateDirectory(_dbDirectory);
                    _logger.LogInformation("创建数据库目录 {Directory}", _dbDirectory);
                }

                await EnsurePragmasAsync();

                const string createMessageAuditTableSql = @"
                    CREATE TABLE IF NOT EXISTS message_audit (
                        MessageId TEXT PRIMARY KEY,
                        Topic TEXT NOT NULL,
                        SourceClientId TEXT NOT NULL,
                        PayloadSize INTEGER NOT NULL,
                        Payload TEXT,
                        Qos INTEGER NOT NULL,
                        Retain INTEGER NOT NULL,
                        Status TEXT NOT NULL,
                        LatencyMs REAL NOT NULL,
                        RetryCount INTEGER NOT NULL,
                        CreatedAt TEXT NOT NULL,
                        UpdatedAt TEXT NOT NULL,
                        ErrorMessage TEXT
                    );";

                const string createMessageIndicesSql = @"
                    CREATE INDEX IF NOT EXISTS idx_msg_created ON message_audit(CreatedAt DESC);
                    CREATE INDEX IF NOT EXISTS idx_msg_status ON message_audit(Status);
                    CREATE INDEX IF NOT EXISTS idx_msg_topic ON message_audit(Topic);
                    CREATE INDEX IF NOT EXISTS idx_msg_src ON message_audit(SourceClientId);";

                const string createClientHistoryTableSql = @"
                    CREATE TABLE IF NOT EXISTS client_connection_history (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ClientId TEXT NOT NULL,
                        Username TEXT,
                        ConnectionId TEXT NOT NULL,
                        Event TEXT NOT NULL,
                        Details TEXT,
                        Timestamp TEXT NOT NULL
                    );";

                const string createClientIndicesSql = @"
                    CREATE INDEX IF NOT EXISTS idx_client_ts ON client_connection_history(Timestamp DESC);
                    CREATE INDEX IF NOT EXISTS idx_client_id ON client_connection_history(ClientId);
                    CREATE INDEX IF NOT EXISTS idx_client_conn ON client_connection_history(ConnectionId);";

                await _db.Ado.ExecuteCommandAsync(createMessageAuditTableSql);
                await _db.Ado.ExecuteCommandAsync(createMessageIndicesSql);
                await _db.Ado.ExecuteCommandAsync(createClientHistoryTableSql);
                await _db.Ado.ExecuteCommandAsync(createClientIndicesSql);

                _logger.LogInformation("SQLite 物理审计数据库初始化成功");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "初始化 SQLite 物理审计数据库时发生致命异常");
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
                await EnsurePragmasAsync();

                const string sql = @"
                    INSERT INTO message_audit (
                        MessageId, Topic, SourceClientId, PayloadSize, Payload, Qos, Retain,
                        Status, LatencyMs, RetryCount, CreatedAt, UpdatedAt, ErrorMessage
                    ) VALUES (
                        @MessageId, @Topic, @SourceClientId, @PayloadSize, @Payload, @Qos, @Retain,
                        @Status, @LatencyMs, @RetryCount, @CreatedAt, @UpdatedAt, @ErrorMessage
                    ) ON CONFLICT(MessageId) DO UPDATE SET
                        Status = excluded.Status,
                        LatencyMs = excluded.LatencyMs,
                        RetryCount = excluded.RetryCount,
                        UpdatedAt = excluded.UpdatedAt,
                        ErrorMessage = excluded.ErrorMessage;";

                await _db.Ado.ExecuteCommandAsync(sql, new[]
                {
                    new SugarParameter("@MessageId", record.MessageId),
                    new SugarParameter("@Topic", record.Topic),
                    new SugarParameter("@SourceClientId", record.SourceClientId),
                    new SugarParameter("@PayloadSize", record.PayloadSize),
                    new SugarParameter("@Payload", (object?)record.Payload ?? DBNull.Value),
                    new SugarParameter("@Qos", record.Qos),
                    new SugarParameter("@Retain", record.Retain ? 1 : 0),
                    new SugarParameter("@Status", record.Status),
                    new SugarParameter("@LatencyMs", record.LatencyMs),
                    new SugarParameter("@RetryCount", record.RetryCount),
                    new SugarParameter("@CreatedAt", record.CreatedAt.ToString("o")),
                    new SugarParameter("@UpdatedAt", record.UpdatedAt.ToString("o")),
                    new SugarParameter("@ErrorMessage", (object?)record.ErrorMessage ?? DBNull.Value)
                });

                var val = Interlocked.Increment(ref _messageInsertCounter);
                if (val % 100 == 0)
                {
                    _ = Task.Run(() => CleanupHistoryAsync(20000, 5000));
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "写入消息 {MessageId} 审计日志到 SQLite 发生异常", record.MessageId);
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
                await EnsurePragmasAsync();

                const string sql = @"
                    INSERT INTO client_connection_history (
                        ClientId, Username, ConnectionId, Event, Details, Timestamp
                    ) VALUES (
                        @ClientId, @Username, @ConnectionId, @Event, @Details, @Timestamp
                    );";

                await _db.Ado.ExecuteCommandAsync(sql, new[]
                {
                    new SugarParameter("@ClientId", record.ClientId),
                    new SugarParameter("@Username", (object?)record.Username ?? DBNull.Value),
                    new SugarParameter("@ConnectionId", record.ConnectionId),
                    new SugarParameter("@Event", record.Event),
                    new SugarParameter("@Details", (object?)record.Details ?? DBNull.Value),
                    new SugarParameter("@Timestamp", record.Timestamp.ToString("o"))
                });

                var val = Interlocked.Increment(ref _clientInsertCounter);
                if (val % 100 == 0)
                {
                    _ = Task.Run(() => CleanupHistoryAsync(20000, 5000));
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "写入设备 {ClientId} 连接历史记录到 SQLite 发生异常", record.ClientId);
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
                await EnsurePragmasAsync();

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
                _logger.LogError(ex, "SQLite 分页查询消息审计日志失败");
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
                await EnsurePragmasAsync();

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
                _logger.LogError(ex, "SQLite 分页查询客户端连接历史记录失败");
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
                await EnsurePragmasAsync();

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
            await _writeLock.WaitAsync();
            try
            {
                await EnsurePragmasAsync();

                const string cleanMessagesSql = @"
                    DELETE FROM message_audit
                    WHERE MessageId NOT IN (
                        SELECT MessageId FROM message_audit
                        ORDER BY CreatedAt DESC
                        LIMIT @KeepMessagesCount
                    );";

                const string cleanClientHistorySql = @"
                    DELETE FROM client_connection_history
                    WHERE Id NOT IN (
                        SELECT Id FROM client_connection_history
                        ORDER BY Timestamp DESC
                        LIMIT @KeepClientHistoryCount
                    );";

                var deletedMessages = await _db.Ado.ExecuteCommandAsync(cleanMessagesSql, new[]
                {
                    new SugarParameter("@KeepMessagesCount", keepMessagesCount)
                });
                var deletedClients = await _db.Ado.ExecuteCommandAsync(cleanClientHistorySql, new[]
                {
                    new SugarParameter("@KeepClientHistoryCount", keepClientHistoryCount)
                });

                if (deletedMessages > 0 || deletedClients > 0)
                {
                    _logger.LogInformation(
                        "SQLite 历史记录自动 Purge 完成，已清理 {MsgCount} 条消息审计，{ClientCount} 条客户端连接历史",
                        deletedMessages,
                        deletedClients);

                    await _db.Ado.ExecuteCommandAsync("VACUUM;");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "清理 SQLite 历史数据发生异常");
            }
            finally
            {
                _writeLock.Release();
            }
        }
    }
}
