using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using MqttRelayService.Models;
using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Services.Implementations
{
    /// <summary>
    /// SQLite 物理持久化审计仓储实现类
    /// </summary>
    public class SqliteAuditRepository : ISqliteAuditRepository
    {
        private const string DbDirectory = "data";
        private const string DbFilePath = "data/audit.db";
        private readonly string _connectionString;
        private readonly ILogger<SqliteAuditRepository> _logger;
        private readonly SemaphoreSlim _dbLock = new(1, 1);

        private long _messageInsertCounter;
        private long _clientInsertCounter;

        public SqliteAuditRepository(ILogger<SqliteAuditRepository> logger)
        {
            _logger = logger;
            _connectionString = $"Data Source={DbFilePath};Cache=Shared;";
        }

        /// <summary>
        /// 初始化数据库表结构及索引
        /// </summary>
        public async Task InitializeAsync()
        {
            await _dbLock.WaitAsync();
            try
            {
                if (!Directory.Exists(DbDirectory))
                {
                    Directory.CreateDirectory(DbDirectory);
                    _logger.LogInformation("创建数据库目录: {Directory}", DbDirectory);
                }

                using var conn = new SqliteConnection(_connectionString);
                await conn.OpenAsync();

                // 1. 创建消息审计表
                var createMessageAuditTableSql = @"
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

                using var cmd1 = new SqliteCommand(createMessageAuditTableSql, conn);
                await cmd1.ExecuteNonQueryAsync();

                // 2. 创建消息索引以加速检索
                var createMessageIndices = @"
                    CREATE INDEX IF NOT EXISTS idx_msg_created ON message_audit(CreatedAt DESC);
                    CREATE INDEX IF NOT EXISTS idx_msg_status ON message_audit(Status);
                    CREATE INDEX IF NOT EXISTS idx_msg_topic ON message_audit(Topic);
                    CREATE INDEX IF NOT EXISTS idx_msg_src ON message_audit(SourceClientId);
                ";
                using var cmdIdx1 = new SqliteCommand(createMessageIndices, conn);
                await cmdIdx1.ExecuteNonQueryAsync();

                // 3. 创建客户端设备连接历史表
                var createClientHistoryTableSql = @"
                    CREATE TABLE IF NOT EXISTS client_connection_history (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ClientId TEXT NOT NULL,
                        Username TEXT,
                        ConnectionId TEXT NOT NULL,
                        Event TEXT NOT NULL,
                        Details TEXT,
                        Timestamp TEXT NOT NULL
                    );";

                using var cmd2 = new SqliteCommand(createClientHistoryTableSql, conn);
                await cmd2.ExecuteNonQueryAsync();

                // 4. 创建客户端索引以加速检索
                var createClientIndices = @"
                    CREATE INDEX IF NOT EXISTS idx_client_ts ON client_connection_history(Timestamp DESC);
                    CREATE INDEX IF NOT EXISTS idx_client_id ON client_connection_history(ClientId);
                    CREATE INDEX IF NOT EXISTS idx_client_conn ON client_connection_history(ConnectionId);
                ";
                using var cmdIdx2 = new SqliteCommand(createClientIndices, conn);
                await cmdIdx2.ExecuteNonQueryAsync();

                _logger.LogInformation("SQLite 物理审计数据库初始化成功");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "初始化 SQLite 物理审计数据库时发生致命异常");
                throw;
            }
            finally
            {
                _dbLock.Release();
            }
        }

        /// <summary>
        /// 记录或更新一条消息的审计状态（使用 Upsert 逻辑）
        /// </summary>
        public async Task RecordMessageAuditAsync(MessageAuditRecord record)
        {
            await _dbLock.WaitAsync();
            try
            {
                using var conn = new SqliteConnection(_connectionString);
                await conn.OpenAsync();

                var sql = @"
                    INSERT INTO message_audit (
                        MessageId, Topic, SourceClientId, PayloadSize, Payload, Qos, Retain, 
                        Status, LatencyMs, RetryCount, CreatedAt, UpdatedAt, ErrorMessage
                    ) VALUES (
                        $MessageId, $Topic, $SourceClientId, $PayloadSize, $Payload, $Qos, $Retain, 
                        $Status, $LatencyMs, $RetryCount, $CreatedAt, $UpdatedAt, $ErrorMessage
                    ) ON CONFLICT(MessageId) DO UPDATE SET
                        Status = excluded.Status,
                        LatencyMs = excluded.LatencyMs,
                        RetryCount = excluded.RetryCount,
                        UpdatedAt = excluded.UpdatedAt,
                        ErrorMessage = excluded.ErrorMessage;";

                using var cmd = new SqliteCommand(sql, conn);
                cmd.Parameters.AddWithValue("$MessageId", record.MessageId);
                cmd.Parameters.AddWithValue("$Topic", record.Topic);
                cmd.Parameters.AddWithValue("$SourceClientId", record.SourceClientId);
                cmd.Parameters.AddWithValue("$PayloadSize", record.PayloadSize);
                cmd.Parameters.AddWithValue("$Payload", (object?)record.Payload ?? DBNull.Value);
                cmd.Parameters.AddWithValue("$Qos", record.Qos);
                cmd.Parameters.AddWithValue("$Retain", record.Retain ? 1 : 0);
                cmd.Parameters.AddWithValue("$Status", record.Status);
                cmd.Parameters.AddWithValue("$LatencyMs", record.LatencyMs);
                cmd.Parameters.AddWithValue("$RetryCount", record.RetryCount);
                cmd.Parameters.AddWithValue("$CreatedAt", record.CreatedAt.ToString("o"));
                cmd.Parameters.AddWithValue("$UpdatedAt", record.UpdatedAt.ToString("o"));
                cmd.Parameters.AddWithValue("$ErrorMessage", (object?)record.ErrorMessage ?? DBNull.Value);

                await cmd.ExecuteNonQueryAsync();

                // 计数并触发异步自动清理
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
                _dbLock.Release();
            }
        }

        /// <summary>
        /// 记录一条设备连接或订阅变动历史
        /// </summary>
        public async Task RecordClientConnectionHistoryAsync(ClientConnectionHistoryRecord record)
        {
            await _dbLock.WaitAsync();
            try
            {
                using var conn = new SqliteConnection(_connectionString);
                await conn.OpenAsync();

                var sql = @"
                    INSERT INTO client_connection_history (
                        ClientId, Username, ConnectionId, Event, Details, Timestamp
                    ) VALUES (
                        $ClientId, $Username, $ConnectionId, $Event, $Details, $Timestamp
                    );";

                using var cmd = new SqliteCommand(sql, conn);
                cmd.Parameters.AddWithValue("$ClientId", record.ClientId);
                cmd.Parameters.AddWithValue("$Username", (object?)record.Username ?? DBNull.Value);
                cmd.Parameters.AddWithValue("$ConnectionId", record.ConnectionId);
                cmd.Parameters.AddWithValue("$Event", record.Event);
                cmd.Parameters.AddWithValue("$Details", (object?)record.Details ?? DBNull.Value);
                cmd.Parameters.AddWithValue("$Timestamp", record.Timestamp.ToString("o"));

                await cmd.ExecuteNonQueryAsync();

                // 计数并触发异步自动清理
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
                _dbLock.Release();
            }
        }

        /// <summary>
        /// 获取分页的消息审计日志列表
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
            await _dbLock.WaitAsync();
            try
            {
                using var conn = new SqliteConnection(_connectionString);
                await conn.OpenAsync();

                var conditions = new List<string>();
                var parameters = new List<(string Name, object Value)>();

                if (!string.IsNullOrEmpty(status))
                {
                    conditions.Add("Status = $Status");
                    parameters.Add(("$Status", status));
                }

                if (!string.IsNullOrEmpty(topic))
                {
                    conditions.Add("Topic = $Topic");
                    parameters.Add(("$Topic", topic));
                }

                if (!string.IsNullOrEmpty(sourceClientId))
                {
                    conditions.Add("SourceClientId = $SourceClientId");
                    parameters.Add(("$SourceClientId", sourceClientId));
                }

                if (!string.IsNullOrEmpty(search))
                {
                    conditions.Add("(MessageId LIKE $Search OR Topic LIKE $Search OR SourceClientId LIKE $Search OR ErrorMessage LIKE $Search)");
                    parameters.Add(("$Search", $"%{search}%"));
                }

                if (startDate.HasValue)
                {
                    conditions.Add("CreatedAt >= $StartDate");
                    parameters.Add(("$StartDate", startDate.Value.ToString("o")));
                }

                if (endDate.HasValue)
                {
                    conditions.Add("CreatedAt <= $EndDate");
                    parameters.Add(("$EndDate", endDate.Value.ToString("o")));
                }

                var whereClause = conditions.Count > 0 ? "WHERE " + string.Join(" AND ", conditions) : "";

                // 1. 查询符合条件的总条数
                var countSql = $"SELECT COUNT(*) FROM message_audit {whereClause};";
                using var countCmd = new SqliteCommand(countSql, conn);
                foreach (var p in parameters) countCmd.Parameters.AddWithValue(p.Name, p.Value);
                var totalCount = Convert.ToInt32(await countCmd.ExecuteScalarAsync());

                // 2. 分页倒序查询具体条目
                var itemsSql = $@"
                    SELECT MessageId, Topic, SourceClientId, PayloadSize, Payload, Qos, Retain, 
                           Status, LatencyMs, RetryCount, CreatedAt, UpdatedAt, ErrorMessage
                    FROM message_audit
                    {whereClause}
                    ORDER BY CreatedAt DESC
                    LIMIT $Limit OFFSET $Offset;";

                using var itemsCmd = new SqliteCommand(itemsSql, conn);
                foreach (var p in parameters) itemsCmd.Parameters.AddWithValue(p.Name, p.Value);
                itemsCmd.Parameters.AddWithValue("$Limit", pageSize);
                itemsCmd.Parameters.AddWithValue("$Offset", (page - 1) * pageSize);

                var items = new List<MessageAuditRecord>();
                using var reader = await itemsCmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new MessageAuditRecord
                    {
                        MessageId = reader.GetString(0),
                        Topic = reader.GetString(1),
                        SourceClientId = reader.GetString(2),
                        PayloadSize = reader.GetInt32(3),
                        Payload = reader.IsDBNull(4) ? null : reader.GetString(4),
                        Qos = reader.GetInt32(5),
                        Retain = reader.GetInt32(6) == 1,
                        Status = reader.GetString(7),
                        LatencyMs = reader.GetDouble(8),
                        RetryCount = reader.GetInt32(9),
                        CreatedAt = DateTime.Parse(reader.GetString(10)).ToUniversalTime(),
                        UpdatedAt = DateTime.Parse(reader.GetString(11)).ToUniversalTime(),
                        ErrorMessage = reader.IsDBNull(12) ? null : reader.GetString(12)
                    });
                }

                return (totalCount, items);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SQLite 分页查询消息审计日志失败");
                return (0, Array.Empty<MessageAuditRecord>());
            }
            finally
            {
                _dbLock.Release();
            }
        }

        /// <summary>
        /// 获取分页的客户端连接与订阅历史记录列表
        /// </summary>
        public async Task<(int TotalCount, IReadOnlyList<ClientConnectionHistoryRecord> Items)> GetPagedClientHistoryAsync(
            int page,
            int pageSize,
            string? clientId = null,
            string? eventType = null,
            string? search = null)
        {
            await _dbLock.WaitAsync();
            try
            {
                using var conn = new SqliteConnection(_connectionString);
                await conn.OpenAsync();

                var conditions = new List<string>();
                var parameters = new List<(string Name, object Value)>();

                if (!string.IsNullOrEmpty(clientId))
                {
                    conditions.Add("ClientId = $ClientId");
                    parameters.Add(("$ClientId", clientId));
                }

                if (!string.IsNullOrEmpty(eventType))
                {
                    conditions.Add("Event = $Event");
                    parameters.Add(("$Event", eventType));
                }

                if (!string.IsNullOrEmpty(search))
                {
                    conditions.Add("(ClientId LIKE $Search OR Username LIKE $Search OR ConnectionId LIKE $Search OR Details LIKE $Search)");
                    parameters.Add(("$Search", $"%{search}%"));
                }

                var whereClause = conditions.Count > 0 ? "WHERE " + string.Join(" AND ", conditions) : "";

                // 1. 查询符合条件的总条数
                var countSql = $"SELECT COUNT(*) FROM client_connection_history {whereClause};";
                using var countCmd = new SqliteCommand(countSql, conn);
                foreach (var p in parameters) countCmd.Parameters.AddWithValue(p.Name, p.Value);
                var totalCount = Convert.ToInt32(await countCmd.ExecuteScalarAsync());

                // 2. 分页倒序查询具体条目
                var itemsSql = $@"
                    SELECT Id, ClientId, Username, ConnectionId, Event, Details, Timestamp
                    FROM client_connection_history
                    {whereClause}
                    ORDER BY Timestamp DESC
                    LIMIT $Limit OFFSET $Offset;";

                using var itemsCmd = new SqliteCommand(itemsSql, conn);
                foreach (var p in parameters) itemsCmd.Parameters.AddWithValue(p.Name, p.Value);
                itemsCmd.Parameters.AddWithValue("$Limit", pageSize);
                itemsCmd.Parameters.AddWithValue("$Offset", (page - 1) * pageSize);

                var items = new List<ClientConnectionHistoryRecord>();
                using var reader = await itemsCmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new ClientConnectionHistoryRecord
                    {
                        Id = reader.GetInt64(0),
                        ClientId = reader.GetString(1),
                        Username = reader.IsDBNull(2) ? null : reader.GetString(2),
                        ConnectionId = reader.GetString(3),
                        Event = reader.GetString(4),
                        Details = reader.IsDBNull(5) ? null : reader.GetString(5),
                        Timestamp = DateTime.Parse(reader.GetString(6)).ToUniversalTime()
                    });
                }

                return (totalCount, items);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SQLite 分页查询客户端连接历史记录失败");
                return (0, Array.Empty<ClientConnectionHistoryRecord>());
            }
            finally
            {
                _dbLock.Release();
            }
        }

        /// <summary>
        /// 清理历史数据，保持数据库在健康大小区间（滑动容量清理）
        /// </summary>
        public async Task CleanupHistoryAsync(int keepMessagesCount, int keepClientHistoryCount)
        {
            await _dbLock.WaitAsync();
            try
            {
                using var conn = new SqliteConnection(_connectionString);
                await conn.OpenAsync();

                // 1. 清理超限的消息审计记录，倒序保留指定数量
                var cleanMessagesSql = @"
                    DELETE FROM message_audit 
                    WHERE MessageId NOT IN (
                        SELECT MessageId FROM message_audit 
                        ORDER BY CreatedAt DESC 
                        LIMIT $KeepMessagesCount
                    );";
                using var cmd1 = new SqliteCommand(cleanMessagesSql, conn);
                cmd1.Parameters.AddWithValue("$KeepMessagesCount", keepMessagesCount);
                var deletedMessages = await cmd1.ExecuteNonQueryAsync();

                // 2. 清理超限的客户端连接历史记录，倒序保留指定数量
                var cleanClientHistorySql = @"
                    DELETE FROM client_connection_history 
                    WHERE Id NOT IN (
                        SELECT Id FROM client_connection_history 
                        ORDER BY Timestamp DESC 
                        LIMIT $KeepClientHistoryCount
                    );";
                using var cmd2 = new SqliteCommand(cleanClientHistorySql, conn);
                cmd2.Parameters.AddWithValue("$KeepClientHistoryCount", keepClientHistoryCount);
                var deletedClients = await cmd2.ExecuteNonQueryAsync();

                if (deletedMessages > 0 || deletedClients > 0)
                {
                    _logger.LogInformation("SQLite 历史记录自动 Purge 完成，已清理 {MsgCount} 条消息审计，{ClientCount} 条客户端连接历史",
                        deletedMessages, deletedClients);
                    
                    // 执行 VACUUM 释放物理空间
                    using var vacuumCmd = new SqliteCommand("VACUUM;", conn);
                    await vacuumCmd.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "清理 SQLite 历史数据发生异常");
            }
            finally
            {
                _dbLock.Release();
            }
        }
    }
}
