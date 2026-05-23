using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Services.Implementations
{
    /// <summary>
    /// 指标统计与收集服务实现，负责原子计数器维护、滑动窗口日志、历史吞吐快照和系统诊断。
    /// </summary>
    public class MetricsService : IMetricsService, IDisposable
    {
        private const int MaxLogCount = 100;
        private const int MaxHistorySnapshots = 60; // 2秒一次，保存120秒（2分钟）的历史

        private readonly InMemoryMessageQueue _queue;
        private readonly IClientRegistry _clientRegistry;
        private readonly ServiceOptions _serviceOptions;
        private readonly MqttOptions _mqttOptions;
        private readonly ReliabilityOptions _reliabilityOptions;
        private readonly IAuditRepository? _auditRepository;
        private readonly ILogger<MetricsService> _logger;
        private readonly CancellationTokenSource _auditWriterCts = new();
        private readonly ConcurrentDictionary<string, MessageAuditRecord> _pendingMessageAudits = new();
        private readonly SemaphoreSlim _pendingAuditSignal = new(0);
        private readonly SemaphoreSlim _dashboardBaselineLock = new(1, 1);
        private readonly Task? _auditWriterTask;
        private int _auditFlushRequested;
        private bool _dashboardBaselineInitialized;
        private long _dashboardBaselineReceived;
        private long _dashboardBaselineSucceeded;
        private long _dashboardBaselineFailed;
        private long _dashboardBaselineDeadLetter;

        // 原子计数器
        private long _totalReceived;
        private long _totalRejected;
        private long _totalSucceeded;
        private long _totalFailed;
        private long _totalDeadLetter;
        private long _totalRetries;

        // 历史统计变量，用于增量计算
        private long _lastReceived;
        private long _lastSucceeded;
        private long _lastFailed;
        private long _lastDeadLetter;

        // 线程安全滑动日志窗口，通过消息 ID 索引以跟踪最终状态
        private readonly ConcurrentDictionary<string, MessageLogEntry> _messageLogs = new();
        private readonly ConcurrentQueue<string> _messageLogKeys = new();

        // 线程安全滑动载荷缓存，限制最大保存100条以防内存溢出
        private readonly ConcurrentDictionary<string, string> _payloads = new();
        private readonly ConcurrentQueue<string> _payloadKeys = new();

        // 历史趋势环形缓冲区
        private readonly List<object> _historySnapshots = new();
        private readonly object _historyLock = new();

        // 定时快照采样器
        private readonly Timer _snapshotTimer;
        private readonly DateTime _startTime = DateTime.Now;

        /// <summary>
        /// 兼容旧版本的构造函数，主要用于测试
        /// </summary>
        public MetricsService(
            InMemoryMessageQueue queue,
            IClientRegistry clientRegistry,
            IOptions<ServiceOptions> serviceOptions,
            IOptions<MqttOptions> mqttOptions,
            IOptions<ReliabilityOptions> reliabilityOptions,
            ILogger<MetricsService> logger)
            : this(queue, clientRegistry, serviceOptions, mqttOptions, reliabilityOptions, null, logger)
        {
        }

        /// <summary>
        /// 构造指标统计与收集服务
        /// </summary>
        public MetricsService(
            InMemoryMessageQueue queue,
            IClientRegistry clientRegistry,
            IOptions<ServiceOptions> serviceOptions,
            IOptions<MqttOptions> mqttOptions,
            IOptions<ReliabilityOptions> reliabilityOptions,
            IAuditRepository? auditRepository,
            ILogger<MetricsService> logger)
        {
            _queue = queue;
            _clientRegistry = clientRegistry;
            _serviceOptions = serviceOptions.Value;
            _mqttOptions = mqttOptions.Value;
            _reliabilityOptions = reliabilityOptions.Value;
            _auditRepository = auditRepository;
            _logger = logger;

            // 每2秒执行一次采样快照，用于前端实时波形图展示
            _snapshotTimer = new Timer(TakeSnapshot, null, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2));
            if (_auditRepository != null)
            {
                _auditWriterTask = Task.Run(ProcessPendingAuditsAsync);
            }

            _logger.LogInformation("主服务内置监控指标收集服务已启动");
        }

        /// <summary>
        /// 记录一条消息成功入队
        /// </summary>
        public void RecordReceived(ForwardMessage message, bool isFirstReceipt = true)
        {
            CachePayload(message.MessageId, message.RouteContext.Payload);

            AddOrUpdateLog(new MessageLogEntry
            {
                MessageId = message.MessageId,
                Topic = message.RouteContext.Topic,
                SourceClientId = message.RouteContext.SourceClientId ?? "System",
                PayloadSize = message.RouteContext.Payload?.Length ?? 0,
                Qos = message.RouteContext.QoS,
                Retain = message.RouteContext.Retain,
                Status = "Queued",
                IsSubscriberHit = false,
                LatencyMs = 0.0,
                RetryCount = message.RetryCount,
                Timestamp = DateTime.Now.ToString("o"),
                ErrorMessage = string.Empty,
                SystemTimestamp = DateTime.Now
            });

            if (!isFirstReceipt)
            {
                return;
            }

            Interlocked.Increment(ref _totalReceived);

            EnqueueMessageAudit(new MessageAuditRecord
            {
                MessageId = message.MessageId,
                Topic = message.RouteContext.Topic,
                SourceClientId = message.RouteContext.SourceClientId ?? "System",
                PayloadSize = message.RouteContext.Payload?.Length ?? 0,
                Payload = GetPayload(message.MessageId),
                Qos = message.RouteContext.QoS,
                Retain = message.RouteContext.Retain,
                Status = "Queued",
                IsSubscriberHit = false,
                LatencyMs = 0.0,
                RetryCount = message.RetryCount,
                CreatedAt = message.RouteContext.Timestamp,
                UpdatedAt = DateTime.Now
            });
        }

        /// <summary>
        /// 记录由于队列满载导致的消息被丢弃事件
        /// </summary>
        public void RecordRejected()
        {
            Interlocked.Increment(ref _totalRejected);
        }

        /// <summary>
        /// 记录消息转发结果
        /// </summary>
        public void RecordForwarded(MqttRelayService.Models.RouteContext context, bool success, int retryCount, double latencyMs, bool isSubscriberHit = false)
        {
            if (success)
            {
                Interlocked.Increment(ref _totalSucceeded);
            }
            else
            {
                Interlocked.Increment(ref _totalFailed);
            }

            if (retryCount > 0)
            {
                Interlocked.Add(ref _totalRetries, retryCount);
            }

            CachePayload(context.MessageId, context.Payload);

            AddOrUpdateLog(new MessageLogEntry
            {
                MessageId = context.MessageId,
                Topic = context.Topic,
                SourceClientId = context.SourceClientId ?? "System",
                PayloadSize = context.Payload?.Length ?? 0,
                Qos = context.QoS,
                Retain = context.Retain,
                Status = success ? "Succeeded" : "Failed",
                IsSubscriberHit = isSubscriberHit,
                LatencyMs = Math.Round(latencyMs, 2),
                RetryCount = retryCount,
                Timestamp = DateTime.Now.ToString("o"),
                ErrorMessage = success ? string.Empty : "转发注入失败，等待重试或移入死信",
                SystemTimestamp = DateTime.Now
            });

            EnqueueMessageAudit(new MessageAuditRecord
            {
                MessageId = context.MessageId,
                Topic = context.Topic,
                SourceClientId = context.SourceClientId ?? "System",
                PayloadSize = context.Payload?.Length ?? 0,
                Payload = GetPayload(context.MessageId),
                Qos = context.QoS,
                Retain = context.Retain,
                Status = success ? "Succeeded" : "Failed",
                IsSubscriberHit = isSubscriberHit,
                LatencyMs = Math.Round(latencyMs, 2),
                RetryCount = retryCount,
                CreatedAt = context.Timestamp,
                UpdatedAt = DateTime.Now,
                ErrorMessage = success ? null : "转发注入失败，等待重试或移入死信"
            });
        }

        /// <summary>
        /// 记录一条消息进入死信队列事件
        /// </summary>
        public void RecordDeadLetter(DeadLetterRecord record)
        {
            Interlocked.Increment(ref _totalDeadLetter);

            if (!string.IsNullOrEmpty(record.PayloadBase64))
            {
                try
                {
                    var payloadBytes = Convert.FromBase64String(record.PayloadBase64);
                    CachePayload(record.MessageId, payloadBytes);
                }
                catch
                {
                    _payloads[record.MessageId] = "[死信载荷格式非有效 Base64 字符串]";
                }
            }

            AddOrUpdateLog(new MessageLogEntry
            {
                MessageId = record.MessageId,
                Topic = record.Topic,
                SourceClientId = record.SourceClientId ?? "System",
                PayloadSize = (int)(record.PayloadBase64?.Length * 3.0 / 4.0 ?? 0), // 估算字节数
                Qos = 0,
                Retain = false,
                Status = "DeadLetter",
                IsSubscriberHit = false,
                LatencyMs = Math.Round((record.LastFailedAt - record.FirstReceivedAt).TotalMilliseconds, 2),
                RetryCount = record.RetryCount,
                Timestamp = DateTime.Now.ToString("o"),
                ErrorMessage = record.FailureReason ?? "达到重试上限",
                SystemTimestamp = DateTime.Now
            });

            EnqueueMessageAudit(new MessageAuditRecord
            {
                MessageId = record.MessageId,
                Topic = record.Topic,
                SourceClientId = record.SourceClientId ?? "System",
                PayloadSize = (int)(record.PayloadBase64?.Length * 3.0 / 4.0 ?? 0),
                Payload = GetPayload(record.MessageId),
                Qos = 0,
                Retain = false,
                Status = "DeadLetter",
                IsSubscriberHit = false,
                LatencyMs = Math.Round((record.LastFailedAt - record.FirstReceivedAt).TotalMilliseconds, 2),
                RetryCount = record.RetryCount,
                CreatedAt = record.FirstReceivedAt,
                UpdatedAt = DateTime.Now,
                ErrorMessage = record.FailureReason ?? "达到重试上限"
            });
        }

        private void EnqueueMessageAudit(MessageAuditRecord record)
        {
            if (_auditRepository == null || string.IsNullOrEmpty(record.MessageId))
            {
                return;
            }

            _pendingMessageAudits.AddOrUpdate(
                record.MessageId,
                record,
                (_, existing) => MergeAuditRecord(existing, record));

            if (Interlocked.Exchange(ref _auditFlushRequested, 1) == 0)
            {
                _pendingAuditSignal.Release();
            }
        }

        private async Task ProcessPendingAuditsAsync()
        {
            while (!_auditWriterCts.Token.IsCancellationRequested)
            {
                try
                {
                    try
                    {
                        await _pendingAuditSignal.WaitAsync(_auditWriterCts.Token);
                    }
                    catch (OperationCanceledException) when (_auditWriterCts.IsCancellationRequested)
                    {
                        break;
                    }

                    Interlocked.Exchange(ref _auditFlushRequested, 0);
                    await FlushPendingAuditsAsync();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "后台审计日志写入循环发生未捕获致命异常");
                }
            }

            try
            {
                await FlushPendingAuditsAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "服务停机时排空审计日志发生异常");
            }
        }

        private async Task FlushPendingAuditsAsync()
        {
            if (_auditRepository == null)
            {
                return;
            }

            while (!_pendingMessageAudits.IsEmpty)
            {
                var snapshot = _pendingMessageAudits.ToArray();
                if (snapshot.Length == 0)
                {
                    break;
                }

                const int batchSize = 100;
                var batch = new List<MessageAuditRecord>(batchSize);

                foreach (var pair in snapshot)
                {
                    if (!_pendingMessageAudits.TryRemove(pair.Key, out var latestRecord))
                    {
                        continue;
                    }

                    batch.Add(latestRecord);

                    if (batch.Count >= batchSize)
                    {
                        await WriteAuditBatchOrRequeueAsync(batch);
                        batch.Clear();
                    }
                }

                if (batch.Count > 0)
                {
                    await WriteAuditBatchOrRequeueAsync(batch);
                    batch.Clear();
                }

                // 每次刷完一轮后，进行短暂的 Yield 释放 CPU 占用，防止高吞吐下完全堵死其他线程的数据库查询
                await Task.Yield();
            }
        }

        /// <summary>
        /// 在服务启动阶段从审计库加载一次累计基线，后续运行期继续只使用内存原子计数增量。
        /// </summary>
        public async Task InitializeDashboardCountersFromAuditAsync()
        {
            if (_auditRepository == null || _dashboardBaselineInitialized)
            {
                return;
            }

            await _dashboardBaselineLock.WaitAsync();
            try
            {
                if (_dashboardBaselineInitialized)
                {
                    return;
                }

                var summary = await _auditRepository.GetDashboardMessageSummaryAsync(1);
                _dashboardBaselineReceived = Math.Max(0, summary.TotalMessages);
                _dashboardBaselineSucceeded = Math.Max(0, summary.TotalSucceeded);
                _dashboardBaselineFailed = Math.Max(0, summary.TotalFailed);
                _dashboardBaselineDeadLetter = Math.Max(0, summary.TotalDeadLetter);
                _dashboardBaselineInitialized = true;
            }
            finally
            {
                _dashboardBaselineLock.Release();
            }
        }

        private async Task WriteAuditBatchOrRequeueAsync(IReadOnlyCollection<MessageAuditRecord> batch)
        {
            if (_auditRepository == null || batch.Count == 0)
            {
                return;
            }

            try
            {
                await _auditRepository.RecordMessageAuditsAsync(batch.ToList());
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "后台批量写入 {Count} 条消息审计记录时发生异常，已重新并入待写队列", batch.Count);
                foreach (var record in batch)
                {
                    EnqueueMessageAudit(record);
                }
            }
        }

        private void CachePayload(string messageId, byte[]? payload)
        {
            if (string.IsNullOrEmpty(messageId)) return;
            if (payload == null || payload.Length == 0)
            {
                _payloads[messageId] = "[空载荷]";
                return;
            }

            try
            {
                // 限制缓存长度，防止超大消息导致内容爆炸
                string payloadText;
                if (payload.Length > 8192)
                {
                    payloadText = $"[载荷超过8KB，已截断] {System.Text.Encoding.UTF8.GetString(payload, 0, 8192)}...";
                }
                else
                {
                    payloadText = System.Text.Encoding.UTF8.GetString(payload);
                }

                _payloads[messageId] = payloadText;
                _payloadKeys.Enqueue(messageId);

                while (_payloadKeys.Count > MaxLogCount)
                {
                    if (_payloadKeys.TryDequeue(out var oldKey))
                    {
                        _payloads.TryRemove(oldKey, out _);
                    }
                }
            }
            catch
            {
                _payloads[messageId] = $"[载荷无法解析为 UTF-8 文本，大小: {payload.Length} 字节]";
            }
        }

        /// <summary>
        /// 根据消息 ID 获取最近缓存的格式化载荷内容
        /// </summary>
        public string? GetPayload(string messageId)
        {
            if (string.IsNullOrEmpty(messageId)) return null;
            _payloads.TryGetValue(messageId, out var payload);
            return payload;
        }

        /// <summary>
        /// 线程安全地添加或更新内存消息日志。
        /// 通过 MessageId 对同条消息进行就地状态覆盖更新，仅保存最终最新状态，同时基于滑动窗口容量上限 (100) 驱逐最旧数据。
        /// </summary>
        private void AddOrUpdateLog(MessageLogEntry entry)
        {
            if (string.IsNullOrEmpty(entry.MessageId)) return;

            _messageLogs.AddOrUpdate(entry.MessageId,
                // 新增消息记录工厂：将 ID 记录至滑动窗口队列以管理生存期
                id =>
                {
                    _messageLogKeys.Enqueue(id);
                    return entry;
                },
                // 更新现有消息记录工厂：就地修改所有可变状态字段
                (id, existing) =>
                {
                    existing.Status = entry.Status;
                    existing.IsSubscriberHit = entry.IsSubscriberHit;
                    existing.LatencyMs = entry.LatencyMs;
                    existing.RetryCount = entry.RetryCount;
                    existing.Timestamp = entry.Timestamp;
                    existing.ErrorMessage = entry.ErrorMessage;
                    existing.SystemTimestamp = entry.SystemTimestamp;

                    // 同步可能需要更新的消息配置信息
                    existing.Topic = entry.Topic;
                    existing.SourceClientId = entry.SourceClientId;
                    existing.PayloadSize = entry.PayloadSize;
                    existing.Qos = entry.Qos;
                    existing.Retain = entry.Retain;

                    return existing;
                }
            );

            // 检查内存中跟踪的消息上限，当超出 100 条时驱逐最旧插入的数据
            while (_messageLogs.Count > MaxLogCount)
            {
                if (_messageLogKeys.TryDequeue(out var oldKey))
                {
                    _messageLogs.TryRemove(oldKey, out _);
                }
                else
                {
                    break;
                }
            }
        }

        private static MessageAuditRecord MergeAuditRecord(MessageAuditRecord existing, MessageAuditRecord incoming)
        {
            if (ShouldKeepExistingAuditState(existing.Status, incoming.Status))
            {
                existing.UpdatedAt = incoming.UpdatedAt;
                existing.IsSubscriberHit = existing.IsSubscriberHit || incoming.IsSubscriberHit;
                existing.RetryCount = Math.Max(existing.RetryCount, incoming.RetryCount);
                existing.LatencyMs = Math.Max(existing.LatencyMs, incoming.LatencyMs);
                existing.ErrorMessage = PreferNonEmpty(existing.ErrorMessage, incoming.ErrorMessage);
                existing.Payload = PreferNonEmpty(existing.Payload, incoming.Payload);
                return existing;
            }

            existing.Topic = incoming.Topic;
            existing.SourceClientId = incoming.SourceClientId;
            existing.PayloadSize = incoming.PayloadSize;
            existing.Payload = PreferNonEmpty(incoming.Payload, existing.Payload);
            existing.Qos = incoming.Qos;
            existing.Retain = incoming.Retain;
            existing.Status = incoming.Status;
            existing.IsSubscriberHit = incoming.IsSubscriberHit;
            existing.LatencyMs = incoming.LatencyMs;
            existing.RetryCount = incoming.RetryCount;
            existing.UpdatedAt = incoming.UpdatedAt;
            existing.ErrorMessage = incoming.ErrorMessage;
            return existing;
        }

        private static bool ShouldKeepExistingAuditState(string existingStatus, string incomingStatus)
        {
            return GetStatusPriority(existingStatus) > GetStatusPriority(incomingStatus);
        }

        private static int GetStatusPriority(string status)
        {
            return status switch
            {
                "DeadLetter" => 5,
                "Failed" => 4,
                "Succeeded" => 4,
                "Forwarding" => 3,
                "Routing" => 2,
                "Queued" => 1,
                _ => 0
            };
        }

        private static string? PreferNonEmpty(string? preferred, string? fallback)
        {
            return string.IsNullOrWhiteSpace(preferred) ? fallback : preferred;
        }

        private void TakeSnapshot(object? state)
        {
            try
            {
                var curReceived = Interlocked.Read(ref _totalReceived);
                var curSucceeded = Interlocked.Read(ref _totalSucceeded);
                var curFailed = Interlocked.Read(ref _totalFailed);
                var curDeadLetter = Interlocked.Read(ref _totalDeadLetter);

                // 计算2秒间隔内的增量（即每2秒处理的消息速率）
                var receivedRate = Math.Max(0, curReceived - _lastReceived);
                var succeededRate = Math.Max(0, curSucceeded - _lastSucceeded);
                var failedRate = Math.Max(0, curFailed - _lastFailed);
                var deadLetterRate = Math.Max(0, curDeadLetter - _lastDeadLetter);

                _lastReceived = curReceived;
                _lastSucceeded = curSucceeded;
                _lastFailed = curFailed;
                _lastDeadLetter = curDeadLetter;

                var snapshot = new
                {
                    Time = DateTime.Now.ToString("HH:mm:ss"),
                    Received = receivedRate,
                    Succeeded = succeededRate,
                    Failed = failedRate,
                    DeadLetter = deadLetterRate,
                    QueueSize = _queue.Count
                };

                lock (_historyLock)
                {
                    _historySnapshots.Add(snapshot);
                    if (_historySnapshots.Count > MaxHistorySnapshots)
                    {
                        _historySnapshots.RemoveAt(0);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "收集实时性能快照时发生异常");
            }
        }

        /// <summary>
        /// 获取当前所有统计指标的聚合快照，用于 Dashboard 前端展示
        /// </summary>
        public async Task<object> GetDashboardDataAsync()
        {
            var activeSessions = await _clientRegistry.GetAllSessionsAsync();
            var process = Process.GetCurrentProcess();

            // Uptime 格式化
            var uptime = DateTime.Now - _startTime;
            var uptimeString = $"{(int)uptime.TotalDays}天 {uptime.Hours:D2}:{uptime.Minutes:D2}:{uptime.Seconds:D2}";

            // 提取在线客户端列表
            var clients = activeSessions.Select(s => new
            {
                s.ClientId,
                s.Username,
                s.ConnectionId,
                ConnectedAt = s.ConnectedAt.ToString("yyyy-MM-dd HH:mm:ss"),
                LastActivityAt = s.LastActivityAt.ToString("yyyy-MM-dd HH:mm:ss"),
                Status = s.Status.ToString(),
                Subscriptions = s.Subscriptions.ToList()
            }).ToList();

            // 增量计算吞吐
            List<object> history;
            lock (_historyLock)
            {
                history = _historySnapshots.ToList();
            }

            var totalReceived = _dashboardBaselineReceived + Interlocked.Read(ref _totalReceived);
            var totalSucceeded = _dashboardBaselineSucceeded + Interlocked.Read(ref _totalSucceeded);
            var totalFailed = _dashboardBaselineFailed + Interlocked.Read(ref _totalFailed);
            var totalDeadLetter = _dashboardBaselineDeadLetter + Interlocked.Read(ref _totalDeadLetter);
            var totalPending = _queue.Count;
            IEnumerable<object> logs = _messageLogs.Values.OrderByDescending(x => x.SystemTimestamp).Cast<object>().ToList();

            return new
            {
                System = new
                {
                    ServiceName = _serviceOptions.Name,
                    MqttPort = _mqttOptions.TcpPort,
                    Uptime = uptimeString,
                    OsVersion = GetFriendlyOsDescription(),
                    DotNetVersion = RuntimeInformation.FrameworkDescription,
                    CpuThreads = Environment.ProcessorCount,
                    MemoryUsageMb = Math.Round(process.WorkingSet64 / 1024.0 / 1024.0, 2),
                    Timestamp = DateTime.Now.ToString("o")
                },
                Counters = new
                {
                    TotalReceived = totalReceived,
                    TotalRejected = Interlocked.Read(ref _totalRejected),
                    TotalPending = totalPending,
                    TotalSucceeded = totalSucceeded,
                    TotalFailed = totalFailed,
                    TotalDeadLetter = totalDeadLetter,
                    TotalRetries = Interlocked.Read(ref _totalRetries)
                },
                Queue = new
                {
                    Current = _queue.Count,
                    Capacity = _queue.Capacity,
                    Peak = _queue.PeakCount,
                    CongestionPercentage = Math.Round((double)_queue.Count / _queue.Capacity * 100, 2)
                },
                Clients = new
                {
                    Count = activeSessions.Count,
                    List = clients
                },
                History = history,
                Logs = logs,
                Configuration = new
                {
                    QueueCapacity = _reliabilityOptions.QueueCapacity,
                    MaxConcurrentHandlers = _reliabilityOptions.MaxConcurrentHandlers,
                    MaxRetryCount = _reliabilityOptions.MaxRetryCount,
                    MaxPendingRetryTasks = _reliabilityOptions.MaxPendingRetryTasks,
                    EnableDeadLetter = _reliabilityOptions.EnableDeadLetter,
                    DeadLetterPath = _reliabilityOptions.DeadLetterPath
                }
            };
        }

        /// <summary>
        /// 获取人性化的操作系统描述。对于 Windows 11，由于其内核大版本号依然是 10.0，
        /// 需要根据 Build 号（>= 22000 为 Win11）进行适配转换。
        /// </summary>
        private static string GetFriendlyOsDescription()
        {
            var desc = RuntimeInformation.OSDescription;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                try
                {
                    var version = Environment.OSVersion.Version;
                    if (version.Major == 10 && version.Minor == 0)
                    {
                        if (version.Build >= 22000)
                        {
                            return desc.Replace("Windows 10.0", "Windows 11")
                                       .Replace("Windows 10", "Windows 11");
                        }
                    }
                }
                catch
                {
                    // 容错降级，如遇异常直接返回默认描述
                }
            }
            return desc;
        }

        public void Dispose()
        {
            _snapshotTimer.Dispose();
            _auditWriterCts.Cancel();
            _pendingAuditSignal.Release();

            try
            {
                _auditWriterTask?.GetAwaiter().GetResult();
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                _auditWriterCts.Dispose();
                _pendingAuditSignal.Dispose();
            }
        }
    }

    /// <summary>
    /// 内存消息日志条目，用于表示单条消息的最终（或最新）处理状态。
    /// 此对象是可变的，支持在多个处理阶段（入队、转发成功/失败、死信）对相同 MessageId 的记录进行就地状态更新覆盖。
    /// </summary>
    public class MessageLogEntry
    {
        /// <summary>
        /// 消息唯一 ID
        /// </summary>
        public string MessageId { get; set; } = string.Empty;

        /// <summary>
        /// 消息主题 (Topic)
        /// </summary>
        public string Topic { get; set; } = string.Empty;

        /// <summary>
        /// 发送客户端 ID
        /// </summary>
        public string SourceClientId { get; set; } = string.Empty;

        /// <summary>
        /// 载荷大小（字节）
        /// </summary>
        public int PayloadSize { get; set; }

        /// <summary>
        /// 服务质量等级 (QoS)
        /// </summary>
        public int Qos { get; set; }

        /// <summary>
        /// 是否保留消息 (Retain)
        /// </summary>
        public bool Retain { get; set; }

        /// <summary>
        /// 当前处理状态 (Queued, Succeeded, Failed, DeadLetter)
        /// </summary>
        public string Status { get; set; } = string.Empty;

        /// <summary>
        /// 是否命中订阅者
        /// </summary>
        public bool IsSubscriberHit { get; set; }

        /// <summary>
        /// 处理耗时（毫秒）
        /// </summary>
        public double LatencyMs { get; set; }

        /// <summary>
        /// 当前已重试次数
        /// </summary>
        public int RetryCount { get; set; }

        /// <summary>
        /// 时间戳 (格式化为 RFC3339/ISO 8601 字符串，供前端使用)
        /// </summary>
        public string Timestamp { get; set; } = string.Empty;

        /// <summary>
        /// 异常错误信息
        /// </summary>
        public string ErrorMessage { get; set; } = string.Empty;

        /// <summary>
        /// 系统级时间戳，用于高效率内存排序
        /// </summary>
        public DateTime SystemTimestamp { get; set; } = DateTime.Now;
    }
}
