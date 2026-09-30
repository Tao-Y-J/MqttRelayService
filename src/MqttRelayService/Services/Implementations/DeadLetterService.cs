using System.Globalization;
using System.Text.Json;
using Microsoft.Extensions.Options;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Services.Implementations
{
    /// <summary>
    /// 死信服务实现，将无法成功转发的消息写入文件
    /// </summary>
    public class DeadLetterService : IDeadLetterService
    {
        private readonly ReliabilityOptions _options;
        private readonly ILogger<DeadLetterService> _logger;
        private string _lastDateDir = string.Empty;
        private DateTime _lastCleanupDate = DateTime.MinValue;
        private readonly object _dirLock = new();

        public DeadLetterService(IOptions<ReliabilityOptions> options, ILogger<DeadLetterService> logger)
        {
            _options = options.Value;
            _logger = logger;
        }

        /// <summary>
        /// 写入死信记录
        /// </summary>
        public async Task WriteAsync(DeadLetterRecord record, CancellationToken cancellationToken = default)
        {
            if (!_options.EnableDeadLetter)
            {
                // 死信被显式关闭时消息会被直接丢弃，必须用 Error 级别让运维可见，不能只留 Warning。
                _logger.LogError("死信功能已禁用，消息 {MessageId} 将被直接丢弃（Topic={Topic}）",
                    record.MessageId, record.Topic);
                return;
            }

            try
            {
                var deadLetterDir = Path.Combine(AppContext.BaseDirectory, _options.DeadLetterPath);
                var today = DateTime.Now.Date;
                var dateDir = Path.Combine(deadLetterDir, today.ToString("yyyyMMdd"));

                EnsureDirectory(dateDir);
                TryCleanupExpiredDeadLetters(deadLetterDir, today);

                var filePath = Path.Combine(dateDir, $"{record.MessageId}.json");
                var json = JsonSerializer.Serialize(record, new JsonSerializerOptions
                {
                    WriteIndented = true
                });

                // 原子落盘：先写临时文件再替换，避免写盘被中断时死信目录残留半截 JSON。
                var tempFilePath = filePath + ".tmp";
                await File.WriteAllTextAsync(tempFilePath, json, cancellationToken);
                File.Move(tempFilePath, filePath, overwrite: true);

                _logger.LogError("消息 {MessageId} 已进入死信，主题 {Topic}，原因 {Reason}，文件 {FilePath}",
                    record.MessageId, record.Topic, record.FailureReason, filePath);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "写入死信记录失败，消息 {MessageId}", record.MessageId);
                throw;
            }
        }

        /// <summary>
        /// 按保留天数清理过期的死信日期目录，避免死信目录随运行时间无界增长。
        /// 每天最多扫描一次，且清理失败不影响死信写入主流程。
        /// </summary>
        private void TryCleanupExpiredDeadLetters(string deadLetterDir, DateTime today)
        {
            if (_options.DeadLetterRetentionDays <= 0)
            {
                return;
            }

            lock (_dirLock)
            {
                if (_lastCleanupDate == today)
                {
                    return;
                }

                _lastCleanupDate = today;
            }

            try
            {
                if (!Directory.Exists(deadLetterDir))
                {
                    return;
                }

                var cutoff = today.AddDays(-_options.DeadLetterRetentionDays);
                var removedCount = 0;

                foreach (var directory in Directory.EnumerateDirectories(deadLetterDir))
                {
                    var name = Path.GetFileName(directory);
                    if (!DateTime.TryParseExact(name, "yyyyMMdd", CultureInfo.InvariantCulture,
                            DateTimeStyles.None, out var directoryDate))
                    {
                        continue;
                    }

                    if (directoryDate >= cutoff)
                    {
                        continue;
                    }

                    try
                    {
                        Directory.Delete(directory, recursive: true);
                        removedCount++;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "清理过期死信目录 {Directory} 失败", directory);
                    }
                }

                if (removedCount > 0)
                {
                    _logger.LogInformation("已清理 {RemovedCount} 个超过 {RetentionDays} 天的死信目录",
                        removedCount, _options.DeadLetterRetentionDays);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "清理过期死信目录时发生异常，已跳过本次清理");
            }
        }

        private void EnsureDirectory(string dateDir)
        {
            if (string.Equals(_lastDateDir, dateDir, StringComparison.Ordinal))
            {
                return;
            }

            lock (_dirLock)
            {
                if (string.Equals(_lastDateDir, dateDir, StringComparison.Ordinal))
                {
                    return;
                }

                Directory.CreateDirectory(dateDir);
                _lastDateDir = dateDir;
            }
        }
    }
}
