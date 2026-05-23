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
                _logger.LogWarning("死信功能已禁用，消息 {MessageId} 将被丢弃", record.MessageId);
                return;
            }

            try
            {
                var deadLetterDir = Path.Combine(AppContext.BaseDirectory, _options.DeadLetterPath);
                var dateDir = Path.Combine(deadLetterDir, DateTime.Now.ToString("yyyyMMdd"));

                if (!Directory.Exists(dateDir))
                {
                    Directory.CreateDirectory(dateDir);
                }

                var filePath = Path.Combine(dateDir, $"{record.MessageId}.json");
                var json = JsonSerializer.Serialize(record, new JsonSerializerOptions
                {
                    WriteIndented = true
                });

                await File.WriteAllTextAsync(filePath, json, cancellationToken);

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
    }
}
