using Microsoft.Extensions.Options;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Services.Implementations;

/// <summary>
/// 重试策略提供器实现，支持指数退避
/// </summary>
public class RetryPolicyProvider : IRetryPolicyProvider
{
    private readonly ReliabilityOptions _options;
    private readonly ILogger<RetryPolicyProvider> _logger;

    public RetryPolicyProvider(IOptions<ReliabilityOptions> options, ILogger<RetryPolicyProvider> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>
    /// 重试次数移位上限，防止 1L 左移 retryCount 位时溢出
    /// （retryCount=30 配合默认 baseDelay=1000ms 已超 12 天，远超 RetryMaxDelayMs 的合理上界）
    /// </summary>
    private const int MaxShiftBits = 30;

    /// <summary>
    /// 根据当前重试次数计算下次重试延迟
    /// </summary>
    public Task<TimeSpan> GetDelayAsync(int retryCount, CancellationToken cancellationToken = default)
    {
        try
        {
            if (retryCount < 0)
            {
                retryCount = 0;
            }

            // 防御性截断，避免 1L << retryCount 在 retryCount 极大时溢出
            var shiftBits = Math.Min(retryCount, MaxShiftBits);

            // 指数退避：delay = baseDelay * 2^retryCount
            var delayMs = (long)_options.RetryBaseDelayMs * (1L << shiftBits);

            // 限制在最大延迟范围内
            if (delayMs <= 0 || delayMs > _options.RetryMaxDelayMs)
            {
                delayMs = _options.RetryMaxDelayMs;
            }

            // 添加少量随机抖动（0-20%）避免惊群，抖动后再次 clamp 保证硬上限
            var jitter = Random.Shared.NextDouble() * 0.2 * delayMs;
            delayMs = Math.Min(delayMs + (long)jitter, _options.RetryMaxDelayMs);

            _logger.LogDebug("重试延迟计算：第 {RetryCount} 次，延迟 {DelayMs}ms", retryCount, delayMs);
            return Task.FromResult(TimeSpan.FromMilliseconds(delayMs));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "计算重试延迟时发生异常，返回基础延迟");
            return Task.FromResult(TimeSpan.FromMilliseconds(_options.RetryBaseDelayMs));
        }
    }
}
