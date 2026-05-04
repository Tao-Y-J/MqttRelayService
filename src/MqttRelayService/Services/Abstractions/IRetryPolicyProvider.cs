namespace MqttRelayService.Services.Abstractions;

/// <summary>
/// 重试策略提供器接口，提供指数退避等重试间隔计算
/// </summary>
public interface IRetryPolicyProvider
{
    /// <summary>
    /// 根据当前重试次数计算下次重试延迟
    /// </summary>
    Task<TimeSpan> GetDelayAsync(int retryCount, CancellationToken cancellationToken = default);
}