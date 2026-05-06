namespace MqttRelayService.Options;

/// <summary>
/// 可靠性配置选项
/// </summary>
public class ReliabilityOptions
{
    /// <summary>
    /// 投递语义，默认 AtLeastOnce
    /// </summary>
    public string DeliverySemantics { get; set; } = "AtLeastOnce";

    /// <summary>
    /// 内部队列容量上限
    /// </summary>
    public int QueueCapacity { get; set; } = 1000;

    /// <summary>
    /// 入队等待超时（毫秒）
    /// </summary>
    public int EnqueueTimeoutMs { get; set; } = 2000;

    /// <summary>
    /// 最大并发处理数
    /// </summary>
    public int MaxConcurrentHandlers { get; set; } = 1;

    /// <summary>
    /// 最大重试次数
    /// </summary>
    public int MaxRetryCount { get; set; } = 3;

    /// <summary>
    /// 运行期等待退避的后台重试调度任务上限，防止失败洪峰绕过内部队列容量造成无界内存增长。
    /// 建议不大于 QueueCapacity；配置值小于 1 时运行期会回退到 QueueCapacity。
    /// </summary>
    public int MaxPendingRetryTasks { get; set; } = 1000;

    /// <summary>
    /// 重试基础延迟（毫秒）
    /// </summary>
    public int RetryBaseDelayMs { get; set; } = 1000;

    /// <summary>
    /// 重试最大延迟（毫秒）
    /// </summary>
    public int RetryMaxDelayMs { get; set; } = 30000;

    /// <summary>
    /// 是否启用死信
    /// </summary>
    public bool EnableDeadLetter { get; set; } = true;

    /// <summary>
    /// 死信文件存储路径
    /// </summary>
    public string DeadLetterPath { get; set; } = "data/deadletter";

    /// <summary>
    /// 单次转发超时（毫秒）
    /// </summary>
    public int ForwardTimeoutMs { get; set; } = 5000;

    /// <summary>
    /// 停机排空超时（毫秒）。
    /// 若希望停机阶段至少覆盖一次失败消息的最大退避等待，应不小于 RetryMaxDelayMs。
    /// </summary>
    public int ShutdownDrainTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// 队列满时是否丢弃新消息（否则阻塞等待）
    /// </summary>
    public bool DropWhenQueueFull { get; set; } = false;
}
