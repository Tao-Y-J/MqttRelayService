using System;
using System.Threading;
using System.Threading.Tasks;

namespace MqttRelayService.Utilities
{
    /// <summary>
    /// 吞吐量与并发控制器，支持动态暂停、单线程速率限制和并发度调节。
    /// </summary>
    public class ThroughputController
    {
        private readonly object _lock = new();
        private readonly object _rateLock = new();
        private readonly int _maxConcurrencyHardLimit;

        private bool _isPaused;
        private int _maxMessagesPerSecond = 0;
        private int _maxConcurrency = 50;
        private int _activeCount = 0;

        private TaskCompletionSource _pauseTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private TaskCompletionSource _concurrencyTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private double _tokens = 0;
        private DateTime _lastRefill = DateTime.Now;

        /// <summary>
        /// 使用默认并发上限 50 构造。
        /// </summary>
        public ThroughputController()
            : this(50)
        {
        }

        /// <summary>
        /// 使用指定的最大并发硬上限构造。
        /// </summary>
        /// <param name="maxConcurrencyHardLimit">并发度硬上限，不小于 1</param>
        public ThroughputController(int maxConcurrencyHardLimit)
        {
            _maxConcurrencyHardLimit = Math.Max(1, maxConcurrencyHardLimit);
            _pauseTcs.TrySetResult();
            _concurrencyTcs.TrySetResult();
        }

        /// <summary>
        /// 是否处于暂停状态。
        /// </summary>
        public bool IsPaused
        {
            get
            {
                lock (_lock)
                {
                    return _isPaused;
                }
            }
        }

        /// <summary>
        /// 单线程每秒最大转发量。0 表示不限速。
        /// </summary>
        public int MaxMessagesPerSecond
        {
            get
            {
                lock (_lock)
                {
                    return _maxMessagesPerSecond;
                }
            }
        }

        /// <summary>
        /// 并发度硬上限（构造时设定，不可运行时变更）。
        /// </summary>
        public int MaxConcurrencyHardLimit
        {
            get
            {
                lock (_lock)
                {
                    return _maxConcurrencyHardLimit;
                }
            }
        }

        /// <summary>
        /// 最大并发工作线程数。
        /// </summary>
        public int MaxConcurrency
        {
            get
            {
                lock (_lock)
                {
                    return _maxConcurrency;
                }
            }
        }

        /// <summary>
        /// 当前处于转发中的活动工作线程数。
        /// </summary>
        public int ActiveCount
        {
            get
            {
                lock (_lock)
                {
                    return _activeCount;
                }
            }
        }

        /// <summary>
        /// 一键暂停转发服务。
        /// </summary>
        public void Pause()
        {
            lock (_lock)
            {
                if (!_isPaused)
                {
                    _isPaused = true;
                    _pauseTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                }
            }
        }

        /// <summary>
        /// 恢复转发服务。
        /// </summary>
        public void Resume()
        {
            lock (_lock)
            {
                if (_isPaused)
                {
                    _isPaused = false;
                    _pauseTcs.TrySetResult();
                }
            }
        }

        /// <summary>
        /// 动态更新单线程每秒最大转发量。
        /// 注意：本方法与 <see cref="ApplyRateLimitingAsync"/> 必须保持一致的加锁顺序（先 _lock 后 _rateLock），
        /// 避免经典 AB-BA 死锁。_rateLock 区段内只读写本方法已持有的局部快照，不再回调任何持 _lock 的成员。
        /// </summary>
        public void UpdateMaxMessagesPerSecond(int mps)
        {
            // 锁顺序：_lock → _rateLock（先持 _lock 读取快照，再持 _rateLock 应用）
            int newMps;
            int effectiveRate;
            lock (_lock)
            {
                _maxMessagesPerSecond = Math.Max(0, mps);
                newMps = _maxMessagesPerSecond;
                // 在仍持有 _lock 时计算有效速率，避免在 _rateLock 区段内再回调持 _lock 的 GetEffectiveRatePerSecond
                effectiveRate = ComputeEffectiveRatePerSecond(_maxMessagesPerSecond, _activeCount);
            }

            lock (_rateLock)
            {
                // 仅当限流仍启用时重置令牌桶，避免限流被关闭后还残留过期令牌
                if (newMps > 0)
                {
                    _tokens = effectiveRate;
                    _lastRefill = DateTime.Now;
                }
            }
        }

        /// <summary>
        /// 当最大并发度更新时触发此事件。
        /// </summary>
        public event Action<int>? ConcurrencyChanged;

        /// <summary>
        /// 动态更新最大活动并发度。
        /// </summary>
        public void UpdateMaxConcurrency(int maxConcurrency)
        {
            int updated;
            lock (_lock)
            {
                _maxConcurrency = Math.Clamp(maxConcurrency, 1, _maxConcurrencyHardLimit);
                updated = _maxConcurrency;
                var oldTcs = _concurrencyTcs;
                _concurrencyTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                oldTcs.TrySetResult();
            }

            ConcurrencyChanged?.Invoke(updated);
        }

        /// <summary>
        /// 消费线程进入转发链路前的拦截与协调。
        /// </summary>
        public async Task WaitAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                Task? pauseTask = null;
                Task? concurrencyTask = null;

                lock (_lock)
                {
                    if (_isPaused)
                    {
                        pauseTask = _pauseTcs.Task;
                    }
                    else if (_activeCount >= _maxConcurrency)
                    {
                        concurrencyTask = _concurrencyTcs.Task;
                    }
                    else
                    {
                        _activeCount++;
                        break;
                    }
                }

                if (pauseTask != null)
                {
                    await pauseTask;
                    continue;
                }

                if (concurrencyTask != null)
                {
                    await Task.Delay(10, cancellationToken);
                    continue;
                }
            }

            await ApplyRateLimitingAsync(cancellationToken);
        }

        /// <summary>
        /// 释放并发占用槽位，并通知等待线程。
        /// </summary>
        public void Release()
        {
            lock (_lock)
            {
                if (_activeCount > 0)
                {
                    _activeCount--;
                }

                var oldTcs = _concurrencyTcs;
                _concurrencyTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                oldTcs.TrySetResult();
            }
        }

        /// <summary>
        /// 以“活动线程数 × 单线程 MPS”作为全局补充速率，保持单线程调控语义。
        /// 锁顺序：_lock → _rateLock。_rateLock 区段内不回调任何持 _lock 的成员，杜绝 AB-BA 死锁。
        /// </summary>
        private async Task ApplyRateLimitingAsync(CancellationToken cancellationToken)
        {
            // 在 _rateLock 之外先用 _lock 读取限流配置快照，避免在持 _rateLock 时再嵌套获取 _lock
            int maxMessagesPerSecond;
            int effectiveRate;
            lock (_lock)
            {
                maxMessagesPerSecond = _maxMessagesPerSecond;
                if (maxMessagesPerSecond <= 0)
                {
                    return;
                }

                effectiveRate = ComputeEffectiveRatePerSecond(maxMessagesPerSecond, _activeCount);
                if (effectiveRate <= 0)
                {
                    return;
                }
            }

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                double delayMs;

                lock (_rateLock)
                {
                    var now = DateTime.Now;
                    var elapsed = (now - _lastRefill).TotalSeconds;
                    _lastRefill = now;

                    _tokens += elapsed * effectiveRate;
                    if (_tokens > effectiveRate)
                    {
                        _tokens = effectiveRate;
                    }

                    if (_tokens >= 1.0)
                    {
                        _tokens -= 1.0;
                        break;
                    }

                    var needed = 1.0 - _tokens;
                    delayMs = (needed / effectiveRate) * 1000.0;
                }

                var sleepTime = Math.Clamp((int)delayMs, 1, 1000);
                await Task.Delay(sleepTime, cancellationToken);
            }
        }

        /// <summary>
        /// 根据“单线程 MPS × 活动工作线程数”计算全局有效补充速率。
        /// 调用方必须已持有 <see cref="_lock"/>；本方法不再自行加锁，确保不会在 _rateLock 区段内嵌套 _lock。
        /// </summary>
        /// <param name="maxMessagesPerSecond">单线程每秒最大转发量</param>
        /// <param name="activeCount">当前活动工作线程数（已持 _lock 读取，保证一致快照）</param>
        private static int ComputeEffectiveRatePerSecond(int maxMessagesPerSecond, int activeCount)
        {
            if (maxMessagesPerSecond <= 0)
            {
                return 0;
            }

            var activeWorkers = Math.Max(activeCount, 1);
            return maxMessagesPerSecond * activeWorkers;
        }
    }
}
