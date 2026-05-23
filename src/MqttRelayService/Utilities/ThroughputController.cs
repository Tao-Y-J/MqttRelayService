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

        private bool _isPaused;
        private int _maxMessagesPerSecond = 0;
        private int _maxConcurrency = 50;
        private int _activeCount = 0;

        private TaskCompletionSource _pauseTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private TaskCompletionSource _concurrencyTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private double _tokens = 0;
        private DateTime _lastRefill = DateTime.Now;

        public ThroughputController()
        {
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
        /// </summary>
        public void UpdateMaxMessagesPerSecond(int mps)
        {
            lock (_lock)
            {
                _maxMessagesPerSecond = Math.Max(0, mps);
            }

            lock (_rateLock)
            {
                _tokens = GetEffectiveRatePerSecond();
                _lastRefill = DateTime.Now;
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
                _maxConcurrency = Math.Clamp(maxConcurrency, 1, 50);
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
        /// </summary>
        private async Task ApplyRateLimitingAsync(CancellationToken cancellationToken)
        {
            if (MaxMessagesPerSecond <= 0)
            {
                return;
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

                    var effectiveRate = GetEffectiveRatePerSecond();
                    if (effectiveRate <= 0)
                    {
                        return;
                    }

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

        private int GetEffectiveRatePerSecond()
        {
            lock (_lock)
            {
                if (_maxMessagesPerSecond <= 0)
                {
                    return 0;
                }

                var activeWorkers = Math.Max(_activeCount, 1);
                return _maxMessagesPerSecond * activeWorkers;
            }
        }
    }
}
