using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MqttRelayService.Utilities
{
    /// <summary>
    /// 吞吐量及并发控制器，支持动态暂停、限流 (MPS) 和并发度调节 (1-50)
    /// </summary>
    public class ThroughputController
    {
        private readonly object _lock = new();
        private readonly object _rateLock = new();

        private bool _isPaused;
        private int _maxMessagesPerSecond = 0; // 0 表示无限制
        private int _maxConcurrency = 50;      // 动态并发数，默认 50
        private int _activeCount = 0;          // 当前活跃的消费线程数

        // 暂停控制的 TaskCompletionSource
        private TaskCompletionSource _pauseTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        // 并发阻塞控制的 TaskCompletionSource
        private TaskCompletionSource _concurrencyTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // 令牌桶状态
        private double _tokens = 0;
        private DateTime _lastRefill = DateTime.UtcNow;

        public ThroughputController()
        {
            // 默认初始状态非暂停，TCS 处于完成状态
            _pauseTcs.TrySetResult();
            _concurrencyTcs.TrySetResult();
        }

        /// <summary>
        /// 是否处于暂停状态
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
        /// 每秒最大消息吞吐量 (MPS)
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
        /// 动态最大并发数
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
        /// 当前正处于转发处理中的活跃线程数
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
        /// 一键暂停转发服务
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
        /// 恢复转发服务
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
        /// 动态更新吞吐速率限制
        /// </summary>
        public void UpdateMaxMessagesPerSecond(int mps)
        {
            lock (_lock)
            {
                _maxMessagesPerSecond = Math.Max(0, mps);
            }
            lock (_rateLock)
            {
                // 速率变化时，重置令牌桶，避免速率瞬间调整后的意外长等待
                _tokens = _maxMessagesPerSecond;
                _lastRefill = DateTime.UtcNow;
            }
        }

        /// <summary>
        /// 当最大并发度更新时触发此事件
        /// </summary>
        public event Action<int>? ConcurrencyChanged;

        /// <summary>
        /// 动态更新最大活跃并发度
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
        /// 消费线程进入转发链路前的拦截与协调
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
                    // 1. 检查是否暂停
                    if (_isPaused)
                    {
                        pauseTask = _pauseTcs.Task;
                    }
                    // 2. 检查并发是否超限
                    else if (_activeCount >= _maxConcurrency)
                    {
                        concurrencyTask = _concurrencyTcs.Task;
                    }
                    else
                    {
                        // 占用并发槽位
                        _activeCount++;
                        break;
                    }
                }

                // 如果暂停，非阻塞等待恢复信号
                if (pauseTask != null)
                {
                    await pauseTask;
                    continue;
                }

                // 如果并发满额，轻量休眠或等待信号，避免高频自旋
                if (concurrencyTask != null)
                {
                    await Task.Delay(10, cancellationToken);
                    continue;
                }
            }

            // 3. 限流 (MPS) 协调
            await ApplyRateLimitingAsync(cancellationToken);
        }

        /// <summary>
        /// 释放并发占用槽位，并通知等待线程
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
        /// 基于高精令牌桶的速率控制器
        /// </summary>
        private async Task ApplyRateLimitingAsync(CancellationToken cancellationToken)
        {
            int mps;
            lock (_lock)
            {
                mps = _maxMessagesPerSecond;
            }

            if (mps <= 0) return; // 0 表示无限制

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                double delayMs = 0;

                lock (_rateLock)
                {
                    var now = DateTime.UtcNow;
                    var elapsed = (now - _lastRefill).TotalSeconds;
                    _lastRefill = now;

                    // 计算并补充令牌
                    _tokens += elapsed * mps;
                    if (_tokens > mps)
                    {
                        _tokens = mps; // 限制积攒上限，防止瞬时 Burst 突发洪峰
                    }

                    if (_tokens >= 1.0)
                    {
                        _tokens -= 1.0;
                        break; // 成功获取令牌，允许通过
                    }

                    // 令牌不足，计算到下一个令牌产生的理论等待毫秒数
                    double needed = 1.0 - _tokens;
                    delayMs = (needed / mps) * 1000.0;
                }

                if (delayMs > 0)
                {
                    int sleepTime = Math.Clamp((int)delayMs, 1, 1000);
                    await Task.Delay(sleepTime, cancellationToken);
                }
            }
        }
    }
}
