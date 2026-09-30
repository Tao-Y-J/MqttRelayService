using System.Threading.Channels;
using Microsoft.Extensions.Options;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Services.Implementations
{
    /// <summary>
    /// 基于 System.Threading.Channels 的内存消息队列实现
    /// </summary>
    public class InMemoryMessageQueue : IMessageQueue
    {
        private readonly Channel<ForwardMessage> _channel;
        private readonly ReliabilityOptions _options;
        private readonly ILogger<InMemoryMessageQueue> _logger;
        private int _peakCount;

        /// <summary>
        /// 取件信号：每次成功入队释放一次。专用消费线程同步阻塞在该信号上，
        /// 由入队方直接唤醒，避免 Channel 异步等待产生的线程池续体（详见 <see cref="TryDequeueBlocking"/>）。
        /// </summary>
        private readonly SemaphoreSlim _itemsAvailable = new(0);

        public InMemoryMessageQueue(IOptions<ReliabilityOptions> options, ILogger<InMemoryMessageQueue> logger)
        {
            _options = options.Value;
            _logger = logger;

            // 统一使用 Wait 模式，满载丢弃通过容量预检实现
            // 避免 BoundedChannelFullMode.DropWrite 导致 TryWrite 返回 true 但消息被静默丢弃
            var channelOptions = new BoundedChannelOptions(_options.QueueCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait
            };

            _channel = Channel.CreateBounded<ForwardMessage>(channelOptions);
            _logger.LogInformation("消息队列已初始化，容量 {Capacity}，满载策略 {FullMode}",
                _options.QueueCapacity, channelOptions.FullMode);
        }

        /// <summary>
        /// 当前队列长度
        /// </summary>
        public int Count => _channel.Reader.Count;

        /// <summary>
        /// 队列容量上限
        /// </summary>
        public int Capacity => _options.QueueCapacity;

        /// <summary>
        /// 历史峰值长度
        /// </summary>
        public int PeakCount => Volatile.Read(ref _peakCount);

        /// <summary>
        /// 将消息入队
        /// </summary>
        public async Task<bool> EnqueueAsync(ForwardMessage message, CancellationToken cancellationToken = default)
        {
            try
            {
                message.Status = MessageProcessStatus.Queued;

                if (_options.DropWhenQueueFull)
                {
                    // Wait 模式下使用 TryWrite 同步判断：满载立即丢弃
                    // 不使用 BoundedChannelFullMode.DropWrite，因为它会静默吞消息并返回 true
                    if (!_channel.Writer.TryWrite(message))
                    {
                        _logger.LogWarning("队列已满，消息 {MessageId} 被丢弃", message.MessageId);
                        return false;
                    }
                }
                else
                {
                    // 等待写入，带超时
                    using var timeoutCts = new CancellationTokenSource(_options.EnqueueTimeoutMs);
                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

                    await _channel.Writer.WriteAsync(message, linkedCts.Token);
                }

                // 入队成功后才释放取件信号，保证专用消费线程被唤醒时队列中确实已有消息
                _itemsAvailable.Release();

                // 更新峰值，使用 CAS 循环保证高并发下峰值只升不降
                var currentCount = Count;
                int initialPeak;
                do
                {
                    initialPeak = Volatile.Read(ref _peakCount);
                    if (currentCount <= initialPeak)
                        break;
                } while (Interlocked.CompareExchange(ref _peakCount, currentCount, initialPeak) != initialPeak);

                _logger.LogDebug("消息 {MessageId} 已入队，当前队列长度 {Count}", message.MessageId, currentCount);
                return true;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // 调用方主动取消：按标准取消语义向上传播，
                // 不能被误判为"队列满载丢弃"，否则停机路径会把本该保留的消息写成死信。
                throw;
            }
            catch (OperationCanceledException)
            {
                // 入队超时
                _logger.LogWarning("消息 {MessageId} 入队超时（{TimeoutMs}ms），队列可能已满",
                    message.MessageId, _options.EnqueueTimeoutMs);
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "消息 {MessageId} 入队时发生异常", message.MessageId);
                return false;
            }
        }

        /// <summary>
        /// 尝试从队列取出消息（非阻塞）
        /// </summary>
        public Task<ForwardMessage?> TryDequeueAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                if (_channel.Reader.TryRead(out var message))
                {
                    return Task.FromResult<ForwardMessage?>(message);
                }

                return Task.FromResult<ForwardMessage?>(null);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "从队列取出消息时发生异常");
                return Task.FromResult<ForwardMessage?>(null);
            }
        }

        /// <summary>
        /// 异步读取队列中所有消息（挂起等待，非轮询）
        /// </summary>
        public IAsyncEnumerable<ForwardMessage> ReadAllAsync(CancellationToken cancellationToken = default)
        {
            return _channel.Reader.ReadAllAsync(cancellationToken);
        }

        /// <summary>
        /// 同步阻塞取出消息，供专用消费线程使用。
        /// 队列为空时阻塞在取件信号上，由入队方直接唤醒本线程，不产生线程池续体；
        /// 而 Channel 的异步等待在默认选项（AllowSynchronousContinuations=false）下必须由线程池线程恢复，
        /// 线程池被审计查询等阻塞型工作占满时，取件唤醒会排队数秒（实测最大 3.5 秒）。
        /// 只能在专用线程上调用，禁止在 MQTT 事件回调和线程池线程上调用，否则会造成线程池饥饿。
        /// </summary>
        public bool TryDequeueBlocking(out ForwardMessage? message, CancellationToken cancellationToken = default)
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (_channel.Reader.TryRead(out message))
                {
                    return true;
                }

                // 队列已关闭且已取空：返回 false 让消费循环正常退出。
                // 注意 Channel 关闭发生在阻塞等待期间时本线程不会自行醒来，依赖取消令牌结束等待。
                if (_channel.Reader.Completion.IsCompleted)
                {
                    message = null;
                    return false;
                }

                // 同步阻塞等待入队信号。排空路径用 TryDequeueAsync 取走消息会留下多余信号，
                // 此时本线程被唤醒后 TryRead 失败，会回到循环顶部继续等待：既不空转也不丢唤醒。
                _itemsAvailable.Wait(cancellationToken);
            }
        }
    }
}