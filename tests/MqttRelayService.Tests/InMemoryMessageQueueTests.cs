using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Implementations;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// InMemoryMessageQueue 单元测试
    /// </summary>
    public class InMemoryMessageQueueTests
    {
        private readonly InMemoryMessageQueue _queue;

        public InMemoryMessageQueueTests()
        {
            var options = Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
            {
                QueueCapacity = 5,
                EnqueueTimeoutMs = 100,
                DropWhenQueueFull = false
            });
            var loggerMock = new Mock<ILogger<InMemoryMessageQueue>>();
            _queue = new InMemoryMessageQueue(options, loggerMock.Object);
        }

        [Fact]
        public async Task EnqueueAsync_NewMessage_IncreasesCount()
        {
            var message = new ForwardMessage
            {
                MessageId = "msg-1",
                RouteContext = new RouteContext { Topic = "test/topic" }
            };

            var result = await _queue.EnqueueAsync(message);

            Assert.True(result);
            Assert.Equal(1, _queue.Count);
        }

        [Fact]
        public async Task TryDequeueAsync_ExistingMessage_ReturnsMessage()
        {
            var message = new ForwardMessage
            {
                MessageId = "msg-1",
                RouteContext = new RouteContext { Topic = "test/topic" }
            };

            await _queue.EnqueueAsync(message);
            var result = await _queue.TryDequeueAsync();

            Assert.NotNull(result);
            Assert.Equal("msg-1", result!.MessageId);
        }

        [Fact]
        public async Task TryDequeueAsync_EmptyQueue_ReturnsNull()
        {
            using var cts = new CancellationTokenSource(500); // 500ms 超时
            var result = await _queue.TryDequeueAsync(cts.Token);
            Assert.Null(result);
        }

        [Fact]
        public async Task EnqueueAsync_FullQueueWithDrop_DropsMessage()
        {
            var options = Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
            {
                QueueCapacity = 2,
                EnqueueTimeoutMs = 100,
                DropWhenQueueFull = true
            });
            var loggerMock = new Mock<ILogger<InMemoryMessageQueue>>();
            var queue = new InMemoryMessageQueue(options, loggerMock.Object);

            await queue.EnqueueAsync(new ForwardMessage { MessageId = "msg-1", RouteContext = new RouteContext() });
            await queue.EnqueueAsync(new ForwardMessage { MessageId = "msg-2", RouteContext = new RouteContext() });

            // 显式丢弃模式下，队列满时入队应返回 false
            var result = await queue.EnqueueAsync(new ForwardMessage { MessageId = "msg-3", RouteContext = new RouteContext() });

            // 队列长度不应超过容量
            Assert.False(result);
            Assert.True(queue.Count <= 2, $"Queue count {queue.Count} should not exceed capacity 2");
        }

        [Fact]
        public async Task EnqueueAsync_FullQueueWithWait_TimeoutReturnsFalse()
        {
            var options = Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
            {
                QueueCapacity = 2,
                EnqueueTimeoutMs = 100,
                DropWhenQueueFull = false
            });
            var loggerMock = new Mock<ILogger<InMemoryMessageQueue>>();
            var queue = new InMemoryMessageQueue(options, loggerMock.Object);

            await queue.EnqueueAsync(new ForwardMessage { MessageId = "msg-1", RouteContext = new RouteContext() });
            await queue.EnqueueAsync(new ForwardMessage { MessageId = "msg-2", RouteContext = new RouteContext() });

            // Wait 模式下，队列满且超时后入队应返回 false
            var result = await queue.EnqueueAsync(new ForwardMessage { MessageId = "msg-3", RouteContext = new RouteContext() });

            Assert.False(result);
        }

        [Fact]
        public async Task ReadAllAsync_ReturnsEnqueuedMessagesAsync()
        {
            var message1 = new ForwardMessage { MessageId = "msg-1", RouteContext = new RouteContext { Topic = "test/topic" } };
            var message2 = new ForwardMessage { MessageId = "msg-2", RouteContext = new RouteContext { Topic = "test/topic" } };

            await _queue.EnqueueAsync(message1);
            await _queue.EnqueueAsync(message2);

            var messages = new List<ForwardMessage>();
            await foreach (var msg in _queue.ReadAllAsync(CancellationToken.None))
            {
                messages.Add(msg);
                if (messages.Count >= 2) break;
            }

            Assert.Equal(2, messages.Count);
            Assert.Contains(messages, m => m.MessageId == "msg-1");
            Assert.Contains(messages, m => m.MessageId == "msg-2");
        }

        [Fact]
        public async Task ReadAllAsync_RespectsCancellationToken()
        {
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            // ChannelReader.ReadAllAsync 在取消时抛出 TaskCanceledException（OperationCanceledException 的子类）
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                await foreach (var _ in _queue.ReadAllAsync(cts.Token))
                {
                    // 不应执行到这里
                }
            });
        }

        [Fact]
        public async Task EnqueueAsync_UpdatesStatusToQueued()
        {
            var message = new ForwardMessage
            {
                MessageId = "msg-1",
                RouteContext = new RouteContext { Topic = "test/topic" },
                Status = MessageProcessStatus.Received
            };

            await _queue.EnqueueAsync(message);

            Assert.Equal(MessageProcessStatus.Queued, message.Status);
        }

        [Fact]
        public async Task TryDequeueBlocking_ExistingMessage_ReturnsMessage()
        {
            var message = new ForwardMessage { MessageId = "msg-1", RouteContext = new RouteContext { Topic = "test/topic" } };
            await _queue.EnqueueAsync(message);

            var taken = _queue.TryDequeueBlocking(out var dequeued, CancellationToken.None);

            Assert.True(taken);
            Assert.Equal("msg-1", dequeued!.MessageId);
        }

        /// <summary>
        /// 空队列时取件必须挂起等待，并在入队后被唤醒。
        /// 这是消费者不再依赖线程池续体的核心行为：入队方直接释放取件信号唤醒专用线程。
        /// </summary>
        [Fact]
        public async Task TryDequeueBlocking_EmptyQueue_BlocksUntilEnqueued()
        {
            var message = new ForwardMessage { MessageId = "msg-blocked", RouteContext = new RouteContext { Topic = "test/topic" } };
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

            var blocked = Task.Run(() =>
            {
                var taken = _queue.TryDequeueBlocking(out var dequeued, cts.Token);
                return (taken, dequeued);
            });

            // 队列为空时不能立即返回
            var finishedEarly = await Task.WhenAny(blocked, Task.Delay(200));
            Assert.NotSame(blocked, finishedEarly);

            await _queue.EnqueueAsync(message);

            var finished = await Task.WhenAny(blocked, Task.Delay(5000));
            Assert.Same(blocked, finished);

            var (taken, dequeued) = await blocked;
            Assert.True(taken);
            Assert.Equal("msg-blocked", dequeued!.MessageId);
        }

        [Fact]
        public async Task TryDequeueBlocking_CancelledWhileBlocked_ThrowsOperationCanceled()
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

            var blocked = Task.Run(() => _queue.TryDequeueBlocking(out _, cts.Token));

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => blocked);
        }

        /// <summary>
        /// 排空路径用 TryDequeueAsync 取走消息会留下多余的取件信号，
        /// 残留信号只能造成一次无效唤醒，不能让取件提前返回，也不能让后续入队的消息丢唤醒。
        /// </summary>
        [Fact]
        public async Task TryDequeueBlocking_WithResidualSignal_StillWaitsAndReceivesNextMessage()
        {
            await _queue.EnqueueAsync(new ForwardMessage { MessageId = "msg-1", RouteContext = new RouteContext { Topic = "test/topic" } });
            await _queue.EnqueueAsync(new ForwardMessage { MessageId = "msg-2", RouteContext = new RouteContext { Topic = "test/topic" } });

            // 排空路径取走第一条（不消耗取件信号）
            var drained = await _queue.TryDequeueAsync();
            Assert.Equal("msg-1", drained!.MessageId);

            // 第二条仍能被同步取件拿到
            var taken = _queue.TryDequeueBlocking(out var second, CancellationToken.None);
            Assert.True(taken);
            Assert.Equal("msg-2", second!.MessageId);

            // 此时队列已空但残留两个信号：取件必须继续等待而不是空转返回
            var message3 = new ForwardMessage { MessageId = "msg-3", RouteContext = new RouteContext { Topic = "test/topic" } };
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var pending = Task.Run(() =>
            {
                var ok = _queue.TryDequeueBlocking(out var dequeued, cts.Token);
                return (ok, dequeued);
            });

            var finishedEarly = await Task.WhenAny(pending, Task.Delay(300));
            Assert.NotSame(pending, finishedEarly);

            await _queue.EnqueueAsync(message3);

            var finished = await Task.WhenAny(pending, Task.Delay(5000));
            Assert.Same(pending, finished);

            var (ok, dequeued) = await pending;
            Assert.True(ok);
            Assert.Equal("msg-3", dequeued!.MessageId);
        }
    }
}