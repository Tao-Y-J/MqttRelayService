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
    }
}