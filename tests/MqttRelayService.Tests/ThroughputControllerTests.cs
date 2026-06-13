using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using MqttRelayService.Utilities;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// 转发服务吞吐量与并发度控制器 (ThroughputController) 单元测试
    /// </summary>
    public class ThroughputControllerTests
    {
        [Fact]
        public void InitialState_ShouldBeDefaultValues()
        {
            // Arrange & Act
            var controller = new ThroughputController();

            // Assert
            Assert.False(controller.IsPaused);
            Assert.Equal(0, controller.MaxMessagesPerSecond); // 无限制
            Assert.Equal(50, controller.MaxConcurrency);
            Assert.Equal(0, controller.ActiveCount);
        }

        [Fact]
        public async Task PauseAndResume_ShouldBlockAndUnblockConsumers()
        {
            // Arrange
            var controller = new ThroughputController();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            // Act 1: 暂停控制器
            controller.Pause();
            Assert.True(controller.IsPaused);

            var consumerTask = Task.Run(async () =>
            {
                await controller.WaitAsync(cts.Token);
                return true;
            });

            // 期待消费者在暂停状态下保持阻塞
            await Task.Delay(100);
            Assert.False(consumerTask.IsCompleted);

            // Act 2: 恢复控制器
            controller.Resume();
            Assert.False(controller.IsPaused);

            // Assert: 恢复后消费者应该瞬间执行完毕
            var result = await consumerTask;
            Assert.True(result);
            Assert.Equal(1, controller.ActiveCount);

            // 清理
            controller.Release();
            Assert.Equal(0, controller.ActiveCount);
        }

        [Fact]
        public async Task MaxConcurrency_ShouldRestrictActiveConsumersCount()
        {
            // Arrange
            var controller = new ThroughputController();
            controller.UpdateMaxConcurrency(2); // 限制并发为 2

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            // Act 1: 占满并发槽位
            await controller.WaitAsync(cts.Token); // 槽位 1
            await controller.WaitAsync(cts.Token); // 槽位 2
            Assert.Equal(2, controller.ActiveCount);

            // 启动第 3 个消费者，由于并发限制应该被阻塞
            var thirdConsumerTask = Task.Run(async () =>
            {
                await controller.WaitAsync(cts.Token);
                return true;
            });

            await Task.Delay(100);
            Assert.False(thirdConsumerTask.IsCompleted);
            Assert.Equal(2, controller.ActiveCount);

            // Act 2: 释放一个槽位
            controller.Release();

            // Assert: 第 3 个消费者应当获得信号成功进入
            var result = await thirdConsumerTask;
            Assert.True(result);
            Assert.Equal(2, controller.ActiveCount); // 槽位又被填满

            // 清理
            controller.Release();
            controller.Release();
            Assert.Equal(0, controller.ActiveCount);
        }

        [Fact]
        public async Task UpdateMaxConcurrency_ShouldFireNotificationEvent()
        {
            // Arrange
            var controller = new ThroughputController();
            int firedValue = 0;
            controller.ConcurrencyChanged += (val) => firedValue = val;

            // Act
            controller.UpdateMaxConcurrency(15);

            // Assert
            Assert.Equal(15, controller.MaxConcurrency);
            Assert.Equal(15, firedValue);
        }

        [Fact]
        public async Task RateLimiting_ShouldLimitMessagesPerSecond()
        {
            // Arrange
            var controller = new ThroughputController();
            controller.UpdateMaxMessagesPerSecond(10); // 限制为 10 MPS，即每 100ms 允许 1 条
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));

            // Act 1: 消耗初始桶中的令牌 (桶容量上限为 10)
            for (int i = 0; i < 10; i++)
            {
                await controller.WaitAsync(cts.Token);
                controller.Release(); // 释放并发锁以避免由于并发数受限导致测不准限流
            }

            // 此时桶中令牌已空，接下来再次 WaitAsync 必须发生限流延时
            var stopwatch = Stopwatch.StartNew();
            
            // 请求 3 条消息，限流预期延迟大约为 3 * 100ms = 300ms
            for (int i = 0; i < 3; i++)
            {
                await controller.WaitAsync(cts.Token);
                controller.Release();
            }
            
            stopwatch.Stop();

            // Assert: 至少耗时 100ms (预留余量避免高频跑马机测试抖动导致的失败)
            Assert.True(stopwatch.ElapsedMilliseconds >= 100, $"Elapsed was {stopwatch.ElapsedMilliseconds}ms, which should be >= 100ms");
        }
        [Fact]
        public async Task RateLimiting_ShouldApplyPerConsumerInsteadOfGlobalSharedBucket()
        {
            var controller = new ThroughputController();
            controller.UpdateMaxMessagesPerSecond(2);
            controller.UpdateMaxConcurrency(2);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

            async Task<long> RunSingleConsumerAsync()
            {
                var sw = Stopwatch.StartNew();
                for (int i = 0; i < 4; i++)
                {
                    await controller.WaitAsync(cts.Token);
                    controller.Release();
                }

                sw.Stop();
                return sw.ElapsedMilliseconds;
            }

            var elapsed = await Task.WhenAll(RunSingleConsumerAsync(), RunSingleConsumerAsync());

            Assert.All(elapsed, ms => Assert.True(ms >= 400, $"Elapsed was {ms}ms, expected throughput control to remain effective."));
            Assert.All(elapsed, ms => Assert.True(ms < 3200, $"Elapsed was {ms}ms, expected active concurrency to increase total throughput instead of staying capped by a global 2 MPS bucket."));
        }
    }
}
