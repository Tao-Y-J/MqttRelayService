using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Moq;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;
using MqttRelayService.Workers;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// 审计数据清理后台服务单元测试。
    /// 覆盖"每天至少清理一次"的调度计算，以及清理轮次的失败隔离语义：
    /// 单轮失败只记日志，不得终止后续轮次。
    /// </summary>
    public class AuditCleanupWorkerTests
    {
        private readonly Mock<IAuditRepository> _auditRepositoryMock = new();

        private AuditCleanupWorker CreateWorker(AuditStorageOptions options)
        {
            return new AuditCleanupWorker(
                _auditRepositoryMock.Object,
                Microsoft.Extensions.Options.Options.Create(options),
                new Mock<ILogger<AuditCleanupWorker>>().Object);
        }

        [Fact]
        public void ComputeNextRunAt_WhenTargetHourNotReached_ShouldReturnTodayTarget()
        {
            var now = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Local);

            var next = AuditCleanupWorker.ComputeNextRunAt(now, cleanupAtHour: 3, intervalMinutes: 1440);

            Assert.Equal(new DateTime(2026, 1, 1, 3, 0, 0, DateTimeKind.Local), next);
        }

        [Fact]
        public void ComputeNextRunAt_WhenTargetHourPassed_ShouldReturnTomorrowTarget()
        {
            var now = new DateTime(2026, 1, 1, 5, 0, 0, DateTimeKind.Local);

            var next = AuditCleanupWorker.ComputeNextRunAt(now, cleanupAtHour: 3, intervalMinutes: 1440);

            Assert.Equal(new DateTime(2026, 1, 2, 3, 0, 0, DateTimeKind.Local), next);
        }

        [Fact]
        public void ComputeNextRunAt_WhenTargetHourIsNull_ShouldReturnIntervalDelay()
        {
            var now = new DateTime(2026, 1, 1, 5, 0, 0, DateTimeKind.Local);

            var next = AuditCleanupWorker.ComputeNextRunAt(now, cleanupAtHour: null, intervalMinutes: 30);

            Assert.Equal(now.AddMinutes(30), next);
        }

        [Fact]
        public async Task RunCleanupOnceAsync_ShouldDeleteBothTablesWithCutoffMatchingRetentionDays()
        {
            _auditRepositoryMock
                .Setup(x => x.DeleteExpiredMessageAuditsAsync(It.IsAny<DateTime>()))
                .ReturnsAsync(3);
            _auditRepositoryMock
                .Setup(x => x.DeleteExpiredClientHistoriesAsync(It.IsAny<DateTime>()))
                .ReturnsAsync(2);

            var worker = CreateWorker(new AuditStorageOptions { RetentionDays = 30, VacuumAfterCleanup = false });
            var expectedCutoff = DateTime.Now.AddDays(-30);

            await worker.RunCleanupOnceAsync(CancellationToken.None);

            _auditRepositoryMock.Verify(
                x => x.DeleteExpiredMessageAuditsAsync(It.Is<DateTime>(cutoff => Math.Abs((cutoff - expectedCutoff).TotalSeconds) < 5)),
                Times.Once);
            _auditRepositoryMock.Verify(
                x => x.DeleteExpiredClientHistoriesAsync(It.Is<DateTime>(cutoff => Math.Abs((cutoff - expectedCutoff).TotalSeconds) < 5)),
                Times.Once);
        }

        [Fact]
        public async Task RunCleanupOnceAsync_WhenRetentionDisabled_ShouldNotTouchRepository()
        {
            var worker = CreateWorker(new AuditStorageOptions { RetentionDays = 0 });

            await worker.RunCleanupOnceAsync(CancellationToken.None);

            _auditRepositoryMock.Verify(x => x.DeleteExpiredMessageAuditsAsync(It.IsAny<DateTime>()), Times.Never);
            _auditRepositoryMock.Verify(x => x.DeleteExpiredClientHistoriesAsync(It.IsAny<DateTime>()), Times.Never);
        }

        [Fact]
        public async Task RunCleanupOnceAsync_WhenDeleteThrows_ShouldSwallowExceptionAndAllowNextRoundToProceed()
        {
            var attempt = 0;
            _auditRepositoryMock
                .Setup(x => x.DeleteExpiredMessageAuditsAsync(It.IsAny<DateTime>()))
                .ReturnsAsync(() =>
                {
                    attempt++;
                    if (attempt == 1)
                    {
                        throw new InvalidOperationException("database is locked");
                    }

                    return 7;
                });
            _auditRepositoryMock
                .Setup(x => x.DeleteExpiredClientHistoriesAsync(It.IsAny<DateTime>()))
                .ReturnsAsync(0);

            var worker = CreateWorker(new AuditStorageOptions { RetentionDays = 30, VacuumAfterCleanup = false });

            // 单轮失败必须被吞掉：Host 使用 BackgroundServiceExceptionBehavior.Ignore，
            // 清理任务异常逃逸会让"每天至少一次"的保证静默失效。
            await worker.RunCleanupOnceAsync(CancellationToken.None);

            // 消息审计持续失败时，客户端历史仍必须被清理，否则该表仍会无界增长。
            _auditRepositoryMock.Verify(x => x.DeleteExpiredClientHistoriesAsync(It.IsAny<DateTime>()), Times.Once);

            await worker.RunCleanupOnceAsync(CancellationToken.None);

            _auditRepositoryMock.Verify(x => x.DeleteExpiredMessageAuditsAsync(It.IsAny<DateTime>()), Times.Exactly(2));
            _auditRepositoryMock.Verify(x => x.DeleteExpiredClientHistoriesAsync(It.IsAny<DateTime>()), Times.Exactly(2));
        }

        [Fact]
        public async Task RunCleanupOnceAsync_WhenCancellationRequested_ShouldThrowOperationCanceled()
        {
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            var worker = CreateWorker(new AuditStorageOptions { RetentionDays = 30, VacuumAfterCleanup = false });

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => worker.RunCleanupOnceAsync(cts.Token));
        }
    }
}
