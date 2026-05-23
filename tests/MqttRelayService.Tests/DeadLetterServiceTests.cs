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
    /// DeadLetterService 单元测试
    /// </summary>
    public class DeadLetterServiceTests : IDisposable
    {
        private readonly string _testDeadLetterPath;
        private readonly DeadLetterService _service;

        public DeadLetterServiceTests()
        {
            _testDeadLetterPath = Path.Combine(Path.GetTempPath(), $"test-deadletter-{Guid.NewGuid()}");

            var options = Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
            {
                EnableDeadLetter = true,
                DeadLetterPath = _testDeadLetterPath
            });
            var loggerMock = new Mock<ILogger<DeadLetterService>>();
            _service = new DeadLetterService(options, loggerMock.Object);
        }

        public void Dispose()
        {
            if (Directory.Exists(_testDeadLetterPath))
            {
                Directory.Delete(_testDeadLetterPath, recursive: true);
            }
        }

        [Fact]
        public async Task WriteAsync_Enabled_CreatesDirectoryAndFile()
        {
            var record = new DeadLetterRecord
            {
                MessageId = "msg-1",
                Topic = "test/topic",
                SourceClientId = "client-1",
                PayloadBase64 = Convert.ToBase64String(new byte[] { 1, 2, 3 }),
                FirstReceivedAt = DateTime.Now,
                LastFailedAt = DateTime.Now,
                FailureReason = "Test failure",
                RetryCount = 3
            };

            await _service.WriteAsync(record);

            var dateDir = Path.Combine(AppContext.BaseDirectory, _testDeadLetterPath, DateTime.Now.ToString("yyyyMMdd"));
            var filePath = Path.Combine(dateDir, "msg-1.json");

            Assert.True(Directory.Exists(dateDir), "Date directory should be created");
            Assert.True(File.Exists(filePath), "Dead letter file should be created");
        }

        [Fact]
        public async Task WriteAsync_Disabled_DoesNotCreateFile()
        {
            var options = Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
            {
                EnableDeadLetter = false,
                DeadLetterPath = _testDeadLetterPath
            });
            var loggerMock = new Mock<ILogger<DeadLetterService>>();
            var service = new DeadLetterService(options, loggerMock.Object);

            var record = new DeadLetterRecord
            {
                MessageId = "msg-2",
                Topic = "test/topic",
                SourceClientId = "client-1",
                FirstReceivedAt = DateTime.Now,
                LastFailedAt = DateTime.Now,
                FailureReason = "Test failure",
                RetryCount = 3
            };

            await service.WriteAsync(record);

            var dateDir = Path.Combine(AppContext.BaseDirectory, _testDeadLetterPath);
            Assert.False(Directory.Exists(dateDir), "Directory should not be created when disabled");
        }

        [Fact]
        public async Task WriteAsync_WhenFileSystemWriteFails_Throws()
        {
            var filePath = Path.Combine(Path.GetTempPath(), $"deadletter-path-file-{Guid.NewGuid()}");
            await File.WriteAllTextAsync(filePath, "not a directory");

            try
            {
                var options = Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
                {
                    EnableDeadLetter = true,
                    DeadLetterPath = filePath
                });
                var service = new DeadLetterService(options, new Mock<ILogger<DeadLetterService>>().Object);
                var record = new DeadLetterRecord
                {
                    MessageId = "msg-write-fails",
                    Topic = "test/topic",
                    SourceClientId = "client-1",
                    FirstReceivedAt = DateTime.Now,
                    LastFailedAt = DateTime.Now,
                    FailureReason = "Test failure",
                    RetryCount = 3
                };

                await Assert.ThrowsAsync<IOException>(() => service.WriteAsync(record));
            }
            finally
            {
                if (File.Exists(filePath))
                {
                    File.Delete(filePath);
                }
            }
        }
    }
}
