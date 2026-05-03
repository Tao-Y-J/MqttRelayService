using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using MqttRelayService.Options;
using MqttRelayService.Services.Implementations;
using Xunit;

namespace MqttRelayService.Tests;

/// <summary>
/// RetryPolicyProvider 单元测试
/// </summary>
public class RetryPolicyProviderTests
{
    private readonly RetryPolicyProvider _provider;

    public RetryPolicyProviderTests()
    {
        var options = Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
        {
            RetryBaseDelayMs = 1000,
            RetryMaxDelayMs = 30000
        });
        var loggerMock = new Mock<ILogger<RetryPolicyProvider>>();
        _provider = new RetryPolicyProvider(options, loggerMock.Object);
    }

    [Theory]
    [InlineData(0, 1000, 1500)]   // 第0次重试：约 1000ms
    [InlineData(1, 1500, 3000)]   // 第1次：约 2000ms + jitter
    [InlineData(2, 3000, 6000)]   // 第2次：约 4000ms + jitter
    [InlineData(3, 6000, 12000)]  // 第3次：约 8000ms + jitter
    public async Task GetDelayAsync_ReturnsExponentialBackoff(int retryCount, int minExpected, int maxExpected)
    {
        var delay = await _provider.GetDelayAsync(retryCount);

        Assert.True(delay.TotalMilliseconds >= minExpected,
            $"Delay {delay.TotalMilliseconds}ms should be >= {minExpected}ms");
        Assert.True(delay.TotalMilliseconds <= maxExpected,
            $"Delay {delay.TotalMilliseconds}ms should be <= {maxExpected}ms");
    }

    [Fact]
    public async Task GetDelayAsync_ExceedsMax_ReturnsMaxDelay()
    {
        var options = Microsoft.Extensions.Options.Options.Create(new ReliabilityOptions
        {
            RetryBaseDelayMs = 1000,
            RetryMaxDelayMs = 5000
        });
        var loggerMock = new Mock<ILogger<RetryPolicyProvider>>();
        var provider = new RetryPolicyProvider(options, loggerMock.Object);

        // 第10次重试应该被限制在最大延迟
        var delay = await provider.GetDelayAsync(10);

        Assert.True(delay.TotalMilliseconds <= 6000, // 5000 + 20% jitter
            $"Delay {delay.TotalMilliseconds}ms should not exceed max delay by much");
    }
}