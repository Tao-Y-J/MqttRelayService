using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Implementations;
using Xunit;

namespace MqttRelayService.Tests;

/// <summary>
/// AuthService 单元测试
/// </summary>
public class AuthServiceTests
{
    private readonly Mock<ILogger<AuthService>> _loggerMock;

    public AuthServiceTests()
    {
        _loggerMock = new Mock<ILogger<AuthService>>();
    }

    [Fact]
    public async Task AuthenticateAsync_AllowAnonymous_EmptyClientId_ReturnsFailure()
    {
        var options = Microsoft.Extensions.Options.Options.Create(new AuthOptions { AllowAnonymous = true });
        var service = new AuthService(options, _loggerMock.Object);

        var result = await service.AuthenticateAsync(new AuthRequest { ClientId = "" });

        Assert.False(result.Success);
        Assert.Contains("ClientId 不能为空", result.ErrorMessage);
    }

    [Fact]
    public async Task AuthenticateAsync_AllowAnonymous_ValidClientId_ReturnsSuccess()
    {
        var options = Microsoft.Extensions.Options.Options.Create(new AuthOptions { AllowAnonymous = true });
        var service = new AuthService(options, _loggerMock.Object);

        var result = await service.AuthenticateAsync(new AuthRequest { ClientId = "test-client" });

        Assert.True(result.Success);
    }

    [Fact]
    public async Task AuthenticateAsync_DisallowAnonymous_EmptyUsername_ReturnsFailure()
    {
        var options = Microsoft.Extensions.Options.Options.Create(new AuthOptions { AllowAnonymous = false });
        var service = new AuthService(options, _loggerMock.Object);

        var result = await service.AuthenticateAsync(new AuthRequest
        {
            ClientId = "test-client",
            Username = ""
        });

        Assert.False(result.Success);
        Assert.Contains("用户名不能为空", result.ErrorMessage);
    }

    [Fact]
    public async Task AuthenticateAsync_DisallowAnonymous_ValidCredentials_ReturnsSuccess()
    {
        var options = Microsoft.Extensions.Options.Options.Create(new AuthOptions
        {
            AllowAnonymous = false,
            Users = new List<AuthUserOptions>
            {
                new() { Username = "app1", Password = "123456" }
            }
        });
        var service = new AuthService(options, _loggerMock.Object);

        var result = await service.AuthenticateAsync(new AuthRequest
        {
            ClientId = "test-client",
            Username = "app1",
            Password = "123456"
        });

        Assert.True(result.Success);
    }

    [Fact]
    public async Task AuthenticateAsync_DisallowAnonymous_InvalidPassword_ReturnsFailure()
    {
        var options = Microsoft.Extensions.Options.Options.Create(new AuthOptions
        {
            AllowAnonymous = false,
            Users = new List<AuthUserOptions>
            {
                new() { Username = "app1", Password = "123456" }
            }
        });
        var service = new AuthService(options, _loggerMock.Object);

        var result = await service.AuthenticateAsync(new AuthRequest
        {
            ClientId = "test-client",
            Username = "app1",
            Password = "wrong"
        });

        Assert.False(result.Success);
    }

    [Fact]
    public async Task AuthenticateAsync_ClientIdPrefixMismatch_ReturnsFailure()
    {
        var options = Microsoft.Extensions.Options.Options.Create(new AuthOptions
        {
            AllowAnonymous = false,
            Users = new List<AuthUserOptions>
            {
                new() { Username = "app1", Password = "123456", ClientIdPrefix = "app1" }
            }
        });
        var service = new AuthService(options, _loggerMock.Object);

        var result = await service.AuthenticateAsync(new AuthRequest
        {
            ClientId = "other-client",
            Username = "app1",
            Password = "123456"
        });

        Assert.False(result.Success);
        Assert.Contains("ClientId 必须以", result.ErrorMessage);
    }
}