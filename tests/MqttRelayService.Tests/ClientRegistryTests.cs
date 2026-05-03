using Microsoft.Extensions.Logging;
using Moq;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Implementations;
using Xunit;

namespace MqttRelayService.Tests;

/// <summary>
/// ClientRegistry 单元测试
/// </summary>
public class ClientRegistryTests
{
    private readonly ClientRegistry _registry;

    public ClientRegistryTests()
    {
        var loggerMock = new Mock<ILogger<ClientRegistry>>();
        _registry = new ClientRegistry(loggerMock.Object);
    }

    [Fact]
    public async Task RegisterAsync_NewClient_IncreasesCount()
    {
        var session = new ClientSessionInfo
        {
            ClientId = "client-1",
            Username = "user1",
            ConnectedAt = DateTime.UtcNow,
            Status = ConnectionStatus.Connected
        };

        await _registry.RegisterAsync(session);

        Assert.Equal(1, _registry.Count);
    }

    [Fact]
    public async Task UnregisterAsync_ExistingClient_DecreasesCount()
    {
        var session = new ClientSessionInfo
        {
            ClientId = "client-1",
            Username = "user1",
            ConnectedAt = DateTime.UtcNow,
            Status = ConnectionStatus.Connected
        };

        await _registry.RegisterAsync(session);
        await _registry.UnregisterAsync("client-1");

        Assert.Equal(0, _registry.Count);
    }

    [Fact]
    public async Task GetSessionAsync_ExistingClient_ReturnsSession()
    {
        var session = new ClientSessionInfo
        {
            ClientId = "client-1",
            Username = "user1",
            ConnectedAt = DateTime.UtcNow,
            Status = ConnectionStatus.Connected
        };

        await _registry.RegisterAsync(session);
        var result = await _registry.GetSessionAsync("client-1");

        Assert.NotNull(result);
        Assert.Equal("client-1", result!.ClientId);
    }

    [Fact]
    public async Task UpdateSubscriptionAsync_AddsTopic()
    {
        var session = new ClientSessionInfo
        {
            ClientId = "client-1",
            Username = "user1",
            ConnectedAt = DateTime.UtcNow,
            Status = ConnectionStatus.Connected
        };

        await _registry.RegisterAsync(session);
        await _registry.UpdateSubscriptionAsync("client-1", "test/topic", true);

        var result = await _registry.GetSessionAsync("client-1");
        Assert.Contains("test/topic", result!.Subscriptions);
    }

    [Fact]
    public async Task UpdateSubscriptionAsync_RemovesTopic()
    {
        var session = new ClientSessionInfo
        {
            ClientId = "client-1",
            Username = "user1",
            ConnectedAt = DateTime.UtcNow,
            Status = ConnectionStatus.Connected
        };

        await _registry.RegisterAsync(session);
        await _registry.UpdateSubscriptionAsync("client-1", "test/topic", true);
        await _registry.UpdateSubscriptionAsync("client-1", "test/topic", false);

        var result = await _registry.GetSessionAsync("client-1");
        Assert.DoesNotContain("test/topic", result!.Subscriptions);
    }

    [Fact]
    public async Task ConcurrentAccess_MultipleClients_HandledCorrectly()
    {
        var tasks = new List<Task>();

        for (int i = 0; i < 100; i++)
        {
            var clientId = $"client-{i}";
            tasks.Add(Task.Run(async () =>
            {
                var session = new ClientSessionInfo
                {
                    ClientId = clientId,
                    Username = "user",
                    ConnectedAt = DateTime.UtcNow,
                    Status = ConnectionStatus.Connected
                };
                await _registry.RegisterAsync(session);
            }));
        }

        await Task.WhenAll(tasks);

        Assert.Equal(100, _registry.Count);
    }

    [Fact]
    public async Task RouteAsync_ConcurrentSubscriptionUpdates_DoesNotThrow()
    {
        var session = new ClientSessionInfo
        {
            ClientId = "client-1",
            Username = "user1",
            ConnectedAt = DateTime.UtcNow,
            Status = ConnectionStatus.Connected
        };

        await _registry.RegisterAsync(session);

        var router = new MessageRouter(
            _registry,
            Microsoft.Extensions.Options.Options.Create(new RoutingOptions { EchoToSender = true }),
            new Mock<ILogger<MessageRouter>>().Object);

        var context = new RouteContext
        {
            MessageId = "msg-1",
            Topic = "test/topic",
            SourceClientId = "client-source"
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var updateTask = Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                await _registry.UpdateSubscriptionAsync("client-1", "test/topic", true, cts.Token);
                await _registry.UpdateSubscriptionAsync("client-1", "test/topic", false, cts.Token);
            }
        });

        var routeTasks = Enumerable.Range(0, 8)
            .Select(_ => Task.Run(async () =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    await router.RouteAsync(context, cts.Token);
                }
            }))
            .ToArray();

        await Task.Delay(TimeSpan.FromMilliseconds(500));
        await cts.CancelAsync();

        var tasks = routeTasks.Append(updateTask).ToArray();
        var ex = await Record.ExceptionAsync(() => Task.WhenAll(tasks));

        if (ex is OperationCanceledException)
        {
            ex = null;
        }

        Assert.Null(ex);
    }
}
