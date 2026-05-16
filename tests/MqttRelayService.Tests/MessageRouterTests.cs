using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;
using MqttRelayService.Services.Implementations;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// MessageRouter 单元测试
    /// </summary>
    public class MessageRouterTests
    {
        private readonly Mock<IClientRegistry> _registryMock;
        private readonly MessageRouter _router;

        public MessageRouterTests()
        {
            _registryMock = new Mock<IClientRegistry>();
            var options = Microsoft.Extensions.Options.Options.Create(new RoutingOptions { EchoToSender = false });
            var loggerMock = new Mock<ILogger<MessageRouter>>();
            _router = new MessageRouter(_registryMock.Object, options, loggerMock.Object);
        }

        [Fact]
        public async Task RouteAsync_MatchingTopic_ReturnsTarget()
        {
            var sessions = new List<ClientSessionInfo>
            {
                new()
                {
                    ClientId = "client-2",
                    Subscriptions = new HashSet<string> { "test/topic" }
                }
            };

            _registryMock.Setup(r => r.GetAllSessionsAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(sessions);

            var context = new RouteContext
            {
                MessageId = "msg-1",
                Topic = "test/topic",
                SourceClientId = "client-1"
            };

            var results = await _router.RouteAsync(context);

            Assert.Single(results);
            Assert.Equal("client-2", results[0].TargetClientId);
        }

        [Fact]
        public async Task RouteAsync_NoMatchingTopic_ReturnsEmpty()
        {
            var sessions = new List<ClientSessionInfo>
            {
                new()
                {
                    ClientId = "client-2",
                    Subscriptions = new HashSet<string> { "other/topic" }
                }
            };

            _registryMock.Setup(r => r.GetAllSessionsAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(sessions);

            var context = new RouteContext
            {
                MessageId = "msg-1",
                Topic = "test/topic",
                SourceClientId = "client-1"
            };

            var results = await _router.RouteAsync(context);

            Assert.Empty(results);
        }

        [Fact]
        public async Task RouteAsync_SenderExcluded_WhenEchoDisabled()
        {
            var sessions = new List<ClientSessionInfo>
            {
                new()
                {
                    ClientId = "client-1",
                    Subscriptions = new HashSet<string> { "test/topic" }
                }
            };

            _registryMock.Setup(r => r.GetAllSessionsAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(sessions);

            var context = new RouteContext
            {
                MessageId = "msg-1",
                Topic = "test/topic",
                SourceClientId = "client-1"
            };

            var results = await _router.RouteAsync(context);

            Assert.Empty(results);
        }

        [Fact]
        public async Task RouteAsync_WildcardHash_MatchesSubTopics()
        {
            var sessions = new List<ClientSessionInfo>
            {
                new()
                {
                    ClientId = "client-2",
                    Subscriptions = new HashSet<string> { "test/#" }
                }
            };

            _registryMock.Setup(r => r.GetAllSessionsAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(sessions);

            var context = new RouteContext
            {
                MessageId = "msg-1",
                Topic = "test/topic/sub",
                SourceClientId = "client-1"
            };

            var results = await _router.RouteAsync(context);

            Assert.Single(results);
        }

        [Fact]
        public async Task RouteAsync_WildcardPlus_MatchesSingleLevel()
        {
            var sessions = new List<ClientSessionInfo>
            {
                new()
                {
                    ClientId = "client-2",
                    Subscriptions = new HashSet<string> { "test/+/sub" }
                }
            };

            _registryMock.Setup(r => r.GetAllSessionsAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(sessions);

            var context = new RouteContext
            {
                MessageId = "msg-1",
                Topic = "test/level/sub",
                SourceClientId = "client-1"
            };

            var results = await _router.RouteAsync(context);

            Assert.Single(results);
        }

        [Fact]
        public async Task RouteAsync_WhenRegistryThrows_PropagatesException()
        {
            _registryMock.Setup(r => r.GetAllSessionsAsync(It.IsAny<CancellationToken>()))
                .ThrowsAsync(new InvalidOperationException("Registry failure"));

            var context = new RouteContext
            {
                MessageId = "msg-1",
                Topic = "test/topic",
                SourceClientId = "client-1"
            };

            await Assert.ThrowsAsync<InvalidOperationException>(() => _router.RouteAsync(context));
        }
    }
}
