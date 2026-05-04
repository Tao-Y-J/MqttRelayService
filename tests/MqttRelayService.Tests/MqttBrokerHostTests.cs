using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MQTTnet.Packets;
using MQTTnet.Protocol;
using MqttRelayService.Options;
using MqttRelayService.Services.Implementations;
using Xunit;

namespace MqttRelayService.Tests;

/// <summary>
/// MqttBrokerHost 单元测试
/// </summary>
public class MqttBrokerHostTests
{
    private static MqttPublishPacket CreatePacketWithUserProperty(string name, string value)
    {
        var packet = new MqttPublishPacket();
        // MqttPublishPacket.UserProperties 默认可能为 null，通过反射初始化
        var prop = typeof(MqttPublishPacket).GetProperty("UserProperties");
        var list = new List<MqttUserProperty> { new MqttUserProperty(name, Encoding.UTF8.GetBytes(value)) };
        prop?.SetValue(packet, list);
        return packet;
    }

    private static MqttPublishPacket CreatePacketWithoutUserProperties()
    {
        var packet = new MqttPublishPacket();
        var prop = typeof(MqttPublishPacket).GetProperty("UserProperties");
        prop?.SetValue(packet, new List<MqttUserProperty>());
        return packet;
    }

    [Fact]
    public void ShouldBlockEchoToSender_EchoEnabled_ReturnsFalse()
    {
        var packet = CreatePacketWithUserProperty("x-source-client-id", "client-1");

        var result = MqttBrokerHost.ShouldBlockEchoToSender(true, "client-1", packet);

        Assert.False(result);
    }

    [Fact]
    public void ShouldBlockEchoToSender_EchoDisabled_SameClient_ReturnsTrue()
    {
        var packet = CreatePacketWithUserProperty("x-source-client-id", "client-1");

        var result = MqttBrokerHost.ShouldBlockEchoToSender(false, "client-1", packet);

        Assert.True(result);
    }

    [Fact]
    public void ShouldBlockEchoToSender_EchoDisabled_DifferentClient_ReturnsFalse()
    {
        var packet = CreatePacketWithUserProperty("x-source-client-id", "client-1");

        var result = MqttBrokerHost.ShouldBlockEchoToSender(false, "client-2", packet);

        Assert.False(result);
    }

    [Fact]
    public void ShouldBlockEchoToSender_NoUserProperty_ReturnsFalse()
    {
        var packet = CreatePacketWithoutUserProperties();

        var result = MqttBrokerHost.ShouldBlockEchoToSender(false, "client-1", packet);

        Assert.False(result);
    }

    [Fact]
    public void ShouldBlockEchoToSender_EmptySourceClientId_ReturnsFalse()
    {
        var packet = CreatePacketWithUserProperty("x-source-client-id", "");

        var result = MqttBrokerHost.ShouldBlockEchoToSender(false, "client-1", packet);

        Assert.False(result);
    }
}
