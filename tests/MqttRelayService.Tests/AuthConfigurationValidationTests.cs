using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Serilog;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// 启动期认证配置一致性校验测试。
    /// 匿名认证开启时 Auth:Users 全部不生效；非匿名但无账号时任何客户端都无法连接，必须启动失败。
    /// </summary>
    public class AuthConfigurationValidationTests
    {
        [Fact]
        public void ValidateAuthConfiguration_NonAnonymousWithoutUsers_Throws()
        {
            var configuration = BuildConfiguration(new Dictionary<string, string?>
            {
                ["Auth:AllowAnonymous"] = "false"
            });

            var exception = Assert.Throws<InvalidOperationException>(
                () => Program.ValidateAuthConfiguration(configuration, CreateSilentLogger()));

            Assert.Contains("Auth:Users", exception.Message, StringComparison.Ordinal);
        }

        [Fact]
        public void ValidateAuthConfiguration_NonAnonymousWithUsers_DoesNotThrow()
        {
            var configuration = BuildConfiguration(new Dictionary<string, string?>
            {
                ["Auth:AllowAnonymous"] = "false",
                ["Auth:Users:0:Username"] = "app1",
                ["Auth:Users:0:Password"] = "secret"
            });

            Program.ValidateAuthConfiguration(configuration, CreateSilentLogger());
        }

        [Fact]
        public void ValidateAuthConfiguration_AnonymousWithUsers_DoesNotThrow()
        {
            var configuration = BuildConfiguration(new Dictionary<string, string?>
            {
                ["Auth:AllowAnonymous"] = "true",
                ["Auth:Users:0:Username"] = "app1",
                ["Auth:Users:0:Password"] = "secret"
            });

            // 匿名开启时仅告警不失败，但配置的账号与 ClientIdPrefix 不生效这一事实必须被记录
            Program.ValidateAuthConfiguration(configuration, CreateSilentLogger());
        }

        private static IConfiguration BuildConfiguration(Dictionary<string, string?> values)
        {
            return new ConfigurationBuilder().AddInMemoryCollection(values).Build();
        }

        private static Serilog.ILogger CreateSilentLogger()
        {
            return new LoggerConfiguration().CreateLogger();
        }
    }
}
