using System;
using System.IO;
using System.Reflection;
using MqttRelayService.Options;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// Dashboard 页面与 HTML 注入相关测试。
    /// </summary>
    public class DashboardPageTests
    {
        [Fact]
        public void BuildDashboardHtml_ShouldEmbedApiKeyBootstrap()
        {
            var method = typeof(MqttRelayService.Program).GetMethod(
                "BuildDashboardHtml",
                BindingFlags.NonPublic | BindingFlags.Static);

            Assert.NotNull(method);

            const string html = "<html><head></head><body></body></html>";
            var withKey = Assert.IsType<string>(method!.Invoke(null, new object[]
            {
                html,
                new WebOptions { ApiKey = "secret-key" }
            }));
            var withoutKey = Assert.IsType<string>(method.Invoke(null, new object[]
            {
                html,
                new WebOptions { ApiKey = null }
            }));

            Assert.Contains("window.__dashboardAuth", withKey);
            Assert.Contains("\"apiKey\":\"secret-key\"", withKey);
            Assert.Contains("window.__dashboardAuth", withoutKey);
            Assert.Contains("\"apiKey\":null", withoutKey);
        }

        [Fact]
        public void DashboardSource_ShouldUseCentralizedFetchHelperAndExactMessageEndpoint()
        {
            var htmlPath = Path.GetFullPath(Path.Combine(
                AppContext.BaseDirectory,
                "..", "..", "..", "..", "..",
                "src", "MqttRelayService", "wwwroot", "index.html"));

            var source = File.ReadAllText(htmlPath);

            Assert.Contains("function dashboardFetch(input, init = {})", source);
            Assert.DoesNotContain("fetch('/api/metrics')", source);
            Assert.DoesNotContain("fetch('/api/settings/throughput')", source);
            Assert.Contains("dashboardFetch('/api/metrics')", source);
            Assert.Contains("dashboardFetch(`/api/messages/${encodeURIComponent(messageId)}`)", source);
            Assert.Contains("dashboardFetch(`/api/payload/${encodeURIComponent(log.messageId)}`)", source);
        }
    }
}
