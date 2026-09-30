using System;
using System.IO;
using System.Reflection;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// Dashboard 页面与前端渲染安全相关测试。
    /// </summary>
    public class DashboardPageTests
    {
        [Fact]
        public void Program_ShouldNotExposeDashboardApiKeyBootstrap()
        {
            // 早期实现通过 BuildDashboardHtml 把 Web:ApiKey 明文注入到无需鉴权的 / 与 /index.html，
            // 使 X-Api-Key 认证彻底失效。该方法必须保持删除状态，不能再被重新引入。
            var method = typeof(MqttRelayService.Program).GetMethod(
                "BuildDashboardHtml",
                BindingFlags.NonPublic | BindingFlags.Static);

            Assert.Null(method);
        }

        [Fact]
        public void DashboardSource_ShouldUseCentralizedFetchHelperAndExactMessageEndpoint()
        {
            var source = ReadDashboardSource();

            Assert.Contains("function dashboardFetch(input, init = {})", source);
            Assert.Contains("dashboardFetch('/api/metrics')", source);
            Assert.Contains("dashboardFetch(`/api/messages/${encodeURIComponent(messageId)}`)", source);
            Assert.Contains("dashboardFetch(`/api/payload/${encodeURIComponent(log.messageId)}`)", source);
        }

        [Fact]
        public void DashboardSource_ShouldReadApiKeyFromSessionStorageInsteadOfServerBootstrap()
        {
            var source = ReadDashboardSource();

            // 密钥只能由运维录入并保存在浏览器会话存储中，不能被服务端注入到未鉴权页面。
            Assert.Contains("sessionStorage", source);
            Assert.Contains("X-Api-Key", source);
            Assert.Contains("response.status === 401", source);
            Assert.DoesNotContain("window.__dashboardAuth", source);
        }

        [Fact]
        public void DashboardSource_ShouldEscapeClientControlledFields()
        {
            var source = ReadDashboardSource();

            // 客户端可控字段（ClientId、用户名、订阅主题、消息 Topic）绝不允许未转义拼入 innerHTML
            Assert.Contains("escapeHtml(c.clientId)", source);
            Assert.Contains("escapeHtml(c.username)", source);
            Assert.Contains("escapeHtml(topic)", source);
            Assert.Contains("escapeHtml(log.sourceClientId)", source);
            Assert.Contains("escapeHtml(log.topic)", source);
            Assert.DoesNotContain("${c.clientId}", source);
            Assert.DoesNotContain("${c.username || ", source);
            Assert.DoesNotContain("${log.sourceClientId}", source);
            Assert.DoesNotContain("${log.topic}", source);

            // 内联 onclick 中的消息 ID 必须编码后传递，避免越出 JS 字符串边界
            Assert.Contains("openMessageDetail('${encodeURIComponent(log.messageId)}')", source);
            Assert.Contains("decodeURIComponent(encodedMessageId", source);
        }

        /// <summary>
        /// 从测试输出目录向上定位仓库根目录后读取 Dashboard 页面源码，
        /// 避免依赖输出目录层级的硬编码相对路径。
        /// </summary>
        private static string ReadDashboardSource()
        {
            var directory = new DirectoryInfo(AppContext.BaseDirectory);
            while (directory != null)
            {
                var candidate = Path.Combine(directory.FullName, "src", "MqttRelayService", "wwwroot", "index.html");
                if (File.Exists(candidate))
                {
                    return File.ReadAllText(candidate);
                }

                directory = directory.Parent;
            }

            throw new FileNotFoundException("未能定位 src/MqttRelayService/wwwroot/index.html");
        }
    }
}
