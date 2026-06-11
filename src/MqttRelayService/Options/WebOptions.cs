namespace MqttRelayService.Options
{
    /// <summary>
    /// Web 管理面配置。
    /// </summary>
    public class WebOptions
    {
        /// <summary>
        /// 是否启用统一 Web 管理面。
        /// </summary>
        public bool Enabled { get; set; } = true;

        /// <summary>
        /// 统一 Web 监听端口。
        /// </summary>
        public int Port { get; set; } = 5000;

        /// <summary>
        /// API 访问密钥。为空时跳过 API 认证校验（不推荐生产环境）。
        /// 客户端需在请求头中携带 X-Api-Key 与此值匹配。
        /// </summary>
        public string? ApiKey { get; set; }
    }
}
