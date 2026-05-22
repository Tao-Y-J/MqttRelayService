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
    }
}
