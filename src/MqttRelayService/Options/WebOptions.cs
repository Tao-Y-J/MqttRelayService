namespace MqttRelayService.Options
{
    /// <summary>
    /// 指标 Web API 服务选项，对应 appsettings.json 中的 "Web" 节点
    /// </summary>
    public class WebOptions
    {
        /// <summary>
        /// Kestrel 监听的指标 API 端口号，默认值为 5000
        /// </summary>
        public int MetricsApiPort { get; set; } = 5000;

        /// <summary>
        /// 是否启用指标 API 终结点，默认值为 true
        /// </summary>
        public bool EnableMetricsApi { get; set; } = true;

        /// <summary>
        /// 是否启用 Dashboard 网页，默认值为 true
        /// </summary>
        public bool EnableDashboard { get; set; } = true;

        /// <summary>
        /// Dashboard 网页监听的端口号，默认值为 5001
        /// </summary>
        public int DashboardPort { get; set; } = 5001;
    }
}
