namespace MqttRelayService.Options
{
    /// <summary>
    /// 服务基本配置选项
    /// </summary>
    public class ServiceOptions
    {
        /// <summary>
        /// 服务名称，同时作为 Windows Service 名称和日志扩展属性
        /// </summary>
        public string Name { get; set; } = "MqttRelayService";
    }
}