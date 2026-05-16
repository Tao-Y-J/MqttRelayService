namespace MqttRelayService.Options
{
    /// <summary>
    /// MQTT Broker 配置选项
    /// </summary>
    public class MqttOptions
    {
        /// <summary>
        /// TCP 监听端口，默认 1883
        /// </summary>
        public int TcpPort { get; set; } = 1883;

        /// <summary>
        /// 默认 QoS 等级
        /// </summary>
        public int DefaultQos { get; set; } = 1;
    }
}