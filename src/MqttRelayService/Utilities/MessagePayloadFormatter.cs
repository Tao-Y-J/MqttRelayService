using System.Text;

namespace MqttRelayService.Utilities
{
    /// <summary>
    /// 消息负载格式化工具
    /// </summary>
    public static class MessagePayloadFormatter
    {
        /// <summary>
        /// 将字节数组转为 Base64
        /// </summary>
        public static string ToBase64(byte[] payload)
        {
            return payload?.Length > 0 ? Convert.ToBase64String(payload) : string.Empty;
        }
    }
}