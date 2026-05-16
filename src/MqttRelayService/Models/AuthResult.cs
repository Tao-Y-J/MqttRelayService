namespace MqttRelayService.Models
{
    /// <summary>
    /// 认证结果
    /// </summary>
    public class AuthResult
    {
        /// <summary>
        /// 是否认证成功
        /// </summary>
        public bool Success { get; set; }

        /// <summary>
        /// 错误信息
        /// </summary>
        public string? ErrorMessage { get; set; }
    }
}