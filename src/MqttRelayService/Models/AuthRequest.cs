namespace MqttRelayService.Models;

/// <summary>
/// 认证请求
/// </summary>
public class AuthRequest
{
    /// <summary>
    /// 客户端标识
    /// </summary>
    public string ClientId { get; set; } = string.Empty;

    /// <summary>
    /// 用户名
    /// </summary>
    public string Username { get; set; } = string.Empty;

    /// <summary>
    /// 密码
    /// </summary>
    public string Password { get; set; } = string.Empty;
}