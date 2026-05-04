using MqttRelayService.Models;

namespace MqttRelayService.Services.Abstractions;

/// <summary>
/// 认证服务接口，负责客户端身份校验
/// </summary>
public interface IAuthService
{
    /// <summary>
    /// 执行客户端认证
    /// </summary>
    Task<AuthResult> AuthenticateAsync(AuthRequest request, CancellationToken cancellationToken = default);
}