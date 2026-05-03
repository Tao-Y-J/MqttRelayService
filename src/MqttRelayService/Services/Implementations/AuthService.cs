using Microsoft.Extensions.Options;
using MqttRelayService.Models;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;

namespace MqttRelayService.Services.Implementations;

/// <summary>
/// 认证服务实现，支持匿名认证和配置账号列表认证
/// </summary>
public class AuthService : IAuthService
{
    private readonly AuthOptions _options;
    private readonly ILogger<AuthService> _logger;

    public AuthService(IOptions<AuthOptions> options, ILogger<AuthService> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>
    /// 执行客户端认证
    /// </summary>
    public Task<AuthResult> AuthenticateAsync(AuthRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            // ClientId 不能为空（无论是否允许匿名）
            if (string.IsNullOrWhiteSpace(request.ClientId))
            {
                _logger.LogWarning("认证失败：ClientId 为空");
                return Task.FromResult(new AuthResult
                {
                    Success = false,
                    ErrorMessage = "ClientId 不能为空"
                });
            }

            // 允许匿名认证时，跳过账号密码校验
            if (_options.AllowAnonymous)
            {
                _logger.LogInformation("客户端 {ClientId} 通过匿名认证", request.ClientId);
                return Task.FromResult(new AuthResult { Success = true });
            }

            // 关闭匿名认证后，必须提供用户名和密码
            if (string.IsNullOrWhiteSpace(request.Username))
            {
                _logger.LogWarning("认证失败：客户端 {ClientId} 用户名为空", request.ClientId);
                return Task.FromResult(new AuthResult
                {
                    Success = false,
                    ErrorMessage = "用户名不能为空"
                });
            }

            if (string.IsNullOrWhiteSpace(request.Password))
            {
                _logger.LogWarning("认证失败：客户端 {ClientId} 密码为空", request.ClientId);
                return Task.FromResult(new AuthResult
                {
                    Success = false,
                    ErrorMessage = "密码不能为空"
                });
            }

            // 按配置用户列表校验
            var user = _options.Users.FirstOrDefault(u =>
                u.Username == request.Username && u.Password == request.Password);

            if (user == null)
            {
                _logger.LogWarning("认证失败：客户端 {ClientId} 用户名或密码错误", request.ClientId);
                return Task.FromResult(new AuthResult
                {
                    Success = false,
                    ErrorMessage = "用户名或密码错误"
                });
            }

            // 校验 ClientIdPrefix（如果配置了）
            if (!string.IsNullOrEmpty(user.ClientIdPrefix) &&
                !request.ClientId.StartsWith(user.ClientIdPrefix))
            {
                _logger.LogWarning("认证失败：客户端 {ClientId} 不符合前缀约束 {Prefix}",
                    request.ClientId, user.ClientIdPrefix);
                return Task.FromResult(new AuthResult
                {
                    Success = false,
                    ErrorMessage = $"ClientId 必须以 '{user.ClientIdPrefix}' 开头"
                });
            }

            _logger.LogInformation("客户端 {ClientId} 认证成功，用户 {Username}",
                request.ClientId, request.Username);
            return Task.FromResult(new AuthResult { Success = true });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "认证过程中发生异常，客户端 {ClientId}", request.ClientId);
            return Task.FromResult(new AuthResult
            {
                Success = false,
                ErrorMessage = "认证过程异常"
            });
        }
    }
}