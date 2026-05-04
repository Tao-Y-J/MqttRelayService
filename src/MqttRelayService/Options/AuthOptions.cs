namespace MqttRelayService.Options;

/// <summary>
/// 认证用户配置
/// </summary>
public class AuthUserOptions
{
    /// <summary>
    /// 用户名
    /// </summary>
    public string Username { get; set; } = string.Empty;

    /// <summary>
    /// 密码
    /// </summary>
    public string Password { get; set; } = string.Empty;

    /// <summary>
    /// 可选的 ClientId 前缀约束
    /// </summary>
    public string? ClientIdPrefix { get; set; }
}

/// <summary>
/// 认证配置选项
/// </summary>
public class AuthOptions
{
    /// <summary>
    /// 是否允许匿名认证；匿名认证开启时仍要求 ClientId 非空，但跳过账号密码校验
    /// </summary>
    public bool AllowAnonymous { get; set; } = true;

    /// <summary>
    /// 认证用户列表，仅在 AllowAnonymous 为 false 时生效
    /// </summary>
    public List<AuthUserOptions> Users { get; set; } = new();
}