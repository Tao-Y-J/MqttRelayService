using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MqttRelayService.Logging;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;
using MqttRelayService.Services.Implementations;
using MqttRelayService.Utilities;
using MqttRelayService.Workers;
using Serilog;

namespace MqttRelayService
{
    /// <summary>
    /// 应用程序入口，统一承载 MQTT Broker、指标 API 与 Dashboard。
    /// </summary>
    public class Program
    {
        /// <summary>
        /// Web API 单页最大条数上限，防止超大 pageSize 一次性物化整张审计表。
        /// </summary>
        private const int MaxApiPageSize = 200;

        /// <summary>
        /// Web API 最大页码，与 MaxApiPageSize 配合保证 Skip 偏移量不溢出。
        /// </summary>
        private const int MaxApiPageNumber = 1000000;

        public static async Task Main(string[] args)
        {
            try
            {
                var bootstrapEnvironmentName =
                    Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT")
                    ?? Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")
                    ?? Environments.Production;

                var bootstrapConfiguration = new ConfigurationBuilder()
                    .SetBasePath(AppContext.BaseDirectory)
                    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
                    .AddJsonFile($"appsettings.{bootstrapEnvironmentName}.json", optional: true, reloadOnChange: false)
                    .AddEnvironmentVariables()
                    .AddCommandLine(args)
                    .Build();

                var bootstrapWebOptions = bootstrapConfiguration.GetSection("Web").Get<WebOptions>() ?? new WebOptions();

                if (bootstrapWebOptions.Enabled)
                {
                    await RunWebHostAsync(args);
                }
                else
                {
                    RunWorkerHost(args);
                }
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "应用程序启动失败");
                throw;
            }
            finally
            {
                Log.CloseAndFlush();
            }
        }

        private static async Task RunWebHostAsync(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            var serviceOptions = builder.Configuration
                .GetSection("Service")
                .Get<ServiceOptions>() ?? new ServiceOptions();

            ConfigureLoggingAndServices(builder.Services, builder.Configuration, builder.Logging, serviceOptions, enableWeb: true);

            var webOptions = builder.Configuration.GetSection("Web").Get<WebOptions>() ?? new WebOptions();
            builder.WebHost.ConfigureKestrel(kestrel =>
            {
                kestrel.ListenAnyIP(webOptions.Port);
            });

            var app = builder.Build();

            using (var scope = app.Services.CreateScope())
            {
                // 审计持久化属于 Web 管理面的可选能力：初始化失败时必须降级为"审计不可用"，
                // 不能因为审计库路径、Provider 或权限问题把整个 MQTT 转发主链路一起拖停。
                try
                {
                    var auditRepository = scope.ServiceProvider.GetRequiredService<IAuditRepository>();
                    await auditRepository.InitializeAsync();

                    if (scope.ServiceProvider.GetRequiredService<IMetricsService>() is MetricsService metricsService)
                    {
                        await metricsService.InitializeDashboardCountersFromAuditAsync();
                    }
                }
                catch (Exception ex)
                {
                    Log.ForContext<Program>().Error(ex,
                        "审计持久化初始化失败，Web 管理面将以“审计不可用”状态继续提供实时指标");
                }
            }

            Log.ForContext<Program>().Information(
                "Web 管理面已启用：监听端口 {Port}，API 认证 {AuthState}",
                webOptions.Port,
                string.IsNullOrWhiteSpace(webOptions.ApiKey) ? "未配置（所有 /api 端点无需鉴权）" : "已通过 X-Api-Key 启用");

            if (string.IsNullOrWhiteSpace(webOptions.ApiKey))
            {
                Log.ForContext<Program>().Warning(
                    "Web 管理面未配置 Web:ApiKey，所有 /api 端点（含审计与载荷查询、客户端清单、吞吐调控）都不做鉴权，安全边界完全依赖网络隔离");
            }

            MapWebEndpoints(app);
            MapDashboard(app);
            await app.RunAsync();
        }

        private static void RunWorkerHost(string[] args)
        {
            var builder = Host.CreateApplicationBuilder(args);

            var serviceOptions = builder.Configuration
                .GetSection("Service")
                .Get<ServiceOptions>() ?? new ServiceOptions();

            ConfigureLoggingAndServices(builder.Services, builder.Configuration, builder.Logging, serviceOptions, enableWeb: false);

            using var host = builder.Build();
            host.Run();
        }

        private static void ConfigureLoggingAndServices(
            IServiceCollection services,
            IConfiguration configuration,
            ILoggingBuilder logging,
            ServiceOptions serviceOptions,
            bool enableWeb)
        {
            var logger = SerilogLogging.CreateLogger(configuration, serviceOptions.Name);
            // 必须同时赋值 Serilog 静态门面：Main 的 Log.Fatal / Log.CloseAndFlush 走的是静态 Log.Logger，
            // 只调用 AddSerilog(ILogger) 不会设置它，启动致命错误与退出 flush 都会变成静默空操作。
            Log.Logger = logger;

            logging.ClearProviders();
            logging.AddSerilog(logger);

            ValidateAuthConfiguration(configuration, logger);

            services.AddWindowsService(options =>
            {
                options.ServiceName = serviceOptions.Name;
            });

            services.Configure<HostOptions>(options =>
            {
                options.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.Ignore;
                var shutdownDrainTimeoutMs = configuration.GetValue("Reliability:ShutdownDrainTimeoutMs", 30000);
                options.ShutdownTimeout = TimeSpan.FromMilliseconds(shutdownDrainTimeoutMs + 5000);
            });

            ConfigureCoreServices(services, configuration, enableWeb);
            RegisterHostedServices(services);
        }

        /// <summary>
        /// 启动时校验认证配置的一致性并给出明确告警。
        /// 匿名认证开启时 Auth:Users 与 ClientIdPrefix 全部不生效；非匿名但没有任何账号时等于无人可连，必须启动失败。
        /// </summary>
        internal static void ValidateAuthConfiguration(IConfiguration configuration, Serilog.ILogger logger)
        {
            var authOptions = configuration.GetSection("Auth").Get<AuthOptions>() ?? new AuthOptions();

            if (authOptions.AllowAnonymous)
            {
                if (authOptions.Users.Count > 0)
                {
                    logger.Warning(
                        "认证配置告警：Auth:AllowAnonymous=true 时 Auth:Users（{UserCount} 个账号）与 ClientIdPrefix 全部不生效，Broker 接受任何客户端的匿名连接，错口令也不会被拒绝",
                        authOptions.Users.Count);
                }
                else
                {
                    logger.Warning("认证配置告警：Auth:AllowAnonymous=true，Broker 接受任何客户端的匿名连接");
                }

                return;
            }

            if (authOptions.Users.Count == 0)
            {
                throw new InvalidOperationException("Auth:AllowAnonymous=false 时必须配置至少一个 Auth:Users 账号，否则任何客户端都无法连接");
            }
        }

        internal static void ConfigureCoreServices(IServiceCollection services, IConfiguration configuration, bool enableWeb)
        {
            services.Configure<ServiceOptions>(configuration.GetSection("Service"));

            services.AddOptions<MqttOptions>()
                .Bind(configuration.GetSection("Mqtt"))
                .Validate(o => o.TcpPort is >= 1 and <= 65535, "Mqtt:TcpPort 必须在 1-65535 之间")
                .ValidateOnStart();

            services.Configure<AuthOptions>(configuration.GetSection("Auth"));
            services.Configure<RoutingOptions>(configuration.GetSection("Routing"));

            // 可靠性配置集中校验：这些值直接决定队列上限、超时与重试语义，
            // 误配（例如 QueueCapacity=0 或 RetryBaseDelayMs=0）必须在启动时就失败，而不是运行期隐式退化。
            services.AddOptions<ReliabilityOptions>()
                .Bind(configuration.GetSection("Reliability"))
                .Validate(o => o.QueueCapacity >= 1, "Reliability:QueueCapacity 必须大于等于 1")
                .Validate(o => o.MaxConcurrentHandlers >= 0, "Reliability:MaxConcurrentHandlers 不能为负数")
                .Validate(o => o.MaxPendingRetryTasks >= 0, "Reliability:MaxPendingRetryTasks 不能为负数")
                .Validate(o => o.EnqueueTimeoutMs > 0, "Reliability:EnqueueTimeoutMs 必须大于 0")
                .Validate(o => o.ForwardTimeoutMs > 0, "Reliability:ForwardTimeoutMs 必须大于 0")
                .Validate(o => o.ShutdownDrainTimeoutMs > 0, "Reliability:ShutdownDrainTimeoutMs 必须大于 0")
                .Validate(o => o.RetryBaseDelayMs > 0, "Reliability:RetryBaseDelayMs 必须大于 0")
                .Validate(o => o.RetryMaxDelayMs >= o.RetryBaseDelayMs,
                    "Reliability:RetryMaxDelayMs 不能小于 Reliability:RetryBaseDelayMs")
                .Validate(o => string.Equals(o.DeliverySemantics, "AtLeastOnce", StringComparison.OrdinalIgnoreCase),
                    "Reliability:DeliverySemantics 目前只支持 AtLeastOnce")
                .ValidateOnStart();

            services.Configure<WebOptions>(configuration.GetSection("Web"));
            services.Configure<AuditStorageOptions>(configuration.GetSection("AuditStorage"));

            ValidateAuditStorageConfiguration(configuration);

            services.AddSingleton(sp =>
            {
                var reliabilityOptions = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<ReliabilityOptions>>().Value;
                return new ThroughputController(reliabilityOptions.MaxConcurrencyHardLimit);
            });
            services.AddSingleton<IAuthService, AuthService>();

            if (enableWeb)
            {
                services.AddSingleton<IAuditRepository, AuditRepository>();
                services.AddSingleton<IMetricsService, MetricsService>();

                // 延迟解析包装器：装饰器与被装饰依赖之间存在构造期互相依赖
                // （MetricsService 依赖 IMessageQueue/IClientRegistry，而这两个装饰器又需要 IMetricsService），
                // 工厂委托形式的注册让容器无法静态识别该环，必须由延迟解析在首次使用时打破。
                services.AddSingleton(typeof(LazyService<>));

                services.AddSingleton<ClientRegistry>();
                services.AddSingleton<DeadLetterService>();
                services.AddSingleton<InMemoryMessageQueue>();
                services.AddSingleton<MqttBrokerHost>();

                services.AddSingleton<IClientRegistry>(sp =>
                    new Services.Implementations.Decorators.MetricsClientRegistry(
                        sp.GetRequiredService<ClientRegistry>(),
                        sp.GetRequiredService<LazyService<IMetricsService>>()));

                services.AddSingleton<IDeadLetterService>(sp =>
                    new Services.Implementations.Decorators.MetricsDeadLetterService(
                        sp.GetRequiredService<DeadLetterService>(),
                        sp.GetRequiredService<IMetricsService>(),
                        sp.GetRequiredService<ILogger<Services.Implementations.Decorators.MetricsDeadLetterService>>()));

                services.AddSingleton<IMessageQueue>(sp =>
                    new Services.Implementations.Decorators.MetricsMessageQueue(
                        sp.GetRequiredService<InMemoryMessageQueue>(),
                        sp.GetRequiredService<LazyService<IMetricsService>>(),
                        sp.GetRequiredService<ILogger<Services.Implementations.Decorators.MetricsMessageQueue>>()));

                services.AddSingleton<IMqttBrokerHost>(sp =>
                    new Services.Implementations.Decorators.MetricsMqttBrokerHost(
                        sp.GetRequiredService<MqttBrokerHost>(),
                        sp.GetRequiredService<IMetricsService>(),
                        sp.GetRequiredService<ILogger<Services.Implementations.Decorators.MetricsMqttBrokerHost>>()));
            }
            else
            {
                services.AddSingleton<IClientRegistry, ClientRegistry>();
                services.AddSingleton<IDeadLetterService, DeadLetterService>();
                services.AddSingleton<IMessageQueue, InMemoryMessageQueue>();
                services.AddSingleton<IMqttBrokerHost, MqttBrokerHost>();
            }

            services.AddSingleton<IRetryPolicyProvider, RetryPolicyProvider>();
            services.AddSingleton<IMessageRouter, MessageRouter>();
            services.AddSingleton<IMessageDeliveryService, MessageDeliveryService>();
        }

        /// <summary>
        /// 校验审计数据清理配置。
        /// 这些值直接决定「每天至少清理一次」的保证：误配（例如把清理间隔配成 30 天、或把清理时刻配成 25 点）
        /// 必须在启动时失败，而不能在运行期退化成静默不清理、让审计库随运行时间无界增长。
        /// <see cref="AuditStorageOptions.RetentionDays"/> 小于 0 视为误配；等于 0 是明确的「关闭清理」语义。
        /// </summary>
        internal static void ValidateAuditStorageConfiguration(IConfiguration configuration)
        {
            var auditStorageOptions = configuration.GetSection("AuditStorage").Get<AuditStorageOptions>()
                ?? new AuditStorageOptions();

            if (auditStorageOptions.RetentionDays < 0)
            {
                throw new InvalidOperationException("AuditStorage:RetentionDays 不能为负数（配置为 0 表示关闭清理）");
            }

            if (auditStorageOptions.CleanupAtHour is < 0 or > 23)
            {
                throw new InvalidOperationException("AuditStorage:CleanupAtHour 必须是 0-23 之间的整点，留空表示按 AuditStorage:CleanupIntervalMinutes 等间隔执行");
            }

            if (auditStorageOptions.CleanupIntervalMinutes is < 1 or > 1440)
            {
                throw new InvalidOperationException("AuditStorage:CleanupIntervalMinutes 必须大于等于 1 且不超过 1440（分钟），以保证每天至少执行一次清理");
            }
        }

        /// <summary>
        /// 构造 Web 管理面「审计持久化存储」卡片所需的只读配置快照。
        /// 刻意只回传展示字段、不回传连接串原文：Web 管理面在未配置 Web:ApiKey 时匿名可访问，
        /// 而连接串可能包含账号密码，属于不能下发到浏览器的信息；非 SQLite 提供程序只回传提供程序名。
        /// </summary>
        internal static AuditStorageSettingsDto BuildAuditStorageSettings(AuditStorageOptions options)
        {
            var provider = options.Provider ?? string.Empty;

            return new AuditStorageSettingsDto(
                provider,
                IsSqliteProvider(provider)
                    ? AuditRepository.ExtractSqliteDataSource(options.ConnectionString)
                    : null,
                options.AutoInitializeSchema,
                options.RetentionDays,
                options.RetentionDays > 0,
                options.CleanupAtHour,
                options.CleanupIntervalMinutes,
                options.VacuumAfterCleanup,
                options.MessageArchiveThreshold,
                options.ClientHistoryArchiveThreshold);
        }

        /// <summary>
        /// 判断提供程序是否为 SQLite。提供程序名非法时按「非 SQLite」处理，
        /// 保证只读展示接口不会因为配置笔误返回 500（真正的非法提供程序由仓储构造时的 ParseDbType 拦截）。
        /// </summary>
        private static bool IsSqliteProvider(string provider)
        {
            try
            {
                return AuditRepository.ParseDbType(provider) == SqlSugar.DbType.Sqlite;
            }
            catch (ArgumentException)
            {
                return false;
            }
        }

        private static void MapWebEndpoints(WebApplication app)
        {
            var webOptions = app.Services.GetRequiredService<Microsoft.Extensions.Options.IOptions<WebOptions>>().Value;
            var api = app.MapGroup("/api");

            // API Key 认证过滤器：ApiKey 为空时跳过校验，否则校验请求头 X-Api-Key
            api.AddEndpointFilter(async (context, next) =>
            {
                var apiKey = webOptions.ApiKey;
                if (string.IsNullOrEmpty(apiKey))
                {
                    return await next(context);
                }

                var requestApiKey = context.HttpContext.Request.Headers["X-Api-Key"].FirstOrDefault();
                if (IsApiKeyMatch(requestApiKey, apiKey))
                {
                    return await next(context);
                }

                return Results.Unauthorized();
            });

            api.MapGet("/metrics", async (IMetricsService metricsService) =>
            {
                var data = await metricsService.GetDashboardDataAsync();
                return Results.Ok(data);
            });

            api.MapGet("/messages", async (
                IAuditRepository auditRepo,
                int? page,
                int? pageSize,
                string? status,
                string? topic,
                string? sourceClientId,
                string? search,
                string? startDate,
                string? endDate) =>
            {
                if (!TryParseFilterDate(startDate, out var start) || !TryParseFilterDate(endDate, out var end))
                {
                    return Results.BadRequest(new { error = "startDate/endDate 必须是可解析的日期时间。" });
                }

                if (start.HasValue && end.HasValue && start.Value > end.Value)
                {
                    return Results.BadRequest(new { error = "startDate 不能晚于 endDate。" });
                }

                var p = NormalizePage(page);
                var ps = NormalizePageSize(pageSize);

                var result = await auditRepo.GetPagedMessagesAsync(p, ps, status, topic, sourceClientId, search, start, end);
                return Results.Ok(new { total = result.TotalCount, items = result.Items });
            });

            api.MapGet("/messages/{messageId}", async (
                string messageId,
                IAuditRepository auditRepo) =>
            {
                var record = await auditRepo.GetMessageByIdAsync(messageId);
                return record is null
                    ? Results.NotFound(new { messageId, error = "Message audit record not found." })
                    : Results.Ok(record);
            });

            api.MapGet("/payload/{messageId}", async (
                string messageId,
                IMetricsService metricsService,
                IAuditRepository auditRepo) =>
            {
                var payloadStr = metricsService.GetPayload(messageId);
                if (payloadStr != null)
                {
                    return Results.Ok(new { messageId, payload = payloadStr });
                }

                var record = await auditRepo.GetMessageByIdAsync(messageId);
                if (record != null)
                {
                    return Results.Ok(new { messageId, payload = record.Payload ?? string.Empty });
                }

                return Results.NotFound(new { messageId, error = "Message payload not found." });
            });

            api.MapGet("/settings/throughput", (ThroughputController controller) =>
            {
                return Results.Ok(new
                {
                    maxMessagesPerSecond = controller.MaxMessagesPerSecond,
                    maxConcurrency = controller.MaxConcurrency,
                    activeCount = controller.ActiveCount
                });
            });

            api.MapPost("/settings/throughput", (ThroughputSettingsDto settings, ThroughputController controller) =>
            {
                controller.UpdateMaxMessagesPerSecond(settings.MaxMessagesPerSecond);
                controller.UpdateMaxConcurrency(settings.MaxConcurrency);

                return Results.Ok(new
                {
                    maxMessagesPerSecond = controller.MaxMessagesPerSecond,
                    maxConcurrency = controller.MaxConcurrency,
                    activeCount = controller.ActiveCount
                });
            });

            // 审计持久化配置只有启动时读取一次，运行期没有改写端点，前端因此只需加载一次
            api.MapGet("/settings/audit-storage", (
                Microsoft.Extensions.Options.IOptions<AuditStorageOptions> auditStorageOptions) =>
            {
                return Results.Ok(BuildAuditStorageSettings(auditStorageOptions.Value));
            });

            api.MapGet("/clients/active", async (IClientRegistry clientRegistry) =>
            {
                var sessions = await clientRegistry.GetAllSessionsAsync();
                var list = sessions.Select(s => new
                {
                    clientId = s.ClientId,
                    username = s.Username,
                    connectionId = s.ConnectionId,
                    connectedAt = s.ConnectedAt.ToString("o"),
                    lastActivityAt = s.LastActivityAt.ToString("o"),
                    status = s.Status.ToString(),
                    subscriptions = s.Subscriptions.ToList()
                }).ToList();
                return Results.Ok(list);
            });

            api.MapGet("/clients/history", async (
                IAuditRepository auditRepo,
                int? page,
                int? pageSize,
                string? clientId,
                string? eventType,
                string? search) =>
            {
                var p = NormalizePage(page);
                var ps = NormalizePageSize(pageSize);

                var result = await auditRepo.GetPagedClientHistoryAsync(p, ps, clientId, eventType, search);
                return Results.Ok(new { total = result.TotalCount, items = result.Items });
            });

            // 健康检查端点 — 不需要 API Key，直接暴露供监控探活
            app.MapGet("/api/health", (IMqttBrokerHost brokerHost, IMessageQueue queue, IClientRegistry clientRegistry) =>
            {
                return Results.Ok(new
                {
                    status = "healthy",
                    brokerRunning = brokerHost.IsRunning,
                    queueDepth = queue.Count,
                    queueCapacity = queue.Capacity,
                    activeClients = clientRegistry.Count,
                    timestamp = DateTime.Now.ToString("o")
                });
            });
        }

        private static void MapDashboard(WebApplication app)
        {
            var staticFilesDir = Path.Combine(AppContext.BaseDirectory, "wwwroot");
            if (!Directory.Exists(staticFilesDir))
            {
                staticFilesDir = Path.Combine(app.Environment.ContentRootPath, "wwwroot");
            }

            if (!Directory.Exists(staticFilesDir))
            {
                return;
            }

            async Task ServeDashboardAsync(HttpContext context)
            {
                context.Response.ContentType = "text/html; charset=utf-8";
                // 页面不再内嵌任何密钥：早期实现把 Web:ApiKey 明文注入到无需鉴权的 / 与 /index.html，
                // 使 X-Api-Key 认证彻底失效。现在由运维在页面上录入密钥，且只保存在浏览器 sessionStorage。
                context.Response.Headers.CacheControl = "no-store";

                var htmlPath = Path.Combine(staticFilesDir, "index.html");
                if (File.Exists(htmlPath))
                {
                    var html = await File.ReadAllTextAsync(htmlPath);
                    await context.Response.WriteAsync(html);
                }
                else
                {
                    context.Response.StatusCode = 404;
                    await context.Response.WriteAsync("MQTT Relay Dashboard page not found.");
                }
            }

            app.MapGet("/", ServeDashboardAsync);
            app.MapGet("/index.html", ServeDashboardAsync);
        }

        /// <summary>
        /// 收敛页码，避免超大页码产生负数 Skip 偏移。
        /// </summary>
        private static int NormalizePage(int? page)
        {
            return Math.Clamp(page ?? 1, 1, MaxApiPageNumber);
        }

        /// <summary>
        /// 收敛页长，避免调用方用超大 pageSize 一次性物化整张审计表。
        /// </summary>
        private static int NormalizePageSize(int? pageSize)
        {
            return Math.Clamp(pageSize ?? 10, 1, MaxApiPageSize);
        }

        /// <summary>
        /// 解析筛选日期参数。解析失败必须返回 400，
        /// 不能让调用方拿到"未按时间过滤"的结果集却以为已经过滤。
        /// 时间语义统一按服务器本机时区解释：不带时区的输入按本机时间处理，
        /// 带 Z 或偏移量的输入换算成本机时间后再与审计表的本地时间列比较。
        /// </summary>
        internal static bool TryParseFilterDate(string? raw, out DateTime? value)
        {
            value = null;
            if (string.IsNullOrEmpty(raw))
            {
                return true;
            }

            if (DateTime.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsed))
            {
                value = parsed.Kind == DateTimeKind.Utc ? parsed.ToLocalTime() : parsed;
                return true;
            }

            return false;
        }

        /// <summary>
        /// 以常量时间比较 API Key，避免通过响应时间差逐字节推测密钥。
        /// 长度不同的输入直接判定不匹配（长度本身不构成机密信息）。
        /// </summary>
        private static bool IsApiKeyMatch(string? provided, string expected)
        {
            if (provided == null)
            {
                return false;
            }

            var providedBytes = Encoding.UTF8.GetBytes(provided);
            var expectedBytes = Encoding.UTF8.GetBytes(expected);

            return providedBytes.Length == expectedBytes.Length
                && CryptographicOperations.FixedTimeEquals(providedBytes, expectedBytes);
        }

        /// <summary>
        /// 按停机安全顺序注册后台服务。
        /// <paramref name="enableWeb"/> 为 false（纯 Worker Host）时不存在审计仓储，因此不注册审计清理任务。
        /// </summary>
        internal static void RegisterHostedServices(IServiceCollection services, bool enableWeb = true)
        {
            // 注意：Host 按注册逆序停止，因此先注册的 Worker 会后停止。
            // 停机顺序必须是：AuditCleanupWorker（最先停）→ DeliveryWorker → BrokerWorker → QueueMetricsWorker（最后停）。
            // DeliveryWorker 先封堵客户端新发布入口，再取消消费者并排空队列；
            // 此时 Broker 仍在运行，排空阶段才能继续向订阅者注入消息。
            // 审计清理任务不参与停机排空，注册在最后（最先停止）可缩短停机时间。
            services.AddHostedService<QueueMetricsWorker>();
            services.AddHostedService<BrokerWorker>();
            services.AddHostedService<DeliveryWorker>();

            if (enableWeb)
            {
                services.AddHostedService<AuditCleanupWorker>();
            }
        }
    }

    /// <summary>
    /// 吞吐量调控传输参数，MaxMessagesPerSecond 表示单线程每秒最大转发量。
    /// </summary>
    public record ThroughputSettingsDto(int MaxMessagesPerSecond, int MaxConcurrency);

    /// <summary>
    /// 审计持久化存储的只读展示参数，用于 Dashboard 展示真实生效的审计配置与保留策略。
    /// 刻意不包含连接串原文：只有 SQLite 会回传 <see cref="SqliteDataSource"/>（连接串里的文件路径）。
    /// <see cref="CleanupEnabled"/> 为 false 表示 <see cref="RetentionDays"/> 配成 0、审计数据清理已关闭。
    /// <see cref="MessageArchiveThreshold"/> 与 <see cref="ClientHistoryArchiveThreshold"/> 只是启动告警阈值，
    /// 不触发任何删除动作。
    /// </summary>
    public record AuditStorageSettingsDto(
        string Provider,
        string? SqliteDataSource,
        bool AutoInitializeSchema,
        int RetentionDays,
        bool CleanupEnabled,
        int? CleanupAtHour,
        int CleanupIntervalMinutes,
        bool VacuumAfterCleanup,
        int MessageArchiveThreshold,
        int ClientHistoryArchiveThreshold);
}
