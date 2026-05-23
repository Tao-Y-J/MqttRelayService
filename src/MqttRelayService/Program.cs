using System;
using System.IO;
using System.Linq;
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
        public static void Main(string[] args)
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
                    RunWebHost(args);
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

        private static void RunWebHost(string[] args)
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
                var auditRepository = scope.ServiceProvider.GetRequiredService<IAuditRepository>();
                auditRepository.InitializeAsync().GetAwaiter().GetResult();

                if (scope.ServiceProvider.GetRequiredService<IMetricsService>() is MetricsService metricsService)
                {
                    metricsService.InitializeDashboardCountersFromAuditAsync().GetAwaiter().GetResult();
                }
            }

            MapWebEndpoints(app);
            MapDashboard(app);
            app.Run();
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
            logging.ClearProviders();
            logging.AddSerilog(logger);

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

        private static void ConfigureCoreServices(IServiceCollection services, IConfiguration configuration, bool enableWeb)
        {
            services.Configure<ServiceOptions>(configuration.GetSection("Service"));
            services.Configure<MqttOptions>(configuration.GetSection("Mqtt"));
            services.Configure<AuthOptions>(configuration.GetSection("Auth"));
            services.Configure<RoutingOptions>(configuration.GetSection("Routing"));
            services.Configure<ReliabilityOptions>(configuration.GetSection("Reliability"));
            services.Configure<WebOptions>(configuration.GetSection("Web"));
            services.Configure<AuditStorageOptions>(configuration.GetSection("AuditStorage"));

            services.AddSingleton<ThroughputController>();
            services.AddSingleton<IAuthService, AuthService>();

            if (enableWeb)
            {
                services.AddSingleton<IAuditRepository, AuditRepository>();
                services.AddSingleton<IMetricsService, MetricsService>();

                services.AddSingleton<ClientRegistry>();
                services.AddSingleton<DeadLetterService>();
                services.AddSingleton<InMemoryMessageQueue>();
                services.AddSingleton<MqttBrokerHost>();

                services.AddSingleton<IClientRegistry>(sp =>
                    new Services.Implementations.Decorators.MetricsClientRegistry(
                        sp.GetRequiredService<ClientRegistry>(),
                        sp.GetRequiredService<IAuditRepository>()));

                services.AddSingleton<IDeadLetterService>(sp =>
                    new Services.Implementations.Decorators.MetricsDeadLetterService(
                        sp.GetRequiredService<DeadLetterService>(),
                        sp.GetRequiredService<IMetricsService>()));

                services.AddSingleton<IMessageQueue>(sp =>
                    new Services.Implementations.Decorators.MetricsMessageQueue(
                        sp.GetRequiredService<InMemoryMessageQueue>(),
                        sp.GetRequiredService<IMetricsService>()));

                services.AddSingleton<IMqttBrokerHost>(sp =>
                    new Services.Implementations.Decorators.MetricsMqttBrokerHost(
                        sp.GetRequiredService<MqttBrokerHost>(),
                        sp.GetRequiredService<IMetricsService>()));
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

        private static void MapWebEndpoints(WebApplication app)
        {
            app.MapGet("/api/metrics", async (IMetricsService metricsService) =>
            {
                var data = await metricsService.GetDashboardDataAsync();
                return Results.Ok(data);
            });

            app.MapGet("/api/messages", async (
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
                int p = page ?? 1;
                int ps = pageSize ?? 10;
                DateTime? start = null;
                if (!string.IsNullOrEmpty(startDate) && DateTime.TryParse(startDate, out var st))
                {
                    start = st;
                }

                DateTime? end = null;
                if (!string.IsNullOrEmpty(endDate) && DateTime.TryParse(endDate, out var et))
                {
                    end = et;
                }

                var result = await auditRepo.GetPagedMessagesAsync(p, ps, status, topic, sourceClientId, search, start, end);
                return Results.Ok(new { total = result.TotalCount, items = result.Items });
            });

            app.MapGet("/api/payload/{messageId}", async (
                string messageId,
                IMetricsService metricsService,
                IAuditRepository auditRepo) =>
            {
                var payloadStr = metricsService.GetPayload(messageId);
                if (payloadStr != null)
                {
                    return Results.Ok(new { messageId, payload = payloadStr });
                }

                var result = await auditRepo.GetPagedMessagesAsync(
                    1,
                    1,
                    status: null,
                    topic: null,
                    sourceClientId: null,
                    search: messageId);

                if (result.Items.Count > 0)
                {
                    var record = result.Items[0];
                    return Results.Ok(new { messageId, payload = record.Payload ?? string.Empty });
                }

                return Results.NotFound(new { messageId, error = "Message payload not found." });
            });

            app.MapGet("/api/settings/throughput", (ThroughputController controller) =>
            {
                return Results.Ok(new
                {
                    maxMessagesPerSecond = controller.MaxMessagesPerSecond,
                    maxConcurrency = controller.MaxConcurrency,
                    activeCount = controller.ActiveCount
                });
            });

            app.MapPost("/api/settings/throughput", (ThroughputSettingsDto settings, ThroughputController controller) =>
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

            app.MapGet("/api/clients/active", async (IClientRegistry clientRegistry) =>
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

            app.MapGet("/api/clients/history", async (
                IAuditRepository auditRepo,
                int? page,
                int? pageSize,
                string? clientId,
                string? eventType,
                string? search) =>
            {
                int p = page ?? 1;
                int ps = pageSize ?? 10;
                var result = await auditRepo.GetPagedClientHistoryAsync(p, ps, clientId, eventType, search);
                return Results.Ok(new { total = result.TotalCount, items = result.Items });
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

            app.UseStaticFiles(new StaticFileOptions
            {
                FileProvider = new Microsoft.Extensions.FileProviders.PhysicalFileProvider(staticFilesDir),
                RequestPath = ""
            });

            app.MapGet("/", async context =>
            {
                context.Response.ContentType = "text/html; charset=utf-8";
                var htmlPath = Path.Combine(staticFilesDir, "index.html");
                if (File.Exists(htmlPath))
                {
                    await context.Response.SendFileAsync(htmlPath);
                }
                else
                {
                    context.Response.StatusCode = 404;
                    await context.Response.WriteAsync("MQTT Relay Dashboard page not found.");
                }
            });
        }

        /// <summary>
        /// 按停机安全顺序注册后台服务。
        /// </summary>
        internal static void RegisterHostedServices(IServiceCollection services)
        {
            services.AddHostedService<QueueMetricsWorker>();
            services.AddHostedService<DeliveryWorker>();
            services.AddHostedService<BrokerWorker>();
        }
    }

    /// <summary>
    /// 吞吐量调控传输参数，MaxMessagesPerSecond 表示单线程每秒最大转发量。
    /// </summary>
    public record ThroughputSettingsDto(int MaxMessagesPerSecond, int MaxConcurrency);
}
