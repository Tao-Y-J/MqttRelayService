using MqttRelayService.Logging;
using MqttRelayService.Options;
using MqttRelayService.Services.Abstractions;
using MqttRelayService.Services.Implementations;
using MqttRelayService.Workers;
using Serilog;

namespace MqttRelayService;

/// <summary>
/// 应用程序入口
/// </summary>
public class Program
{
    public static void Main(string[] args)
    {
        try
        {
            var builder = Host.CreateApplicationBuilder(args);

            // 读取服务名称（用于 Windows Service 和日志）
            var serviceOptions = builder.Configuration
                .GetSection("Service")
                .Get<ServiceOptions>() ?? new ServiceOptions();

            // 配置 Serilog 日志
            var logger = SerilogLogging.CreateLogger(builder.Configuration, serviceOptions.Name);
            builder.Logging.ClearProviders();
            builder.Logging.AddSerilog(logger);

            // 注册 Windows Service 支持
            builder.Services.AddWindowsService(options =>
            {
                options.ServiceName = serviceOptions.Name;
            });

            // 设置后台服务异常行为：不停止 Host
            builder.Services.Configure<HostOptions>(options =>
            {
                options.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.Ignore;
                options.ShutdownTimeout = TimeSpan.FromMilliseconds(
                    builder.Configuration.GetValue("Reliability:ShutdownDrainTimeoutMs", 10000));
            });

            // 注册配置选项
            builder.Services.Configure<ServiceOptions>(builder.Configuration.GetSection("Service"));
            builder.Services.Configure<MqttOptions>(builder.Configuration.GetSection("Mqtt"));
            builder.Services.Configure<AuthOptions>(builder.Configuration.GetSection("Auth"));
            builder.Services.Configure<RoutingOptions>(builder.Configuration.GetSection("Routing"));
            builder.Services.Configure<ReliabilityOptions>(builder.Configuration.GetSection("Reliability"));

            // 注册核心服务（单例生命周期，因为内部维护状态）
            builder.Services.AddSingleton<IAuthService, AuthService>();
            builder.Services.AddSingleton<IClientRegistry, ClientRegistry>();
            builder.Services.AddSingleton<IRetryPolicyProvider, RetryPolicyProvider>();
            builder.Services.AddSingleton<IDeadLetterService, DeadLetterService>();
            builder.Services.AddSingleton<IMessageQueue, InMemoryMessageQueue>();
            builder.Services.AddSingleton<IMessageRouter, MessageRouter>();
            builder.Services.AddSingleton<IMqttBrokerHost, MqttBrokerHost>();
            builder.Services.AddSingleton<IMessageDeliveryService, MessageDeliveryService>();

            // 注册后台服务
            builder.Services.AddHostedService<BrokerWorker>();
            builder.Services.AddHostedService<QueueMetricsWorker>();
            builder.Services.AddHostedService<DeliveryWorker>();

            var host = builder.Build();

            host.Run();
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
}