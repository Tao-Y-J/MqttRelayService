using System.Text.Json;
using MqttRelayService.Options;
using MqttRelayService.Services.Implementations;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// 审计存储只读展示配置的单元测试，覆盖 Web 管理面「审计持久化存储」卡片的数据来源：
    /// 卡片只允许展示真实生效的配置，且不得把数据库连接串原文回传给浏览器。
    /// </summary>
    public class AuditStorageSettingsTests
    {
        [Fact]
        public void BuildAuditStorageSettings_WithSqlite_ShouldReturnDataSourceFromConnectionString()
        {
            var settings = Program.BuildAuditStorageSettings(new AuditStorageOptions
            {
                Provider = "Sqlite",
                ConnectionString = "Data Source=data/audit.db"
            });

            Assert.Equal("Sqlite", settings.Provider);
            Assert.Equal("data/audit.db", settings.SqliteDataSource);
            Assert.True(settings.AutoInitializeSchema);
            Assert.True(settings.CleanupEnabled);
            Assert.Equal(30, settings.RetentionDays);
            Assert.Equal(3, settings.CleanupAtHour);
            Assert.Equal(1440, settings.CleanupIntervalMinutes);
            Assert.True(settings.VacuumAfterCleanup);
            Assert.Equal(5000000, settings.MessageArchiveThreshold);
            Assert.Equal(1000000, settings.ClientHistoryArchiveThreshold);
        }

        [Fact]
        public void BuildAuditStorageSettings_WithRetentionDaysZero_ShouldReportCleanupDisabled()
        {
            var settings = Program.BuildAuditStorageSettings(new AuditStorageOptions
            {
                Provider = "Sqlite",
                RetentionDays = 0
            });

            Assert.Equal(0, settings.RetentionDays);
            Assert.False(settings.CleanupEnabled);
        }

        [Fact]
        public void BuildAuditStorageSettings_ShouldKeepBothScheduleModes()
        {
            var daily = Program.BuildAuditStorageSettings(new AuditStorageOptions
            {
                CleanupAtHour = 3,
                CleanupIntervalMinutes = 1440
            });

            Assert.Equal(3, daily.CleanupAtHour);
            Assert.Equal(1440, daily.CleanupIntervalMinutes);

            var interval = Program.BuildAuditStorageSettings(new AuditStorageOptions
            {
                CleanupAtHour = null,
                CleanupIntervalMinutes = 30
            });

            Assert.Null(interval.CleanupAtHour);
            Assert.Equal(30, interval.CleanupIntervalMinutes);
        }

        [Fact]
        public void BuildAuditStorageSettings_WithNonSqliteProvider_ShouldNotExposeConnectionString()
        {
            const string connectionString = "Server=db;User Id=sa;Password=Pa55word";

            var json = JsonSerializer.Serialize(Program.BuildAuditStorageSettings(new AuditStorageOptions
            {
                Provider = "SqlServer",
                ConnectionString = connectionString
            }));

            // 未配置 Web:ApiKey 时页面匿名可访问，连接串中的账号密码绝不允许出现在接口响应里
            Assert.DoesNotContain("User Id", json);
            Assert.DoesNotContain("Password", json);
            Assert.DoesNotContain("Pa55word", json);
            Assert.DoesNotContain(connectionString, json);

            var settings = JsonSerializer.Deserialize<AuditStorageSettingsDto>(json);
            Assert.NotNull(settings);
            Assert.Equal("SqlServer", settings!.Provider);
            Assert.Null(settings.SqliteDataSource);
        }

        [Fact]
        public void BuildAuditStorageSettings_WithUnknownProvider_ShouldNotThrow()
        {
            var settings = Program.BuildAuditStorageSettings(new AuditStorageOptions
            {
                Provider = "NotADatabase",
                ConnectionString = "Data Source=data/audit.db"
            });

            Assert.Equal("NotADatabase", settings.Provider);
            Assert.Null(settings.SqliteDataSource);
        }

        [Theory]
        [InlineData("Data Source=data/audit.db", "data/audit.db")]
        [InlineData("data source = C:\\audit\\audit.db;Cache=Shared", "C:\\audit\\audit.db")]
        [InlineData("Server=.;Database=audit", null)]
        [InlineData("", null)]
        [InlineData(null, null)]
        public void ExtractSqliteDataSource_ShouldParseDataSourceKeyOnly(string? connectionString, string? expected)
        {
            Assert.Equal(expected, AuditRepository.ExtractSqliteDataSource(connectionString));
        }
    }
}
