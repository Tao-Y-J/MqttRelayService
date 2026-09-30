namespace MqttRelayService.Options
{
    /// <summary>
    /// 审计持久化配置。
    /// </summary>
    public class AuditStorageOptions
    {
        /// <summary>
        /// 数据库提供程序。
        /// </summary>
        public string Provider { get; set; } = "Sqlite";

        /// <summary>
        /// 数据库连接字符串。
        /// </summary>
        public string ConnectionString { get; set; } = "Data Source=data/audit.db";

        /// <summary>
        /// 是否自动初始化表结构。
        /// </summary>
        public bool AutoInitializeSchema { get; set; } = true;

        /// <summary>
        /// 消息审计保留上限。
        /// </summary>
        public int MessageArchiveThreshold { get; set; } = 5000000;

        /// <summary>
        /// 客户端历史保留上限。
        /// </summary>
        public int ClientHistoryArchiveThreshold { get; set; } = 1000000;

        /// <summary>
        /// 审计数据（消息审计表与客户端历史表）保留天数。
        /// 清理时删除早于「当前本机时间 - 保留天数」的记录；配置为 0 或负数表示关闭清理，历史数据由运维手工维护。
        /// </summary>
        public int RetentionDays { get; set; } = 30;

        /// <summary>
        /// 每日清理时刻（本机时间整点，取值 0-23），默认 3 点。
        /// 配置该值时每天在该时刻执行一次清理；留空（null）时改为按 <see cref="CleanupIntervalMinutes"/> 等间隔执行。
        /// </summary>
        public int? CleanupAtHour { get; set; } = 3;

        /// <summary>
        /// <see cref="CleanupAtHour"/> 留空时的清理间隔（分钟），上限 1440 分钟（24 小时）。
        /// 该校验上限是「每天至少清理一次」的机制保证，不允许配置成更长的间隔。
        /// </summary>
        public int CleanupIntervalMinutes { get; set; } = 1440;

        /// <summary>
        /// 清理后是否对 SQLite 执行 VACUUM 回收数据库文件空间。
        /// VACUUM 期间数据库被独占且需要重写整库，大库上耗时可观；需要更短的清理窗口时关闭该开关。
        /// </summary>
        public bool VacuumAfterCleanup { get; set; } = true;
    }
}
