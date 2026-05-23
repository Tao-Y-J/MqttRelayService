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
    }
}
