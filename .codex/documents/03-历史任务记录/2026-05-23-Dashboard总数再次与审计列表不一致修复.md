# Dashboard 总数再次与审计列表不一致修复

## 1. 实际修改

- 运行态排查确认 `/api/metrics` 与 `/api/messages` 再次读取了不同口径：
  - `/api/messages` 直接返回 `message_audit` 实时总记录数。
  - `/api/metrics` 使用了“启动时审计基线 + 当前进程内存增量”的累计方式。
- 在高并发压测后，审计写入存在自然异步滞后；当首页卡片使用内存累计、而列表使用数据库实时值时，两者会再次出现总数不一致。
- 本次将 `MetricsService.GetDashboardDataAsync()` 调整为：
  - 当存在 `IAuditRepository` 时，`Counters` 优先读取 `GetDashboardMessageSummaryAsync(...)` 的实时审计汇总。
  - `Logs` 也优先返回审计仓储中的最近记录，和消息审计页保持一致。
  - 队列长度、在线客户端、吞吐历史曲线仍保留实时内存口径。

## 2. 结果边界

- Dashboard 首页“收到总消息数 / 转发成功数 / 注入失败数 / 死信数 / 当前待处理数”现在重新与消息审计列表对齐。
- 本次没有修改消息转发、重试、死信或 SQLite 刷盘主链路。
- 审计仓储不可用时，Dashboard 仍会回退到当前进程内存口径，保证首页可用性。