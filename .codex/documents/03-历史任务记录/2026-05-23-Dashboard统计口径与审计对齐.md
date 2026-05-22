# Dashboard 统计口径与审计对齐

## 1. 本次实际修改

- 为 `IAuditRepository` 增加 `GetDashboardMessageSummaryAsync(int recentCount)`，由审计持久化直接返回消息总数、成功数、失败数、死信数以及最近消息列表。
- `MetricsService.GetDashboardDataAsync()` 在存在审计仓储时，`Counters` 与 `Logs` 优先采用持久化审计摘要，而不是仅使用当前进程启动后的内存累计值与内存日志窗口。
- 在线客户端数量与吞吐历史曲线仍保留实时内存口径，未改成历史持久化口径。

## 2. 对齐后的口径

- Dashboard 的 `收到总消息数` 现在对齐 `message_audit` 表总记录数。
- Dashboard 的 `转发成功数`、`注入失败数`、`死信数` 现在对齐 `message_audit` 表按最终状态聚合后的数量。
- Dashboard 首页“最新转发消息队列”现在优先显示审计持久化中的最近记录，顺序与消息审计页一致。

## 3. 当前边界

- 这次对齐的是“Dashboard 与消息审计日志”的口径，不是把所有指标都改成历史总量。
- `在线客户端` 仍表示当前活跃连接，不会与 `client_connection_history` 历史记录总数对齐。
- 吞吐波形图仍表示当前运行期的实时采样窗口，不会与持久化历史总量对齐。
