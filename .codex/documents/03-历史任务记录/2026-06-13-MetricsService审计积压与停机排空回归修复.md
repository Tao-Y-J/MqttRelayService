# MetricsService 审计积压与停机排空回归修复

## 1. 实际修改

- `MetricsService.EnqueueMessageAudit(...)` 改为先判断 `MessageId` 是否已在待写字典中；当待写队列达到 `50000` 上限时，已有消息仍允许继续 `AddOrUpdate` 覆盖到最新终态，仅拒绝真正新增的消息 ID。
- `MetricsService.Dispose()` 恢复有序收尾：先停止快照定时器，再取消后台审计 writer，显式唤醒等待中的 `SemaphoreSlim`，等待 writer 执行停机阶段的最后一次 `FlushPendingAuditsAsync()`，最后才释放同步对象。
- `MetricsService` 新增内部安全唤醒方法，避免 `Dispose` 期间重复唤醒或同步对象已释放时抛出异常，保持停机路径稳定。

## 2. 技术边界

- 本次没有修改审计模型；`message_audit` 仍按 `MessageId` 保留单条最新最终态快照，不引入事件流。
- 本次没有修改 `MaxPendingAudits` 默认值、Web API、MQTT 转发主链路、SqlSugar 仓储接口或任何配置文件。
- 本次没有新增 public API 或依赖，修复范围限制在 `MetricsService` 内部行为和对应单元测试。

## 3. 验证

- 新增测试覆盖“待写队列打满时，已有 `MessageId` 仍可更新为终态，新 `MessageId` 继续被上限拒绝”。
- 新增测试覆盖“`Dispose()` 会等待后台审计 writer 完成最后一次刷盘，而不是提前释放同步对象导致尾部审计丢失”。
- 目标测试与完整测试项目均执行通过，验证现有最终态合并逻辑未回归。
