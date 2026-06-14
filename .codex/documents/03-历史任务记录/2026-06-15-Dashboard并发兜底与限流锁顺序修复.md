# Dashboard 并发兜底与限流锁顺序修复

## 1. 实际修改

- `MetricsService.GetDashboardDataAsync()` 增加顶层 `try/catch` 兜底：`_clientRegistry.GetAllSessionsAsync()` 与 `Process.GetCurrentProcess()` 的异常被分别降级处理（客户端列表置空、内存用量置 0），任一子调用失败不再让整个 Dashboard HTTP 接口返回 500；方法体整体抛异常时返回带 `System.Degraded = true` 的最小降级快照，保证前端不会白屏。
- `MetricsService.GetDashboardDataAsync()` 的拥挤度计算 `CongestionPercentage` 增加容量守卫：当 `_queue.Capacity <= 0`（误配或自定义队列实现）时直接返回 0，避免产生 `NaN` 污染前端 JSON。
- `ThroughputController` 统一加锁顺序为 `_lock → _rateLock`：`UpdateMaxMessagesPerSecond` 在持 `_lock` 时计算有效速率快照，再持 `_rateLock` 应用；`ApplyRateLimitingAsync` 改为在进入 `_rateLock` 之前先用 `_lock` 读取限流配置快照，`_rateLock` 区段内不再回调任何持 `_lock` 的成员。原 `GetEffectiveRatePerSecond()` 拆分为静态 `ComputeEffectiveRatePerSecond(maxMessagesPerSecond, activeCount)`，由调用方在已持 `_lock` 时传入快照，彻底消除 AB-BA 死锁路径。
- `AuditRepository._schemaEnsured` 由普通 `bool` 改为 `volatile bool`，修复双检锁跨线程可见性问题：读路径（`GetPagedMessagesAsync`、`GetPagedClientHistoryAsync`、`GetDashboardMessageSummaryAsync`）不持 `_writeLock` 也会读该标志，加 `volatile` 后写线程设置 `true` 对所有读线程立即可见，避免重复进入 `InitTables`。

## 2. 技术边界

- 本次没有修改 MQTT 转发主链路、消息模型、审计表结构、重试/死信语义、配置文件或 DI 注册。
- 限流算法的语义未变：仍是“单线程 MPS × 活动工作线程数”作为全局补充速率；本次只调整锁的获取顺序与快照读取时机，令牌桶数学逻辑完全保留。
- Dashboard 降级快照不保证客户端列表与队列实时值的准确性，仅保证接口可用性；恢复正常后下一次轮询会重新返回完整快照。
- `AuditRepository._schemaEnsured` 加 `volatile` 不改变 `InitTables` 的幂等行为，只是让标志位的可见性符合双检锁规范。

## 3. 验证

- 新增 `MetricsService_GetDashboardDataAsync_ZeroCapacityQueue_ShouldNotThrowDivideByZero`：验证容量为 0 时拥挤度按 0 返回，不产生异常或 NaN。
- 新增 `MetricsService_GetDashboardDataAsync_ClientRegistryThrows_ShouldNotPropagateException`：验证客户端注册表抛异常时 Dashboard 返回降级快照而非 500。
- 新增 `ThroughputControllerTests.ConcurrentUpdateMpsAndWaitAsync_ShouldNotDeadlockOrCrash`：并发压测持续变更 MPS 与限流等待，用 5s 超时断言确保不发生死锁、不抛异常。
- 全量回归：测试总数 125（原 122 + 新增 3），全部通过；主项目 Release 构建零警告零错误。
