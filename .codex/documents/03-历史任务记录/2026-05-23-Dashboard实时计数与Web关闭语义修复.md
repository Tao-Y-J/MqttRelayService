# Dashboard实时计数与Web关闭语义修复

## 1. 任务背景

- 近两天 Web Dashboard、审计写入和 SQLite 运行目录改造完成后，代码审查发现实时计数、首入队审计、审计写入失败收敛和 `Web:Enabled` 关闭语义存在运行边界偏差。
- 本次修复只覆盖上述边界，不调整 MQTT 路由、重试策略、死信落盘格式和前端视觉布局。

## 2. 实际修复

- `MetricsMessageQueue` 在调用真实队列入队前记录消息是否为首次接收，避免 `InMemoryMessageQueue` 将状态改为 `Queued` 后使首入队审计被误判为重试回队。
- `MetricsService.RecordReceived` 只在首次接收时增加接收总数并写入 `Queued` 审计，重试回队只更新内存日志，不重复推高接收总数。
- `MetricsService.GetDashboardDataAsync` 保持首页计数来自内存原子计数器和队列实时水位，不再用审计库汇总覆盖实时值。
- `AuditRepository.RecordMessageAuditAsync` 与 `RecordMessageAuditsAsync` 写入失败后重新抛出异常，`MetricsService` 捕获后把失败批次重新并入待写集合，等待后台写线程下一轮刷盘。
- `Program.Main` 启动前从 `AppContext.BaseDirectory` 读取 `Web:Enabled` 并选择 Web Host 或普通 Worker Host；关闭 Web 时不创建 `WebApplication`，不会启动默认 Kestrel 监听。

## 3. 验证

- 新增 Dashboard 指标回归测试，覆盖真实 `MetricsMessageQueue -> InMemoryMessageQueue` 首入队路径和审计库存在时首页仍使用内存计数。
- 新增审计批量写入失败后重新并入 pending 并完成下一轮写入的回归测试。
- 执行 `dotnet test tests\MqttRelayService.Tests\MqttRelayService.Tests.csproj --no-restore`，测试通过。

## 4. 当前边界

- Web Dashboard 开启时仍使用单个 `Web:Port` 同时承载 `/api/*` 与静态首页。
- Web Dashboard 关闭时审计持久化装饰器不会注册，主服务保持 MQTT Broker、内存队列、转发、重试和死信链路运行。
- 审计写入失败保留的是每个 `MessageId` 的最新合并状态，符合当前后台审计 coalescing 设计。
