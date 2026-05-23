# Dashboard启动加载数据库初始计数

## 1. 本次实际修改

- `MetricsService` 新增 `InitializeDashboardCountersFromAuditAsync()`，在服务启动阶段从审计库读取一次累计总量基线。
- Dashboard 首页累计卡片继续使用内存原子计数，但返回值改为“数据库启动基线 + 当前进程运行期增量”。
- `Program.RunWebHost()` 在完成 `IAuditRepository.InitializeAsync()` 后，立即调用 `MetricsService.InitializeDashboardCountersFromAuditAsync()`，不把数据库读取放到首页请求路径中。

## 2. 当前口径

- `收到总消息数`、`转发成功数`、`注入失败数`、`死信数` 在服务启动后会先继承审计库中的历史累计值。
- 服务运行期间这些累计卡片只由内存计数继续递增，不会在每次 `/api/metrics` 请求时重新查询数据库。
- 吞吐曲线、在线客户端、队列水位和最近日志窗口仍然保持当前进程内的实时口径。

## 3. 当前边界

- 这次没有改动消息审计分页接口，分页总数仍直接来自 `message_audit` 持久化查询。
- 如果服务未开启 Web Host，就不会初始化 Dashboard 指标服务，也不会执行这次的启动基线加载。
