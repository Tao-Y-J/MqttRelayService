# Dashboard 鉴权与精确消息查询修复

## 1. 实际修改

- `Program.MapWebEndpoints()` 新增 `GET /api/messages/{messageId}`，按 `MessageId` 精确返回单条 `MessageAuditRecord`；原分页 `GET /api/messages` 保持模糊搜索与分页语义不变。
- `IAuditRepository` / `AuditRepository` 新增 `GetMessageByIdAsync(string messageId)`，查询条件改为 `MessageId == ...` 的精确匹配；`/api/payload/{messageId}` 的数据库回退路径改为调用该精确查询，不再复用模糊搜索分页接口。
- `AuditRepository.GetPagedMessagesAsync()` 与 `GetDashboardMessageSummaryAsync()` 的最近记录排序从 `CreatedAt DESC` 改为 `UpdatedAt DESC`，让 Dashboard 和消息审计列表按“最新最终态更新时间”展示，而不是按最早创建时间展示。
- `Program.MapDashboard()` 不再直接发送原始 `index.html` 文件，而是在返回 `/` 与 `/index.html` 时动态注入 `window.__dashboardAuth` bootstrap 脚本；当 `Web.ApiKey` 有值时，页面可直接拿到同值用于后续 `/api/*` 请求头注入。
- `wwwroot/index.html` 新增统一 `dashboardFetch()` helper：所有 Dashboard 内部 API 调用统一通过该 helper 发送，并在有 `ApiKey` 时自动补 `X-Api-Key`。消息抽屉详情回退查询改为请求 `/api/messages/{messageId}`，不再使用 `?search=` 模糊命中。

## 2. 技术边界

- 本次没有修改审计表结构、消息 upsert 模型、MQTT 转发主链路、重试/死信策略或 Web 配置结构。
- 消息筛选中的 `startDate` / `endDate` 仍按 `CreatedAt` 过滤；只修改“最近记录排序”所依赖的字段，不改变搜索时间窗口语义。
- `/api/health` 继续保持免鉴权，供探活或外部监控使用。
- Dashboard 鉴权 bootstrap 只注入到页面 HTML 响应，不新增单独的未鉴权 JSON 配置接口。

## 3. 验证

- 新增 `AuditRepositoryTests.GetMessageByIdAsync_ShouldReturnExactMessageOnly`：验证精确查询不会被 `Topic`、`ErrorMessage` 或相似 `MessageId` 的模糊命中干扰。
- 新增 `AuditRepositoryTests.GetPagedMessagesAsync_ShouldOrderByUpdatedAtDescending`：验证分页消息列表按 `UpdatedAt` 倒序。
- 新增 `AuditRepositoryTests.GetDashboardMessageSummaryAsync_ShouldOrderRecentItemsByUpdatedAtDescending`：验证 Dashboard 最近消息按 `UpdatedAt` 倒序。
- 新增 `DashboardPageTests.BuildDashboardHtml_ShouldEmbedApiKeyBootstrap`：验证页面 HTML 会注入 `window.__dashboardAuth`，并覆盖有密钥和无密钥两种情况。
- 新增 `DashboardPageTests.DashboardSource_ShouldUseCentralizedFetchHelperAndExactMessageEndpoint`：验证前端源码已统一走 `dashboardFetch()`，并使用精确详情接口与精确 payload 接口。
- 全量回归：`dotnet test tests/MqttRelayService.Tests/MqttRelayService.Tests.csproj /p:UseSharedCompilation=false --no-restore`，`129/129` 通过。
