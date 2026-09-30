# 长期经验记录

本文档记录跨任务的长期经验、踩坑记录和设计决策，供后续会话参考。

## 1. Windows 与中文文本编码（已踩坑）

这是本项目脚本和中文文档维护的最大坑点，必须严格遵守：

### 1.1 默认编码基线

- 仓库内所有文本文件的读写默认使用 `UTF-8 with BOM`。
- 当前唯一已确认的例外是 `.cmd` / `.bat`，必须保持 `GBK (936)`。
- 只要新增、批量改写或修复中文文本文件，就先确认编码，再检查可读性。

### 1.2 编码对照表

| 文件类型 | 必须编码 | 代码页设置 | 原因 |
|----------|----------|-----------|------|
| 其他文本文件（含 `.ps1`、`.md`、`.json`、`.yml`、`.xml`、`.cs`） | **UTF-8 with BOM** | 无需设置 | 仓库长期以中文文本为主，Windows / PowerShell 5.1 读取无 BOM 的 UTF-8 文件时容易按系统编码（GBK）解码，导致乱码 |
| `.cmd` / `.bat` | **GBK (936)** | `chcp 936` | Windows cmd 默认用 GBK 解码脚本文件，UTF-8 会导致中文被拆成多个字符，进而被解析为命令名，产生`'XX' 不是内部或外部命令` 错误 |

### 1.3 常见错误现象

- **cmd 乱码**：`chcp 65001` 后仍然乱码，因为 cmd 文件本身是 UTF-8 编码，但 cmd 解析器在某些 Windows 版本上对 UTF-8 支持不完整
- **cmd 命令解析错误**：中文字符被拆成多个字节，每个字节被当作一个命令名执行，出现 `']' 不是内部或外部命令`
- **ps1 乱码**：脚本中的中文注释和字符串显示为乱码，因为 PS 5.1 用 GBK 解码了 UTF-8 字节

### 1.4 正确的文件写入方式

```powershell
# 其他文本文件 - 使用 UTF-8 with BOM
[System.IO.File]::WriteAllText("file.md", $content, [System.Text.Encoding]::UTF8)

# .cmd 文件 - 使用 GBK 编码
$encoding = [System.Text.Encoding]::GetEncoding(936)
[System.IO.File]::WriteAllText("file.cmd", $content, $encoding)

# .ps1 文件 - 使用 UTF-8 with BOM
[System.IO.File]::WriteAllText("file.ps1", $content, [System.Text.Encoding]::UTF8)
# 注意：[System.Text.Encoding]::UTF8 默认会写入 BOM
```

### 1.5 脚本执行方式

**错误方式**（弹出新窗口，执行完立即关闭，用户看不到输出）：
```cmd
powershell -ExecutionPolicy Bypass -File "install-service.ps1"
```

**正确方式**（在当前窗口执行，输出保留）：
```cmd
powershell -ExecutionPolicy Bypass -NoProfile -Command "& '%~dp0install-service.ps1'"
```

## 2. C# 命名空间风格

- 全仓库 `.cs` 文件统一使用块级命名空间写法：`namespace MqttRelayService.Utilities { ... }`。
- 不使用文件作用域命名空间写法：`namespace MqttRelayService.Utilities;`。
- 新增或修改 C# 文件时，必须保持块级命名空间风格，避免重新引入文件作用域 namespace。

## 3. MQTTnet 5.x 迁移经验

从 4.x 升级到 5.x 的重大变化：

- **Server 类型**：`IMqttServer` 接口被移除，直接使用 `MqttServer` 类
- **事件异步化**：所有事件处理器改为 `Async` 后缀，同步处理器已移除
- **Payload 类型**：`PayloadSegment`（ArraySegment<byte>）改为 `Payload`（ReadOnlySequence<byte>）
- **消息注入**：使用 `InjectApplicationMessage(new InjectedMqttApplicationMessage(message))`
- **连接验证**：`ValidatingConnectionAsync` 替代 `ConnectionValidator`

## 4. .NET SDK 默认包含项

.NET SDK 会自动包含项目目录中的某些文件类型，显式添加会导致 `NETSDK1022` 错误：

- `appsettings.json` 和 `appsettings.*.json` 自动作为 `Content`
- 不需要在 `.csproj` 中显式 `<Content Include="appsettings.json">`
- 如果需要自定义 `CopyToOutputDirectory`，使用 `<None Update="...">` 或关闭默认项：`EnableDefaultContentItems=false`

## 5. 可靠性设计原则

- **事件回调必须轻量**：MQTT 事件处理中只做入队，不做复杂逻辑
- **非阻塞出队**：`Channel.Reader.TryRead()` 替代 `WaitToReadAsync()`，避免测试和停机时卡住
- **异常隔离**：每条消息独立 try/catch，单条失败不影响其他消息和消费循环
- **状态机完整**：Received → Queued → Routing → Forwarding → Succeeded/Failed/DeadLetter

## 6. 测试注意事项

- `Options.Create<T>()` 在 `using MqttRelayService.Options` 和 `using Microsoft.Extensions.Options` 同时存在时会产生歧义，必须使用完全限定名 `Microsoft.Extensions.Options.Options.Create(...)`
- `InMemoryMessageQueue.TryDequeueAsync` 如果使用 `WaitToReadAsync`，空队列测试会无限等待，必须使用带 CancellationToken 的超时或改为非阻塞实现
- `BoundedChannelFullMode.DropWrite` 模式下 `TryWrite` 始终返回 `true`（消息被静默丢弃），测试断言需要调整预期

## 7. 主链路可靠性修复经验（2026-05-03）

### 6.1 BoundedChannelFullMode.DropWrite 陷阱

**现象**：队列满时 `TryWrite` 返回 `true`，调用方以为入队成功，但消息实际被静默丢弃。

**根因**：`DropWrite` 的设计是丢弃**最旧**消息为新消息腾出空间，`TryWrite` 的语义是"写入操作已完成"，而非"消息一定在队列中"。

**解决**：
- 不再使用 `DropWrite` 模式
- 统一使用 `Wait` 模式
- 满载丢弃由应用层通过 `Count >= Capacity` 预检实现，确保调用方返回 `false` 并记录 Warning

### 6.2 消费循环从轮询到异步等待

**现象**：`TryDequeueAsync` + `Task.Delay(100)` 造成 CPU 空转，且延迟敏感。

**解决**：
- 扩展 `IMessageQueue` 接口，新增 `ReadAllAsync` 返回 `IAsyncEnumerable<T>`
- 使用 `ChannelReader.ReadAllAsync(cancellationToken)` 实现
- 消费循环改用 `await foreach`，空队列时真正异步挂起
- 取消 token 触发后，消费者正确退出，无需额外轮询判断

### 6.3 重试退避必须区分运行期与停机期两种调度

**现象**：`HandleFailureAsync` 设置 `NextRetryAt` 后立即重新入队，消费循环不检查时间戳，重试消息被瞬间再次消费。

**解决**：当前实现按运行期与停机期分别处理，不再让运行期消费者阻塞在退避上：

- 运行期：`ScheduleRetryEnqueueAsync` 把退避等待和重新入队放进后台任务，消费者立即返回处理下一条消息；后台调度任务数量受 `ReliabilityOptions.MaxPendingRetryTasks` 限制（见 6.11）。
- 停机排空阶段：`DelayAndRequeueDuringStopAsync` 同步等待该次退避结束后再尝试重新入队，保证 drain 不会在消息重新出现之前提前宣布排空完成（见 6.9）。

早期实现在 `HandleFailureAsync` 中直接 `await Task.Delay(delay)` 再入队，退避期间消费者无法处理其他消息，与 6.11 的“运行期非阻塞后台重试调度”自相矛盾，已按上述分支改写。未实现独立延迟队列。

### 6.4 EchoToSender 必须在 Broker 分发层拦截

**现象**：仅在 `MessageRouter.RouteAsync` 中过滤发送方无法阻止 Broker 将消息分发给发送方自己。`InjectApplicationMessage` 是按 Topic 广播，Broker 不知道谁是"原始发送方"。

**解决**：
- 在注入消息时通过 MQTT 5.0 `UserProperties` 附加 `x-source-client-id`
- 注册 `InterceptingOutboundPacketAsync` 出站拦截器
- 在分发阶段检查目标 `ClientId` 是否等于 `x-source-client-id`，如果是则 `ProcessPacket = false`

**注意**：此方案依赖 MQTT 5.0 User Properties。如果降级到 MQTT 3.1.1，需要使用 Payload 或 Topic 携带标记。

### 6.5 停机排空需要两阶段设计

**现象**：直接取消 `_cts` 会导致 `ReadAllAsync` 立即退出，队列中剩余消息不会被处理。

**解决**：
- 阶段 1：取消 `_cts`，让 `ReadAllAsync` 退出消费循环
- 阶段 2：使用 `TryDequeueAsync`（非阻塞）循环消费队列剩余消息，直到超时或队列为空
- 最后记录排空数量和剩余数量

**注意**：`TryDequeueAsync` 必须使用非阻塞实现（`ChannelReader.TryRead`），否则 drain 阶段会卡死。

### 6.6 发布拦截必须先阻断默认分发

客户端原始发布进入 `InterceptingPublishAsync` 后，必须先设置 `ProcessPublish=false`，再执行活动时间更新、Payload 转换和入队。这样即使拦截链路发生异常，消息也不会回落到 Broker 默认分发路径，避免绕过内部队列、重试、死信和 `EchoToSender` 控制。

服务端注入的转发消息必须先识别并放行，避免转发消息再次进入内部队列形成循环。

### 6.7 注册表返回快照，不暴露内部可变集合

客户端订阅集合会被 MQTT 订阅事件更新，同时被路由线程枚举。`HashSet<T>` 不能并发读写，注册表对外返回会话时必须返回快照，不能暴露内部 `Subscriptions` 集合。

### 6.8 Worker 停机先取消循环，再停止被监控对象

如果后台 Worker 的监控循环会根据 `IsRunning=false` 触发重启，停机时必须先让监控循环退出，再停止被监控对象。反过来先停止 Broker 会让监控循环误判为异常停止并触发重启。

### 6.9 停机排空超时必须覆盖最大退避时间

`MessageDeliveryService` 在停机 drain 阶段遇到失败消息时，会同步等待该次退避结束后再尝试重新入队，等待使用 `ShutdownDrainTimeoutMs` 的取消 token。

**结论**：
- 如果希望停机阶段至少覆盖一次失败消息的最大退避等待，配置上必须保持 `ShutdownDrainTimeoutMs >= RetryMaxDelayMs`
- 如果 `ShutdownDrainTimeoutMs < RetryMaxDelayMs`，停机超时会先触发，消息会进入“保留回队列或死信”的收敛分支，而不会完成当次下一次注入尝试

### 6.10 可复用服务实例必须区分“已退出消费者”和“悬挂消费者”

`MessageDeliveryService` 停止后必须释放 `_cts`，并且只移除已经完成的 `_consumerTasks`。如果仍有未响应取消的消费者，必须保留任务引用并拒绝再次启动；否则同一实例再次执行 `StartAsync -> StopAsync` 时，会与上一轮悬挂消费者并存，突破 `MaxConcurrentHandlers`，并在“已停止”状态下继续处理消息。

### 6.11 运行期后台重试调度必须有容量上限

运行期非阻塞重试会让失败消息暂时离开内部队列，由后台调度任务持有退避等待。该调度层必须有独立容量上限，否则 Broker 注入持续失败时会绕过 `QueueCapacity` 形成无界内存增长。

**解决**：`MessageDeliveryService` 使用 `ReliabilityOptions.MaxPendingRetryTasks` 限制后台重试调度任务数量。达到上限时，新失败消息直接进入死信。

### 6.12 同 ClientId 重连必须区分连接实例

MQTT 客户端在网络抖动或自动重连时可能使用相同 ClientId 建立新连接。注册表不能只按 ClientId 处理断开事件，否则旧连接的延迟断开事件会误删新连接，表现为客户端在线但订阅路由丢失。

- 解决：MqttBrokerHost 在连接事件中生成 ConnectionId 并写入 MQTTnet SessionItems，断开事件带回该值；ClientRegistry.UnregisterAsync 只移除与当前 ConnectionId 匹配的会话，过期断开事件只记录 Warning 并忽略。

### 6.13 停机必须最后才停 Broker

停机阶段如果先停止 Broker，排空阶段就无法再向订阅者注入消息，队列里剩余的消息只能进入死信或丢失。正确顺序是：

1. `IMqttBrokerHost.StopAcceptingClientPublishes()` 封堵客户端新发布入口，Broker 保持运行。
2. 取消消费者（停止 `ReadAllAsync`），等在途消息回队。
3. 在 `ShutdownDrainTimeoutMs` 内多轮排空队列，此时 Broker 仍在运行，注入仍能送达订阅者。
4. 排空结束后才由 `BrokerWorker` 停止 Broker。

HostedService 注册顺序必须与停机顺序匹配：Host 按注册逆序停止，因此 `QueueMetricsWorker` 最先注册（最后停止）、`BrokerWorker` 其次、`DeliveryWorker` 最后注册（最先停止并执行排空）。

### 6.14 工厂委托注册的装饰器会隐藏循环依赖，并表现为“启动卡死且无任何报错”

**现象**：以默认配置（`Web:Enabled=true`）启动时，进程打印到「审计持久化初始化成功」「消息队列已初始化」后永久停住：既不绑定 1883/5000，也不抛异常，CPU 不增长。仓库内 `bin/Debug/net8.0/win-x64/Logs/relay-2026061821.log`（2026-06-18 两次启动）就是这样截断的。

**根因**：`Program.ConfigureCoreServices` 中装饰器使用工厂委托注册（`AddSingleton<IMessageQueue>(sp => new MetricsMessageQueue(..., sp.GetRequiredService<IMetricsService>()))`），容器无法静态识别其依赖，因此循环依赖不会被报成 `A circular dependency was detected`。当时的环是：`MetricsService` → `IMessageQueue`/`IClientRegistry` → `MetricsMessageQueue`/`MetricsClientRegistry` → `IMetricsService`。

**结论**：
- 任何装饰器与它装饰的依赖之间都不允许在构造期互相解析；装饰器需要指标服务时使用 `Utilities/LazyService<T>`，只在业务调用时读取 `Value`。
- `LazyService<T>` 只能有一个可从容器解析的公开构造函数，否则容器会因构造函数歧义抛 `Unable to activate type`；测试用固定实例走静态 `From(T)`。
- 静态 Review 无法发现这类缺陷。任何改动 DI 注册的提交都必须跑 `ServiceRegistrationTests`（按生产注册顺序解析全部关键单例）与 `HostLifecycleTests`（真实 `IHost` 启动 + 真实 MQTT 转发 + 优雅停机）。
- 排查这类“启动无异常卡死”时，先在真实进程上启动一次并观察日志停在哪一步，比继续读代码更快定位。

### 6.15 同一“上限/故障”判定出现在多条路径时，必须共用同一个去重标志

**问题**：审计待写队列达到上限时，失败批次回填路径用 `_pendingAuditOverflowLogged` 做了告警去重，而正常入队路径对每条新消息都打一条 Warning。审计库长时间故障叠加高吞吐时，日志会被同一条告警刷满，真正的故障信号被淹没。

**做法**：把“达到上限”这一判定视为同一个故障窗口，两条路径共用同一个去重标志；标志只能在条件真正解除（队列排空）后复位。新增同类上限保护时，先搜一遍是否已有同义路径和现成的去重标志。

### 6.16 停机路径上的任何等待都必须有界，包括 Dispose

**问题**：`MetricsService.Dispose` 用 `GetAwaiter().GetResult()` 无界等待审计 writer，而该 writer 退出前还要做最后一轮写库。`Dispose` 由 DI 容器在 `HostOptions.ShutdownTimeout` 之外调用，审计库无响应时进程无法退出——这与“停机排空必须有超时”是同一类问题，只是发生在资源释放阶段。

**做法**：`Dispose` 里的等待统一改成限时 `Task.Wait(ms)`；超时后记录 Warning 并继续释放其余资源。限时等待超时说明 writer 可能仍在运行，此时**不要**释放它还要使用的 `SemaphoreSlim` / `CancellationTokenSource`，否则会把它推进 `ObjectDisposedException`。

### 6.17 有界淘汰队列只在键首次写入时登记

**问题**：`SetBoundedPayload` 对同一 `MessageId` 的每次写入都入队一个驱逐键，而同一条消息在 Received / Forwarded / DeadLetter 阶段会被反复写入。重复键挤占驱逐队列，导致 `while (queue.Count > Max) 淘汰` 把仍在缓存中的载荷提前删掉：写入 150 次热键 + 99 个新键后，缓存只剩 99 条且热键已被驱逐。

**做法**：淘汰队列与缓存字典必须一一对应——先判断键是否已在字典中，只有新键才登记驱逐键；键被淘汰后再次写入会重新登记。仅靠“缓存字典有上限”并不能保证命中率，还要保证驱逐队列不重复计数。

## 8. 现代 Web 零侵入 Dashboard 与指标拦截经验

对于高可用且对稳定性要求极高（如 Windows Service）的后台服务，构建可视化监控 Dashboard 时必须兼顾“零侵入”与“零故障风险”。

### 8.1 装饰器模式实现无侵入指标拦截

- **原则**：绝对不为指标统计修改任何原有的核心业务逻辑（如队列、Broker 宿主、死信写入）。
- **实践**：在 DI 容器中利用装饰器模式包装并替换原始服务。
  ```csharp
  // 注册原始服务
  builder.Services.AddSingleton<IMessageQueue, InMemoryMessageQueue>();
  // 注册装饰器并利用 DI 容器代理原始服务
  builder.Services.Decorate<IMessageQueue, MetricsMessageQueue>();
  ```
- **优势**：原始服务完全不知道自己被监控，业务逻辑 100% 保持纯净，核心单元测试无需做任何逻辑修改。

### 8.2 单端口统一 Kestrel 同时提供 Dashboard 与 API

- **实际实现**：`Web:Enabled=true` 时 `Program` 走 `WebApplication.CreateBuilder`，`builder.WebHost.ConfigureKestrel` 只调用一次 `kestrel.ListenAnyIP(webOptions.Port)`；Dashboard 静态页由 `MapDashboard` 映射 `/` 与 `/index.html` 并从 `wwwroot` 读取 `index.html` 返回，`/api/*` 端点映射在同一个 `WebApplication` 上。Dashboard 与 API 因此同源，不需要任何 CORS 配置，也不需要独立进程或代理。
- **鉴权**：`/api` 组上一个端点过滤器校验请求头 `X-Api-Key`；`Web:ApiKey` 为空时直接放行。页面不内嵌密钥，运维在弹窗录入后只保存到浏览器 `sessionStorage`。`/api/health` 注册在 `/api` 组之外，始终匿名可访问。
- **边界**：不存在 5001 端口、独立 Dashboard 可执行文件或 `IHttpClientFactory` 代理链路。

### 8.3 有界写入是载荷缓存的唯一写入路径

- **背景**：实时大屏审计需要追溯最新的消息 Payload。
- **原则**：不允许内存缓冲区无界增长，任何收集到的采样、日志与 Payload 都必须做物理容量截断。
- **实际实现**：
  1. 采样曲线：`_historySnapshots` 上限 60 条（每 2 秒一个采样点，覆盖最近 2 分钟）。
  2. 审计日志：`_messageLogs` 按 `MessageId` 就地覆盖最终态，上限 100 条，超出即按 `_messageLogKeys` 顺序驱逐最旧条目。
  3. Payload 缓存：`SetBoundedPayload` 是唯一写入路径，写入 `ConcurrentDictionary` 的同时把 `MessageId` 登记到 `_payloadKeys`，超过 `MaxPayloadCount`（100 条）即淘汰最旧条目；单条载荷超过 8192 字节只保留前 8KB 预览（UTF-8 预览不可用时退化为 HEX 预览）。
- **踩坑**：历史实现只在非空载荷分支登记驱逐键，空载荷条目永久驻留并形成无界内存增长。现在 `CachePayload` 对空载荷、长载荷和死信回填都统一调用 `SetBoundedPayload`，空载荷固定写入 `[空载荷]` 占位并同样参与淘汰，因此载荷缓存的条目数始终不超过 100 条。

### 8.4 高吞吐审计写库使用最终态快照

- **原则**：Dashboard 和审计库只需要每条消息的当前最新态/最终态时，不写状态事件流水。
- **实践**：`MetricsService` 在后台刷盘前按 `MessageId` 合并 Queued 与终态，快速完成的消息只落一次最终快照；`AuditRepository` 用 SqlSugar ORM 批量 Upsert，不写某个数据库方言专用 SQL，因此 `AuditStorage:Provider` 换成其它 `DbType` 时同一套写入路径仍然可用。
- **边界**：此策略不改变 MQTT 转发、重试、死信主链路；审计持久化仍是后台异步能力，写库失败只影响审计追平，不阻塞实时转发。

## 9. 仓库脚本事实与 Windows cmd 编码踩坑

仓库当前只有以下脚本，不存在一键启停脚本，也不存在独立的 Dashboard 可执行文件：

- `run-stress-60s.cmd`（仓库根目录）：纯 ASCII，调用同目录的 `stress_mqtt_1883.py`，对 `127.0.0.1:1883` 压测 60 秒。
- `stress_mqtt_1883.py`（仓库根目录）：Python 压测工具，由 `run-stress-60s.cmd` 调用。
- `src/MqttRelayService/Scripts/install-service.cmd` 与 `install-service.ps1`：Windows Service 安装，随发布输出一起复制到发布目录。
- `src/MqttRelayService/Scripts/uninstall-service.cmd` 与 `uninstall-service.ps1`：Windows Service 卸载。

历史上存在过的 `start-dev.cmd`、`stop-dev.cmd` 与 `MqttRelayService.Dashboard.exe` 均已删除，针对这些文件的脚本维护经验不再适用。当前只有单进程 `MqttRelayService.exe`（Web 管理面开启时它就是承载 MQTT + Kestrel 的唯一进程）。

### 9.1 用内存字节流写入无 BOM 的 GBK 批处理脚本

- **问题**：在 Windows `cmd.exe` 下执行 `.cmd` 文件时，如果文件头部包含 UTF-8 BOM 字节（`EF BB BF`），系统会把 BOM 强行解析为非 ASCII 字符，导致首行 `@echo off` 被解析为非法命令并产生严重乱码。
- **做法**：使用 PowerShell 写入脚本时，不使用会隐式添加 BOM 的常规输出重定向命令，而是通过指定字符集直接导出内存二进制字节流：
  ```powershell
  $content = "..."
  $bytes = [System.Text.Encoding]::GetEncoding(936).GetBytes($content)
  [System.IO.File]::WriteAllBytes("x.cmd", $bytes)
  ```
- **效果**：首字节严格为 `@`（十进制 64），cmd 原生解析兼容。仓库根目录的 `run-stress-60s.cmd` 就是这种无 BOM 状态（前三字节为 64、101、99，即 `@ec`）。

### 9.2 批处理脚本文本保持纯 ASCII

- **问题**：不同 Windows 环境的默认 ANSI 代码页不同（纯英文版系统默认 `437`；启用“Beta: 全局 Unicode UTF-8 语言支持”后 CMD 默认按 UTF-8 解码）。在这类环境下，即使脚本按 GBK (936) 写入并调用 `chcp 936`，CMD 仍可能按错误代码页解码，把中文字符的高位字节误判为 `&`、`|`、`>` 等管道/重定向符，并把中文当作命令执行。
- **做法**：批处理脚本的打印文本、提示、控制符与注释全部使用 **纯 ASCII（0-127）**。ASCII 字符在 UTF-8、GBK、Shift-JIS、Latin-1、OEM 437 等代码页下字节完全一致。根目录的 `run-stress-60s.cmd` 所有输出文本都是纯 ASCII。

## 10. 数据库数据保留清理经验（2026-10-01）

### 10.1 SQLite 批量删除不要用 `Deleteable().Take(n)`

- **问题**：按保留天数清理本地库时，直觉写法是 `Deleteable<T>().Where(过期条件).Take(批大小)`。SQLite 的 `DELETE ... LIMIT` 语法依赖编译期选项 `SQLITE_ENABLE_UPDATE_DELETE_LIMIT`，并非所有 SQLite 构建都启用；该选项未启用时执行会直接语法报错。
- **做法**：统一改为两段式——先按时间升序查出这一批主键，再按主键 `Where(idChunk.Contains(x.Id))` 分块删除。该写法在所有 `SqlSugar DbType` 上行为一致，同时避免单个长事务。
- **配套上限**：单条 SQL 的 IN 参数个数必须留余量。SQLite 默认 `SQLITE_MAX_VARIABLE_NUMBER = 999`，因此分块大小取 900；忽略该限制时大批量删除会以 `too many SQL variables` 失败。
- **退出条件**：批删循环除了「本批不足批大小即结束」之外，还必须加一条「本批实际删除行数为 0 就立即退出」。并发写入可能让已选中的主键不再满足删除条件，否则会反复选中同一批主键形成死循环。

### 10.2 SQLite `VACUUM` 必须在写锁之外执行

- **问题**：`DELETE` 不会缩小 SQLite 数据库文件，不回收时磁盘占用停留在历史峰值；而 `VACUUM` 需要独占数据库并重写整库，大库上耗时可观。把它放进仓储的写入互斥锁里执行，会把审计写入队列（上限 50000）堵到内存上限。
- **做法**：删除在写锁内执行，`VACUUM` 在写锁之外执行，失败只记 Warning 并在下一轮清理后重试。代价是 `VACUUM` 可能偶发遇到 SQLite 忙锁，这个代价低于让审计写入队列溢出。
- **边界**：`VACUUM` 是 SQLite 专有语句，必须先判断 `DbType`；把它暴露在仓储接口之外（通过具体类型判定后调用），不要在 `IAuditRepository` 这类多数据库抽象接口上暴露方言专有操作。

### 10.3 「每天至少一次」应写成机制而不是配置约定

- **做法**：调度上给出两条路径——配置整点时取「今天的该整点，已过则取明天」；未配置整点时按间隔执行，并把间隔的**校验上限**钉在 1440 分钟。这样「超过 24 小时不清理」在配置层面就无法表达，误配会在启动时失败，而不是运行期静默不清理。
- **配套**：清理任务启动时先清理一轮，用来收敛进程停止期间堆积的超期数据；周期任务必须顶层 `try/catch`（`BackgroundServiceExceptionBehavior.Ignore` 下异常逃逸等于静默停止）。
- **隔离**：多张表共用一次清理时，每张表独立 `try/catch`。否则一张表持续失败会把另一张表的清理一起饿死，故障表之外的容量目标仍然失效。
