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

### 6.3 重试退避必须阻塞消费者或引入延迟队列

**现象**：`HandleFailureAsync` 设置 `NextRetryAt` 后立即重新入队，消费循环不检查时间戳，重试消息被瞬间再次消费。

**解决**：在 `HandleFailureAsync` 中计算退避延迟后 `await Task.Delay(delay)` 再重新入队。多消费者场景下，单个消费者的延迟阻塞不影响其他消费者。未实现独立延迟队列。

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

## 8. 现代 Web 零侵入 Dashboard 独立部署与代理经验

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

### 8.2 独立进程部署与代理设计规避 CORS 限制

- **背景**：直接将大屏网页集成在主服务中，会增大主服务发布、热重载以及静态网页修改时的相互牵连，甚至容易因为浏览器 CORS 跨域安全限制导致大屏访问受阻。
- **方案**：
  1. 主服务（5000端口）仅提供轻量 JSON 指标 API。
  2. Dashboard 作为一个独立的 Web 应用（5001端口），只托管静态 `wwwroot`。
  3. Dashboard 后台挂载 Minimal API 代理：当浏览器请求 5001 端口的 `/api/metrics` 时，其后台通过 `IHttpClientFactory` 透明代理至 5000 端口并回传。
- **优势**：完美避开了浏览器的 CORS 跨域问题，主服务不需要配置任何跨域头，保障了内网环境的极简和纯粹安全；大屏前端页面修改发布无需重启主服务。

### 8.3 线程安全与滑动容量限制防范内存泄漏

- **背景**：实时大屏审计经常需要追溯最新的消息 Payload。
- **原则**：绝对不能允许内存缓冲区无界增长，必须对任何收集到的指标、日志以及 Payload 做物理容量截断。
- **方案**：
  1. 采样曲线数据：使用环形缓冲区或限制 `ConcurrentQueue` 的容量上限（如 60 条采样记录，最近 2 分钟历史）。
  2. 审计日志：只记录最近 100 条简要快照。
  3. Payload 缓存：使用并发字典（`ConcurrentDictionary`）实现滑动内存缓存，上限 100 条，单条超 8KB 截断。确保大屏既能审计真实明细，又决不会造成哪怕 1 字节的内存溢出。

## 9. 本地开发一键启停脚本与 Windows 进程树强杀经验

对于包含多个解耦子系统的微服务/后台应用，在本地 Windows 开发环境下，提供一键启停脚本能极大提升开发效率。但在实现时必须防范子进程残留与中文乱码两大缺陷。

### 9.1 利用内存 Byte 数组写入纯净无 BOM 的 GBK 批处理脚本

- **问题**：在 Windows `cmd.exe` 下执行 `.cmd` 文件时，如果文件头部包含 UTF-8 BOM 字节（`EF BB BF`），系统会把 BOM 强行解析为非 ASCII 字符，导致首行 `@echo off` 被解析为非法命令并产生严重乱码。
- **方案**：使用 PowerShell 写入脚本时，严禁使用会隐式添加 BOM 的常规输出重定向命令，而是通过指定字符集直接导出内存二进制字节流：
  ```powershell
  $content = "..."
  $bytes = [System.Text.Encoding]::GetEncoding(936).GetBytes($content)
  [System.IO.File]::WriteAllBytes("start-dev.cmd", $bytes)
  ```
- **效果**：首字节严格为 `@`（十进制 64），保证 cmd 原生解析 100% 兼容，彻底杜绝乱码和命令解析错误。

### 9.2 强力清除进程树以防范端口占用与控制台挂起

- **背景**：通过 `dotnet run` 启动项目时，底层会先拉起 `dotnet.exe`（父进程），然后再拉起最终的 `MqttRelayService.exe` 与 `MqttRelayService.Dashboard.exe`（实际运行子进程）。
- **缺陷**：如果直接 `taskkill /im dotnet.exe`，会误杀系统上其他无关的 .NET 运行进程；而如果仅强杀 `dotnet.exe` 进程本身，底层的子进程会因为变成孤儿进程而悬挂残留，并继续强占端口（如 1883、5000、5001），直接导致下一次启动因为端口冲突而失败。
- **方案**：
  1. 精准杀灭子可执行程序：使用 `taskkill /f /t /im MqttRelayService.exe` 和 `taskkill /f /t /im MqttRelayService.Dashboard.exe`。通过 `/t` 强行杀死指定映像的整棵进程树。
  2. 回收 dotnet 宿主：子进程树销毁后，`dotnet.exe` 宿主检测到执行体退出，会自我销毁，从而优雅地彻底释放端口。
  3. 清理残留窗口：配合 `taskkill /f /fi "WINDOWTITLE eq MqttRelayService*"`，强制关闭所有窗口标题匹配 `MqttRelayService*` 的独立 CMD 控制台，实现干净的桌面自愈清理。

### 9.3 跨语系/跨默认代码页环境下的纯 ASCII 脚本方案

- **问题**：在跨国团队或多样化开发环境下，不同 Windows 操作系统的默认 ANSI 代码页不同（如纯英文版系统默认代码页为 `437`，或者用户系统开启了“Beta: 全局 Unicode UTF-8 语言支持”使得 CMD 默认以 UTF-8 解码）。在此类环境下，即使使用了 GBK (936) 编码写入脚本并调用 `chcp 936`，CMD 也极易因为底层代码页不支持或强行解码为 UTF-8 而产生字节切分和命令解析错乱（例如 Chinese 字符的高位字节被误判为 `&`、`|`、`>` 等管道/重定向符，进而强行将中文字符当作命令执行，引发 `'清理完毕。' 不是内部或外部命令` 等系列报错）。
- **方案**：采用 **纯 ASCII（0-127）** 编写启动与关闭脚本的所有打印文本、提示、控制符与注释。标准 ASCII 字符在 UTF-8、GBK、Shift-JIS、Latin-1、OEM 437 等世界上所有代码页和编码中均是 100% 完全等价且一致的。
- **效果**：不仅完美保留了开发期自愈和强杀进程树的核心功能，更彻底消除了任何 Windows 版本、任何系统语系、任何控制台默认代码页设置下的乱码与字节拆分报错风险。

