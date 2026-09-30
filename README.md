# MqttRelayService

基于 `.NET 8 Worker Service + MQTTnet` 的单机 MQTT 消息转发服务。

## 项目用途

MqttRelayService 是一个轻量级单机 MQTT Broker，用于在局域网或单台服务器上提供 MQTT 消息接入和转发能力。服务启动后会监听指定 TCP 端口，接收客户端发布的消息，通过内部队列进行 Topic 匹配和转发，支持重试、死信和优雅停机排空。

主要能力：
- 单机 MQTT Broker（默认端口 `1883`）
- 基于 Topic 的消息路由（支持 `#` 和 `+` 通配符）
- 内部有界队列 + 后台消费投递
- 转发失败自动重试（指数退避）
- 超过重试次数进入死信（JSON 文件记录）
- 优雅停机时尽量排空队列中剩余消息
- 可选阻止消息回发给发送方自身（`EchoToSender=false`）
- 统一 Web 管理面（默认端口 `5000`，Dashboard 页面与 JSON API 共用同一个 Kestrel 监听）
- 消息审计与客户端连接/订阅历史持久化（SqlSugar ORM，默认 SQLite）
- 运行期吞吐量调控（每秒转发量与并发度可在线调整，并发度受硬上限约束）

## 运行环境

- **操作系统**：Windows 10/11、Windows Server 2019/2022
- **运行时**：.NET 8 SDK（[下载](https://dotnet.microsoft.com/download/dotnet/8.0)）
- **目标平台**：`win-x64`
- **默认端口**：MQTT Broker `1883`；Web 管理面 `5000`（`Web:Enabled=true` 时服务同时监听 `1883` 与 `5000`）

## 本地调试

从仓库根目录执行：

```powershell
dotnet run --project src/MqttRelayService/MqttRelayService.csproj
```

服务启动后会输出日志到控制台，并监听 `1883`（MQTT）与 `5000`（Web 管理面）端口。按 `Ctrl+C` 可触发优雅停机。浏览器访问 `http://127.0.0.1:5000/` 打开 Web 管理面。

## 发布方式

```powershell
dotnet publish src/MqttRelayService/MqttRelayService.csproj -c Release -r win-x64 --self-contained
```

发布输出位于：
```
src/MqttRelayService/bin/Release/net8.0/win-x64/publish/
```

## Windows Service 安装与卸载

### 安装

1. 先完成发布（见上方命令）
2. 以**管理员身份**打开命令提示符
3. 进入发布目录：
   ```powershell
   cd src/MqttRelayService/bin/Release/net8.0/win-x64/publish
   ```
4. 执行安装脚本：
   ```powershell
   Scripts\install-service.cmd
   ```

仓库根目录还提供了一个本地压测快捷脚本：
```powershell
run-stress-60s.cmd
```
它会直接调用 `stress_mqtt_1883.py`，默认对 `127.0.0.1:1883` 连续压测 60 秒。

### 卸载

```powershell
Scripts\uninstall-service.cmd
```

安装成功后，服务名称默认使用 `appsettings.json` 中 `Service:Name` 的值（默认为 `MqttRelayService`），启动类型为 `Automatic`。可通过 Windows 服务管理器查看状态。修改 `Service:Name` 后需重新发布并执行安装脚本，脚本会自动读取配置中的名称。

## 配置说明

所有配置通过 `appsettings.json` 管理，发布后该文件与可执行文件位于同一目录，修改后需重启服务生效。

### 配置项一览

```json
{
    "Service": {
        "Name": "MqttRelayService"
    },
    "Mqtt": {
        "TcpPort": 1883,
        "DefaultQos": 1
    },
    "Auth": {
        "AllowAnonymous": true,
        "Users": [
            {
                "Username": "app1",
                "Password": "123456",
                "ClientIdPrefix": "app1"
            }
        ]
    },
    "Routing": {
        "EchoToSender": false
    },
    "Reliability": {
        "DeliverySemantics": "AtLeastOnce",
        "QueueCapacity": 10000,
        "EnqueueTimeoutMs": 5000,
        "MaxConcurrentHandlers": 3,
        "MaxRetryCount": 3,
        "MaxPendingRetryTasks": 1000,
        "RetryBaseDelayMs": 1000,
        "RetryMaxDelayMs": 30000,
        "EnableDeadLetter": true,
        "DeadLetterPath": "data/deadletter",
        "DeadLetterRetentionDays": 30,
        "ForwardTimeoutMs": 5000,
        "ShutdownDrainTimeoutMs": 30000,
        "DropWhenQueueFull": false,
        "MaxConcurrencyHardLimit": 200
    },
    "Web": {
        "Enabled": true,
        "Port": 5000,
        "ApiKey": null
    },
    "AuditStorage": {
        "Provider": "Sqlite",
        "ConnectionString": "Data Source=data/audit.db",
        "AutoInitializeSchema": true,
        "MessageArchiveThreshold": 5000000,
        "ClientHistoryArchiveThreshold": 1000000,
        "RetentionDays": 30,
        "CleanupAtHour": 3,
        "CleanupIntervalMinutes": 1440,
        "VacuumAfterCleanup": true
    },
    "Serilog": {
        "FileNamePrefix": "relay",
        "RetentionDays": 30,
        "IncludeCallerInfo": false,
        "MinimumLevel": {
            "Default": "Information",
            "Override": {
                "Microsoft": "Warning",
                "Microsoft.Hosting.Lifetime": "Information"
            }
        }
    }
}
```

### 配置项说明

| 配置节 | 键 | 说明 |
|--------|-----|------|
| **Service** | `Name` | Windows Service 名称，同时作为日志中的 `ServiceName` 字段 |
| **Mqtt** | `TcpPort` | Broker 监听端口，启动时校验取值范围 `1-65535` |
| **Mqtt** | `DefaultQos` | 默认 QoS 等级；注入消息的 QoS 越界时回退到该值 |
| **Auth** | `AllowAnonymous` | 是否允许匿名连接 |
| **Auth** | `Users` | 预设用户名/密码/ClientId 前缀列表 |
| **Routing** | `EchoToSender` | `true` 时发送方会收到自己发布的消息；`false` 时出站拦截器阻止回发给发送方 |
| **Reliability** | `DeliverySemantics` | 投递语义，当前只接受 `AtLeastOnce`，配置为其它值会在启动时失败 |
| **Reliability** | `QueueCapacity` | 内部转发队列容量上限，启动时校验必须大于等于 `1` |
| **Reliability** | `EnqueueTimeoutMs` | 入队等待超时（毫秒），超时按“本次入队未成功”处理并记录 Warning |
| **Reliability** | `MaxConcurrentHandlers` | 后台消费者初始数量（最小值为 1），并会被 `MaxConcurrencyHardLimit` 收敛 |
| **Reliability** | `MaxRetryCount` | 单条消息最大重试次数 |
| **Reliability** | `MaxPendingRetryTasks` | 运行期等待退避的后台重试调度任务上限，建议不大于 `QueueCapacity`，超限消息直接进入死信；配置小于 1 时回退到 `QueueCapacity` |
| **Reliability** | `RetryBaseDelayMs` | 重试退避基础延迟（毫秒），必须大于 0 |
| **Reliability** | `RetryMaxDelayMs` | 单次重试退避最大延迟（毫秒），不得小于 `RetryBaseDelayMs` |
| **Reliability** | `EnableDeadLetter` | 是否启用死信记录；配置为 `false` 时无法转死信的消息被直接丢弃并记录 Error 日志 |
| **Reliability** | `DeadLetterPath` | 死信文件存储目录，相对路径以程序基目录 `AppContext.BaseDirectory` 为基准，按 `yyyyMMdd` 子目录分日存放 |
| **Reliability** | `DeadLetterRetentionDays` | 死信日期目录保留天数，写入死信时清理更早的日期目录；配置为 0 或负数表示不清理 |
| **Reliability** | `ForwardTimeoutMs` | 单次向 Broker 注入消息的超时（毫秒），必须大于 0 |
| **Reliability** | `ShutdownDrainTimeoutMs` | 停机排空总超时（毫秒），必须大于 0；Host 的 `ShutdownTimeout` 取该值加 5000ms |
| **Reliability** | `DropWhenQueueFull` | `true` 时队列满立即丢弃新消息；`false` 时等待 `EnqueueTimeoutMs` |
| **Reliability** | `MaxConcurrencyHardLimit` | 吞吐调控的并发度硬上限（默认 200），运行期通过 API 调整并发度时不得超过该值 |
| **AuditStorage** | `Provider` | 审计持久化数据库提供程序，直接填写 `SqlSugar DbType` 名称，例如 `Sqlite`、`SqlServer`、`MySql`、`PostgreSQL`、`Oracle`、`Dm` |
| **AuditStorage** | `ConnectionString` | 审计持久化数据库连接字符串；SQLite 的相对 `Data Source` 以程序基目录为基准 |
| **AuditStorage** | `AutoInitializeSchema` | 是否在启动时自动初始化审计表结构；SQLite 数据文件所在目录不存在时会被创建 |
| **AuditStorage** | `MessageArchiveThreshold` | 启动时按该条数检查消息审计表规模，达到即记录迁移提示日志，不触发自动删除 |
| **AuditStorage** | `ClientHistoryArchiveThreshold` | 启动时按该条数检查客户端历史表规模，达到即记录迁移提示日志，不触发自动删除 |
| **AuditStorage** | `RetentionDays` | 审计数据（消息审计表与客户端历史表）保留天数，默认 `30`；清理时删除早于「当前本机时间 − 保留天数」的记录，配置为 `0` 表示关闭清理 |
| **AuditStorage** | `CleanupAtHour` | 每日清理时刻（本机时间整点，`0-23`），默认 `3`；留空（`null`）时改为按 `CleanupIntervalMinutes` 等间隔执行 |
| **AuditStorage** | `CleanupIntervalMinutes` | `CleanupAtHour` 留空时的清理间隔（分钟），默认 `1440`；启动时校验范围 `1-1440`，用上限保证每天至少清理一次 |
| **AuditStorage** | `VacuumAfterCleanup` | 清理后是否对 SQLite 执行 `VACUUM` 回收 `.db` 文件空间，默认 `true`；`DELETE` 本身不会缩小数据库文件，关闭该开关时磁盘占用会停留在历史峰值 |
| **Web** | `Enabled` | 是否启用统一 Web 管理面；`false` 时退化为纯 Worker Host，不监听 Web 端口 |
| **Web** | `Port` | 统一 Web 监听端口，Dashboard 页面与 `/api` 共用同一个 Kestrel 监听（`ListenAnyIP`） |
| **Web** | `ApiKey` | API 访问密钥；为空时所有 `/api` 端点都不校验鉴权，非空时要求请求头 `X-Api-Key` 与该值完全一致 |
| **Serilog** | `FileNamePrefix` | 日志文件前缀，滚动文件名为 `{前缀}-yyyyMMddHH.log` |
| **Serilog** | `RetentionDays` | 日志保留天数，实际按“天数 × 24”换算为保留的小时文件数量上限 |
| **Serilog** | `IncludeCallerInfo` | 是否启用调用者信息富集（每条日志解析 StackTrace，性能成本较高，默认关闭） |
| **Serilog** | `MinimumLevel:Default` | 默认最低日志级别，解析失败时回退到 `Information` |
| **Serilog** | `MinimumLevel:Override` | 按日志源类别前缀覆盖级别，例如 `Microsoft: Warning`、`Microsoft.Hosting.Lifetime: Information` |

停机排空使用 `ShutdownDrainTimeoutMs` 作为总超时，Host 的 `ShutdownTimeout` 取该值加 5000ms。当前默认配置为 `30000ms`，与默认 `RetryMaxDelayMs` 一致。停机 drain 阶段遇到失败消息时会同步等待该次退避结束后再尝试重新入队；如果 `ShutdownDrainTimeoutMs` 小于 `RetryMaxDelayMs`，启动时会记录配置提示日志，排空超时会先触发，消息进入“保留回队列或转死信”的收敛分支，不再完成当次下一次注入尝试。

默认使用 SQLite 审计库存储，连接串 `Data Source=data/audit.db` 会被解析到程序基目录下的 `data` 目录；如果数据库文件不存在，启动时会自动创建目录、建库并初始化表结构。

审计数据会按保留天数自动清理：服务启动后立即清理一次，之后每天至少清理一次（默认每天 03:00），删除早于「当前本机时间 − `RetentionDays`」的消息审计与客户端历史记录。`RetentionDays` 配置为 `0` 表示关闭清理，数据由运维手工维护。清理动作在独立后台任务里执行，不阻塞 MQTT 转发主链路；单轮清理失败只记录日志，不影响后续轮次。

清理只删除行，不会自动缩小 SQLite 数据库文件，因此默认在每次清理后执行 `VACUUM` 回收空间（`VacuumAfterCleanup=true`）。`VACUUM` 需要独占数据库且会重写整库，数据量大时耗时可观；需要更短的清理窗口时可以关闭该开关，磁盘空间改由运维手工回收。

`MessageArchiveThreshold` 与 `ClientHistoryArchiveThreshold` 是清理之外的规模提示：启动时统计一次表规模，达到阈值即记录迁移提示日志，不触发额外删除。

Dashboard 顶部的累计消息总数是启动时读取的累计基线加运行期增量，清理不会让它回落；清理窗口之外的消息在审计列表与按 ID 精确查询中不再可见。

审计持久化属于 Web 管理面的可选能力：初始化失败时服务记录 Error 日志并降级为“审计不可用”，实时指标与 MQTT 转发主链路继续运行。

Dashboard 消息审计页里的“延迟 / 处理耗时”表示消息被 Broker 拦截接收后，到服务成功重新注入 Broker 为止的内部处理耗时，不表示发布端到订阅端的端到端网络延迟。

## Web 管理面与 API Key

`Web:Enabled=true`（默认）时，服务同时监听 MQTT 端口 `1883`（MQTTnet TCP 监听）与 Web 端口 `5000`（Kestrel）。Dashboard 页面（`/`、`/index.html`）与 JSON API（`/api/*`）共用 `5000` 这一个 Kestrel 监听，不需要额外的代理或反向代理。

API Key 认证的真实行为：

- 页面**不内嵌**任何密钥。`/` 与 `/index.html` 是无鉴权的静态页面，服务端不再向页面注入 `Web:ApiKey`（早期实现把密钥明文写进未鉴权页面，导致认证形同虚设）。
- 密钥由运维在页面弹窗中录入，只保存在浏览器 `sessionStorage`；请求返回 `401` 时页面再次弹窗要求录入。
- 前端在所有 `/api/*` 请求上携带请求头 `X-Api-Key`。
- `Web:ApiKey` 为空（默认 `null`）时，所有 `/api` 端点都不校验鉴权。
- `Web:ApiKey` 非空时，`/api` 组内的端点要求 `X-Api-Key` 与该值严格相等（区分大小写），不匹配返回 `401`。
- `/api/health` 是唯一的例外：它注册在鉴权过滤器之外，无论是否配置 `Web:ApiKey` 都无需密钥即可访问，供监控探活。

Web 管理面的安全边界完全依赖网络可达性与 `Web:ApiKey`：服务本身不提供用户/角色体系、ACL 或 TLS，`/api/health` 始终匿名可访问。生产环境必须把 `5000` 端口限制在受信网络内，并配置非空 `Web:ApiKey`。

## Topic 规范

服务支持标准 MQTT Topic 格式，层级使用 `/` 分隔，支持以下通配符：

- `#`：匹配该层级及所有后续层级（必须放在 Topic 末尾）
- `+`：匹配单个层级

示例：

| Topic | 说明 |
|-------|------|
| `apps/{appId}/up` | 设备上行数据 |
| `apps/{appId}/down` | 平台下行指令 |
| `broadcast/all` | 全量广播 |
| `events/{eventType}` | 事件通知 |
| `rpc/{clientId}/request` | RPC 请求 |
| `rpc/{clientId}/response` | RPC 响应 |

## 可靠性边界

当前版本实现以下可靠性保证：

- **至少一次（At-Least-Once）**：消息转发失败后自动重试，最多 `MaxRetryCount` 次；运行期重试是非阻塞后台调度（`ScheduleRetryEnqueueAsync`），消费者不会被退避延迟占住
- **有界队列**：内部队列容量上限为 `QueueCapacity`，满时按 `DropWhenQueueFull` 选择立即丢弃或等待 `EnqueueTimeoutMs`
- **有界重试调度**：运行期等待退避的后台重试调度任务受 `MaxPendingRetryTasks` 限制，超限消息直接进入死信
- **有界死信目录**：死信按 `yyyyMMdd` 日期目录写入，写入时按 `DeadLetterRetentionDays` 清理更早的日期目录
- **有界审计待写队列**：审计待写队列上限 50000 条，客户端历史待写队列上限 10000 条；两者超限都丢弃新记录并写入日志，不使用无界队列
- **有界审计数据**：审计数据按 `AuditStorage:RetentionDays`（默认 30 天）自动清理，启动清理一次后每天至少一次；清理后按 `VacuumAfterCleanup` 回收 SQLite 文件空间
- **异常隔离**：单条消息处理失败不会导致消费者退出或其他消息受影响
- **确定性停机顺序**：先封堵客户端新发布入口（`StopAcceptingClientPublishes`，Broker 保持运行）→ 取消消费者 → 多轮排空队列（Broker 仍在运行，排空阶段仍能向订阅者注入消息）→ 排空结束后才由 BrokerWorker 停止 Broker

停机顺序不可颠倒：如果先停 Broker，排空阶段无法再注入消息，剩余消息只能进入死信或丢失。

**当前限制**：
- 使用**内存队列**（`InMemoryMessageQueue`），进程异常退出或机器宕机时，未完成转发的内存消息会丢失
- Retained Message 使用 MQTTnet Broker 的运行期内存 retained 语义；服务进程重启后 retained 消息不会恢复
- 死信记录写入本地 JSON 文件，不依赖外部存储
- 未实现磁盘队列或消息持久化
- 只有停机排空阶段会同步等待退避（`DelayAndRequeueDuringStopAsync`）；停机 drain 是否来得及覆盖一次失败消息的最大退避，取决于 `ShutdownDrainTimeoutMs` 是否不小于 `RetryMaxDelayMs`
- 审计数据按保留天数删除行，不提供按天分区、归档导出或迁移工具；超出 `RetentionDays` 的记录会被直接删除
- `EchoToSender=false` 只对 MQTT 5.0 订阅者生效：出站拦截依赖注入消息携带的 MQTT 5.0 User Properties（`x-source-client-id`），MQTT 3.1.1 协议本身没有 User Properties 字段，因此 MQTT 3.1.1 订阅者仍会收到发送方自己发布的消息

## 当前不支持的能力

以下能力在当前版本中**未实现**：

- 磁盘队列或消息持久化
- Retained Message 的磁盘持久化
- 集群部署或多节点桥接
- 连接外部 MQTT Broker（桥接模式）
- 严格按客户端级别的点对点直投（当前采用 Topic 注入 + 出站拦截实现）
- 完整的 ACL（访问控制列表），当前仅支持基于预设用户的简单认证
- 用户/角色体系与细粒度权限控制
- TLS 加密：MQTT 监听与 Web 管理面都只提供明文 TCP/HTTP
- MQTT 3.1.1 下的 `EchoToSender=false` 兼容（当前依赖 MQTT 5.0 User Properties）
- 审计数据的归档导出、按天分区与手工触发清理的 API（当前只有按保留天数删除行）

**Web 管理面的安全边界完全依赖网络可达性与 `Web:ApiKey`**：没有完整的 ACL、没有 TLS、没有用户/角色体系，`/api/health` 始终匿名可访问，未配置 `Web:ApiKey` 时所有 `/api` 端点都不校验鉴权。

## 架构概览

```
┌─────────────┐     ┌──────────────┐     ┌──────────────────┐
│ MQTT Client │────▶│ MQTT Broker  │────▶│ IMessageQueue    │
│  (发布消息)  │     │ (拦截 + 入队) │     │ (有界内存队列)    │
└─────────────┘     └──────────────┘     └──────────────────┘
                                                    │
                                                    ▼
                                           ┌──────────────────┐
                                           │ MessageDelivery  │
                                           │ Service          │
                                           │ (消费 + 路由     │
                                           │  + 转发 + 重试)  │
                                           └──────────────────┘
                                                    │
                                                    ▼
                                           ┌──────────────────┐
                                           │ IMqttBrokerHost  │
                                           │ (InjectApplication│
                                           │  Message 注入)   │
                                           └──────────────────┘
                                                    │
                                                    ▼
                                           ┌──────────────────┐
                                           │ MQTT Client      │
                                           │ (订阅接收)        │
                                           └──────────────────┘
```

数据流：
1. 客户端发布消息 → Broker 拦截（`InterceptingPublishAsync`）→ 消息入队
2. 投递服务消费队列 → 路由匹配（`MessageRouter.RouteAsync`）→ 向 Topic 注入消息
3. Broker 按订阅分发给所有匹配客户端
4. 出站拦截器（`InterceptingOutboundPacketAsync`）在 `EchoToSender=false` 时阻止回发给发送方（依赖注入消息上的 MQTT 5.0 User Properties，仅对 MQTT 5.0 订阅者生效）

## 日志

日志默认输出到：
- **控制台**（运行时可见）
- **文件**：程序基目录（`AppContext.BaseDirectory`）下的 `Logs` 目录，按小时滚动，滚动文件名为 `relay-YYYYMMDDHH.log`（前缀由 `Serilog:FileNamePrefix` 决定）

`Serilog:RetentionDays` 按“天数 × 24”换算成保留的小时文件数量上限。`Serilog:IncludeCallerInfo=true` 时日志行额外输出 `{Caller}` 字段（每条日志解析 StackTrace，默认关闭）。日志级别通过 `Serilog:MinimumLevel:Default` 与 `Serilog:MinimumLevel:Override` 调整。
