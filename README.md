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

## 运行环境

- **操作系统**：Windows 10/11、Windows Server 2019/2022
- **运行时**：.NET 8 SDK（[下载](https://dotnet.microsoft.com/download/dotnet/8.0)）
- **目标平台**：`win-x64`
- **默认端口**：`1883`

## 本地调试

从仓库根目录执行：

```powershell
dotnet run --project src/MqttRelayService/MqttRelayService.csproj
```

服务启动后会输出日志到控制台，并监听 `1883` 端口。按 `Ctrl+C` 可触发优雅停机。

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
    "QueueCapacity": 1000,
    "EnqueueTimeoutMs": 2000,
    "MaxConcurrentHandlers": 1,
    "MaxRetryCount": 3,
    "MaxPendingRetryTasks": 1000,
    "RetryBaseDelayMs": 1000,
    "RetryMaxDelayMs": 30000,
    "EnableDeadLetter": true,
    "DeadLetterPath": "data/deadletter",
    "ForwardTimeoutMs": 5000,
    "ShutdownDrainTimeoutMs": 30000,
    "DropWhenQueueFull": false
  },
  "AuditStorage": {
    "Provider": "Sqlite",
    "ConnectionString": "Data Source=data/audit.db",
    "AutoInitializeSchema": true,
    "MessageRetentionCount": 20000,
    "ClientHistoryRetentionCount": 5000
  },
  "Web": {
    "Enabled": true,
    "Port": 5000
  },
  "Serilog": {
    "FileNamePrefix": "relay",
    "RetentionDays": 30,
    "MinimumLevel": {
      "Default": "Information"
    }
  }
}
```

### 配置项说明

| 配置节 | 键 | 说明 |
|--------|-----|------|
| **Service** | `Name` | Windows Service 名称 |
| **Mqtt** | `TcpPort` | Broker 监听端口 |
| **Mqtt** | `DefaultQos` | 默认 QoS 等级 |
| **Auth** | `AllowAnonymous` | 是否允许匿名连接 |
| **Auth** | `Users` | 预设用户名/密码/ClientId 前缀列表 |
| **Routing** | `EchoToSender` | `true` 时发送方会收到自己发布的消息；`false` 时不会 |
| **Reliability** | `QueueCapacity` | 内部转发队列容量上限 |
| **Reliability** | `MaxConcurrentHandlers` | 后台消费并发数（最小值为 1） |
| **Reliability** | `MaxRetryCount` | 单条消息最大重试次数 |
| **Reliability** | `MaxPendingRetryTasks` | 运行期等待退避的后台重试调度任务上限，建议不大于 `QueueCapacity`，超限消息进入死信 |
| **Reliability** | `RetryBaseDelayMs` | 重试退避基础延迟（毫秒） |
| **Reliability** | `RetryMaxDelayMs` | 单次重试退避最大延迟（毫秒） |
| **Reliability** | `ShutdownDrainTimeoutMs` | 停机时队列排空超时（毫秒） |
| **Reliability** | `EnableDeadLetter` | 是否启用死信记录 |
| **Reliability** | `DeadLetterPath` | 死信文件存储目录 |
| **AuditStorage** | `Provider` | 审计持久化数据库提供程序，直接填写 `SqlSugar DbType` 名称，例如 `Sqlite`、`SqlServer`、`MySql`、`PostgreSQL`、`Oracle`、`Dm` |
| **AuditStorage** | `ConnectionString` | 审计持久化数据库连接字符串 |
| **AuditStorage** | `AutoInitializeSchema` | 是否在启动时自动初始化审计表结构 |
| **AuditStorage** | `MessageRetentionCount` | 消息审计自动清理保留上限 |
| **AuditStorage** | `ClientHistoryRetentionCount` | 客户端历史自动清理保留上限 |
| **Web** | `Enabled` | 是否启用统一 Web 管理面 |
| **Web** | `Port` | 统一 Web 监听端口，Dashboard 与 API 共用 |
| **Serilog** | `RetentionDays` | 日志文件保留天数 |

停机排空使用 `ShutdownDrainTimeoutMs` 作为总超时。当前默认配置已将 `ShutdownDrainTimeoutMs` 设置为 `30000ms`，与默认 `RetryMaxDelayMs` 一致。停机 drain 阶段遇到失败消息时会同步等待该次退避结束后再尝试重新入队；如果将 `ShutdownDrainTimeoutMs` 调小到 `RetryMaxDelayMs` 以下，排空超时会先触发，消息将按当前逻辑保留回队列或转入死信收敛，不再继续当次下一次注入尝试。

默认使用 SQLite 审计库存储，连接串 `Data Source=data/audit.db` 会被解析到运行目录下的 `data` 目录；如果数据库文件不存在，启动时会自动创建目录、建库并初始化表结构。

Dashboard 消息审计页里的“延迟 / 处理耗时”表示消息被 Broker 拦截接收后，到服务成功重新注入 Broker 为止的内部处理耗时，不表示发布端到订阅端的端到端网络延迟。

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

- **至少一次（At-Least-Once）**：消息转发失败后会自动重试，最多 `MaxRetryCount` 次
- **有界队列**：内部队列有容量上限，满时根据配置选择等待或丢弃
- **有界重试调度**：运行期等待退避的后台重试调度任务受 `MaxPendingRetryTasks` 限制，超限消息直接进入死信
- **异常隔离**：单条消息处理失败不会导致消费者退出或其他消息受影响
- **优雅停机**：收到停止信号后，先在超时内排空队列中剩余消息再退出

**当前限制**：
- 使用**内存队列**（`InMemoryMessageQueue`），进程异常退出或机器宕机时，未完成转发的内存消息会丢失
- Retained Message 使用 MQTTnet Broker 的运行期内存 retained 语义；服务进程重启后 retained 消息不会恢复
- 死信记录写入本地 JSON 文件，不依赖外部存储
- 未实现磁盘队列或消息持久化
- 停机 drain 是否来得及覆盖一次失败消息的最大退避，取决于 `ShutdownDrainTimeoutMs` 是否不小于 `RetryMaxDelayMs`

## 当前不支持的能力

以下能力在当前版本中**未实现**，如后续有需求需单独评估：

- 磁盘队列或消息持久化
- Retained Message 的磁盘持久化
- 集群部署或多节点桥接
- 连接外部 MQTT Broker（桥接模式）
- 严格按客户端级别的点对点直投（当前采用 Topic 注入 + 出站拦截实现）
- 完整的 ACL（访问控制列表），当前仅支持基于预设用户的简单认证
- MQTT 3.1.1 下的 `EchoToSender=false` 兼容（当前依赖 MQTT 5.0 User Properties）

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
4. 出站拦截器（`InterceptingOutboundPacketAsync`）阻止回发给发送方（`EchoToSender=false` 时）

## 日志

日志默认输出到：
- **控制台**（运行时可见）
- **文件**（`logs/` 目录，按小时滚动，`relay-YYYYMMDD-HH.log`）

日志级别可通过 `appsettings.json` 中 `Serilog:MinimumLevel` 调整。
