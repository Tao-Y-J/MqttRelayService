# MQTT Windows 转发服务需求文档

## 0. 文档状态

本文档已根据当前 `MqttRelayService` 现有实现反向更新，用于描述当前仓库已经落地的需求事实和后续演进边界。

当前实现入口：

- 解决方案：`MqttRelayService.slnx`
- 主项目：`src/MqttRelayService`
- 测试项目：`tests/MqttRelayService.Tests`
- Windows Service 脚本：`src/MqttRelayService/Scripts`

当前版本已经具备可运行主链路：MQTT Broker 启停、异常停止自动重启、基础认证、在线客户端注册、订阅跟踪、内部有界队列、后台异步转发、重试、死信、优雅停机、队列指标快照和 Windows Service 安装/卸载脚本。

当前版本不内置磁盘队列，可靠性边界仍然是“进程存活期间的至少一次转发”。

## 1. 项目概述

开发一个基于 **C# / .NET** 的 **Windows Service**，作为内部应用之间的 **MQTT 消息转发服务**。  
该服务运行在 Windows 上，对外提供 MQTT 接入能力，允许多个业务 App 作为 MQTT Client 连接到该服务，通过约定的 Topic 进行消息发布、订阅和转发。

技术选型要求如下：

- 宿主模型：`.NET Worker Service`
- 部署形态：`Windows Service`
- MQTT 组件：`MQTTnet`
- 配置方式：`appsettings.json`
- 日志方式：`Microsoft.Extensions.Logging` 抽象 + `Serilog` 控制台/文件落盘
- 运行环境：`.NET 8`
- 发布目标：`win-x64`

---

## 2. 项目目标

实现一个可长期稳定运行的 MQTT 转发服务，满足以下目标：

1. 允许多个内部 App 通过 MQTT 连接到本服务
2. 支持客户端认证
3. 支持 Topic 订阅与发布
4. 根据 Topic 规则将消息转发给目标订阅方
5. 记录客户端上下线、消息收发、错误日志
6. 支持作为 Windows Service 安装、启动、停止、自动拉起
7. 提供清晰、可扩展的代码结构，便于后续增加权限、监控、消息持久化等能力
8. 优先保证服务健壮性与消息可靠转发能力

---

## 3. 非目标

本期 **不实现** 以下功能：

- 集群部署
- 多节点 Broker 共享会话
- 离线消息持久化到远程基础设施
- Retained Message 的高级持久化策略
- Web 管理后台
- 大规模分布式路由
- 与外部 Broker 桥接
- 高级 ACL 规则引擎
- 多租户隔离

> 说明：本期目标是先实现一个 **单机、稳定、可恢复、便于扩展** 的轻量 MQTT 转发服务。

---

## 4. 总体架构

### 4.1 架构定位

本服务本质上是一个：

- **轻量级 MQTT Broker**
- 同时带有 **自定义消息路由能力**

其他业务 App 直接连接本服务，本服务负责：

- 建立 MQTT 连接
- 校验客户端身份
- 处理订阅
- 接收消息
- 根据 Topic 规则转发消息
- 输出日志与运行状态

### 4.2 推荐架构分层

```text
Host (Worker Service / Windows Service)
├─ BrokerHost
│  ├─ 启动 MQTT Server
│  ├─ 监听客户端连接/断开
│  ├─ 监听消息发布
│  └─ 监听订阅事件
├─ Auth
│  └─ 客户端认证
├─ Session
│  └─ 在线客户端注册表
├─ Routing
│  ├─ Topic 匹配
│  ├─ 消息路由
│  └─ 消息转发
├─ Reliability
│  ├─ 内部可靠队列
│  ├─ 重试
│  ├─ 死信
│  └─ 优雅停机排空
├─ Config
│  └─ 配置读取与绑定
├─ Logging
│  └─ 控制台日志 / 文件日志 / 错误日志 / 审计字段
└─ Metrics
   └─ 队列长度与峰值快照
```

### 4.3 架构原则

- Broker 接入层与业务转发层解耦
- MQTT 事件回调只做轻量逻辑
- 所有复杂处理通过内部队列异步执行
- 优先保证“不静默丢失”
- 优先实现“至少一次”转发语义
- 允许重复，后续通过幂等机制处理

### 4.4 当前实现分层

当前仓库已经按接口和实现拆分服务层：

```text
src/MqttRelayService
├─ Program.cs
├─ Workers
│  ├─ BrokerWorker.cs
│  └─ QueueMetricsWorker.cs
├─ Logging
│  └─ SerilogLogging.cs
├─ Options
├─ Models
├─ Services
│  ├─ Abstractions
│  └─ Implementations
├─ Utilities
├─ Scripts
│  ├─ install-service.cmd
│  ├─ install-service.ps1
│  ├─ uninstall-service.cmd
│  └─ uninstall-service.ps1
├─ appsettings.json
└─ appsettings.Development.json
```

服务接口统一位于 `Services/Abstractions`，实现统一位于 `Services/Implementations`。`Program.cs` 只负责 Host 构建、配置校验、日志接入和依赖注入，不承载业务编排。

---

## 5. 功能需求

## 5.1 服务启动

### 需求
服务启动后应自动完成以下动作：

1. 读取配置文件
2. 初始化日志系统
3. 初始化 MQTT Server
4. 初始化内部可靠队列和后台消费者
5. 开始监听指定 TCP 端口
6. 进入运行状态

### 验收标准
- 服务启动成功后，日志中输出启动成功信息
- 服务能监听配置端口
- 客户端可正常发起 MQTT 连接

---

## 5.2 Windows Service 运行

### 需求
服务必须支持以 Windows Service 方式运行。

### 验收标准
- 可作为 Windows Service 安装
- 可通过系统服务管理器启动/停止
- 机器重启后可自动启动
- 停止服务时应优雅释放资源

### 补充约束
- 服务不得依赖桌面交互
- 服务不得弹出 UI 窗口

---

## 5.3 MQTT 接入能力

### 需求
服务需提供 MQTT TCP 接入能力，至少支持：

- Client 连接
- Client 断开
- Publish
- Subscribe
- Unsubscribe

### 要求
- 默认监听端口：`1883`
- 支持配置修改监听端口
- 支持多个客户端同时连接
- 第一版仅支持内网明文 TCP 接入

---

## 5.4 客户端认证

### 需求
服务需支持基础认证能力。

### 认证规则
客户端连接时至少校验：

- `ClientId`
- `Username`
- `Password`

### 校验要求
- `ClientId` 不能为空
- 关闭匿名认证后，`Username` 不能为空
- 关闭匿名认证后，`Password` 不能为空
- 认证失败时拒绝连接
- 认证成功后记录连接信息
- 当前默认配置允许匿名认证；匿名认证开启时仍要求 `ClientId` 不能为空，但跳过账号密码校验
- 关闭匿名认证后，必须按配置文件中的 `Auth:Users` 校验用户名、密码和可选 `ClientIdPrefix`

### 后续扩展预留
- 支持按账号绑定允许访问的 Topic 前缀或 ACL

---

## 5.5 在线客户端注册表

### 需求
服务需维护当前在线客户端信息。

### 记录内容
每个在线客户端至少记录：

- ClientId
- Username
- 连接时间
- 最近活动时间
- 连接状态
- 订阅的 Topic 列表

### 说明
在线客户端注册表供后续路由、监控和诊断使用。

### 数据结构建议
使用线程安全结构，如：

- `ConcurrentDictionary<string, ClientSessionInfo>`

---

## 5.6 Topic 订阅管理

### 需求
服务需跟踪客户端订阅信息。

### 要求
- 客户端订阅成功后，记录 Topic
- 客户端取消订阅后，移除 Topic
- 客户端断开后，清理其订阅信息
- 服务内部可根据订阅关系决定消息转发目标

---

## 5.7 消息转发

### 需求
服务应根据 Topic 实现消息转发。

### 基本规则
- 某客户端发布消息后
- 服务识别该消息 Topic
- 将消息转发给所有匹配订阅规则的其他客户端
- 默认不回发给消息发送方，除非配置明确允许

### 转发要求
- 保留原始 Topic
- 保留原始 Payload
- 保留 QoS 信息
- 记录消息来源 ClientId
- 转发过程异常不能导致服务崩溃

### 第一版范围
仅实现基础广播/订阅式转发，不实现复杂业务转换。

---

## 5.8 Topic 规范

### 需求
项目中需预定义 Topic 命名规范，供各接入 App 统一使用。

### 推荐规范
```text
apps/{appId}/up
apps/{appId}/down
broadcast/all
events/{eventType}
rpc/{targetAppId}/request
rpc/{sourceAppId}/response
```

### 语义说明
- `apps/{appId}/up`：某 App 的上行消息
- `apps/{appId}/down`：发送给某 App 的下行消息
- `broadcast/all`：广播给所有订阅方
- `events/{eventType}`：通用事件流
- `rpc/{targetAppId}/request`：面向目标 App 的请求
- `rpc/{sourceAppId}/response`：请求响应

### 要求
- Topic 区分大小写
- Topic 规则写入配置或常量定义
- 路由逻辑不得硬编码在大量 `if/else` 中

---

## 5.9 消息路由模块

### 需求
服务需具备独立的消息路由模块，而不是将全部逻辑写在 MQTT 回调中。

### 实现要求
- 路由逻辑独立封装
- 支持后续扩展多条路由规则
- 支持按 Topic 前缀或模式匹配
- 不允许在 MQTT 回调里直接写复杂业务逻辑

### 建议接口
```csharp
public interface IMessageRouter
{
    Task RouteAsync(RouteContext context, CancellationToken cancellationToken);
}
```

---

## 5.10 日志记录

### 需求
服务需通过 `Microsoft.Extensions.Logging` 抽象输出结构化日志，并使用 `Serilog` 同时输出控制台日志和运行目录下的文件日志。

### 当前实现
- `SerilogLogging` 集中管理日志初始化、输出模板、文件滚动和扩展属性
- 日志默认输出到运行目录下的 `Logs` 文件夹
- 日志按小时滚动，保留数量按 `Serilog:RetentionDays` 折算
- 默认所有级别写入同一组文本日志文件
- 可通过 `Serilog:IncludeCallerInfo` 临时开启源码调用位置输出
- `appsettings.Development.json` 将默认日志级别调整为 `Debug`

### 必须记录的日志类型
- 服务启动/停止
- MQTT Server 启动/关闭
- Broker 异常停止后的自动重启尝试与结果
- 客户端连接成功
- 客户端连接失败
- 客户端断开
- 客户端订阅/取消订阅
- 消息接收
- 消息转发成功
- 消息转发失败
- 消息重试
- 死信写入
- 队列长度和峰值变化
- 未处理异常捕获日志

### 日志级别建议
- Information：正常运行信息
- Warning：可恢复异常、认证失败、非法订阅、队列积压
- Error：服务级错误、消息转发异常、死信异常

---

## 5.11 配置管理

### 需求
服务使用 `appsettings.json` 管理配置。

### 必须支持的配置项

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
    "RetryBaseDelayMs": 1000,
    "RetryMaxDelayMs": 30000,
    "EnableDeadLetter": true,
    "DeadLetterPath": "data/deadletter",
    "ForwardTimeoutMs": 5000,
    "ShutdownDrainTimeoutMs": 10000,
    "DropWhenQueueFull": false
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

### 要求
- 配置项必须绑定到强类型 Options
- 配置读取失败时，服务启动失败并写出明确错误日志
- 默认配置应可直接用于本地开发
- 当前默认配置允许匿名认证，便于本地联调；生产环境应关闭 `Auth:AllowAnonymous`，并配置账号列表
- 匿名认证只保留 `Auth:AllowAnonymous` 一个配置项，`Mqtt` 配置不再提供匿名开关
- `Service:Name` 同时作为 Windows Service 名称和日志扩展属性

---

## 6. 非功能需求

## 6.1 稳定性

### 要求
- 任意单条消息处理失败，不得导致服务退出
- 任意单个客户端行为异常，不得影响其他客户端
- 服务停止时应优雅关闭 MQTT Server
- 所有后台任务必须支持 `CancellationToken`

---

## 6.2 可维护性

### 要求
- 必须按模块分层
- 不允许把所有逻辑写进单个 `Worker` 类
- 关键模块需定义接口
- 类命名、文件命名、目录结构清晰
- 便于后续接入权限控制、监控接口、数据库持久化

---

## 6.3 可扩展性

### 要求
后续应方便扩展以下能力：

- Topic ACL
- HTTP 健康检查接口
- 消息审计
- 客户端黑名单
- 离线消息存储
- 外部 Broker 桥接

---

## 6.4 性能

### 第一版性能目标
- 支持 20~50 个客户端同时在线
- 支持普通文本消息转发
- 不追求高并发极限性能
- 优先保证代码清晰和稳定性

---

## 7. 健壮性与可靠性专项要求

## 7.1 总体目标

本服务的首要目标不是功能丰富，而是**长期稳定运行**与**可靠转发消息**。

系统设计必须满足以下原则：

1. 不能因单条消息处理异常导致整个服务退出
2. 不能因单个客户端异常导致其他客户端连接受影响
3. 不能因内部未处理异常导致 Broker 进程退出
4. 消息转发必须优先保证“不静默丢失”
5. 在无法绝对保证“恰好一次”时，应明确采用“至少一次”语义，并允许业务侧做幂等处理

---

## 7.2 可靠性设计原则

### 必须遵守的原则

- 允许重复，不允许无日志丢失
- 所有失败必须可观测
- 所有关键路径必须有超时控制
- 所有后台任务必须可取消
- 所有连接状态变化必须记录
- 所有异常必须被捕获、记录并隔离影响范围

### 解释

对于转发服务而言，最危险的问题不是“消息偶尔重复”，而是：

- 消息丢了但没有日志
- 服务悄悄退出
- 某个异常把整个服务拖死
- 某个客户端反复异常导致整体连接抖动

因此，第一版的可靠性目标应优先是：

**至少一次转发 + 可重试 + 可审计 + 可恢复**

---

## 7.3 宿主异常处理要求

### 强制要求

- 任何 `BackgroundService`、MQTT 事件处理回调、后台队列消费者中，不得让异常冒泡到宿主层
- 所有后台入口必须使用顶层 `try/catch`
- 任何异常都必须写日志
- 即使发生异常，也应保证服务主循环继续运行，除非属于不可恢复错误

### 不可恢复错误示例

以下情况允许服务启动失败或主动退出：

- 配置文件损坏或关键配置缺失
- 监听端口被占用
- 必要依赖初始化失败
- Broker 核心组件无法创建

### 可恢复错误示例

以下情况不得导致服务退出：

- 单条消息路由失败
- 单个客户端发布非法 Topic
- 某客户端认证失败
- 某客户端断线
- 某次转发失败
- 某个后台处理任务异常

---

## 7.4 连接稳定性要求

### 目标

本服务作为 MQTT Broker，必须尽量保证：

- 自身进程不因普通业务异常而退出
- 现有正常客户端连接不因个别异常被波及
- 客户端断开后可重新连接
- 断开、重连、认证失败等状态都有清晰日志

### 强制要求

1. 服务不得因为单个客户端连接异常导致 Broker 停止
2. 服务不得因为单个 Topic 非法或消息格式异常导致 Broker 停止
3. 客户端连接、断开、认证失败必须分别记录日志
4. 对频繁异常的客户端，允许拒绝其请求，但不得拖垮整个服务
5. 所有 MQTT 事件处理必须快速返回，不允许长时间阻塞网络处理线程

---

## 7.5 消息可靠性交付语义

### 第一版交付语义

本项目第一版默认采用：

**至少一次（At-Least-Once）转发语义**

### 原因

对于内部转发服务第一版，优先目标应是：

- 不轻易丢消息
- 服务内部可恢复
- 允许业务侧做幂等去重

### 强制要求

- 默认转发策略应优先支持 **QoS 1**
- 不得默认将所有消息都按 QoS 0 处理
- 若接收到更高 QoS，转发层应尽量保留原有 QoS 或按明确规则降级
- 如发生重复投递，系统视为可接受行为，但必须避免无记录丢失

---

## 7.6 内部消息处理模型要求

### 强制要求

MQTT 事件回调中不得直接执行复杂、耗时或不稳定逻辑。  
必须采用：

**接收与处理解耦模型**

推荐流程：

```text
客户端发布消息
→ MQTT Server 收到消息
→ 转换为内部消息对象
→ 写入内部可靠队列
→ 后台消费者执行路由与转发
→ 记录成功/失败结果
```

### 内部队列要求

- 使用线程安全队列结构
- 推荐使用 `System.Threading.Channels`
- 队列应支持有界容量
- 队列满时必须有明确策略，不允许静默丢弃

### 队列满载策略

必须通过配置选择以下策略之一：

1. 拒绝新消息并记录错误
2. 阻塞等待有限时间后失败
3. 写入失败日志并触发告警

### 明确禁止

- 在 MQTT 消息回调里直接做数据库访问
- 在 MQTT 消息回调里直接做复杂转发
- 在 MQTT 消息回调里执行长时间同步阻塞操作

---

## 7.7 内部消息持久化要求

### 目标

为了提高可靠性，服务需要支持“消息进入转发流程后尽量不丢”。

### 第一版最低要求

至少提供以下两档实现能力中的一种：

#### 档位 A：内存可靠队列
- 消息进入内部队列后再处理
- 适合第一版快速落地
- 服务进程崩溃时，内存中未处理消息可能丢失

### 强制要求

当前项目不内置磁盘队列，必须在文档中明确声明：

> 当前版本只能保证进程存活期间的至少一次转发；若进程异常退出，尚未完成转发的内存消息可能丢失。

---

## 7.8 转发确认与重试要求

### 目标

消息从“收到”到“完成转发”之间必须有明确状态。

### 必须定义的状态

每条内部转发消息至少具备以下状态：

- `Received`
- `Queued`
- `Routing`
- `Forwarding`
- `Succeeded`
- `Failed`
- `DeadLetter`

### 重试要求

- 转发失败后必须支持自动重试
- 重试次数可配置
- 重试间隔可配置
- 超过最大重试次数后进入死信队列或死信目录
- 进入死信后必须记录错误日志

### 推荐默认值

- 最大重试次数：3
- 初始重试间隔：1 秒
- 指数退避：开启
- 最大退避上限：30 秒

### 说明

这里的“转发成功”是指：

- 服务已经成功调用 Broker 内部发送逻辑  
或
- 服务已完成内部定义的投递确认步骤

具体确认粒度可根据 MQTTnet 实现方式落地，但不得只靠“调用了某个方法”就视为绝对成功。

---

## 7.9 死信处理要求

### 需求

转发失败且超过最大重试次数的消息，必须进入死信通道，而不是直接丢弃。

### 最低要求

至少实现以下一种：

- 死信文件目录
- 死信 SQLite 表
- 死信日志单独输出

### 每条死信至少记录

- 消息唯一标识
- 原始 Topic
- 来源 ClientId
- 目标 ClientId 或目标规则
- Payload 摘要
- 首次接收时间
- 最后失败时间
- 失败原因
- 已重试次数

---

## 7.10 幂等与去重要求

### 原则

由于第一版采用“至少一次”语义，因此业务上必须接受“重复消息可能发生”。

### 要求

- 每条转发消息应生成唯一消息标识 `MessageId`
- 日志中必须记录 `MessageId`
- 后续如需支持去重，应可基于 `MessageId` 扩展
- 若接入方业务要求严格避免重复，则应由接入方基于 `MessageId` 做幂等处理

---

## 7.11 心跳与会话健康检查

### 要求

服务需具备最基本的连接健康能力：

- 记录客户端最近活动时间
- 记录连接建立时间
- 对断开事件进行清理
- 对长时间无活动客户端保留可观测信息

### 推荐扩展

- 可配置客户端空闲超时时间
- 可选输出在线客户端清单
- 可选输出每个客户端最近收发时间

---

## 7.12 背压与流量保护

### 需求

当消息量突增时，系统必须具备自我保护机制，避免因无限堆积导致内存耗尽或线程池耗尽。

### 强制要求

- 内部队列必须支持容量上限
- 处理并发数必须可配置
- 单个客户端发送速率可监控
- 对明显异常流量要记录 Warning 日志

### 推荐能力

- 单客户端限流
- 单 Topic 限流
- 总队列长度监控
- 队列积压报警阈值

### 明确禁止

- 无上限内存队列
- 无限制创建后台任务
- 每条消息单独 `Task.Run` 后不受控

---

## 7.13 超时控制要求

### 强制要求

所有关键处理路径必须有超时控制，包括但不限于：

- 入队等待
- 路由处理
- 转发执行
- 死信写入
- 停机排空等待

### 推荐默认值

- 入队超时：2 秒
- 单次转发超时：5 秒
- 死信写入超时：3 秒

### 要求

超时必须被明确识别、记录和计数，不得被当作普通异常模糊处理。

---

## 7.14 启停可靠性要求

### 启动要求

服务启动时必须按以下顺序进行：

1. 读取配置
2. 初始化日志
3. 初始化 Broker
4. 初始化内部队列与后台消费者
5. 开始监听端口
6. 输出启动成功日志

### 停止要求

服务停止时必须：

1. 停止接收新消息
2. 停止接受新客户端连接
3. 尽量完成当前正在处理的消息
4. 在可配置超时时间内优雅退出
5. 输出关闭完成日志

---

## 7.15 可观测性要求

### 必须具备的指标或日志字段

至少应能从日志中观察到：

- 当前在线客户端数
- 总接收消息数
- 总转发成功数
- 总转发失败数
- 当前队列长度
- 死信数量
- 重试次数
- 最近一次严重错误时间

### 日志字段要求

关键日志至少包含：

- `Timestamp`
- `MessageId`
- `ClientId`
- `Topic`
- `QoS`
- `Action`
- `Result`
- `Error`

### 目标

做到：

- 消息丢了能追
- 重复了能查
- 堵塞了能看见
- 服务断了能定位原因

---

## 8. 项目结构要求

当前实现采用如下目录结构：

```text
src/
  MqttRelayService/
    Program.cs
    Logging/
      SerilogLogging.cs
    Workers/
      BrokerWorker.cs
      QueueMetricsWorker.cs
    Options/
      ServiceOptions.cs
      MqttOptions.cs
      AuthOptions.cs
      RoutingOptions.cs
      ReliabilityOptions.cs
    Models/
      ClientSessionInfo.cs
      RouteContext.cs
      ForwardMessage.cs
      ForwardResult.cs
      DeadLetterRecord.cs
      MessageProcessStatus.cs
      ConnectionStatus.cs
      AuthRequest.cs
      AuthResult.cs
    Services/
      Abstractions/
        IMqttBrokerHost.cs
        IClientRegistry.cs
        IMessageRouter.cs
        IAuthService.cs
        IMessageQueue.cs
        IMessageDeliveryService.cs
        IDeadLetterService.cs
        IRetryPolicyProvider.cs
      Implementations/
        MqttBrokerHost.cs
        ClientRegistry.cs
        MessageRouter.cs
        AuthService.cs
        InMemoryMessageQueue.cs
        MessageDeliveryService.cs
        DeadLetterService.cs
        RetryPolicyProvider.cs
    Utilities/
      MessagePayloadFormatter.cs
    Scripts/
      install-service.cmd
      install-service.ps1
      uninstall-service.cmd
      uninstall-service.ps1
    appsettings.json
    appsettings.Development.json
    MqttRelayService.csproj
tests/
  MqttRelayService.Tests/
```

说明：

- 当前版本只实现 `InMemoryMessageQueue`，不提供 `DiskBackedMessageQueue`
- Windows Service 日常安装/卸载入口是 `.cmd`，实际服务操作由 `.ps1` 承担
- 脚本随构建和发布复制到输出目录

---

## 9. 关键实现要求

## 9.1 Program.cs

### 要求
- 使用 `Host.CreateApplicationBuilder`
- 注册 Windows Service 支持
- 注册 Options
- 注册核心服务
- 注册 HostedService
- 接入 Serilog 控制台和文件日志
- 设置 `BackgroundServiceExceptionBehavior.Ignore`，避免后台服务异常直接停止 Host
- 配置 `HostOptions.ShutdownTimeout`

---

## 9.2 BrokerWorker

### 职责
- 服务启动时调用 BrokerHost 启动 Broker
- 服务停止时调用 BrokerHost 关闭 Broker
- 不在 Worker 中直接处理业务路由
- 定期检查 Broker 状态，发现异常停止后触发自动重启
- 自动重启 Broker 时不得关闭内部队列写入端，避免影响消息处理链路

---

## 9.3 MqttBrokerHost

### 职责
- 创建并启动 MQTT Server
- 监听客户端连接事件
- 监听客户端断开事件
- 监听订阅事件
- 监听消息发布事件
- 将事件转发给 Auth、Registry、Router、Queue 模块处理

### 注意
- 不将复杂逻辑直接写在事件回调里
- 事件回调中只做最小必要操作
- 收到消息后优先入队，不直接做复杂转发

---

## 9.4 AuthService

### 职责
- 根据配置校验用户名密码
- 校验 ClientId 是否符合约束
- 返回认证结果

---

## 9.5 ClientRegistry

### 职责
- 管理在线客户端
- 管理客户端订阅信息
- 提供按 ClientId 查询能力
- 提供获取在线客户端列表能力

---

## 9.6 MessageRouter

### 职责
- 根据 Topic 规则决定消息去向
- 过滤发送方自身
- 完成目标客户端转发
- 输出转发日志

---

## 9.7 MessageQueue

### 职责
- 承接接入层与处理层之间的解耦
- 管理内部待处理消息
- 提供有界容量
- 提供入队超时控制

---

## 9.8 MessageDeliveryService

### 职责
- 消费内部队列消息
- 执行转发
- 管理消息状态
- 执行重试
- 超过重试次数后写入死信

---

## 9.9 DeadLetterService

### 职责
- 接收无法成功转发的消息
- 写入死信记录
- 保留必要上下文信息
- 输出单独错误日志

## 9.10 QueueMetricsWorker

### 职责
- 周期性读取内部队列当前长度和进程内峰值
- 仅在队列长度或峰值变化时写出指标快照
- 指标快照输出到运行目录下的 `data/metrics/queue-metrics.json`
- 指标写入失败只记录 Warning，不得影响 Broker 或投递链路

---

## 10. 错误处理要求

### 要求
- 所有外部入口必须捕获异常
- 所有异常必须写日志
- 不允许吞异常
- 不允许因为单个客户端异常导致服务退出
- 配置异常、端口占用等启动失败场景必须明确抛出并记录

---

## 11. 部署要求

### 发布方式
- 发布为 `win-x64`
- 当前项目固定 `RuntimeIdentifier=win-x64`，`PlatformTarget=x64`
- Release 构建保留嵌入式调试符号，便于排查生产日志和异常栈
- 是否采用单文件 exe 发布由后续发布命令决定，当前项目文件不强制单文件

### 交付物
- 可运行 exe
- `appsettings.json`
- `Scripts/install-service.cmd`
- `Scripts/install-service.ps1`
- `Scripts/uninstall-service.cmd`
- `Scripts/uninstall-service.ps1`
- README

### 脚本要求
- `.cmd` 作为日常入口，负责管理员权限检查、UTF-8 控制台初始化和控制台日志
- `.ps1` 承载实际 Windows Service 安装、启动、停止和卸载操作
- 脚本不得承载业务逻辑
- 脚本不会自动提权，用户需要以管理员身份运行 `.cmd`

---

## 12. README 要求

仓库需补充并维护 `README.md`，至少说明：

1. 项目用途
2. 运行环境
3. 本地调试方式
4. 发布方式
5. 安装为 Windows Service 的示例命令
6. 配置说明
7. Topic 规范说明
8. 可靠性边界说明
9. 当前版本不支持或暂未实现的能力

---

## 13. 开发约束

### 必须遵守
- 使用 .NET 8
- 使用 `async/await`
- 使用依赖注入
- 使用强类型配置
- 使用接口解耦核心模块
- 所有公共类和关键方法添加必要注释

### 不允许
- 将全部逻辑写到一个文件
- 将认证、路由、会话管理耦合在一起
- 在消息回调里直接执行业务长耗时任务
- 直接使用静态全局状态管理所有功能

---

## 14. 可接受的简化

第一版允许简化为：

- 仅支持内网明文 TCP
- 仅支持用户名密码认证
- 仅支持内存级在线客户端注册
- 仅支持基础 Topic 转发
- 仅支持单实例运行
- 可先只实现内存可靠队列，但必须明确可靠性边界

---

## 15. 交付清单

当前仓库应保持以下交付物完整可用；其中 `README.md` 当前仍需补齐：

1. 完整项目代码
2. 可编译运行
3. `appsettings.json`
4. `README.md`（待补齐）
5. `Scripts/install-service.cmd`
6. `Scripts/install-service.ps1`
7. `Scripts/uninstall-service.cmd`
8. `Scripts/uninstall-service.ps1`
9. 单元测试与真实 MQTT Broker 联调测试
10. 需求文档、代码地图和历史任务记录保持一致

---

## 16. 验收标准

满足以下条件视为通过验收：

1. 项目可成功编译运行
2. 服务可作为 Windows Service 启动
3. MQTT 客户端能成功连接
4. 认证成功与失败逻辑正常
5. 客户端可订阅 Topic
6. 客户端发布消息后，其他匹配订阅者可收到转发消息
7. 发送者默认不会收到自己发送的消息
8. 客户端断开后，注册表可正确清理
9. 日志完整可读
10. 服务停止时能优雅关闭

---

## 17. 可靠性验收补充

### 场景 1：单条消息处理异常
- 构造一条必然失败的消息
- 预期：该消息失败并记录日志
- 预期：服务仍继续运行
- 预期：其他消息仍可正常转发

### 场景 2：单个客户端异常断开
- 强制断开一个客户端
- 预期：该客户端从注册表移除
- 预期：其他客户端不受影响
- 预期：服务继续运行

### 场景 3：转发失败自动重试
- 构造临时失败场景
- 预期：消息进入重试
- 预期：重试次数符合配置
- 预期：成功后状态变为 `Succeeded`

### 场景 4：超过最大重试进入死信
- 构造永久失败场景
- 预期：消息最终进入死信
- 预期：死信中包含完整上下文信息

### 场景 5：队列堆积保护
- 人工压入大量消息
- 预期：系统不崩溃
- 预期：出现背压日志或拒绝策略日志
- 预期：内存不无限增长

### 场景 6：服务优雅停止
- 服务停止时队列中仍有未处理消息
- 预期：服务在超时时间内尽量排空
- 预期：超时后明确记录剩余未处理数量

### 场景 7：未处理异常防护
- 人为制造后台消费者异常
- 预期：异常被捕获记录
- 预期：宿主不退出
- 预期：消费者可继续工作

---

## 18. 给后续实现方的维护指令

后续迭代必须基于当前 `MqttRelayService` 现有结构继续演进，要求：

- 保持项目类型为 `.NET 8 Worker Service`
- 保持 Windows Service 运行能力
- 保持 MQTTnet Server/Broker 作为接入入口
- 保持客户端认证、在线客户端注册、Topic 订阅管理、消息转发、日志记录、配置绑定这些核心能力
- 所有后台入口必须捕获异常，不允许未处理异常导致 Host 停止
- 所有 MQTT 事件处理只做轻量操作，消息必须先进入内部队列
- 默认保持“至少一次”转发语义
- 每条消息必须分配唯一 `MessageId`
- 转发失败必须支持重试
- 超过最大重试次数必须进入死信
- 队列必须有容量上限，不允许无界内存增长
- 服务停止时必须支持优雅排空
- 必须通过日志能追踪单条消息的生命周期
- 若继续不实现磁盘队列，README 和需求文档必须明确说明可靠性边界
- 新增核心能力时应同时更新测试、代码地图和必要的历史任务记录
- 不要把接口/实现拆分退回到单目录或单文件式实现

---

## 19. 可靠性边界声明

当前项目不内置磁盘队列，README 中必须明确写明：

> 当前版本通过内存队列与重试机制提供进程存活期间的“至少一次”转发能力。  
> 若服务进程异常退出或机器宕机，尚未完成转发的内存消息可能丢失。  
> 如需进一步提高可靠性，应接入外部持久化消息基础设施或另行设计持久化方案。

---

## 20. 给实现方的补充说明

### 20.1 第一版设计取向
第一版优先级排序：

1. 服务不轻易退出
2. 消息不静默丢失
3. 转发失败可重试
4. 故障可观测
5. 结构清晰，便于扩展

### 20.2 不追求的目标
第一版不强求：

- 集群高可用
- 绝对意义上的端到端恰好一次
- 超大规模连接数
- 完整运营后台

### 20.3 推荐实现策略
建议优先实现：

- Worker Service 托管
- MQTTnet Server 接入
- 有界 `Channel`
- 转发消费者
- 指数退避重试
- 死信文件目录
- 优雅停机排空
- 结构化日志

这样可以较快得到一个能上线做内部使用的稳定版本。
