# ChatRoom 语音通话功能技术选型与开发方案

## 1. 目标与结论

本文档用于给当前 `ChatRoomBackend` 项目增加“语音通话”能力时的技术选型与落地思路说明。

结合当前项目已有栈：

- 后端：`Spring Boot 3`
- 实时连接：`WebSocket`、`Netty`
- 鉴权：`JWT`
- 中间件：`Redis`、`RabbitMQ`
- 存储：`MySQL`

推荐方案如下：

1. 音频传输层使用 `WebRTC`
2. 通话信令层复用现有 `Netty WebSocket`
3. NAT 穿透采用 `STUN + TURN`
4. 第一阶段只做 `1v1 语音通话`
5. 群语音或多人会议阶段，再引入 `SFU`

一句话结论：

`WebRTC 负责传音频，现有后端负责 HTTP 发起、消息落库、缓存同步、信令转发和状态管理，TURN 负责弱网/复杂 NAT 下的中继兜底，MVP 不建议自研音频传输协议。`

---

## 2. 为什么这样选型

### 2.1 为什么不用“后端自己转发语音数据”

不推荐让当前 Java 后端直接承接音频流并做媒体转发，原因有这些：

- 语音实时性要求高，延迟、抖动、丢包处理都比普通聊天消息复杂
- 音频流量远大于文本消息，后端压力会迅速放大
- 自己做音频编解码、回声消除、抖动缓冲、带宽自适应，研发成本很高
- 浏览器和移动端对 `WebRTC` 的支持已经很成熟，直接复用行业标准更稳

### 2.2 为什么优先选 WebRTC

`WebRTC` 适合这个场景，原因是：

- 浏览器原生支持实时音频采集与传输
- 内置回声消除、降噪、自动增益控制
- 支持 `Opus` 音频编码，低码率下通话效果较好
- 支持 `ICE / STUN / TURN`，便于跨网络环境建立连接
- 1v1 场景下多数情况下可走 P2P，后端只负责信令，服务成本较低

### 2.3 为什么先做 1v1，再做群语音

1v1 和多人语音的架构复杂度差距很大：

- `1v1`：通常可以直接使用 WebRTC P2P
- `多人语音`：P2P 网状连接会导致每个客户端维护多条上行/下行链路，性能很差
- `多人语音`：更适合引入 `SFU`，例如 `mediasoup`、`LiveKit`、`Janus`、`Jitsi`

所以建议路线：

- 阶段 1：先把 1v1 语音打通
- 阶段 2：补齐通话状态、未接来电、忙线、超时、通话记录
- 阶段 3：如需群语音/多人会议，再引入 `SFU`

---

## 3. 技术栈选型建议

## 3.1 前端选型

如果你的前端是 Web 或桌面 Web 技术栈，推荐直接使用浏览器原生 WebRTC API：

- `navigator.mediaDevices.getUserMedia()`
- `RTCPeerConnection`
- `RTCSessionDescription`
- `RTCIceCandidate`

如果你后续有移动端：

- Android：优先使用官方 `WebRTC Native SDK`
- iOS：优先使用官方 `WebRTC Native SDK`
- Flutter：可考虑 `flutter_webrtc`

MVP 阶段不建议额外引入很重的前端音视频框架，先用原生 WebRTC API 即可。

## 3.2 后端选型

后端继续沿用当前栈即可：

- 业务服务：`Spring Boot`
- 长连接信令：优先复用当前 `Netty WebSocket`
- 分布式状态：`Redis`
- 异步通知：`RabbitMQ` 可选
- 数据持久化：`MySQL`

建议把语音通话功能定位为一个独立模块，例如：

- `module/call/controller`
- `module/call/service`
- `module/call/domain`
- `module/call/mapper`

## 3.3 音频通话核心组件选型

推荐组合：

- 音频引擎：`WebRTC`
- 编码：`Opus`
- 信令协议：`WebSocket + JSON`
- 穿透服务：`STUN + TURN`

推荐原则：

- `STUN` 用于帮助客户端收集公网映射地址等 `ICE candidate`
- `TURN` 用于 P2P 打不通时提供中继地址
- 双端通过后端交换的是 `SDP offer/answer` 与 `ICE candidates`，不是只交换一个公网 IP
- 生产环境一定要准备 `TURN`，不能只配 `STUN`

### 3.4 TURN / STUN 服务推荐

MVP 和中小规模项目推荐：

- `coturn`

原因：

- 成熟稳定
- 部署简单
- 社区资料多
- 与 WebRTC 兼容好

典型配置思路：

- 一台独立云主机部署 `coturn`
- 开放 `3478`（UDP/TCP）和中继端口范围
- 开启长期凭证或动态临时凭证

### 3.5 群语音阶段的 SFU 选型

如果后续要支持多人语音房间，推荐优先考虑：

1. `LiveKit`
2. `mediasoup`

建议：

- 如果你想更快落地，优先 `LiveKit`
- 如果你要更强的自定义控制能力，考虑 `mediasoup`

当前阶段不建议：

- 自研 SFU
- 用 Java 自己处理媒体流

---

## 4. 适配当前项目的总体架构

你的项目已经有两套实时连接思路：

- Spring WebSocket：`module/websocket`
- Netty WebSocket：`module/netty`

从现状看，更推荐把“语音通话信令”统一接入当前 Netty 通道，原因是：

- 现有项目已经有独立端口的 Netty WebSocket 服务
- Netty 更适合承接高频实时信令
- 后续如果要扩展在线状态、忙线状态、通话控制，Netty 更灵活

建议架构如下：

```text
+--------------------+          WebRTC 音频流          +--------------------+
| Caller Client      | <----------------------------> | Callee Client      |
|                    |                                |                    |
| WebRTC             |                                | WebRTC             |
| WebSocket Signaling| -----> ChatRoom Backend <----- | WebSocket Signaling|
+--------------------+          (Netty)               +--------------------+
                                      |
                                      |
                               +-------------+
                               | Redis       |
                               | 通话状态    |
                               | 在线路由    |
                               +-------------+
                                      |
                                      |
                               +-------------+
                               | MySQL       |
                               | 通话记录    |
                               +-------------+
                                      |
                                      |
                               +-------------+
                               | coturn      |
                               | TURN/STUN   |
                               +-------------+
```

职责划分：

- 客户端和客户端之间：传输真实音频流
- 后端：负责 HTTP 发起、消息落库、缓存同步、呼叫状态管理、`SDP/ICE candidate` 信令转发
- Redis：存在线状态、通话状态、分布式路由
- MySQL：存通话记录、呼叫结果、时长等
- coturn：提供 NAT 穿透和必要时的媒体中继

---

## 5. 功能范围建议

## 5.0 推荐业务主流程

结合当前项目现有的消息链路，推荐把语音通话拆成“两段式流程”：

### 第一段：HTTP 发起业务动作

先通过一个 HTTP 接口创建这次呼叫，再开始实时建链。

建议接口：

- `POST /api/call/initiate`

这个接口的职责：

1. 校验会话是否存在、双方是否是会话成员
2. 校验被叫是否在线
3. 校验双方是否忙线
4. 创建一条聊天消息，消息类型例如 `VOICE_CALL`
5. 同步更新消息缓存、会话缓存、最近消息
6. 创建一条 `call_record` 记录，保存本次通话的业务信息
7. 提交事务后，通过 WebSocket/Netty 向被叫推送 `CALL_INVITE`

为什么这样设计：

- 与当前“发送消息”主链路保持一致
- 即使后续没有接通，也能在聊天记录里保留“未接来电/已拒绝/已取消”
- 通话详情与聊天消息解耦，便于后续扩展时长、结果、失败原因

### 第二段：WebSocket 承接实时信令

HTTP 只负责“发起业务动作”，不负责整个实时通话过程。实际通话协商继续走 WebSocket：

- `CALL_INVITE`
- `CALL_RINGING`
- `CALL_ACCEPT`
- `CALL_REJECT`
- `CALL_CANCEL`
- `CALL_HANGUP`
- `WEBRTC_OFFER`
- `WEBRTC_ANSWER`
- `WEBRTC_ICE_CANDIDATE`

这样可以保证：

- 实时性足够
- 状态同步更自然
- 后端更容易做状态机和幂等控制

## 5.1 第一阶段 MVP

第一阶段建议只做这些功能：

- 单聊会话发起 1v1 语音呼叫
- 被叫收到来电提醒
- 被叫可接听或拒绝
- 双方建立 WebRTC 音频连接
- 支持挂断
- 支持忙线
- 支持无人接听超时
- 记录通话结果

通话结果建议至少包括：

- `MISSED` 未接
- `REJECTED` 已拒绝
- `CANCELED` 主叫取消
- `COMPLETED` 正常结束
- `BUSY` 忙线
- `FAILED` 建链失败

## 5.2 第二阶段增强

- 来电推送联动
- 通话时长统计
- 静音
- 免提
- 网络状态提示
- 通话质量监控
- 断线重连与重协商

## 5.3 第三阶段扩展

- 多人语音房
- 语音转文字
- 通话录音
- 后台质检或审核

---

## 6. 信令设计

`WebRTC` 本身不规定业务信令协议，所以需要你自己的后端来定义消息结构。建议复用现有 WebSocket 通道，统一用 JSON。

### 6.1 建议的信令消息类型

```json
{
  "type": "CALL_INVITE",
  "callId": "191000000001",
  "fromUserId": 10001,
  "toUserId": 10002,
  "sessionId": 88801,
  "timestamp": 1710000000000,
  "payload": {}
}
```

建议至少定义这些类型：

- `CALL_INVITE` 发起呼叫
- `CALL_RINGING` 被叫振铃中
- `CALL_ACCEPT` 被叫接听
- `CALL_REJECT` 被叫拒绝
- `CALL_CANCEL` 主叫取消
- `CALL_HANGUP` 任一方挂断
- `CALL_BUSY` 被叫忙线
- `CALL_TIMEOUT` 超时无人接听
- `WEBRTC_OFFER` WebRTC offer
- `WEBRTC_ANSWER` WebRTC answer
- `WEBRTC_ICE_CANDIDATE` ICE candidate 交换
- `CALL_STATE_SYNC` 状态同步

### 6.2 推荐时序

#### 场景一：正常接通

```text
1. A -> Backend(HTTP): POST /api/call/initiate
2. Backend: 校验会话/成员/在线/忙线
3. Backend: 写 message 表中的语音通话消息
4. Backend: 同步消息缓存、会话缓存、最近消息
5. Backend: 写 call_record 表
6. Backend -> B(WebSocket): CALL_INVITE
7. B -> Backend: CALL_RINGING
8. B -> Backend: CALL_ACCEPT
9. Backend -> A: CALL_ACCEPT
10. A -> Backend: WEBRTC_OFFER
11. Backend -> B: WEBRTC_OFFER
12. B -> Backend: WEBRTC_ANSWER
13. Backend -> A: WEBRTC_ANSWER
14. A/B -> Backend: WEBRTC_ICE_CANDIDATE
15. Backend 双向转发 candidate
16. A/B 建立音频连接
17. 任一方挂断 -> CALL_HANGUP
18. Backend 更新 call_record 状态、结束原因、通话时长
```

#### 场景二：被叫拒绝

```text
1. A -> Backend: CALL_INVITE
2. Backend -> B: CALL_INVITE
3. B -> Backend: CALL_REJECT
4. Backend -> A: CALL_REJECT
5. Backend 持久化本次通话结果
```

#### 场景三：无人接听

```text
1. A 发起呼叫
2. Backend 创建呼叫状态，设置 30s 超时
3. B 未接听
4. Backend 到时触发 CALL_TIMEOUT
5. Backend 通知 A，并落库
```

---

## 7. 后端模块设计建议

## 7.1 模块划分

建议新增 `call` 模块：

```text
src/main/java/com/example/chatroom/module/call
├── controller
├── service
├── service/impl
├── domain
│   ├── dto
│   ├── entity
│   └── vo
├── mapper
├── handler
└── enums
```

建议职责：

- `controller`：提供发起通话、查询通话记录、获取 TURN 凭证等 HTTP 接口
- `service`：处理发起、接听、拒绝、挂断等业务
- `handler`：处理 WebSocket 上行信令
- `mapper`：通话记录与状态落库

## 7.2 核心实体建议

建议增加一张通话记录表，例如 `call_record`，用于承接“聊天消息之外”的通话业务信息。

推荐做法：

- `chat_message` 里保留一条“语音通话消息”，用于会话列表和历史消息展示
- `call_record` 单独保存本次通话的详细业务状态
- 通过 `message_id` 把消息表和通话记录表关联起来

字段建议：

- `id`
- `call_id`
- `session_id`
- `message_id`
- `call_type`，如 `VOICE`
- `caller_id`
- `callee_id`
- `status`
- `invite_status`
- `start_time`
- `answer_time`
- `end_time`
- `duration_seconds`
- `end_reason`
- `created_at`
- `updated_at`

如果后续做多人通话，再拆：

- `call_room`
- `call_room_member`
- `call_signal_log`（可选）

## 7.3 Redis Key 建议

可以增加以下缓存结构：

- `call:active:user:{userId}` 当前用户是否在通话中
- `call:detail:{callId}` 当前呼叫详情
- `call:timeout:{callId}` 呼叫超时控制
- `call:route:{callId}` 当前呼叫的节点路由信息

用途：

- 判定忙线
- 支持多实例部署
- 支持超时控制
- 支持信令跨节点转发

---

## 8. 与现有项目的集成思路

## 8.1 复用现有鉴权能力

当前项目已有：

- `JwtUtil`
- WebSocket/Netty 鉴权处理

所以语音信令连接不需要额外设计独立登录体系，可直接复用当前 `token` 方案。

## 8.2 复用现有在线状态能力

当前 WebSocket 已有在线状态和 Redis 同步逻辑，这非常适合扩展来电场景：

- 发起呼叫前先判断被叫是否在线
- 在线则发实时来电信令
- 不在线则直接返回“对方不在线”或走离线通知策略

## 8.3 复用现有 Netty WebSocket

建议不要再新开一套“语音专用 WebSocket 服务”，而是在现有 Netty 信道里扩展消息类型，例如：

- 原有聊天消息继续走现有逻辑
- 新增 `CALL_*` 与 `WEBRTC_*` 类型信令

这样好处是：

- 减少连接数量
- 统一鉴权与在线管理
- 更方便做状态机控制

## 8.4 复用现有消息主链路

从当前 `MessageServiceImpl` 的实现看，项目已经具备“先写缓存，再写事务表，再异步落地”的消息链路思路。语音通话建议最大程度复用这一套能力。

推荐流程：

1. `POST /api/call/initiate` 进入后端
2. 在事务中创建一条 `VOICE_CALL` 消息
3. 同步更新消息缓存、会话最新消息、必要的未读状态
4. 再创建一条 `call_record`
5. 事务提交后，再发 `CALL_INVITE` 实时信令

这样能保证：

- 聊天消息和通话业务状态一一对应
- 即使 WebRTC 建链失败，消息历史仍然完整
- 前端会话页、消息页、通话详情页的数据来源清晰

## 8.5 RabbitMQ 的使用建议

语音通话实时性强，核心信令不要依赖 MQ 做主链路转发。推荐：

- 实时信令：WebSocket 直发
- 非核心异步任务：RabbitMQ

RabbitMQ 适合的事情：

- 记录通话事件日志
- 发送未接来电通知
- 触发消息提醒或系统通知

不建议：

- `CALL_ACCEPT`
- `WEBRTC_OFFER`
- `WEBRTC_ANSWER`
- `WEBRTC_ICE_CANDIDATE`

这些都走 MQ 主链路。

---

## 9. 后端状态机建议

建议给通话定义明确状态，避免并发下出现脏状态。

状态示例：

- `INIT`
- `INVITING`
- `RINGING`
- `ACCEPTED`
- `CONNECTING`
- `IN_CALL`
- `REJECTED`
- `CANCELED`
- `TIMEOUT`
- `ENDED`
- `FAILED`

关键约束：

- 一个用户同一时刻只能有一个活动通话
- 只有主叫能取消未接通的呼叫
- 只有通话参与方能发送该 `callId` 的信令
- `ENDED`、`REJECTED`、`TIMEOUT` 等终态不可重复进入

并发控制建议：

- 使用 `Redis + 分布式锁` 或 `CAS` 风格状态更新
- 对关键动作进行幂等设计

例如：

- 重复点击接听，只允许第一次成功
- 挂断消息重复到达时，只处理一次

---

## 10. WebRTC 关键实现思路

## 10.1 前端基本流程

前端在接听或发起时的核心动作：

1. 调用 `getUserMedia({ audio: true, video: false })`
2. 创建 `RTCPeerConnection`
3. 设置 `iceServers`
4. 把本地音频轨加入连接
5. 监听 `onicecandidate`
6. 监听 `ontrack`
7. 通过 WebSocket 与服务端交换 `offer / answer / candidate`

`iceServers` 示例：

```javascript
const pc = new RTCPeerConnection({
  iceServers: [
    { urls: "stun:stun.l.google.com:19302" },
    {
      urls: "turn:your-turn.example.com:3478",
      username: "demo",
      credential: "demo-password"
    }
  ]
});
```

生产环境建议：

- `TURN` 地址不要写死在前端
- 由后端提供临时凭证接口，动态下发

## 10.2 后端对 WebRTC 的角色

后端只做信令交换，不处理音频内容。

后端主要职责：

- 转发 `offer`
- 转发 `answer`
- 转发 `candidate`
- 校验消息发送者是否合法
- 维护 `callId` 和参与者关系
- 维护通话状态

### 10.3 WebRTC、ICE、STUN、TURN 的正确分工

这一节专门解释语音通话背后的计网原理，因为这部分会直接影响后端架构设计。

先说一个核心结论：

- 语音通话想要尽量走 `P2P`
- 但真实网络里，用户大多处在路由器、家庭宽带、公司网络、移动网络之后
- 这些网络通常都会经过 `NAT`
- `NAT` 会让“内网主机之间直接互连”这件事变复杂

所以，WebRTC 的核心问题并不是“怎么传音频”这么简单，而是：

- 双方到底能不能找到彼此
- 双方到底能不能穿透 NAT 成功建链
- 如果穿透失败，是否有一个中继节点可以兜底

#### 10.3.1 为什么 P2P 建链会困难

如果两个客户端都直接暴露在公网，有独立公网 IP，那么双方直接互发数据就可以建立连接。

但现实里更常见的是：

- 客户端 A 在家庭路由器后面
- 客户端 B 在公司或校园网后面
- 他们拿到的通常是内网地址，例如 `192.168.x.x`、`10.x.x.x`

这些内网地址在公网不可直接路由，所以必须经过出口设备做地址转换，这就是 `NAT`。

`NAT` 的影响是：

- 客户端自己只知道本机内网 IP 和端口
- 对外表现出来的是被路由器映射后的公网 IP 和公网端口
- 这个映射关系不一定稳定，也不一定允许任意公网主机主动回包

这就是为什么“两个都在内网里的用户想直接通话”没有看起来那么简单。

#### 10.3.2 STUN 到底在做什么

`STUN` 的作用不是简单地“告诉你一个公网 IP”。

更准确地说，客户端会向 `STUN Server` 发请求，服务端从收到的报文里观察：

- 这个请求来自哪个公网 IP
- 这个请求来自哪个公网端口

然后把它看到的结果返回给客户端。

于是客户端就能知道：

- 我在内网里绑定的本地端口
- 经过 NAT 后，对外暴露成了哪个公网 IP 和端口

这一步非常重要，因为它让客户端收集到一个可用于外部连接尝试的地址，也就是 WebRTC 中常说的 `srflx candidate`。

但是，`STUN` 只能“帮助发现映射”，不能保证对方一定能打进来。

#### 10.3.3 为什么只知道公网映射还不够

即使双方都通过 `STUN` 知道了自己的公网映射地址，也不代表连接一定能建立。

原因在于很多 NAT 设备都会限制“谁可以给我回包”。

常见现象是：

- 只有我先向某个目标地址发过包，对方从这个目标地址回来的包我才能收到
- 或者还要更严格，必须是相同 IP 且相同端口返回的数据才会被接受

所以问题不是“有没有公网 IP”这么简单，而是：

- NAT 表里是否已经存在一条映射
- NAT 是否允许目标端回包
- 允许回包时检查的是 IP，还是 IP + 端口

这就是打洞技术存在的根本原因。

#### 10.3.4 打洞的本质是什么

你刚才说的那段理解，放到更标准的表述里，可以概括为：

- 双方先通过中心服务器交换各自可尝试连接的地址
- 双方都向对方可能可达的地址发探测包
- 这样做的目的，是让各自 NAT 设备里都出现“允许此方向通信”的映射记录
- 一旦双方的映射条件刚好匹配，后续的媒体流就有机会直接通过

这就是 UDP 打洞的本质。

在 WebRTC 里，这套动作通常不是由业务代码自己手工定时发包完成，而是由 `ICE connectivity check` 自动完成。

所以从工程视角看：

- 你的业务后端负责交换协商信息
- WebRTC/ICE 负责自动做候选收集和连通性探测
- 你不需要自己手搓底层探测协议

#### 10.3.5 为什么有些 NAT 能打洞，有些不行

用比较容易理解的话说，NAT 可以粗略分成几类限制程度不同的行为。

传统上常会提到这几种：

- `Full Cone NAT`
- `Restricted Cone NAT`
- `Port Restricted Cone NAT`
- `Symmetric NAT`

可以这样理解：

- 限制越松，P2P 越容易成功
- 限制越严，P2P 越难成功

其中：

- `Restricted Cone NAT` 可以理解为更看重“对端 IP 是否匹配”
- `Port Restricted Cone NAT` 可以理解为不仅看 IP，还要看端口
- `Symmetric NAT` 最麻烦，因为它常常会针对不同目标地址分配不同的外部映射

这和你刚才说的业务理解基本一致：

- 如果 NAT 只要求“目标 IP 匹配”或者虽然要求 `IP + 端口` 匹配，但外部映射本身相对稳定，那么双方通过同时发包，通常仍有机会打洞成功
- 真正致命的情况不是“回包条件严格”本身，而是“访问不同目标时，外部映射结果也跟着变了”
- 一旦双方手里交换到的地址，和真正访问对端时使用的地址不一致，打洞就会很难成功

尤其是 `Symmetric NAT`，往往就是“必须 TURN 中继兜底”的主要原因。

#### 10.3.6 TURN 为什么是刚需

要理解 `TURN` 为什么必要，关键要先理解前面三类 NAT 和 `Symmetric NAT` 的本质差别。

前面三类 NAT 虽然限制程度不同，但通常还有一个共同点：

- 客户端访问 `STUN` 时得到的公网映射地址，后续在访问真实对端时，大概率仍然是“同一个外部映射”或者至少是“可被预测和复用的映射”
- 这意味着双方先向 `STUN` 查询，再通过业务服务器交换这些地址后，后续通过同时发探测包，仍有机会让两边 NAT 都建立起允许回包的映射记录
- 即使是 `Port Restricted Cone NAT`，虽然它会校验对端的 `IP + 端口`，但只要双方都朝彼此正确的目标地址发包，仍然有机会形成一对能互通的路径

所以前三类 NAT 的难点，更多是“限制回包条件”，而不是“把你对外的身份彻底改掉”。

但是 `Symmetric NAT` 不一样，它的问题更本质。

##### `Symmetric NAT` 为什么经常打洞失败

两个客户端第一次都会先访问 `STUN` 服务器。

此时发生的事情是：

1. 客户端 A 向 `STUN` 发包
2. A 所在 NAT 为“访问 STUN 这个目标”创建一条外部映射，例如 `1.2.3.4:62001`
3. 客户端 B 也向 `STUN` 发包
4. B 所在 NAT 同样为“访问 STUN 这个目标”创建一条外部映射，例如 `5.6.7.8:53001`
5. 双方把各自从 `STUN` 拿到的公网 `ip:port` 通过信令服务器交换给彼此

如果是前三类 NAT，到这里通常还有较大机会继续打洞。

但如果 A 或 B 在 `Symmetric NAT` 后面，问题就来了：

- 当它下一次访问的目标不再是 `STUN`，而是“对方的公网 `ip:port`”时
- `Symmetric NAT` 往往会认为“这是一个新的目标地址”
- 然后重新分配一个新的外部映射端口

也就是说：

- A 访问 `STUN` 时，对外可能是 `1.2.3.4:62001`
- 但 A 真正访问 B 时，对外可能变成 `1.2.3.4:62035`

这时最关键的问题就出现了：

- B 通过信令拿到的是 A 访问 `STUN` 时暴露出来的地址，也就是 `1.2.3.4:62001`
- 但 A 真正发给 B 的流量，对外却变成了 `1.2.3.4:62035`
- B 回给 `62001` 的包，未必能落到 A 当前和 B 通信所用的那条映射上

所以不是“原先的 STUN 映射立刻失效了”，而是：

- 那条映射通常还在
- 但它只对原来的目标，也就是 `STUN`，有意义
- 当真实通信目标换成对端时，NAT 又给了你一套新的外部映射
- 于是双方手里交换的地址，和真实通信时使用的地址对不上

这就是 `Symmetric NAT` 最难处理的地方。

##### 为什么这会直接推导出 TURN 的必要性

一旦双方通过 `STUN` 拿到并交换的地址信息，不再能代表后续真实对端通信时使用的地址，那么：

- 双方即使知道彼此“某个公网 `ip:port`”
- 这个地址也未必是对当前对端真正有效的地址
- 于是 `ICE` 做再多探测，也可能始终找不到一对可直连的 candidate pair

此时如果没有 `TURN`，结果通常就是：

- 直连失败
- 通话建立失败

而 `TURN` 的意义就在于提供一个“双方都能稳定访问的中间地址”。

与其让 A 和 B 互相猜测“你现在对外到底映射成了哪个端口”，不如：

- A 先连到 `TURN`
- B 也先连到 `TURN`
- 双方都把音频流发给 `TURN`
- 再由 `TURN` 进行中继转发

这时链路不再依赖双方 NAT 是否愿意彼此放行，也不再依赖 `STUN` 探测出来的映射是否还能复用。

当直连失败时，必须有一个双方都能访问的中间节点代替双方转发媒体流，这就是 `TURN Server`。

此时链路从：

- `A <-> B`

退化为：

- `A <-> TURN <-> B`

代价是：

- 延迟更高
- 带宽成本更高
- 服务端流量压力更大

但好处是：

- 成功率大幅提升
- 能覆盖复杂 NAT、企业网络、防火墙限制等场景

所以生产环境里，`TURN` 不是“锦上添花”，而是“可用性保障”。

如果只配置 `STUN` 不配置 `TURN`，常见结果就是：

- 开发环境能通
- 一部分用户线上也能通
- 但总会有一批真实用户在复杂 NAT、企业网络或防火墙环境下完全打不通

所以可以把这件事记成一句话：

- `STUN` 负责“告诉我我可能长什么样”
- `ICE` 负责“帮我试出哪条路能走”
- `TURN` 负责“如果前面都不行，至少还有一条一定能走的路”

#### 10.3.7 ICE 在整个过程中扮演什么角色

`ICE` 可以理解为 WebRTC 的“连通性决策框架”。

它把前面的几个组件串起来：

1. 收集候选地址
2. 通过信令把候选地址交换给对端
3. 对多个候选地址组合做连通性测试
4. 选出最优的一条路径

候选地址通常包括：

- 本机内网地址 `host candidate`
- 通过 `STUN` 获取的公网映射地址 `srflx candidate`
- 通过 `TURN` 分配的中继地址 `relay candidate`

所以更准确的工程描述应该是：

- 后端不是简单交换“公网 IP”
- 后端交换的是 `SDP offer/answer` 和多个 `ICE candidates`
- WebRTC 会自动尝试多种候选路径
- 能直连就直连
- 不能直连就走中继

#### 10.3.8 这套理论落到你这个项目意味着什么

这部分理论会直接反映到你的后端设计上。

后端应该做的事情：

- 提供 `HTTP` 接口发起通话业务
- 先落消息表、更新缓存、落 `call_record`
- 通过 `WebSocket` 转发 `CALL_INVITE`
- 在后续实时链路中转发 `offer / answer / candidate`
- 校验当前用户是否属于该 `callId`
- 维护忙线、超时、接听、挂断等状态

后端不应该做的事情：

- 自己实现音频流转发
- 自己根据公网 IP 手动设计打洞协议
- 把 WebRTC 底层探测逻辑搬到业务层重写

所以你的最终架构可以概括成一句话：

- 业务后端负责“建单、记账、转信令、管状态”
- WebRTC 负责“建链和传音频”
- `STUN/TURN/ICE` 负责“让这条链尽可能建立成功”

### 10.4 是否需要后端保存 SDP/ICE

建议：

- `MVP` 阶段不需要长期持久化 SDP/ICE
- 可以短暂缓存在 Redis 中，便于断线恢复或排障
- 通话结束后只保留业务结果，不保留敏感媒体协商详情

---

## 11. 数据库设计建议

一个简化版 `call_record` 表结构示例：

```sql
CREATE TABLE call_record (
    id BIGINT PRIMARY KEY,
    call_id BIGINT NOT NULL,
    session_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    call_type VARCHAR(32) NOT NULL,
    caller_id BIGINT NOT NULL,
    callee_id BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL,
    invite_status VARCHAR(32) NULL,
    start_time DATETIME NULL,
    answer_time DATETIME NULL,
    end_time DATETIME NULL,
    duration_seconds INT NOT NULL DEFAULT 0,
    end_reason VARCHAR(64) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_call_id (call_id),
    KEY idx_caller_id (caller_id),
    KEY idx_callee_id (callee_id),
    KEY idx_session_id (session_id)
);
```

如果你后续要做“聊天列表里展示通话消息”，可以额外生成一条消息记录，例如：

- “你发起了一次语音通话”
- “对方已拒绝”
- “通话时长 02:36”

这样可以无缝融入现有聊天会话历史。

---

## 12. 接口与消息设计建议

## 12.1 HTTP 接口

建议增加：

### 发起语音通话

`POST /api/call/initiate`

入参建议：

```json
{
  "sessionId": 88801,
  "callType": "VOICE",
  "clientMsgNo": "call-init-001"
}
```

接口行为建议：

- 校验当前用户是否属于该会话
- 校验当前会话是否为 1v1 会话
- 校验对方是否在线、是否忙线
- 创建聊天消息
- 同步消息缓存
- 创建通话记录
- 返回 `callId`、`messageId`、初始状态

返回示例：

```json
{
  "callId": 191000000001,
  "messageId": 191000000010,
  "status": "INVITING"
}
```

### 获取 TURN 配置

`GET /api/call/turn-config`

返回：

```json
{
  "urls": ["turn:turn.example.com:3478"],
  "username": "temp-user",
  "credential": "temp-password",
  "ttl": 600
}
```

### 查询通话记录

`GET /api/call/records?sessionId=xxx`

## 12.2 WebSocket 上行消息

建议统一结构：

```json
{
  "type": "CALL_INVITE",
  "requestId": "req-001",
  "callId": "191000000001",
  "sessionId": 88801,
  "payload": {}
}
```

`requestId` 用于：

- 幂等
- 排障
- 前后端日志串联

## 12.3 服务端下行消息

例如：

```json
{
  "type": "CALL_ACCEPT",
  "callId": "191000000001",
  "fromUserId": 10002,
  "timestamp": 1710000000000,
  "payload": {}
}
```

---

## 13. 开发步骤建议

## 13.1 第一阶段：打通最小链路

目标：`1v1 语音呼叫可用`

开发步骤：

1. 新增 `call` 模块基础实体、表结构、枚举
2. 增加 `POST /api/call/initiate`，串起消息表、缓存、通话记录表
3. 定义 `VOICE_CALL` 消息类型和消息扩展字段
4. 在 Netty WebSocket 中新增 `CALL_*` 和 `WEBRTC_*` 消息类型
5. 实现呼叫状态机与 Redis 忙线控制
6. 新增 TURN 配置接口
7. 前端接入 WebRTC，完成 offer/answer/candidate 交换
8. 完成接听、拒绝、取消、挂断、超时
9. 通话结束落库并回写通话结果

验收标准：

- 双方在线时可正常语音通话
- 忙线可正确拦截
- 超时和拒绝状态准确
- 弱网场景下 TURN 可兜底

## 13.2 第二阶段：提升可用性

开发重点：

- 幂等控制
- 多端登录冲突处理
- 通话中的断线重连
- 失败重试与日志追踪
- 通话消息写入会话历史

## 13.3 第三阶段：面向多人通话升级

如果业务确认要做群语音，再新增：

1. `SFU` 服务
2. 房间模型
3. 入会、退会、静音、主持控制
4. 音量指示与活跃说话人

---

## 14. 生产环境部署建议

## 14.1 基础部署

至少准备：

- ChatRoom 后端服务实例
- Redis
- MySQL
- coturn

如果只有 1v1 语音，这套就可以支撑初期上线。

## 14.2 网络与安全

注意事项：

- `TURN` 服务器必须有公网可达地址
- 打开 UDP，很多情况下音频质量更依赖 UDP
- 为 WebSocket、HTTP、TURN 做明确域名和证书规划
- 通话接口和信令必须做鉴权校验
- 校验 `callId`、`sessionId` 与参与者身份绑定关系

## 14.3 监控建议

建议监控：

- 呼叫发起成功率
- 接通率
- 平均接通时延
- ICE 失败率
- TURN 使用率
- 平均通话时长
- 忙线比例

这些指标能帮助你判断：

- 是产品问题
- 还是网络问题
- 还是 TURN 资源不足

---

## 15. 风险点与规避建议

### 15.1 最大风险：把媒体流和信令混在一起设计

要明确：

- 信令是信令
- 音频流是音频流

后端尽量不要碰实际媒体数据。

### 15.2 最大技术坑：只配 STUN，不配 TURN

现实网络环境里，很多用户会因为 NAT、企业网络、防火墙导致 P2P 失败。

如果没有 `TURN`：

- 能开发成功
- 但上线后会有一批用户打不通

### 15.3 状态不一致问题

比如：

- A 已取消，但 B 又点了接听
- B 已接听，但 A 网络断开
- 双端重复发挂断

解决思路：

- 明确状态机
- Redis 锁或原子状态更新
- 所有关键动作做幂等

---

## 16. 最终推荐方案

针对你当前项目，最推荐的落地组合是：

- 前端音频：`WebRTC`
- 信令链路：复用当前 `Netty WebSocket`
- 鉴权：复用当前 `JWT`
- 状态协调：`Redis`
- 记录持久化：`MySQL`
- 穿透与中继：`coturn`
- 异步通知：`RabbitMQ` 只做辅助链路

推荐实施顺序：

1. 先做 `1v1 语音 + 通话状态机 + TURN`
2. 再补通话记录、消息沉淀、异常恢复
3. 最后再评估是否要上 `SFU` 支持多人语音

---

## 17. 对你这个项目的具体建议

如果让我直接给出适合当前仓库的工程建议，我会这样落地：

1. 在现有 Netty WebSocket 的消息处理逻辑里新增 `call` 信令分发
2. 新建 `module/call` 模块，统一管理呼叫状态
3. Redis 记录用户是否处于通话中，避免并发呼叫
4. MySQL 记录每次通话结果与时长
5. 增加 `TURN` 凭证接口，由后端动态下发
6. 前端基于 WebRTC 做 1v1 音频链路

这个方案的优点是：

- 改造量可控
- 最大程度复用现有后端
- 先把业务跑通，再按规模升级

---

## 18. 后续可继续输出的内容

如果你需要，我下一步可以继续帮你补这三类文档或代码设计：

1. `语音通话数据库表设计.sql`
2. `WebSocket 信令协议详细字段文档.md`
3. `后端 call 模块的代码骨架设计`

如果你希望，我也可以直接继续把这个方案拆成：

- 实体类
- DTO / VO
- 枚举
- Redis Key 常量
- WebSocket 消息协议
- 开发任务清单

这样你就可以直接开始编码。
