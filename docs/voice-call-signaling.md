# 语音通话信令模块开发文档

## 1. 模块概述

本模块基于 WebSocket 实现语音通话的信令交换，配合浏览器原生 WebRTC API 完成 P2P 语音直连。后端角色为**纯信令服务器**，不参与任何音视频媒体流的传输。

核心职责：
- 转发 SDP Offer/Answer 和 ICE Candidate（WebRTC 协商信令）
- 驱动通话状态机（CallStateMachine），维护通话生命周期
- 管理 busy key（通话占线标识）
- 超时检测（邀请无响应、连接超时等）
- 更新 call_record 表记录通话详情

---

## 2. 整体架构

```
┌─────────────┐          WebSocket           ┌─────────────────────┐          WebSocket           ┌─────────────┐
│  主叫前端    │ ◄──────────────────────────► │    后端信令服务器     │ ◄──────────────────────────► │  被叫前端    │
│             │                              │                     │                              │             │
│ RTCPeer     │   ← 转发 SDP/ICE →          │ - 信令路由           │   ← 转发 SDP/ICE →          │ RTCPeer     │
│ Connection  │                              │ - 状态机驱动         │                              │ Connection  │
│             │                              │ - busy key 管理     │                              │             │
│  getUserMedia                              │ - 超时检测           │                              │ getUserMedia│
│  (麦克风)   │                              │ - call_record 更新  │                              │  (麦克风)   │
└─────────────┘                              └─────────────────────┘                              └─────────────┘
       │                                                                                                 │
       └─────────────────────── P2P 音频直连（不经过服务器）──────────────────────────────────────────────┘
```

---

## 3. 信令消息格式

### 3.1 前端 → 后端（上行信令）

```json
{
  "type": "call_signal",
  "callId": 1234567890,
  "event": "OFFER",
  "targetUserId": 1001,
  "payload": { ... }
}
```

| 字段         | 类型   | 必填 | 说明                                                         |
|-------------|--------|------|--------------------------------------------------------------|
| type        | String | 是   | 固定 "call_signal"，与普通聊天消息区分                          |
| callId      | Long   | 是   | 通话唯一标识（initiateCall 接口返回的 callId）                  |
| event       | String | 是   | 信令事件枚举：RING/ACCEPT/OFFER/ANSWER/ICE/CONNECT/HANG_UP/CANCEL/REJECT |
| targetUserId| Long   | 是   | 信令目标用户 ID（转发给谁）                                    |
| payload     | Object | 否   | 事件携带数据（SDP、ICE candidate 等）                          |

### 3.2 后端 → 前端（下行信令）

```json
{
  "type": "call_signal",
  "callId": 1234567890,
  "event": "OFFER",
  "fromUserId": 1002,
  "payload": { ... }
}
```

| 字段       | 类型   | 说明                            |
|-----------|--------|-------------------------------|
| type      | String | 固定 "call_signal"             |
| callId    | Long   | 通话 ID                        |
| event     | String | 信令事件                        |
| fromUserId| Long   | 信令发起者 ID                   |
| payload   | Object | 透传数据                        |

---

## 4. 通话完整时序

### 4.1 正常通话流程

```
时间轴 →

主叫                          后端                              被叫
 │                             │                                │
 │ POST /call/initiate ──→     │                                │
 │ ←── {callId, status:sending}│                                │
 │                             │ (MQ消费后WS推送来电消息)          │
 │                             │ ──── [msgType=7 来电] ──→       │
 │                             │                                │ 弹出来电 UI
 │                             │ ←── {event:RING} ──────         │ 用户已看到
 │ ←── 转发 {event:RING} ──   │                                │
 │ 显示"对方响铃中..."          │                                │
 │                             │                                │ 用户点击接听
 │                             │ ←── {event:ACCEPT} ────         │
 │ ←── 转发 {event:ACCEPT} ── │                                │
 │                             │ 状态机: RINGING→CONNECTING       │
 │                             │                                │
 │ 创建 RTCPeerConnection      │                                │
 │ getUserMedia(audio)         │                                │
 │ createOffer()               │                                │
 │ setLocalDescription(offer)  │                                │
 │                             │                                │
 │ ── {event:OFFER, sdp} ──→  │ ── 转发 ──→                    │
 │                             │                                │ 创建 RTCPeerConnection
 │                             │                                │ setRemoteDescription(offer)
 │                             │                                │ getUserMedia(audio)
 │                             │                                │ createAnswer()
 │                             │                                │ setLocalDescription(answer)
 │                             │ ←── {event:ANSWER, sdp} ──     │
 │ ←── 转发 {event:ANSWER} ── │                                │
 │ setRemoteDescription(answer)│                                │
 │                             │                                │
 │ ── {event:ICE, candidate}─→ │ ── 转发 ──→                    │ addIceCandidate()
 │ addIceCandidate() ←──────── │ ←── {event:ICE, candidate} ── │
 │   (多轮 ICE 交换)           │                                │
 │                             │                                │
 │ ═══════════ ICE 连通性检查通过，P2P 直连建立 ═══════════════   │
 │                             │                                │
 │ ── {event:CONNECT} ──→      │                                │
 │                             │ 状态机: CONNECTING→CONNECTED    │
 │                             │ SET busy key (双方)             │
 │                             │ UPDATE call_record.start_time   │
 │                             │                                │
 │ ══════════════ 通话中，语音数据 P2P 直传 ═══════════════════   │
 │                             │                                │
 │ ── {event:HANG_UP} ──→      │ ── 转发 {event:HANG_UP} ──→    │
 │                             │ 状态机: CONNECTED→ENDED         │
 │                             │ DEL busy key (双方)             │
 │                             │ UPDATE call_record:              │
 │                             │   end_time, duration, end_reason│
 │                             │                                │ 关闭 RTCPeerConnection
 │ 关闭 RTCPeerConnection      │                                │
```

### 4.2 被叫拒接

```
主叫                          后端                              被叫
 │                             │ ←── {event:REJECT} ──────       │
 │ ←── 转发 {event:REJECT} ── │                                │
 │                             │ 状态机: RINGING→ENDED           │
 │                             │ end_reason=REJECTED             │
```

### 4.3 主叫取消

```
主叫                          后端                              被叫
 │ ── {event:CANCEL} ──→       │ ── 转发 {event:CANCEL} ──→     │
 │                             │ 状态机: →ENDED                  │ 关闭来电 UI
 │                             │ end_reason=CANCELLED            │
```

### 4.4 超时无响应

```
主叫                          后端                              被叫
 │                             │                                │
 │                             │ (30s 定时器到期)                 │
 │                             │ 状态机: INVITING/RINGING→ENDED  │
 │ ←── {event:TIMEOUT} ────── │ ── {event:TIMEOUT} ──→          │
 │                             │ end_reason=TIMEOUT              │
```

---

## 5. 后端信令处理器设计

### 5.1 核心类：CallSignalHandler

位置：`module/call/signal/CallSignalHandler.java`

职责：接收 WebSocket 信令 → 路由处理 → 转发给对端

```java
@Component
public class CallSignalHandler {

    /**
     * 处理前端发来的通话信令
     * 由 WebSocketFrameHandler 在识别到 type=call_signal 时调用
     */
    public void handleSignal(Long fromUserId, CallSignalDTO signal) {
        switch (signal.getEvent()) {
            case RING    -> handleRing(fromUserId, signal);
            case ACCEPT  -> handleAccept(fromUserId, signal);
            case OFFER   -> forwardSignal(fromUserId, signal);  // 纯转发
            case ANSWER  -> forwardSignal(fromUserId, signal);  // 纯转发
            case ICE     -> forwardSignal(fromUserId, signal);  // 纯转发
            case CONNECT -> handleConnect(fromUserId, signal);
            case HANG_UP -> handleHangUp(fromUserId, signal);
            case CANCEL  -> handleCancel(fromUserId, signal);
            case REJECT  -> handleReject(fromUserId, signal);
        }
    }
}
```

### 5.2 核心类：CallStateHandler（工具类）

位置：`module/call/signal/CallStateHandler.java`

封装固定逻辑：状态机流转 + DB 更新 + busy key 管理

```java
@Component
public class CallStateHandler {

    /**
     * 执行状态流转
     * @return 流转后的新状态，流转失败返回 null
     */
    public CallStatus transition(Long callId, CallEvent event) {
        // 1. 查 call_record 当前状态（Redis 缓存 or DB）
        // 2. 调 CallStateMachine.transition(currentStatus, event)
        // 3. 更新 call_record.status
        // 4. 按事件类型做附加操作（设 busy key、记 startTime 等）
        // 5. 返回新状态
    }
}
```

### 5.3 信令 DTO

位置：`module/call/domain/dto/CallSignalDTO.java`

```java
@Data
public class CallSignalDTO {
    private Long callId;
    private CallEvent event;
    private Long targetUserId;
    private Object payload;  // SDP 或 ICE candidate，透传不解析
}
```

---

## 6. 状态机与事件对照表

| 当前状态      | 事件      | 下一状态      | 附加操作                                        |
|--------------|----------|--------------|------------------------------------------------|
| INVITING     | RING     | RINGING      | 重置超时计时器（30s → 30s）                       |
| INVITING     | CANCEL   | ENDED        | end_reason=CANCELLED                           |
| INVITING     | TIMEOUT  | ENDED        | end_reason=TIMEOUT                             |
| RINGING      | ACCEPT   | CONNECTING   | 无                                             |
| RINGING      | REJECT   | ENDED        | end_reason=REJECTED                            |
| RINGING      | CANCEL   | ENDED        | end_reason=CANCELLED                           |
| RINGING      | TIMEOUT  | ENDED        | end_reason=TIMEOUT                             |
| CONNECTING   | CONNECT  | CONNECTED    | SET busy key, UPDATE start_time                |
| CONNECTING   | HANG_UP  | ENDED        | end_reason=HANG_UP                             |
| CONNECTING   | TIMEOUT  | ENDED        | end_reason=TIMEOUT (ICE协商超时)                 |
| CONNECTING   | ERROR    | ENDED        | end_reason=ERROR                               |
| CONNECTED    | HANG_UP  | ENDED        | DEL busy key, UPDATE end_time/duration          |
| CONNECTED    | ERROR    | ENDED        | DEL busy key, end_reason=ERROR                 |

---

## 7. 超时检测机制

### 7.1 超时场景

| 阶段         | 超时时间 | 触发条件                        | 处理方式                          |
|-------------|---------|-------------------------------|----------------------------------|
| INVITING    | 30s     | 发起通话后 30s 被叫无任何响应    | 触发 TIMEOUT 事件，双方推送超时通知  |
| RINGING     | 30s     | 被叫响铃后 30s 未接听            | 触发 TIMEOUT 事件                 |
| CONNECTING  | 15s     | 被叫接听后 15s ICE 协商未完成    | 触发 TIMEOUT 事件                 |

### 7.2 实现方案

使用 Redis 延迟队列（复用已有的 DelayQueueService 思路）或 `ScheduledExecutorService` 延迟任务：

- 进入 INVITING 状态时：注册一个 30s 延迟任务，key = `call:timeout:{callId}`
- 状态流转时：取消旧任务，根据新状态注册新的延迟任务
- 进入 ENDED/CONNECTED 时：取消所有延迟任务

推荐使用 `HashedWheelTimer`（Netty 自带）或 `ScheduledExecutorService`：
- 通话超时是秒级精度，且每通通话一个定时器，适合用时间轮
- 不需要持久化（通话是临时状态，服务重启后通话自然断开）

---

## 8. Busy Key 管理

### 8.1 Key 设计

```
key   = call:busy:{userId}
value = callId
TTL   = 通话最大时长 + buffer（如 3600s + 60s = 3660s）
```

### 8.2 生命周期

| 时机                     | 操作                                     |
|-------------------------|------------------------------------------|
| CONNECT 事件（P2P 建立） | SET call:busy:{callerId} 和 call:busy:{calleeId}，TTL=3660s |
| ENDED 事件（通话结束）    | DEL call:busy:{callerId} 和 call:busy:{calleeId}            |
| 异常断开（WebSocket 断连）| 由 WebSocket 断连监听器触发 HANG_UP 事件，进而删除 busy key     |

### 8.3 TTL 兜底

设置 TTL 是为了防止极端情况下 busy key 永远不被删除（比如服务器崩溃、网络分区等），导致用户永久"忙线"。正常流程中 ENDED 事件会主动删除，TTL 只是保险。

---

## 9. WebSocket 断连处理

用户 WebSocket 断开时（网络中断、App 后台杀死等），后端需要自动处理正在进行的通话：

1. `WebSocketSessionManager` 监听到连接断开
2. 查 Redis `call:busy:{userId}` 是否存在
3. 存在则说明该用户正在通话中，自动触发 HANG_UP 事件
4. 走正常的状态机流转 → ENDED → 删 busy key → 通知对端

---

## 10. ICE Server 配置下发

### 10.1 时机

被叫点击接听（ACCEPT）后，后端在转发 ACCEPT 信令时，额外携带 ICE Server 配置给双方：

```json
{
  "type": "call_signal",
  "callId": 123,
  "event": "ACCEPT",
  "fromUserId": 1001,
  "iceServers": [
    { "urls": "stun:stun.l.google.com:19302" },
    { "urls": "stun:stun1.l.google.com:19302" },
    {
      "urls": "turn:turn.example.com:3478",
      "username": "user",
      "credential": "pass"
    }
  ]
}
```

### 10.2 为什么在 ACCEPT 时下发

- 不在 INVITE 时下发：避免被叫未接听就暴露 TURN 凭证
- ACCEPT 时双方都准备好了，立即开始创建 RTCPeerConnection，需要 ICE Server 配置

---

## 11. 通话状态 Redis 缓存

为了避免每次信令处理都查 DB（call_record 表），将活跃通话的状态缓存在 Redis：

```
key   = call:state:{callId}
value = JSON { "status": "RINGING", "callerId": 1001, "calleeId": 1002, "sessionId": 100 }
TTL   = 3660s（与 busy key 一致）
```

| 时机           | 操作                                |
|---------------|-------------------------------------|
| MQ 消费端写入 call_record 后 | SET call:state:{callId}  |
| 每次状态流转后   | 更新 value 中的 status 字段         |
| ENDED         | DEL call:state:{callId}             |

好处：信令处理全程走 Redis，不打 DB。只在 ENDED 时异步更新 call_record 表（end_time、duration 等）。

---

## 12. 需要新建的文件清单

| 文件路径                                            | 说明                          |
|---------------------------------------------------|-------------------------------|
| module/call/signal/CallSignalHandler.java          | 信令路由主入口                  |
| module/call/signal/CallStateHandler.java           | 状态流转 + DB更新 + busy key   |
| module/call/signal/CallTimeoutManager.java         | 超时检测管理器                  |
| module/call/domain/dto/CallSignalDTO.java          | 上行信令 DTO                   |
| module/call/domain/dto/CallSignalPushDTO.java      | 下行信令推送 DTO               |

需要改动的文件：

| 文件路径                                            | 改动内容                                    |
|---------------------------------------------------|-------------------------------------------|
| module/netty/handler/WebSocketFrameHandler.java    | 识别 type=call_signal，路由到 CallSignalHandler |
| module/websocket/handler/ChatWebSocketHandler.java | 同上（看你用的哪套 WebSocket）                |
| common/constant/RedisKeyConst.java                 | 新增 call:state:、call:timeout: key         |
| module/call/mapper/CallRecordMapper.java           | 新增 updateStatus、updateEndInfo 等方法      |

---

## 13. 配置项

需要在 `application.yml` 中新增以下配置：

```yaml
# 语音通话配置
chat:
  call:
    # STUN/TURN 服务器配置（下发给前端用于 WebRTC ICE 协商）
    ice-servers:
      - urls: stun:stun.l.google.com:19302
      - urls: stun:stun1.l.google.com:19302
      - urls: turn:turn.example.com:3478
        username: ${TURN_USERNAME:chatroom}
        credential: ${TURN_CREDENTIAL:chatroom123}
    # 超时配置（秒）
    timeout:
      invite: 30        # 邀请超时：发起后 30s 无响应
      ring: 30          # 响铃超时：响铃后 30s 未接听
      connecting: 15    # 连接超时：接听后 15s ICE 协商未完成
    # 通话最大时长（秒），超过自动挂断
    max-duration: 3600
    # busy key TTL（秒）= max-duration + 60s buffer
    busy-ttl: 3660
```

---

## 14. TURN 服务器部署（coturn）

生产环境需要部署 TURN 服务器，作为 P2P 打洞失败时的中继兜底：

```bash
# Docker 一键部署
docker run -d --name coturn \
  --network=host \
  coturn/coturn:latest \
  -n \
  --realm=chatroom.example.com \
  --fingerprint \
  --lt-cred-mech \
  --user=chatroom:chatroom123 \
  --external-ip=$(curl -s ifconfig.me) \
  --min-port=49152 \
  --max-port=65535 \
  --log-file=stdout
```

注意事项：
- TURN 服务器需要公网 IP
- 需要开放 3478 端口（TCP/UDP）和 49152-65535 端口范围（UDP，媒体中继）
- 生产环境建议使用临时凭证（TURN REST API），避免固定密码泄露
- 国内环境建议自建，Google STUN 在部分网络下不可达

---

## 15. 前端关键代码参考（不需要后端实现，仅供理解）

```javascript
// 被叫接听后，双方执行：
const pc = new RTCPeerConnection({
  iceServers: response.iceServers  // 后端下发的 ICE 配置
});

// 获取本地音频流
const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
stream.getTracks().forEach(track => pc.addTrack(track, stream));

// ICE candidate 产生时发给后端转发
pc.onicecandidate = (event) => {
  if (event.candidate) {
    ws.send(JSON.stringify({
      type: 'call_signal',
      callId: callId,
      event: 'ICE',
      targetUserId: remoteUserId,
      payload: { candidate: event.candidate }
    }));
  }
};

// 收到远端音频流
pc.ontrack = (event) => {
  audioElement.srcObject = event.streams[0];
};
```

---

## 16. 安全考虑

- **信令鉴权**：所有 WebSocket 信令必须校验 fromUserId 是否为当前连接的认证用户，防止伪造
- **通话参与者校验**：处理信令时校验 fromUserId 是否为该 callId 的 caller 或 callee
- **TURN 凭证**：生产环境使用临时凭证（有效期 5 分钟），通过后端 API 动态签发
- **信令防重放**：每条信令可携带时间戳，超过 30s 的信令丢弃
- **限流**：单用户信令频率限制（如 50条/秒），防止恶意刷信令
