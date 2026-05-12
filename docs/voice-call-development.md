# 语音通话功能开发文档

> 版本：v1.0  
> 适用项目：ChatRoom Backend  
> 编写日期：2026-05-12  
> 状态：待开发

---

## 1. 功能概述

本文档定义 ChatRoom 项目「1v1 语音通话」功能的完整开发规范，涵盖技术选型、系统架构、信令协议、数据模型、接口契约、状态机设计、Redis 缓存策略及分阶段开发计划。

功能定位：在现有单聊会话基础上，支持用户发起实时语音通话，通话记录以消息形式沉淀到聊天历史中。

---

## 2. 技术选型

| 层面 | 选型 | 说明 |
|------|------|------|
| 音频传输 | WebRTC | 浏览器原生支持，内置回声消除、降噪、自动增益 |
| 音频编码 | Opus | 低码率高质量，WebRTC 默认编解码器 |
| 信令传输 | Netty WebSocket（复用现有） | 高频实时信令，统一连接管理 |
| 业务发起 | Spring Boot HTTP | 通话创建、记录查询、TURN 凭证下发 |
| 状态协调 | Redis | 忙线检测、呼叫路由、超时控制 |
| 数据持久化 | MySQL | 通话记录、通话结果 |
| NAT 穿透 | coturn（STUN + TURN） | 地址发现 + 中继兜底 |
| 异步通知 | RabbitMQ（辅助链路） | 未接来电通知、事件日志 |
| 鉴权 | JWT（复用现有） | 统一身份校验 |

---

## 3. 系统架构

### 3.1 架构总览

```text
+--------------------+        WebRTC P2P 音频流        +--------------------+
|   Caller Client    | <------------------------------> |   Callee Client    |
|                    |                                  |                    |
| WebRTC Engine      |                                  | WebRTC Engine      |
| WS Signaling       | ------> ChatRoom Backend <------ | WS Signaling       |
+--------------------+     (Spring Boot + Netty)        +--------------------+
                                    |
                    +---------------+---------------+
                    |               |               |
              +-----------+   +-----------+   +-----------+
              |   Redis   |   |   MySQL   |   |  coturn   |
              | 通话状态   |   | 通话记录   |   | STUN/TURN |
              | 忙线检测   |   | 消息记录   |   | NAT穿透   |
              | 超时控制   |   |           |   | 媒体中继   |
              +-----------+   +-----------+   +-----------+
```

### 3.2 职责划分

**ChatRoom Backend（信令服务器）**：HTTP 发起通话业务、消息落库、缓存同步、通话状态管理、SDP/ICE candidate 信令转发。不处理任何音频数据。

**WebRTC（客户端）**：音频采集、编解码、P2P 连接建立、ICE 候选收集与连通性探测。

**coturn（TURN/STUN 服务）**：为客户端提供公网映射地址发现（STUN），在 P2P 不可达时提供媒体中继（TURN）。

**Redis**：分布式通话状态存储，支持多实例部署下的一致性保障。

**MySQL**：通话记录持久化，与聊天消息通过 message_id 关联。

### 3.3 两段式流程设计

通话建立分为两段：

第一段（HTTP）：通过 `POST /api/call/initiate` 完成业务校验、消息创建、缓存同步、通话记录初始化。该段保证即使后续 WebRTC 建链失败，聊天历史中仍有完整记录。

第二段（WebSocket）：HTTP 事务提交后，通过 Netty WebSocket 推送实时信令，承接呼叫协商、WebRTC offer/answer/candidate 交换、状态流转等全部实时交互。

---

## 4. 模块结构

在现有项目中新增 `call` 模块：

```text
src/main/java/com/example/chatroom/module/call
├── controller/
│   └── CallController.java              // HTTP 接口：发起通话、查询记录、TURN 配置
├── service/
│   ├── CallService.java                 // 业务接口
│   └── impl/
│       └── CallServiceImpl.java         // 业务实现：发起、接听、拒绝、挂断、超时
├── handler/
│   └── CallSignalHandler.java           // WebSocket 信令处理器
├── statemachine/
│   └── CallStateMachine.java            // 通话状态机
├── domain/
│   ├── entity/
│   │   └── CallRecord.java             // 通话记录实体
│   ├── dto/
│   │   └── CallInitiateRequest.java    // 发起通话请求
│   ├── vo/
│   │   └── CallRecordVO.java           // 通话记录视图
│   └── enums/
│       ├── CallStatus.java             // 通话状态枚举
│       ├── CallType.java               // 通话类型枚举
│       └── CallEndReason.java          // 结束原因枚举
├── mapper/
│   └── CallRecordMapper.java           // MyBatis Mapper
└── constants/
    └── CallRedisKey.java               // Redis Key 常量
```

---

## 5. 数据库设计

### 5.1 call_record 表

```sql
CREATE TABLE `call_record` (
    `id`               BIGINT         NOT NULL COMMENT '主键ID',
    `call_id`          BIGINT         NOT NULL COMMENT '通话唯一标识',
    `session_id`       BIGINT         NOT NULL COMMENT '所属会话ID',
    `message_id`       BIGINT         NOT NULL COMMENT '关联聊天消息ID',
    `call_type`        VARCHAR(32)    NOT NULL COMMENT '通话类型: VOICE',
    `caller_id`        BIGINT         NOT NULL COMMENT '主叫用户ID',
    `callee_id`        BIGINT         NOT NULL COMMENT '被叫用户ID',
    `status`           VARCHAR(32)    NOT NULL COMMENT '通话状态: INIT/INVITING/RINGING/ACCEPTED/CONNECTING/IN_CALL/ENDED',
    `end_reason`       VARCHAR(64)    NULL     COMMENT '结束原因: COMPLETED/MISSED/REJECTED/CANCELED/BUSY/FAILED/TIMEOUT',
    `start_time`       DATETIME       NULL     COMMENT '呼叫发起时间',
    `answer_time`      DATETIME       NULL     COMMENT '接听时间',
    `end_time`         DATETIME       NULL     COMMENT '通话结束时间',
    `duration_seconds` INT            NOT NULL DEFAULT 0 COMMENT '通话时长(秒)',
    `created_at`       DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at`       DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_call_id` (`call_id`),
    KEY `idx_session_id` (`session_id`),
    KEY `idx_caller_id` (`caller_id`),
    KEY `idx_callee_id` (`callee_id`),
    KEY `idx_message_id` (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语音通话记录表';
```

### 5.2 消息类型扩展

在现有 `chat_message` 表的消息类型中新增 `VOICE_CALL` 类型。消息扩展字段（extra/content JSON）存储通话摘要信息：

```json
{
    "callId": 191000000001,
    "callType": "VOICE",
    "duration": 156,
    "endReason": "COMPLETED"
}
```

### 5.3 关联关系

`call_record.message_id` → `chat_message.id`，一条语音通话消息对应一条通话详情记录。前端通过消息列表展示通话卡片，点击可查看通话详情。

---

## 6. 状态机设计

### 6.1 状态定义

| 状态 | 含义 | 进入条件 |
|------|------|----------|
| `INIT` | 通话初始化 | HTTP 接口创建通话记录 |
| `INVITING` | 发起邀请中 | 向被叫推送 CALL_INVITE |
| `RINGING` | 被叫振铃中 | 收到被叫 CALL_RINGING |
| `ACCEPTED` | 被叫已接听 | 收到被叫 CALL_ACCEPT |
| `CONNECTING` | WebRTC 建链中 | 开始 offer/answer 交换 |
| `IN_CALL` | 通话进行中 | ICE 连接建立成功 |
| `ENDED` | 通话已结束 | 终态 |

### 6.2 状态转换图

```text
INIT → INVITING → RINGING → ACCEPTED → CONNECTING → IN_CALL → ENDED
           |          |          |            |                    ↑
           |          |          |            +----[建链失败]------+
           |          |          +-------[主叫取消]---------------+
           |          +----------[被叫拒绝]---------------------+
           +---------------[超时无人接听]----------------------+
           +---------------[被叫忙线]--------------------------+
```

### 6.3 结束原因（end_reason）

| 值 | 含义 | 触发场景 |
|----|------|----------|
| `COMPLETED` | 正常结束 | 任一方主动挂断 |
| `MISSED` | 未接来电 | 超时无人接听 |
| `REJECTED` | 已拒绝 | 被叫主动拒绝 |
| `CANCELED` | 已取消 | 主叫在被叫接听前取消 |
| `BUSY` | 忙线 | 被叫正在另一通话中 |
| `FAILED` | 建链失败 | WebRTC ICE 连接失败 |
| `TIMEOUT` | 超时 | 呼叫超时（30秒） |

### 6.4 并发控制

状态流转使用 Redis + Lua 脚本实现原子 CAS 更新，保证同一 callId 的状态变更串行化。所有关键动作（接听、拒绝、挂断）设计为幂等操作，重复请求不产生副作用。

---

## 7. 信令协议

### 7.1 信令消息类型

| 类型 | 方向 | 说明 |
|------|------|------|
| `CALL_INVITE` | Server → Callee | 来电邀请 |
| `CALL_RINGING` | Callee → Server | 被叫振铃确认 |
| `CALL_ACCEPT` | Callee → Server → Caller | 被叫接听 |
| `CALL_REJECT` | Callee → Server → Caller | 被叫拒绝 |
| `CALL_CANCEL` | Caller → Server → Callee | 主叫取消 |
| `CALL_HANGUP` | Any → Server → Other | 挂断通话 |
| `CALL_BUSY` | Server → Caller | 被叫忙线 |
| `CALL_TIMEOUT` | Server → Both | 超时未接 |
| `WEBRTC_OFFER` | Caller → Server → Callee | SDP Offer |
| `WEBRTC_ANSWER` | Callee → Server → Caller | SDP Answer |
| `WEBRTC_ICE_CANDIDATE` | Any → Server → Other | ICE Candidate |
| `CALL_STATE_SYNC` | Server → Client | 状态同步 |

### 7.2 消息格式

上行消息（客户端 → 服务端）：

```json
{
    "type": "CALL_ACCEPT",
    "requestId": "req-uuid-001",
    "callId": "191000000001",
    "sessionId": 88801,
    "payload": {}
}
```

下行消息（服务端 → 客户端）：

```json
{
    "type": "CALL_ACCEPT",
    "callId": "191000000001",
    "fromUserId": 10002,
    "toUserId": 10001,
    "sessionId": 88801,
    "timestamp": 1710000000000,
    "payload": {}
}
```

WebRTC 信令 payload 示例：

```json
// WEBRTC_OFFER / WEBRTC_ANSWER
{
    "type": "WEBRTC_OFFER",
    "requestId": "req-uuid-002",
    "callId": "191000000001",
    "sessionId": 88801,
    "payload": {
        "sdp": "v=0\r\no=- 46117317...",
        "type": "offer"
    }
}

// WEBRTC_ICE_CANDIDATE
{
    "type": "WEBRTC_ICE_CANDIDATE",
    "requestId": "req-uuid-003",
    "callId": "191000000001",
    "sessionId": 88801,
    "payload": {
        "candidate": "candidate:842163049 1 udp...",
        "sdpMLineIndex": 0,
        "sdpMid": "audio"
    }
}
```

### 7.3 requestId 用途

每条上行信令携带 `requestId`，用于幂等控制、排障追踪和前后端日志串联。服务端对同一 requestId 的重复请求返回相同结果而不重复执行。

---

## 8. HTTP 接口设计

### 8.1 发起语音通话

**请求**

```
POST /api/call/initiate
Content-Type: application/json
Authorization: Bearer {token}
```

```json
{
    "sessionId": 88801,
    "callType": "VOICE",
    "clientMsgNo": "call-init-uuid-001"
}
```

**处理逻辑**

1. 校验当前用户属于该会话
2. 校验会话类型为单聊（1v1）
3. 确定被叫用户，校验被叫在线状态
4. 校验双方均不在通话中（Redis 忙线检测）
5. 创建 `VOICE_CALL` 类型聊天消息，同步消息缓存与会话最近消息
6. 创建 `call_record` 记录，状态为 `INVITING`
7. 设置 Redis 忙线标记与超时控制
8. 通过 Netty WebSocket 向被叫推送 `CALL_INVITE`
9. 返回通话信息

**响应**

```json
{
    "code": 200,
    "data": {
        "callId": 191000000001,
        "messageId": 191000000010,
        "status": "INVITING",
        "callType": "VOICE",
        "calleeId": 10002,
        "createdAt": "2026-05-12T10:00:00"
    }
}
```

**错误码**

| code | 说明 |
|------|------|
| 4001 | 会话不存在或无权限 |
| 4002 | 非单聊会话 |
| 4003 | 对方不在线 |
| 4004 | 对方忙线中 |
| 4005 | 您当前有通话进行中 |

### 8.2 获取 TURN 配置

**请求**

```
GET /api/call/turn-config
Authorization: Bearer {token}
```

**响应**

```json
{
    "code": 200,
    "data": {
        "iceServers": [
            {
                "urls": ["stun:stun.example.com:3478"]
            },
            {
                "urls": ["turn:turn.example.com:3478"],
                "username": "temp-1715500800-10001",
                "credential": "hmac-sha1-generated-password",
                "credentialType": "password"
            }
        ],
        "ttl": 600
    }
}
```

**说明**：TURN 凭证由后端动态生成，使用 HMAC-SHA1 签名，有效期 600 秒。前端不硬编码 TURN 地址和凭证。

### 8.3 查询通话记录

**请求**

```
GET /api/call/records?sessionId=88801&page=1&size=20
Authorization: Bearer {token}
```

**响应**

```json
{
    "code": 200,
    "data": {
        "records": [
            {
                "callId": 191000000001,
                "callType": "VOICE",
                "callerId": 10001,
                "calleerId": 10002,
                "status": "ENDED",
                "endReason": "COMPLETED",
                "startTime": "2026-05-12T10:00:00",
                "answerTime": "2026-05-12T10:00:08",
                "endTime": "2026-05-12T10:02:44",
                "durationSeconds": 156
            }
        ],
        "total": 15,
        "page": 1,
        "size": 20
    }
}
```

---

## 9. Redis 缓存设计

### 9.1 Key 规范

| Key | 类型 | TTL | 说明 |
|-----|------|-----|------|
| `call:busy:{userId}` | String (callId) | 跟随通话生命周期 | 用户忙线标记，值为当前 callId |
| `call:detail:{callId}` | Hash | 300s（通话结束后延迟清理） | 通话详情：callerId, calleeId, sessionId, status, startTime |
| `call:timeout:{callId}` | String (标记) | 30s | 呼叫超时控制，过期触发超时处理 |
| `call:route:{callId}` | String (nodeId) | 跟随通话生命周期 | 信令路由节点，支持多实例部署 |

### 9.2 忙线检测逻辑

发起通话前执行 Redis 原子操作：

```lua
-- Lua 脚本：原子检测并设置忙线
local callerKey = KEYS[1]  -- call:busy:{callerId}
local calleeKey = KEYS[2]  -- call:busy:{calleeId}
local callId = ARGV[1]

if redis.call('EXISTS', callerKey) == 1 then
    return 'CALLER_BUSY'
end
if redis.call('EXISTS', calleeKey) == 1 then
    return 'CALLEE_BUSY'
end

redis.call('SET', callerKey, callId)
redis.call('SET', calleeKey, callId)
return 'OK'
```

### 9.3 超时控制

使用 Redis Key 过期事件（Keyspace Notification）或定时任务轮询实现 30 秒呼叫超时。超时触发后：更新通话状态为 ENDED、设置 end_reason 为 TIMEOUT、清除忙线标记、通知双方。

---

## 10. 通话时序

### 10.1 正常接通流程

```text
Caller              Backend                    Callee
  |                     |                        |
  |-- POST /api/call/initiate -->|               |
  |                     |-- 校验/落库/缓存 ------>|
  |<-- Response(callId) |                        |
  |                     |-- WS: CALL_INVITE ---->|
  |                     |                        |
  |                     |<-- WS: CALL_RINGING ---|
  |<-- WS: CALL_RINGING|                        |
  |                     |                        |
  |                     |<-- WS: CALL_ACCEPT ----|
  |<-- WS: CALL_ACCEPT |                        |
  |                     |                        |
  |-- WS: WEBRTC_OFFER ---------->|             |
  |                     |-- WS: WEBRTC_OFFER --->|
  |                     |                        |
  |                     |<-- WS: WEBRTC_ANSWER --|
  |<-- WS: WEBRTC_ANSWER         |              |
  |                     |                        |
  |-- WS: ICE_CANDIDATE -------->|              |
  |                     |-- WS: ICE_CANDIDATE -->|
  |<-- WS: ICE_CANDIDATE --------|              |
  |                     |<-- WS: ICE_CANDIDATE --|
  |                     |                        |
  |========= WebRTC P2P 音频连接建立 ===========|
  |                     |                        |
  |-- WS: CALL_HANGUP ---------->|              |
  |                     |-- WS: CALL_HANGUP ---->|
  |                     |-- 更新call_record ---->|
  |                     |-- 清除Redis状态 ------>|
```

### 10.2 被叫拒绝流程

```text
Caller              Backend                    Callee
  |-- POST /api/call/initiate -->|               |
  |<-- Response(callId)          |               |
  |                     |-- WS: CALL_INVITE ---->|
  |                     |<-- WS: CALL_REJECT ----|
  |<-- WS: CALL_REJECT |                        |
  |                     |-- 更新状态/清除忙线 -->|
```

### 10.3 超时未接流程

```text
Caller              Backend                    Callee
  |-- POST /api/call/initiate -->|               |
  |<-- Response(callId)          |               |
  |                     |-- WS: CALL_INVITE ---->|
  |                     |                        |
  |           [30秒超时触发]     |               |
  |                     |                        |
  |<-- WS: CALL_TIMEOUT |-- WS: CALL_TIMEOUT -->|
  |                     |-- 更新状态/清除忙线 -->|
```

### 10.4 主叫取消流程

```text
Caller              Backend                    Callee
  |-- POST /api/call/initiate -->|               |
  |<-- Response(callId)          |               |
  |                     |-- WS: CALL_INVITE ---->|
  |-- WS: CALL_CANCEL --------->|               |
  |                     |-- WS: CALL_CANCEL ---->|
  |                     |-- 更新状态/清除忙线 -->|
```

---

## 11. 与现有系统集成

### 11.1 Netty WebSocket 扩展

在现有 `WebSocketFrameHandler` 的消息分发逻辑中新增 `CALL_*` 和 `WEBRTC_*` 类型路由，转发到 `CallSignalHandler` 处理。不新建独立 WebSocket 服务，统一连接管理和鉴权。

### 11.2 消息主链路复用

语音通话消息复用现有 `MessageService` 的消息创建链路：写消息表 → 同步 Redis 消息缓存 → 更新会话最近消息 → 更新未读计数。保证语音通话消息与普通聊天消息在会话列表和历史记录中的一致性体验。

### 11.3 在线状态复用

复用现有 `WebSocketSessionManager` 的在线状态能力。发起通话前判断被叫是否在线，不在线直接返回错误而非等待超时。

### 11.4 鉴权复用

信令消息的身份校验复用现有 JWT + Netty 鉴权体系。额外增加 callId 与参与者的绑定校验：只有该通话的主叫和被叫可以发送对应 callId 的信令。

### 11.5 RabbitMQ 使用策略

实时信令（CALL_ACCEPT、WEBRTC_OFFER、WEBRTC_ANSWER、ICE_CANDIDATE）直走 WebSocket 不经 MQ。RabbitMQ 仅用于非核心异步任务：未接来电推送通知、通话事件日志记录、离线消息补偿。

---

## 12. 前端 WebRTC 接入指引

### 12.1 核心流程

1. 收到 `CALL_ACCEPT` 后（主叫）或发送 `CALL_ACCEPT` 后（被叫），调用 `navigator.mediaDevices.getUserMedia({ audio: true, video: false })`
2. 创建 `RTCPeerConnection`，配置后端下发的 iceServers
3. 将本地音频轨添加到 PeerConnection
4. 主叫创建 offer，设置 localDescription，通过 WebSocket 发送
5. 被叫收到 offer，设置 remoteDescription，创建 answer 并发送
6. 双方监听 `onicecandidate` 事件，通过 WebSocket 发送 candidate
7. 收到对方 candidate 后调用 `addIceCandidate`
8. 监听 `ontrack` 事件获取远端音频流并播放

### 12.2 iceServers 配置示例

```javascript
// 从 GET /api/call/turn-config 获取配置
const config = await fetchTurnConfig();
const pc = new RTCPeerConnection({ iceServers: config.iceServers });
```

### 12.3 注意事项

TURN 凭证有 TTL，前端应在发起/接听通话时实时获取，不缓存过期凭证。getUserMedia 调用需要 HTTPS 环境（localhost 除外）。建议在接听确认后再申请麦克风权限，避免在振铃阶段弹出权限弹窗。

---

## 13. coturn 部署规范

### 13.1 基础配置

部署一台独立云主机运行 coturn 服务，要求具备公网 IP。

关键配置项：

```ini
listening-port=3478
relay-ip=<公网IP>
external-ip=<公网IP>
min-port=49152
max-port=65535
fingerprint
lt-cred-mech
use-auth-secret
static-auth-secret=<与后端共享的密钥>
realm=chatroom.example.com
```

### 13.2 动态凭证生成

后端基于 `static-auth-secret` 使用 HMAC-SHA1 生成临时凭证：

```
username = {timestamp}:{userId}
credential = HMAC-SHA1(static-auth-secret, username)
```

timestamp 为过期时间戳（当前时间 + TTL），coturn 会自动校验凭证有效性。

### 13.3 网络要求

开放端口：3478（TCP/UDP）、49152-65535（UDP，中继端口范围）。建议配置 TLS（5349 端口），支持 TURNS 以穿透严格防火墙。

---

## 14. 开发计划

### 14.1 第一阶段：核心链路（预计 2 周）

| 序号 | 任务 | 说明 |
|------|------|------|
| 1 | 创建 call 模块骨架 | entity、enums、mapper、service、controller、handler |
| 2 | 数据库表创建 | call_record 表 DDL，chat_message 新增 VOICE_CALL 类型 |
| 3 | 实现 POST /api/call/initiate | 业务校验 + 消息创建 + 通话记录创建 + 信令推送 |
| 4 | 实现 Redis 忙线检测 | Lua 脚本原子操作 |
| 5 | 实现通话状态机 | 状态定义 + CAS 转换 + 幂等控制 |
| 6 | Netty 信令扩展 | 新增 CALL_*/WEBRTC_* 消息路由 |
| 7 | 实现信令转发逻辑 | ACCEPT/REJECT/CANCEL/HANGUP/OFFER/ANSWER/ICE |
| 8 | 实现超时控制 | Redis 过期事件或定时任务 |
| 9 | 实现 TURN 配置接口 | 动态凭证生成 |
| 10 | 部署 coturn | 基础配置 + 凭证联调 |
| 11 | 前端 WebRTC 接入 | offer/answer/candidate 交换 + 音频播放 |
| 12 | 联调验收 | 正常通话、拒绝、取消、超时、忙线全场景 |

### 14.2 第二阶段：可用性增强（预计 1 周）

| 序号 | 任务 |
|------|------|
| 1 | 幂等控制完善（重复接听/挂断） |
| 2 | 多端登录冲突处理 |
| 3 | 通话中断线重连与 ICE restart |
| 4 | 通话结束回写消息内容（时长/结果） |
| 5 | 失败重试与异常日志追踪 |
| 6 | 未接来电离线通知（RabbitMQ） |

### 14.3 第三阶段：体验优化（预计 1 周）

| 序号 | 任务 |
|------|------|
| 1 | 静音/免提功能 |
| 2 | 网络质量监控与提示 |
| 3 | 通话时长实时计时 |
| 4 | 通话质量指标上报 |
| 5 | ICE 失败率/TURN 使用率监控 |

### 14.4 第四阶段：多人通话（规划中）

引入 SFU（推荐 LiveKit 或 mediasoup），新增 call_room + call_room_member 模型，实现入会/退会/静音/主持控制等群语音能力。该阶段视业务需求决定是否启动。

---

## 15. 验收标准

### 15.1 功能验收

- 双方在线时可正常发起并接通语音通话
- 通话音质清晰，延迟可接受（< 300ms RTT）
- 忙线状态可正确拦截新呼叫
- 超时 30 秒自动结束并通知双方
- 拒绝、取消操作即时生效
- 通话记录正确落库，消息历史中可见通话卡片
- 弱网/复杂 NAT 场景下 TURN 中继可兜底

### 15.2 技术验收

- 状态机无脏状态，并发操作幂等
- Redis 忙线标记在通话结束后正确清除（含异常场景）
- WebSocket 信令仅转发给通话参与方，无越权风险
- TURN 凭证动态生成，不暴露共享密钥
- 通话相关接口鉴权完备

---

## 16. 风险与规避

| 风险 | 影响 | 规避措施 |
|------|------|----------|
| 只配 STUN 不配 TURN | 复杂 NAT 下通话不可用 | 生产环境必须部署 coturn 并配置 TURN |
| 状态不一致（如取消与接听并发） | 双方状态矛盾 | Redis 原子状态更新 + 状态机严格校验 |
| Redis 忙线标记泄漏 | 用户永久无法发起新通话 | 通话结束/异常时多重清理 + 定时巡检 |
| WebSocket 断连 | 信令丢失导致状态卡死 | 心跳检测 + 断连后自动清理通话状态 |
| 媒体流与信令混淆设计 | 架构腐化，后端承压 | 明确信令/媒体边界，后端不碰音频数据 |

---

## 17. 监控指标

生产环境建议监控以下关键指标：

| 指标 | 说明 | 告警阈值建议 |
|------|------|-------------|
| 呼叫发起成功率 | initiate 接口成功率 | < 99% |
| 接通率 | 成功建立通话 / 发起次数 | < 60% |
| 平均接通时延 | 从 INVITE 到 IN_CALL 的时间 | > 10s |
| ICE 失败率 | WebRTC 连接建立失败比例 | > 15% |
| TURN 使用率 | 走中继的通话比例 | > 40%（需评估 TURN 容量） |
| 平均通话时长 | 业务分析指标 | — |
| 忙线拒绝率 | 忙线 / 总呼叫 | — |

---

## 附录 A：枚举定义

### CallType

```java
public enum CallType {
    VOICE("语音通话");
    // 未来扩展: VIDEO("视频通话")
}
```

### CallStatus

```java
public enum CallStatus {
    INIT,
    INVITING,
    RINGING,
    ACCEPTED,
    CONNECTING,
    IN_CALL,
    ENDED;
}
```

### CallEndReason

```java
public enum CallEndReason {
    COMPLETED,   // 正常结束
    MISSED,      // 未接
    REJECTED,    // 拒绝
    CANCELED,    // 取消
    BUSY,        // 忙线
    FAILED,      // 建链失败
    TIMEOUT;     // 超时
}
```

---

## 附录 B：Redis Key 常量

```java
public class CallRedisKey {
    /** 用户忙线标记 call:busy:{userId} -> callId */
    public static final String BUSY_PREFIX = "call:busy:";
    
    /** 通话详情 call:detail:{callId} -> Hash */
    public static final String DETAIL_PREFIX = "call:detail:";
    
    /** 超时控制 call:timeout:{callId} -> flag, TTL=30s */
    public static final String TIMEOUT_PREFIX = "call:timeout:";
    
    /** 节点路由 call:route:{callId} -> nodeId */
    public static final String ROUTE_PREFIX = "call:route:";
}
```

---

## 附录 C：WebSocket 信令类型常量

```java
public class CallSignalType {
    public static final String CALL_INVITE = "CALL_INVITE";
    public static final String CALL_RINGING = "CALL_RINGING";
    public static final String CALL_ACCEPT = "CALL_ACCEPT";
    public static final String CALL_REJECT = "CALL_REJECT";
    public static final String CALL_CANCEL = "CALL_CANCEL";
    public static final String CALL_HANGUP = "CALL_HANGUP";
    public static final String CALL_BUSY = "CALL_BUSY";
    public static final String CALL_TIMEOUT = "CALL_TIMEOUT";
    public static final String WEBRTC_OFFER = "WEBRTC_OFFER";
    public static final String WEBRTC_ANSWER = "WEBRTC_ANSWER";
    public static final String WEBRTC_ICE_CANDIDATE = "WEBRTC_ICE_CANDIDATE";
    public static final String CALL_STATE_SYNC = "CALL_STATE_SYNC";
}
```
