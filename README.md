# ChatRoom Backend

一个基于 Java 17 和 Spring Boot 3.2 的即时通讯后端 MVP，覆盖用户、好友、单聊、群聊、消息可靠投递、离线拉取、分片上传和 1v1 语音通话的基础业务。

项目重点不是普通 CRUD，而是围绕即时通讯场景实现消息幂等、Redis 热数据缓存、Outbox 最终一致性、RabbitMQ 异步落库、消息分表和多节点 WebSocket 推送。

> 当前状态：核心用户/会话/消息链路已具备较完整实现；通知、Netty 上行信令、真实对象存储、多节点 Netty 推送整合和自动化测试仍需完善。请先阅读[已知限制](#已知限制与上线前检查)，再决定部署等级。

## 目录

- [功能与完成度](#功能与完成度)
- [系统架构](#系统架构)
- [技术栈](#技术栈)
- [项目结构](#项目结构)
- [核心设计](#核心设计)
- [快速开始](#快速开始)
- [手动开发环境](#手动开发环境)
- [配置说明](#配置说明)
- [API 概览](#api-概览)
- [WebSocket 接入](#websocket-接入)
- [部署方案](#部署方案)
- [运维与数据安全](#运维与数据安全)
- [已知限制与上线前检查](#已知限制与上线前检查)
- [开发与验证](#开发与验证)

## 功能与完成度

| 模块 | 能力 | 状态 |
| --- | --- | --- |
| 认证 | 登录、退出、Access Token、Refresh Token、JWT 黑名单 | 已实现 |
| 用户 | 注册、个人资料、头像、密码、搜索、注销 | 已实现 |
| 好友 | 好友列表、添加、删除、备注 | 已实现 |
| 会话 | 单聊幂等创建、群聊、成员邀请/移除、会话详情、已读位置 | 已实现 |
| 消息 | 发送、撤回、单条/全部已读、游标分页、离线拉取 | 已实现 |
| 可靠消息 | Outbox、Publisher Confirm、消费幂等、延迟补偿、死信落表 | 已实现 |
| 缓存 | 用户/会话缓存、消息 ZSet、Bloom Filter、大群分片 | 已实现 |
| 上传 | 初始化、分片上传、断点状态、合并 | 已实现，当前只提供本地存储实现 |
| WebSocket | JWT 握手、心跳、在线状态、服务端下行 | 基础实现 |
| Netty | 独立 WebSocket 端口、连接管理、心跳 | 基础实现，上行业务路由待补齐 |
| 语音通话 | HTTP 发起、通话记录模型、状态机、WebRTC 方案文档 | 部分实现 |
| 通知 | 数据模型与接口占位 | 待实现 |
| 测试 | 单元测试、集成测试、容器化测试 | 待补齐 |

## 系统架构

### 组件关系

```text
                       ┌─────────────────────────┐
                       │       Web / App         │
                       └────────────┬────────────┘
                                    │ HTTPS / WSS
                              ┌─────▼─────┐
                              │   Nginx   │
                              └──┬─────┬──┘
                         HTTP API│     │WebSocket
                    ┌────────────▼─┐ ┌─▼─────────────┐
                    │ Spring Boot  │ │ Netty :9090   │
                    │ HTTP :8080   │ │ 长连接与心跳   │
                    └──┬────┬────┬─┘ └──────┬────────┘
                       │    │    │          │
             ┌─────────┘    │    └──────────┤ Redis Pub/Sub
             │              │               │
       ┌─────▼─────┐  ┌─────▼─────┐  ┌──────▼──────┐
       │   MySQL   │  │ RabbitMQ  │  │    Redis    │
       │ 16 消息分表│  │ 消息异步化 │  │ 缓存/锁/在线 │
       └───────────┘  └───────────┘  └─────────────┘
```

### 消息发送链路

```text
POST /api/v1/messages
  → JWT 鉴权与单用户限流
  → Bloom Filter 判断会话是否可能存在
  → Redis Set 判断发送者是否为会话成员
  → Redisson 锁 + msgNo 映射保证发送幂等
  → 生成消息 ID，写入 Redis ZSet 热缓冲
  → 在本地事务表 local_msg_outbox 中记录待发送消息
  → RabbitMQ Publisher Confirm
  → 消费者异步写 chat_message_N 分片表
  → 更新 session.last_msg_* 和 Outbox 状态
  → 向在线会话成员推送；离线成员上线后按游标拉取
```

HTTP 返回的初始状态为 `sending`。MQ 落库失败时会执行进程内重试、死信记录和 Outbox 补偿，前端不应把首次 HTTP 成功等同于消息已经最终持久化。

## 技术栈

| 类别 | 技术 |
| --- | --- |
| 语言与框架 | Java 17、Spring Boot 3.2.5 |
| Web | Spring MVC、Spring WebSocket、Jakarta Validation |
| 长连接 | Netty 4.1.109 |
| 持久化 | MySQL、MyBatis-Plus 3.5.7 |
| 分片 | ShardingSphere JDBC 5.4.1 |
| 缓存与锁 | Redis、Lettuce、Redisson 3.29 |
| 消息队列 | RabbitMQ、Spring AMQP |
| 任务调度 | XXL-Job 2.4.1 |
| 认证 | JWT（jjwt 0.12.5）、BCrypt |
| 工具 | Lombok、MapStruct、Guava、Snowflake ID |
| 构建与部署 | Maven、Docker、Docker Compose、Nginx |

完整依赖版本见 [`pom.xml`](./pom.xml)。

## 项目结构

```text
ChatRoomBackend/
├── src/main/java/com/example/chatroom/
│   ├── cache/                  # 用户、会话、消息缓存和 Bloom Filter
│   ├── common/                 # 鉴权、异常、限流、OSS、配置、工具类
│   ├── mq/                     # RabbitMQ 生产者、消费者和消息 DTO
│   ├── module/
│   │   ├── auth/               # 登录、刷新、退出
│   │   ├── user/               # 用户、资料、好友
│   │   ├── session/            # 单聊、群聊、成员和会话列表
│   │   ├── message/            # 消息发送、查询、撤回和已读
│   │   ├── upload/             # 分片上传
│   │   ├── websocket/          # Spring WebSocket 过渡实现
│   │   ├── netty/              # Netty WebSocket 实现
│   │   ├── call/               # 语音通话业务与状态机
│   │   └── notification/       # 系统通知，占位实现
│   └── task/                   # XXL-Job 补偿、归档、Token 清理
├── src/main/resources/
│   ├── application.yml         # 公共配置
│   ├── application-dev.yml     # 本地开发配置
│   ├── application-prod.yml    # 生产环境变量配置
│   └── sql/init.sql            # 数据库初始化脚本
├── docs/                       # 语音通话设计与开发文档
├── deploy/
│   ├── personal/               # 个人级全栈 Compose
│   └── production/             # 生产级应用节点基线
├── Dockerfile                  # Java 17 多阶段镜像构建
└── pom.xml
```

代码采用“按业务模块纵向拆分、模块内 Controller/Service/Mapper/DTO/VO/Entity 分层”的模块化单体结构。

## 核心设计

### 1. Outbox 与最终一致性

消息首先写入 `local_msg_outbox`，再发布到 RabbitMQ。Publisher Confirm 更新投递状态，消费者成功落库后将状态更新为完成。延迟队列和 XXL-Job 会扫描未完成记录进行补偿。

消费者收到 MQ 消息后先通过 Redis `SET NX` 快速去重，再以数据库 `msg_no` 唯一约束和查询兜底。连续失败的消息写入 `msg_dead_letter`，保留原始 payload 和错误原因，便于人工恢复。

### 2. 消息分表

逻辑表 `chat_message` 被映射为：

```text
chat_message_0 ... chat_message_15
```

分片键是 `session_id`，算法为 `session_id % 16`。任何消息 Mapper 查询都必须携带 `session_id`，否则可能触发全路由查询或得到错误结果。

### 3. 三级消息查询

向前翻页（查看更旧消息）按以下顺序读取：

1. 最近消息 ZSet：`msg:buf:{sessionId}`；
2. 历史回填 ZSet：`msg:history:{sessionId}`；
3. MySQL 分片表，并把结果回填历史缓存。

接口使用消息 ID 作为游标，单页最大 50 条。客户端应保存最后已读消息 ID，不要使用页码推导消息位置。

### 4. 大群缓存分片

当成员数达到 `chat.group.write-fanout-threshold`（默认 1000）时，消息缓存会拆到多个 ZSet，避免单个 Redis Key 成为热点。用户按 `userId` 路由到自己的读取分片。

### 5. 多节点在线状态

每个应用节点通过 `MACHINE_ID` 标识自身。在线状态存储在：

```text
ws:online:{userId} -> Hash { machineId: timestamp }
```

跨节点定向推送使用 Redis Pub/Sub：

```text
ws:push:{machineId}
```

生产环境中每个应用实例必须使用唯一 `MACHINE_ID`。当前 Netty Pub/Sub 监听注册仍需收口，详见已知限制。

### 6. 语音通话

目标方案是 WebRTC 1v1 音频通话：

- HTTP 负责发起通话、鉴权和生成通话记录；
- WebSocket 负责 INVITE、ACCEPT、REJECT、SDP、ICE 等实时信令；
- STUN/TURN 负责 NAT 穿透与中继；
- 音频媒体流由客户端 WebRTC 直接传输，业务后端不转发音频包。

当前只完成部分后端能力。详细设计见：

- [`docs/voice-call-design.md`](./docs/voice-call-design.md)
- [`docs/voice-call-development.md`](./docs/voice-call-development.md)
- [`docs/voice-call-signaling.md`](./docs/voice-call-signaling.md)

## 快速开始

推荐使用个人级 Docker Compose，它不要求宿主机安装 Java 或 Maven。

### 环境要求

- Docker Engine 24+
- Docker Compose v2
- 建议 2 CPU、4 GB RAM

### 一键启动

```bash
cd deploy/personal
cp .env.example .env
```

修改 `.env` 中的示例密码和 `JWT_SECRET`：

```bash
docker compose --env-file .env up -d --build
docker compose ps
docker compose logs -f app
```

默认地址：

| 服务 | 地址 |
| --- | --- |
| API | `http://localhost:8080/api/v1` |
| WebSocket | `ws://localhost:8080/ws/chat?token=<accessToken>` |
| 网关存活检查 | `http://localhost:8080/healthz` |
| RabbitMQ 管理台 | `http://127.0.0.1:15672` |

完整说明见 [`deploy/personal/README.md`](./deploy/personal/README.md)。

### 最小调用示例

注册：

```bash
curl -X POST http://localhost:8080/api/v1/users/register \
  -H 'Content-Type: application/json' \
  -d '{
    "username": "demo_user",
    "password": "DemoPass123",
    "nickname": "Demo"
  }'
```

登录：

```bash
curl -X POST http://localhost:8080/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{
    "username": "demo_user",
    "password": "DemoPass123",
    "deviceInfo": "curl"
  }'
```

创建单聊，需要把返回的 Access Token 和对方 `userNo` 代入：

```bash
curl -X POST http://localhost:8080/api/v1/sessions/single \
  -H 'Authorization: Bearer <accessToken>' \
  -H 'Content-Type: application/json' \
  -d '{"targetUserNo":"<targetUserNo>"}'
```

发送文本消息：

```bash
curl -X POST http://localhost:8080/api/v1/messages \
  -H 'Authorization: Bearer <accessToken>' \
  -H 'Content-Type: application/json' \
  -d '{
    "sessionId": 1,
    "msgNo": "550e8400-e29b-41d4-a716-446655440000",
    "msgType": 1,
    "content": "hello"
  }'
```

## 手动开发环境

如果不使用 Docker 启动应用，需要：

- JDK 17
- Maven 3.9+
- MySQL 8.x
- Redis 7.x
- RabbitMQ 3.13+/4.x
- 可选：XXL-Job Admin 2.4.1

### 1. 初始化数据库

```bash
mysql -uroot -p < src/main/resources/sql/init.sql
```

### 2. 修改开发配置

编辑 `src/main/resources/application-dev.yml`，配置本地 MySQL、Redis 和 RabbitMQ。开发配置中包含示例密码，只适用于本机，不得直接用于公网或生产环境。

### 3. 启动

```bash
mvn clean package -DskipTests
java -jar target/chatroom-backend-1.0.0-SNAPSHOT.jar \
  --spring.profiles.active=dev
```

或：

```bash
mvn spring-boot:run
```

Spring HTTP 默认监听 8080，Netty WebSocket 默认监听 9090，XXL-Job 执行器默认监听 9999。

## 配置说明

### Profiles

| Profile | 用途 | 配置方式 |
| --- | --- | --- |
| `dev` | 本地开发 | `application-dev.yml` 中的本机地址 |
| `prod` | 容器/生产 | 必需配置由环境变量注入 |

### 关键环境变量

| 变量 | 必需 | 说明 |
| --- | --- | --- |
| `SPRING_PROFILES_ACTIVE` | 是 | 部署时使用 `prod` |
| `DB_URL` | 是 | MySQL JDBC URL |
| `DB_USERNAME` / `DB_PASSWORD` | 是 | 数据库凭证 |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_PASSWORD` | 是 | Redis 连接 |
| `RABBITMQ_HOST` / `RABBITMQ_PORT` | 是 | RabbitMQ 地址 |
| `RABBITMQ_USERNAME` / `RABBITMQ_PASSWORD` | 是 | RabbitMQ 凭证 |
| `JWT_SECRET` | 是 | JWT 签名密钥，至少 32 字节随机值 |
| `MACHINE_ID` | 多节点必需 | 每实例唯一的节点标识 |
| `XXL_JOB_ADMIN_ADDRESSES` | 可选 | XXL-Job Admin 地址，个人环境可留空 |
| `XXL_JOB_ACCESS_TOKEN` | 推荐 | XXL-Job 调度鉴权令牌 |
| `UPLOAD_TMP_DIR` | 推荐 | 上传分片临时目录 |
| `OSS_URL_PREFIX` | 生产必需 | OSS/CDN URL 前缀，仅在真实 OSS 实现完成后有效 |
| `JAVA_OPTS` | 推荐 | JVM 内存、GC 与诊断参数 |

Spring Boot 支持通过大写下划线环境变量覆盖点号配置，例如：

```text
CHAT_GROUP_MAX_MEMBER_COUNT → chat.group.max-member-count
THREAD_POOL_MSG_PERSIST_MAX_SIZE → thread-pool.msg-persist.max-size
```

不要提交 `.env`、TLS 私钥或真实生产密码。仓库中的 `.env.example` 只描述变量，不包含可用密钥。

## API 概览

除注册、登录、刷新 Token 和 WebSocket 握手外，API 均需要：

```http
Authorization: Bearer <accessToken>
```

统一响应结构：

```json
{
  "code": 0,
  "msg": "success",
  "data": {},
  "traceId": null
}
```

### 认证与用户

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/users/register` | 注册并返回 Token |
| POST | `/api/v1/auth/login` | 登录 |
| POST | `/api/v1/auth/logout` | 退出并使当前 Token 失效 |
| POST | `/api/v1/auth/refresh` | 刷新 Token |
| GET | `/api/v1/users/me` | 当前用户 |
| PUT / DELETE | `/api/v1/users/me` | 更新/注销当前用户 |
| GET | `/api/v1/users/{userNo}` | 按用户编号查询 |
| GET | `/api/v1/users/search?keyword=` | 搜索用户 |
| GET / PUT | `/api/v1/users/me/profile` | 查询/更新扩展资料 |
| PUT | `/api/v1/users/me/avatar` | 更新头像 URL |
| PUT | `/api/v1/users/me/password` | 修改密码 |

### 好友

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/v1/users/me/friends` | 好友列表 |
| POST | `/api/v1/users/me/friends?friendUserNo=` | 添加好友 |
| DELETE | `/api/v1/users/me/friends/{friendId}` | 删除好友 |
| PUT | `/api/v1/users/me/friends/{friendId}` | 更新好友备注 |

### 会话

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/v1/sessions` | 会话列表 |
| GET | `/api/v1/sessions/read-positions` | 分页获取已读位置 |
| POST | `/api/v1/sessions/single` | 获取或创建单聊 |
| POST | `/api/v1/sessions/group` | 创建群聊 |
| GET / PUT / DELETE | `/api/v1/sessions/{sessionId}` | 详情、更新群信息、退出会话 |
| GET / POST | `/api/v1/sessions/{sessionId}/members` | 成员列表、邀请成员 |
| DELETE | `/api/v1/sessions/{sessionId}/members/{userId}` | 移除成员 |

### 消息

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/messages` | 发送消息，`msgNo` 为客户端幂等键 |
| GET | `/api/v1/messages` | 按 `sessionId/cursor/size/direction` 查询 |
| DELETE | `/api/v1/messages/{msgId}` | 撤回消息 |
| POST | `/api/v1/messages/{msgId}/read` | 标记单条已读 |
| POST | `/api/v1/messages/read-all?sessionId=` | 标记会话全部已读 |

普通消息类型：1 文本、2 图片、3 语音、4 视频、5 文件。语音通话邀请由专用 Call API 生成内部类型 7，不通过普通发送 DTO 直接提交。

### 上传、通知与通话

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/upload/init` | 初始化分片上传 |
| POST | `/api/v1/upload/{taskId}/chunk` | 上传单个分片 |
| GET | `/api/v1/upload/{taskId}/status` | 查询已上传分片 |
| POST | `/api/v1/upload/{taskId}/complete` | 合并并保存文件 |
| POST | `/api/v1/calls/initiate` | 发起 1v1 语音通话 |
| GET / PUT | `/api/v1/notifications/**` | 通知占位接口，当前未完成 |

## WebSocket 接入

部署后统一连接地址：

```text
ws://host/ws/chat?token=<accessToken>
wss://chat.example.com/ws/chat?token=<accessToken>
```

开发时也可直接连接 Netty：

```text
ws://localhost:9090/ws/chat?token=<accessToken>
```

Token 在握手阶段校验，成功后连接会绑定 `userId` 并写入 Redis 在线状态。客户端应实现：

- 定期心跳和断线指数退避重连；
- 重连成功后使用每个会话的 `lastReceivedMsgId` 调用消息列表接口补拉离线消息；
- 分别维护 `lastReceivedMsgId`（已持久化到客户端）和 `lastReadMsgId`（用户已读位置）；
- 对收到的消息按 `msgNo` 或 `msgId` 去重。

聊天消息下行格式：

```json
{
  "type": "MESSAGE",
  "msgId": 123,
  "sessionId": 10,
  "data": {}
}
```

客户端按 `msgId` 幂等保存成功后，通过同一 WebSocket 连接返回：

```json
{"type":"MESSAGE_ACK","msgId":123,"sessionId":10}
```

服务端在未收到 ACK 时执行最多 3 次重发，采用指数退避与 Equal Jitter 抖动；达到上限后服务端会主动关闭连接，客户端必须自动重连，并在每次连接成功后使用 `lastReadMsgId` 和 `direction=after` 分页补拉直至 `hasMore=false`。客户端应按 `msgId` 幂等保存，收到重复消息时仍需返回 ACK。`MESSAGE_ACK` 只表示客户端已保存消息，不等同于已读回执，不能推进 `lastReadMsgId`。

Spring WebSocket 支持文本心跳：

```json
{"type":"PING"}
```

响应：

```json
{"type":"PONG"}
```

Netty 使用 WebSocket Ping/Pong 帧处理心跳。当前业务消息仍通过 HTTP 上行，WebSocket 主要承担服务端下行推送；Netty 文本上行当前支持 `MESSAGE_ACK`，其他业务帧路由尚未实现。

## 部署方案

### 个人级

适合本地体验、开发联调和个人服务器：

- 一套 Compose 同时运行应用、MySQL、Redis、RabbitMQ 和 Nginx；
- 中间件管理端口只绑定 `127.0.0.1`；
- 上传文件保存在 Docker Volume，并由 Nginx 读取；
- 不提供跨主机高可用和自动容灾。

入口：[`deploy/personal`](./deploy/personal/)

### 生产级

适合预发布或完成上线阻断项后的正式环境：

- 应用与中间件解耦；
- 外部高可用 MySQL、Redis、RabbitMQ、OSS；
- TLS、只读根文件系统、非 root 用户、资源限制和日志轮转；
- 每台主机部署一个唯一节点，多节点置于负载均衡后滚动发布；
- 使用镜像版本或 Git SHA 发布，支持快速回滚应用镜像。

入口：[`deploy/production`](./deploy/production/)

部署对比：

| 维度 | 个人级 | 生产级 |
| --- | --- | --- |
| 目标 | 快速可用 | 可运维、可扩展、可恢复 |
| 中间件 | 单机容器 | 托管服务或独立集群 |
| TLS | 默认无 | 强制 HTTPS/WSS |
| 数据备份 | 手工 | 自动备份 + 恢复演练 |
| 发布 | 停机重建可接受 | 多节点滚动/灰度 |
| 密钥 | 本地 `.env` | KMS/Vault/平台 Secret |
| 上传 | 本地 Volume | OSS + CDN |
| 监控 | 容器日志 | Metrics、Tracing、集中日志、告警 |

## 运维与数据安全

### 数据库初始化

`src/main/resources/sql/init.sql` 创建业务表和 16 张消息分片表。个人 Compose 仅在 MySQL 数据卷首次创建时自动执行。正式环境应由 DBA 审核后执行，并尽快引入 Flyway 或 Liquibase 管理增量变更。

### XXL-Job 任务

| Handler | 用途 |
| --- | --- |
| `outboxCompensateJobHandler` | 重试未落库/未完成的 Outbox 消息 |
| `outboxArchiveJobHandler` | 清理或归档已完成的 Outbox |
| `tokenCleanJobHandler` | 清理过期 Refresh Token |

个人部署默认不启动 XXL-Job Admin，因此这些定时任务不会被中心调度；需要时可单独部署 Admin 并配置执行器。

### 建议的备份对象

- MySQL 全量备份、binlog 和恢复文档；
- Redis AOF/RDB（缓存可重建，但在线状态和短期幂等数据会影响恢复行为）；
- RabbitMQ 定义、队列策略和必要的持久消息；
- OSS 文件及版本；
- Nginx、环境配置和密钥元数据；
- Outbox、死信表和人工补偿审计记录。

## 已知限制与上线前检查

### 当前已知限制

1. `notification` Controller 返回占位结果，通知业务尚未实现。
2. `WebSocketFrameHandler` 尚未把 Netty 文本帧路由到消息/通话信令处理器。
3. `NettyPubSubListener` 已存在，但当前 Redis Listener 容器只注册了 Spring WebSocket Listener，多节点 Netty 下行推送尚未完全接通。
4. Spring WebSocket 与 Netty 两套连接管理并存，消息消费者的本机在线判断主要依赖 Spring WebSocket Manager，需要统一。
5. `LocalOssClient` 只适用于单节点本地磁盘；配置 OSS 环境变量并不会自动把文件上传到云 OSS。
6. 语音通话只实现发起链路和基础状态模型，CallSignalHandler、TURN 临时凭证、通话记录查询等仍待开发。
7. 仓库当前没有 `src/test`，缺少自动化回归保障。
8. 没有数据库迁移框架，`init.sql` 只能解决首次初始化。
9. 尚未接入 Actuator、Prometheus、Tracing，健康检查只能覆盖端口或网关。

### 正式上线前最低检查

- [ ] 修复并压测 Netty 多节点推送链路；
- [ ] 接入真实 OSS，并验证分片合并、失败清理和鉴权；
- [ ] 增加单元、集成、MQ 重复消费和故障恢复测试；
- [ ] 引入数据库版本迁移并演练回滚兼容；
- [ ] 加入 readiness/liveness、指标、链路追踪和告警；
- [ ] 对登录、上传、消息、WebSocket 做限流和安全测试；
- [ ] 演练 MySQL 恢复、Redis 故障、RabbitMQ 堆积和 Outbox 补偿；
- [ ] 替换全部示例密钥，关闭公网中间件端口；
- [ ] 确认隐私、日志脱敏、数据保留与删除策略。

## 开发与验证

### Maven 命令

```bash
mvn clean compile -DskipTests
mvn test
mvn clean package -DskipTests
```

### Docker 镜像

```bash
docker build -t chatroom-backend:local .
```

应用启动需要 MySQL、Redis 和 RabbitMQ，单独运行镜像时必须注入 `prod` Profile 所需环境变量。

### 提交前建议

```bash
mvn test
docker compose --env-file deploy/personal/.env \
  -f deploy/personal/compose.yml config --quiet
```

然后至少验证注册、登录、创建单聊、发送消息、MQ 落库、消息拉取、WebSocket 推送、撤回和已读链路。

## License

仓库当前未声明开源许可证。对外发布或允许第三方使用前，请补充明确的 License。
