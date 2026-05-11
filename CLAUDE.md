# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
# Build
mvn clean compile -DskipTests

# Run (default profile: dev)
mvn spring-boot:run

# Run tests
mvn test
mvn test -Dtest=MessageServiceImplTest          # single test class
mvn test -Dtest=MessageServiceImplTest#sendMessage  # single method
```

## Technology Stack

Java 17, Spring Boot 3.2.5, MyBatis-Plus 3.5.7, ShardingSphere 5.4.1, Redisson 3.29.0, Netty 4.1.109, RabbitMQ (spring-amqp), Redis (Lettuce), XXL-Job 2.4.1, JWT (jjwt 0.12.5), Guava 33.2, MapStruct 1.5.5, Lombok.

## Architecture Overview

**Layered by module**: `com.example.chatroom.module.<domain>` with nested `controller` / `service` / `domain.dto` / `domain.vo` / `domain.entity` / `mapper` — a vertical-slice structure within a horizontal Spring Boot scaffold.

```
common/        — cross-cutting: JWT filter, rate-limit AOP, Snowflake ID gen, OSS client, exception handler
cache/         — Redis caching layer: message buffer (ZSet), session info, user info, Bloom filters
mq/            — RabbitMQ producer/consumer (Outbox pattern)
module/
  auth/        — login, register, token refresh
  user/        — user CRUD, friend management
  session/     — session (chat) lifecycle, member management, session list with cursor pagination
  message/     — send, revoke, listMessages (3-tier cursor pagination), markRead
  upload/      — chunked upload with Redis Bitmap tracking
  websocket/   — Spring WebSocket (transitional/discovery phase)
  netty/       — Netty WebSocket (production path, port 9090)
  notification/— system notifications
task/          — XXL-Job handlers (outbox archive)
```

## Key Design Patterns

### Message Send Flow (Outbox Pattern)

```
HTTP POST /api/messages/send
  → rate-limit (Guava, per-user 10/s)
  → Bloom filter check (session exists)
  → SISMEMBER auth (user in session)
  → Redisson lock (msgNo idempotency)
  → Snowflake ID generation
  → Write ZSet cache (msg:buf:{sessionId})
  → INSERT local_msg_outbox (status=0)
  → RabbitMQ publish (Publisher Confirm → status=1)
  → Return {msgId, msgNo, status="sending"}
```

The consumer side:
```
MQ consumer → async thread pool → Redis dedup → INSERT chat_message_N → UPDATE session.last_msg
  → UPDATE outbox.status=2 → push to online members via WebSocket
  → offline: message already in ZSet buffer, frontend pulls on reconnect
```

### Sharding

`chat_message` is sharded into 16 tables (`chat_message_0` ~ `chat_message_15`) by `session_id % 16` via ShardingSphere MOD algorithm. No other tables are sharded. The shard key (`session_id`) must be present on every query to these tables.

### Message Query: 3-Tier Cursor Pagination

Before direction (scroll up):
1. **Hot ZSet** (`msg:buf:{sessionId}`) — recent messages, 7d TTL
2. **History ZSet** (`msg:history:{sessionId}`) — DB backfill cache, 10min TTL
3. **DB** (`chat_message_N`) — final source of truth, results written to history ZSet

After direction (scroll down): ZSet hit → DB miss (no merging). This avoids complexity since "after" scrolling almost always hits cache.

### Large Group Optimization

When `memberCount >= chat.group.write-fanout-threshold` (default 1000):
- **Write**: fan-out to 4 shard ZSets (`msg:buf:shard:{sessionId}:{0..3}`)
- **Read**: each user reads only their shard (`userId % 4`)
- Prevents single-key hot spots on large groups

### Cache Eviction

Always use `SCAN` + `UNLINK` (not `KEYS` + `DEL`). SCAN avoids blocking Redis single thread; UNLINK does async memory reclamation.

### WebSocket (Production: Netty)

Netty server runs on port 9090, independent of Spring HTTP (8080). Pipeline order:
HttpServerCodec → HttpObjectAggregator → ChunkedWriteHandler → IdleStateHandler → TokenAuthHandler → HeartbeatHandler → WebSocketFrameHandler

Token passed as query param: `ws://host:9090/ws/chat?token=xxx`
Online status tracked in Redis Hash: `ws:online:{userId}` → `{machineId: timestamp}`
Cross-node push via Redis Pub/Sub channel `ws:push:{machineId}`.
Spring WebSocket (`module/websocket/`) is the transitional path — Netty is preferred for production.

### Idempotency

Sending side: Redisson lock on `lock:msg:idem:{msgNo}` + Double Check with `msg:idem:{msgNo}` → msgId mapping (2min TTL). Consumer side: Redis `SET NX` on same key prevents duplicate persistence.

### Refresh Token Login Reuse

`user:login:current:{userId}` in Redis tracks the current valid refresh token hash. On login, if key exists → re-sign access token directly without going through full auth flow.

## Configuration

- `application.yml` — shared config (JWT, Netty, thread pools, upload limits, group thresholds)
- `application-dev.yml` — DB/Redis/RabbitMQ local connection details, ShardingSphere rules, SQL logging
- `application-prod.yml` — production overrides
- `RedisClusterConfig.java` — entirely commented out; uncomment enabling steps documented inline when migrating from standalone to Cluster

## Database

Single MySQL database `chatroom`. Run `src/main/resources/sql/init.sql` to initialize (creates all tables including 16 chat_message shards via stored procedure). MyBatis-Plus logical delete: `deleted_at = NULL` means active.
