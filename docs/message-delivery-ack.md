# 消息投递 ACK

## 语义边界

`MESSAGE_ACK` 表示客户端已将消息持久化到本地，不等同于用户已读。已读位置继续由消息已读接口维护。

当前 pending 使用 `(userId, msgId)` 标识，语义是“同一用户任意一个活跃端确认后，该用户的实时投递完成”。客户端必须按 `msgId` 或 `msgNo` 幂等保存；其他设备通过会话游标同步消息。

该键在当前语义下不会造成消息丢失，但不支持以下能力：

- 每台设备分别确认；
- 区分同一消息的多次独立投递代次；
- 对每次投递尝试单独审计。

需要这些能力时再引入 `deliveryId`，当前版本不增加协议复杂度。

## 单条 ACK

```json
{"type":"MESSAGE_ACK","msgId":123,"sessionId":10}
```

服务端根据已鉴权连接取得 `userId`，校验 `msgId` 和 `sessionId` 后，从本地 pending 删除记录。重复或晚到 ACK 按幂等成功处理。

## 批量 ACK

```json
{
  "type": "MESSAGE_ACK_BATCH",
  "items": [
    {"msgId": 123, "sessionId": 10},
    {"msgId": 124, "sessionId": 10}
  ]
}
```

- 默认每批最多 50 条；
- 支持 Spring WebSocket 和 Netty；
- 本地 pending 逐条 O(1) 删除；
- Redis 恢复影子合并为一次 `HDEL` 和一次 `ZREM`；
- 超过上限的批次整体拒绝，客户端应拆批重发。

## 重试

```text
首次发送
  → 1～2 秒后第一次重试
  → 2～4 秒后第二次重试
  → 4～8 秒后第三次重试
  → 仍未确认则关闭连接，客户端重连并按游标补拉
```

默认最多重发 3 次，采用指数退避和 Equal Jitter，单次延迟上限为 10 秒。

## Redis 恢复影子

本地 `ConcurrentHashMap + DelayQueue` 是 ACK 快路径，Redis 只用于节点重启恢复：

```text
ws:ack:pending:{machineId}   Hash，field=userId:msgId，value=pending JSON
ws:ack:deadline:{machineId}  ZSet，score=expireAt，member=userId:msgId
```

写入、更新和删除均由单线程有界队列异步执行，保证同一记录的操作顺序，且不阻塞 WebSocket IO。Redis 故障或异步队列满时，服务端保留本地 ACK 能力，最终由数据库消息和客户端游标补拉兜底。

节点启动时：

1. 按到期时间读取本节点最多 `recovery-batch-size` 条记录；
2. 恢复到本地 pending 和 `DelayQueue`；
3. 等待 `recovery-delay-ms`，给客户端重连留出时间；
4. 在线则继续剩余重试，离线则清理恢复影子并依赖游标补拉。

`MACHINE_ID` 必须在重启前后保持稳定，否则新进程无法找到旧节点的恢复影子。

## 配置

```yaml
chat:
  websocket:
    ack:
      max-retries: 3
      base-delay-ms: 2000
      max-delay-ms: 10000
      max-pending-per-user: 200
      max-pending-total: 10000
      max-batch-size: 50
      shadow-ttl-seconds: 86400
      recovery-batch-size: 10000
      recovery-delay-ms: 5000

thread-pool:
  ws-ack-store:
    queue-capacity: 10000
```

## 稳定性保证

- ACK 热路径不等待 Redis；
- pending 在发送前登记，避免快速 ACK 竞态；
- 重复投递由客户端幂等处理；
- Redis 影子恢复失败不会影响应用启动和实时 ACK；
- 消息正文已经持久化到 MySQL，实时投递最终失败时通过游标补拉恢复。
