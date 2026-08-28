# 生产级部署基线

这是一套“单应用节点”的生产部署单元：容器内只运行 ChatRoom Backend 与 Nginx，MySQL、Redis、RabbitMQ、对象存储和 XXL-Job Admin 均使用外部服务。高可用环境应在至少两台主机上各部署一套，并放在负载均衡后面。

## 1. 推荐拓扑

```text
                    ┌─ chatroom-node-01 (Nginx + App)
Internet / LB ──────┤
                    └─ chatroom-node-02 (Nginx + App)
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
  MySQL 主从/云 RDS     Redis 高可用/云缓存    RabbitMQ 集群
        │                     │                     │
        └──────────── OSS/CDN、XXL-Job、监控 ──────┘
```

每台应用节点必须拥有唯一 `MACHINE_ID`。同一台主机上不要使用相同 `MACHINE_ID` 启动多个实例，否则在线状态和跨节点推送路由会互相覆盖。

## 2. 上线前置条件

- Linux x86_64/arm64，Docker Engine 24+，Docker Compose v2
- 域名已解析到负载均衡或本机公网 IP
- TLS 证书已放入 `certs/fullchain.pem` 和 `certs/privkey.pem`
- 已初始化 MySQL `chatroom` 数据库及 16 张消息分片表
- 外部 Redis、RabbitMQ 网络可达且已启用认证
- 镜像已由 CI 构建、扫描并推送到镜像仓库
- 主机时间通过 NTP 同步，时区策略明确

## 3. 当前代码的生产阻断项

部署文件提供的是生产基础设施基线，但当前仓库在承诺正式 SLA 前仍应完成以下工作：

1. 实现真正的 `OssClient`，当前 `LocalOssClient` 只写本地磁盘，不支持多节点。
2. 将 `NettyPubSubListener` 注册到 Redis 监听容器，并统一 Spring WebSocket 与 Netty 推送路径。
3. 实现 Netty 上行信令路由；当前 WebSocket 主要用于服务端下行推送和心跳。
4. 补齐自动化测试、数据库版本迁移、指标监控和告警。
5. 对 JWT、上传、限流、消息补偿与数据恢复做安全和故障演练。

在这些事项完成前，建议将其定位为预发布或小流量试运行方案。

## 4. 配置与启动

```bash
cd deploy/production
cp .env.example .env
```

编辑 `.env`：

- 设置真实镜像地址 `CHATROOM_IMAGE`；
- 为当前节点设置唯一 `MACHINE_ID`；
- 填写数据库、Redis、RabbitMQ、JWT、OSS、XXL-Job 配置；
- 用密钥管理系统下发密码，不要在仓库或 CI 日志中打印；
- 根据压测结果调整 JVM 和线程池参数。

启动并观察：

```bash
docker compose --env-file .env pull
docker compose --env-file .env config --quiet
docker compose --env-file .env up -d
docker compose ps
docker compose logs -f --tail=200 app
```

验证：

```bash
curl -fsS http://127.0.0.1/healthz
curl -fsS https://chat.example.com/healthz
```

`/healthz` 目前只证明 Nginx 正常。应用容器的健康检查只验证 8080 端口已监听；生产环境建议增加 Spring Boot Actuator，并区分 liveness、readiness 与依赖服务监控。

## 5. 发布与回滚

镜像必须使用不可变版本号或 Git SHA，不要在生产使用 `latest`。

```bash
# 发布：修改 .env 中 CHATROOM_IMAGE 后
docker compose --env-file .env pull app
docker compose --env-file .env up -d --no-deps app
docker compose ps

# 回滚：把 CHATROOM_IMAGE 改回上一版本
docker compose --env-file .env up -d --no-deps app
```

多节点环境应逐台滚动：摘除负载均衡流量、等待 WebSocket 连接排空、更新并验证、重新挂载流量，再处理下一台。数据库变更必须满足向前/向后兼容，不能把镜像回滚当作数据库回滚。

## 6. 数据与灾备

- MySQL：每天全量备份并持续保存 binlog；定期进行时间点恢复演练。
- Redis：启用主从/哨兵或 Cluster，并根据可接受数据损失设置 AOF/RDB。
- RabbitMQ：使用奇数节点集群和 quorum queue；监控堆积、未确认消息和死信队列。
- OSS：开启版本控制、生命周期策略和跨区域复制（按业务等级选择）。
- 配置与密钥：进入专用密钥系统，保留变更审计，不跟随数据卷备份明文保存。

恢复顺序通常是 MySQL → Redis/RabbitMQ → 应用节点 → 网关。Outbox 和死信表恢复后要先核对幂等键，再执行消息补偿。

## 7. 网络与安全

- 公网只开放 80/443；MySQL、Redis、RabbitMQ 和 XXL-Job 执行器均放在私网。
- 80 仅用于跳转 HTTPS；WebSocket 使用 `wss://`。
- 限制 Nginx 请求体、连接数和速率，并在云防火墙/WAF 再做一层保护。
- JWT 密钥至少 32 字节，定期轮换；数据库和中间件账户遵循最小权限。
- RabbitMQ 管理端、Redis、MySQL 不应直接暴露到公网。
- 容器以非 root 用户运行，应用根文件系统为只读，仅 `/data` 和 `/tmp` 可写。

## 8. 监控与告警建议

至少覆盖：

- JVM：堆、GC、线程数、CPU、容器 OOM；
- HTTP：QPS、P95/P99、4xx/5xx；
- WebSocket：在线用户、连接数、重连率、心跳超时；
- MQ：发布失败、消费延迟、队列堆积、死信数量；
- MySQL：慢查询、连接池、复制延迟、磁盘；
- Redis：命中率、内存、热 Key、延迟、连接数；
- 业务：消息发送成功率、落库耗时、Outbox 待处理数量、补偿失败数量。

日志应输出到 stdout，由宿主机或日志采集器统一接管。本 Compose 已配置单容器日志轮转，防止宿主机磁盘被无限占用。

## 9. XXL-Job

应用内部执行器端口为 9999。若 XXL-Job Admin 不在同一 Docker 网络，需要通过私网地址暴露该端口，并只允许 Admin 节点访问。不要把 9999 开放到公网。

建议注册三个任务：

- `outboxCompensateJobHandler`：补偿未完成 Outbox；
- `outboxArchiveJobHandler`：归档已完成 Outbox；
- `tokenCleanJobHandler`：清理过期刷新令牌。

具体 Cron 应结合数据量和数据库压力设置，首次上线前先在预发布环境验证执行耗时和并发行为。
