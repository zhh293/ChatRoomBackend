# 个人级部署

这套方案适合本地体验、开发联调或一台 2 核 4 GB 左右的个人云服务器。它会启动：

- MySQL 8.4
- Redis 7.4（AOF 持久化）
- RabbitMQ 4.3（带管理界面）
- ChatRoom Backend
- Nginx（统一代理 API、Netty WebSocket 和本地文件）

## 1. 前置条件

- Docker Engine 24+，Docker Compose v2
- 建议至少 2 CPU、4 GB RAM、10 GB 可用磁盘
- Linux、macOS 或启用 Linux 容器的 Docker Desktop

## 2. 启动

在项目根目录执行：

```bash
cd deploy/personal
cp .env.example .env
```

编辑 `.env`，至少替换 MySQL、Redis、RabbitMQ、JWT 和 XXL-Job 的示例密码，然后启动：

```bash
docker compose --env-file .env up -d --build
docker compose ps
docker compose logs -f app
```

访问地址：

- API：`http://localhost:8080/api/v1`
- WebSocket：`ws://localhost:8080/ws/chat?token=<accessToken>`
- 网关存活检查：`http://localhost:8080/healthz`
- RabbitMQ 管理界面：`http://127.0.0.1:15672`

如果修改了 `APP_PORT`，请同步修改 `PUBLIC_BASE_URL`。

## 3. 常用操作

```bash
# 查看全部日志
docker compose logs -f

# 重建应用，不重建中间件数据
docker compose up -d --build app gateway

# 停止服务，保留数据卷
docker compose down

# 删除服务和全部个人环境数据（不可恢复）
docker compose down -v
```

最后一条命令会删除 MySQL、Redis、RabbitMQ 和上传文件的数据卷，只应在确认不再需要数据时执行。

## 4. 初始化与升级

`init.sql` 只会在 MySQL 数据卷第一次创建时自动执行。后续 SQL 结构变更不应通过反复修改初始化脚本完成，建议引入 Flyway/Liquibase 后执行版本化迁移。

如果首次初始化失败，可查看：

```bash
docker compose logs mysql
```

个人测试环境需要重新初始化时，先确认数据可丢弃，再运行 `docker compose down -v` 后重新启动。

## 5. 资源调优

低内存机器可将 `.env` 中的 `JAVA_OPTS` 改为：

```dotenv
JAVA_OPTS=-Xms128m -Xmx512m -XX:+ExitOnOutOfMemoryError -Dfile.encoding=UTF-8
```

个人方案为了便于调试，把 MySQL、Redis 和 RabbitMQ 端口绑定到了 `127.0.0.1`。不要把这些端口直接暴露到公网。
