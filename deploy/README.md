# 部署目录

本目录提供两套部署基线：

| 目录 | 使用场景 | 中间件 | 对外入口 |
| --- | --- | --- | --- |
| [`personal/`](./personal/) | 本地体验、个人服务器、开发联调 | MySQL、Redis、RabbitMQ 与应用一起由 Compose 启动 | `http://localhost:8080` |
| [`production/`](./production/) | 正式环境的单节点部署单元 | 使用外部数据库、Redis、RabbitMQ、OSS 与 XXL-Job | HTTPS 443、HTTP 80 跳转 |

生产高可用不是在一台机器上多开几个容器。建议在至少两台主机上分别部署
`production` 单元，为每个节点配置唯一 `MACHINE_ID`，再由云负载均衡或独立
Nginx 接入。数据库、中间件和对象存储使用云托管服务或独立高可用集群。

详细步骤见各目录 README：

- [个人级部署](./personal/README.md)
- [生产级部署](./production/README.md)
