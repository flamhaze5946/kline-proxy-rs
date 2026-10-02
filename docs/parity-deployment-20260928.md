# 0.4.13 VPS 部署记录

本轮兼容性修复已部署到 `root@192.0.2.40`，公网入口为 [market-mirror.feng.dog](https://market-mirror.feng.dog)。Java 实例未变更。

| 项目 | 结果 |
|---|---|
| 发布版本 | 0.4.12 → **0.4.13** |
| 切换时间 | 2026-09-28 15:16:34 UTC / 19:16:34 迪拜 |
| 首次观测到全量就绪 | 15:22:16 UTC，距切换约 5 分 43 秒，采样间隔 15 秒 |
| 最终复核 | 15:28:09 UTC，**2456/2456 条序列就绪** |
| WebSocket | 14/14 连接建立且收到数据 |
| 自动重启 | 0 次，PID 194703 |
| 原生端口功能检查 | **43/43 通过** |
| 东京测试机经公网 Nginx/TLS 检查 | **43/43 通过** |
| 与 Java 的已收盘 K 线对比 | **20/20 组、200 根 K 线逐字段一致** |

发布前核对了上阶段验收的 85 个工作区文件。本轮只将 workspace 版本号从 0.4.12 递增为 0.4.13，Rust 实现与 golden fixtures 未再修改。Linux x86_64/glibc 2.28 发布构建使用本机已有依赖离线完成，压缩发布包约 7.1 MB。

先在 VPS 的 `127.0.0.1:1890` 启动隔离预检，使用独立数据目录、BTC/ETH 的现货与合约小时线/日线共 8 条序列。全部就绪后验证两市场最近 10 根 closed bulk 与管理接口，再停止预检进程。正式服务按 SIGTERM 正常停止，备份配置、unit 与停止后的一致持久化快照，原子切换发布目录并启动新二进制。

启动从磁盘恢复 **1228 条序列、1,198,969 根 K 线，损坏数为 0**。沿用原配置的仅 1h 持久化规则，其余启动覆盖与连接代次由 REST/WS 完成恢复。原有 `strict_readiness=false` 保持不变；恢复阶段 `/health/ready` 返回 503，bulk 并未被全局就绪门槛拦截，因此不能把恢复阶段描述为所有窗口已经完整。

最终配置、systemd unit 和 Nginx 站点配置哈希均与部署前一致，端口 1889 的监听 backlog 仍为 1024。未修改 Java 部署、域名或 Nginx 路由。`nginx -t` 通过，最终采集的服务日志没有 WARN、ERROR 或 panic。

功能检查覆盖 ready/Actuator/Prometheus、交易规则、价格与 24h ticker、单币和多币查询、现货与合约 K 线及 GET/POST bulk、资金费率、premium、Yama 数值和 PNG、HEAD 与参数错误。东京测试机另按相同截止时间，对 BTC/ETH/SOL/BNB/XRP 的两市场 × 1h/1d 最近 10 根已收盘 K 线与 `market.feng.dog` 比较，全部相同。

探针初稿误把 JSON `Accept: application/json;charset=UTF-16` 预期为 200；实际 Java 与 Rust 都返回 406。已用两个在线实例复核，并补查 `UTF-16BE` 返回 200，最终 43 项探针采用正确预期。保留初稿结果作审计，未为探针修改产品代码。最初完整市场原始 JSON 的大体积传输已主动停止，最终探针在验证完整响应后归档大响应的 SHA-256、尺寸和结构摘要。

部署后一个观测点的进程 RSS 为 **529.3 MiB**，3 秒 CPU 样本约为单核 **12.3%**。这些是启动后检查期间的样本，不构成 Java/Rust 性能对比，也不是整点或 200 客户端的 p99 测试。

## 发布与回滚

- 当前发布：`/opt/kline-proxy-rs/releases/0.4.13`
- 保留版本：`/opt/kline-proxy-rs/releases/0.4.12`
- 配置与数据备份：`/opt/kline-proxy-rs/backups/0.4.13-20260928T151632Z`
- 新二进制 SHA-256：`a90889182b133329ff6b473ff94167f81da98dbf58131930de37978d6452c084`
- 生效配置 SHA-256：`d31d727e02d3faf615aa0fb186fa4d10c8dbe8d386e2ab6edb035b93bf7094e5`

配置与持久化格式没有升级，常规回滚只需在 Rust VPS 上恢复旧二进制链接；本轮未执行回滚：

```sh
systemctl stop kline-proxy-rs
ln -sfn /opt/kline-proxy-rs/releases/0.4.12 /opt/kline-proxy-rs/current
systemctl start kline-proxy-rs
curl -fsS http://127.0.0.1:1889/health/ready
```

回滚后的进程也需要等待数据恢复与连接就绪。无需默认恢复旧快照而丢失升级后积累的行情；已保留的完整备份用于额外故障恢复。

## 证据

- [部署摘要](../research/results/parity-deployment-20260928/deployment.json)、[构建与源码指纹](../research/results/parity-deployment-20260928/release-manifest.json)
- [切换与备份记录](../research/results/parity-deployment-20260928/activation.json)、[完整就绪采样](../research/results/parity-deployment-20260928/readiness.jsonl)
- [部署前状态](../research/results/parity-deployment-20260928/before.json)、[部署后状态](../research/results/parity-deployment-20260928/after.json)、[最终主机与日志检查](../research/results/parity-deployment-20260928/host-final.json)
- [原生端口 43 项检查](../research/results/parity-deployment-20260928/smoke-local.json)、[公网 43 项检查](../research/results/parity-deployment-20260928/smoke-public-tokyo.json)、[HEAD Java/Rust 复核](../research/results/parity-deployment-20260928/head-reference.json)
- [20 组 Java/Rust closed bar 原始对比](../research/results/parity-deployment-20260928/closed-bar-java-rust.json)

[上阶段兼容性报告](api-parity-completion-20260928.md)保留部署前的 801 次业务差分和 162 个测试结论，其中“尚未部署”描述该阶段结束时的状态；后续部署状态以本文为准。资金费率超过 1000 个小时分块的全市场窗口保护继续保留。
