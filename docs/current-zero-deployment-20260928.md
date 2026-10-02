# Java / Rust 部署记录：2026-09-28

两套生产服务已部署成功。以下时间均为 UTC（迪拜时间加 4 小时）。证据汇总见 [summary.json](../research/results/current-zero-deploy-20260928/summary.json)。

| 项目 | Java | Rust |
|---|---|---|
| 版本 | 1.8.2 | 0.4.15 |
| 域名 | https://market.feng.dog | https://market-mirror.feng.dog |
| 实例 | 192.0.2.30 | 192.0.2.40 |
| 启动时间 | 18:48:19 | 18:49:36 |
| 生产服务 | kline-proxy | kline-proxy-rs |
| 核验结果 | active、健康 UP | active、2456/2456 就绪 |
| 自动重启计数 | 0 | 0 |

## 上线内容

现货和合约仅在本进程收到并接受紧邻上一根 WebSocket `x=true` 后，才能在当前真实 bar 缺失时返回临时零成交量 bar。占位不写入缓存或快照，不进入 closed-only 查询，不跨周期外推；真实数据到达后立即替换。完整规则见 [实现说明](current-zero-bar-20260928.md)。

Rust 同时取消最终帧迟到、缺少当前 bar、stale_tail、静默重订阅及 recheck 提示触发的即时 K 线 REST 修复，等待定时订正。线上已报告 `latest_repair_jobs=0`、`forming_repair_jobs=0`、`tail_rest_policy=scheduled_reconciliation_only`。启动/新序列加载、容量扩展及实际重连恢复仍保留；非 K 线业务和既有显式透传接口不在本次限制范围内。

## 验证结果与范围

- 发布前 Java 227 项测试中 226 通过、1 项原有跳过；Rust 174 项全部通过，Clippy 与格式检查通过。Java jar 的 205 个 class 条目与已测试 class 一致；线上 jar / 二进制 SHA256 与本地产物一致。
- Java 恢复 496 条现货及 732 条合约小时快照。部署前后 732 条合约 1h、727 条合约 1d 的最近两根收盘数据逐字段一致；现货 5 个币种 × 两个周期的最近 10 根收盘数据也一致。
- Rust 恢复 2451 条序列、2,187,679 根 bar，损坏快照为 0；18:54:20 首次采样到 2456/2456 就绪、14/14 连接有数据、`stale_tail=0`、`observed_stale_tail=0`。5 秒采样只能界定就绪时间，不能作为精确切换时刻。
- 东京测试机经 HTTPS 对两市场 × 1h/1d × 5 个币种的最近 10 根收盘数据进行比较，20 组全部一致；Rust 两市场 bulk GET 检查通过。见 [公网数据核验](../research/results/current-zero-deploy-20260928/public-smoke.json)。
- 两端合约 bulk POST 的 `closed_only=true/false` 共 4 项均 HTTP 200、无 pending、每币返回 10 根。最后核验时间 18:56:36。见 [POST 证据](../research/results/current-zero-deploy-20260928/public-post.json)。
- 本次启动日志均未见 WARN/ERROR；本机接口检查 Java 10 项、Rust 12 项通过。见 [Java 状态](../research/results/current-zero-deploy-20260928/java-final-status.json)、[Rust 状态](../research/results/current-zero-deploy-20260928/rust-final-status.json)。

本次覆盖部署、快照恢复、数据一致性及接口冒烟检查；尚未观察部署后的新整点临时补零过程，也未开展性能压测。重启不会恢复历史 `x=true` 授权，必须收到本进程对应的新最终帧后才能临时补零。

## 产物与回滚

Java jar：`/opt/kline-proxy/kline-proxy-1.8.2-zero-dcddee5e229b.jar`，SHA256 `dcddee5e229b1365ea3e36cebaa6d1557c1e5cc404b3a0775c24fd55bee63a07`。旧 1.8.1 jar 保留；一致性快照、原 service 和配置备份位于 `/opt/kline-proxy/deployments/1.8.2-zero-20260928T184801Z`。回滚时恢复备份 service，执行 daemon-reload 并重启服务。

Rust 当前软链接指向 `/opt/kline-proxy-rs/releases/0.4.15`，二进制 SHA256 `6e2bbd82794fa63dc629a3de45f6f9c6bf2c590353057c2e1eab1f33fa6b7097`。旧 0.4.14 release 保留；停止写入后的快照、service 和配置备份位于 `/opt/kline-proxy-rs/backups/0.4.15-20260928T184934Z`。回滚时停止服务，将 current 原子切回 0.4.14，再启动服务。部署经过独立小规模 Linux 预检，未触发回滚。

两端生产配置及 Nginx 配置均保持原样，配置指纹和备份位置记录在 summary.json；Rust 全订阅周期快照继续开启。发布清单见 [release manifest](../research/results/current-zero-deploy-20260928/release-manifest-0.4.15.json)。
