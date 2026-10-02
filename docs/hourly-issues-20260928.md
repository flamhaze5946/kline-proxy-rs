**整点问题排查结果：已修复并测试 Rust 活动柱补齐排队缺陷。修复已随 0.4.14 于 16:32 UTC 部署，整点验收见[后续部署记录](hourly-deployment-0414-20260928.md)。资金费率空快照来自两边相同的既有配置。**

以下是部署前的排查与回归验证，接续 [16:00 UTC 整点观测](hourly-0413-20260928.md)。采集问题证据时，生产 Rust 为 0.4.13，PID 194703，二进制 SHA-256 为 `a90889182b133329ff6b473ff94167f81da98dbf58131930de37978d6452c084`。该排查阶段未修改实例配置、发布链接或服务进程；后续部署单独留有记录。

**stale_tail 与恢复缓慢的原因**

线上小时收盘柱在整点后约 137 ms（合约）/144 ms（现货）全部到齐，但这不代表每个币种的新一小时活动柱也已到达。就绪检查要求缓存尾部满足新鲜度条件，默认宽限为 15 秒；只有上一小时的终态柱，超过宽限后仍会被标记为 `stale_tail`。这项检查与“最新 closed bar 已终态”是两个条件。

原来的调度遗漏了一类情况：快速补齐任务只在 `needs_final` 为真时启动。如果收盘柱已到、活动柱缺失，则不会启动快速补齐，而是走普通 `Recovery::sync`。线上普通历史同步有整点前 150 秒、后 30 秒的保护窗口，默认只有 4 个 worker，且每次还要重查最多 99 根历史 K 线。因此活动柱缺失会等待历史保护窗口和其他历史任务。

已用真实异步调度器和本地 HTTP 模拟上游复现：先初始化序列，再收到上一周期的 WebSocket final；保持普通同步在 30 秒保护窗口内，让时钟越过 15 秒新鲜度宽限。修复前，即使上游已有活动柱，缓存也无法通过该普通队列及时取到，复现测试失败；修复后，同一个测试通过。

这证实了恢复路径的排队缺陷，与线上“closed 已收齐、latest_repair_jobs=0、stale_tail 持续存在”的记录相符。但旧健康接口没有保存受影响币种明细，所以不能把本次 25→10→2→1 条 stale 序列的每一秒延迟，都确定归因于同一种上游或排队因素。

**已经实现的修复**

- 新增活动柱尾部补齐，单次只查询最新两根 K 线，独立于普通历史同步及其整点保护窗口。
- 活动柱补齐与收盘柱补齐使用独立任务池。前者使用 `Foreground`，后者继续使用 `Urgent`；两者仍遵守同一市场的总限流预算和 429/418 冷却，不绕过限流。
- WebSocket 新数据到达时，可取消正在等限流或在途的尾部 REST；继续使用已有的缓存合并规则，不让迟到 REST 覆盖已收到的有效流数据。
- 只返回旧数据的 REST 不算补齐成功，不伪造活动柱，也不把整段历史标记为完成恢复。活动柱补齐不会替代断线、历史缺口或初始化恢复检查。
- `/health/ready` 增加活动柱补齐任务数、采样时钟、新鲜度宽限和最多 16 条 stale 序列明细，以后可直接看到币种、周期、最后开收盘时间及是否仍缺最新 final。

改动文件：[补齐逻辑](../crates/kline-runtime/src/recovery.rs)、[恢复调度与诊断](../crates/kline-runtime/src/lifecycle.rs)、[回归测试](../crates/kline-runtime/tests/recovery.rs)。未改 API 业务返回、资金费率配置或原有 closed bulk 等待语义。

本修复在观察到 tail 过期后启动补齐，不保证就绪检查永远不出现短暂 503。若 WebSocket 与 REST 都没有新数据，或上游处于限流冷却，仍应如实报告未就绪。目标是避免活动柱恢复被大段历史工作拖延；实际线上恢复时长要在部署后测量。

**验证结果**

| 验证 | 结果 |
|---|---|
| 修复前重现“final 已到、forming 缺失、历史同步被保护窗口挡住” | 失败，已留存日志 |
| 修复后相同场景 | 通过，只发出一次 limit=2 的补齐请求 |
| 上游仅返回旧数据 | 保持 stale，不合成新柱 |
| WebSocket 抢先于在途 REST | 取消补齐，保留流数据 |
| 等待限流期间收到 WebSocket 活动柱 | 取消等待，其他请求仍遵守原冷却时间 |
| 原有收盘补齐、缺口、断线、参数与 API 等工作区测试 | 全部通过 |
| `cargo test --offline --workspace` | **166 passed，0 failed，0 ignored** |
| 修改文件 rustfmt 检查 | 通过 |

本轮新增 4 个针对性回归测试。测试使用本地模拟上游，没有对生产实例执行故障注入，也没有进行新的生产整点验收。

**资金费率告警的核对**

Java 在 **16:00:00.018 UTC** 同样记录了该整点资金费率分块为空；Rust 的对应记录在 **16:00:00.025 UTC**。线上 Java 为 `funding.publicationGraceMs: 0`，Rust 为 `market_api.funding.publication_grace_ms: 0`，没有漏迁移等待参数。

Java 线上配置备注说明，0 毫秒是此前停止使用资金费率过滤条件后主动选择的值，并记录了这样会过早采样、可能缓存不完整快照的代价。因此这项告警不应被归为 Rust 独有问题。本轮保留该业务配置，没有自行增加整点请求等待时间。配置备注只证明这项设置的历史意图，不证明所有当前调用方仍不使用资金费率。

Java 与 Rust 均在整点后 **5 分钟**重取该分块。16:15 UTC 后以相同 `[16:00,17:00)` 时间范围查询，两边返回 **783 个币种、783 条资金费率记录，业务字段全部相等**，没有单边币种和字段差异。这验证的是重试后的最终状态；不是对整点最初几秒数据完整性的保证。

**证据**

- [线上 Rust 配置、版本与健康状态](../research/results/hourly-issues-20260928/rust-inputs.json)、[Java 配置备注及同小时资金费率告警](../research/results/hourly-issues-20260928/java-inputs.json)
- [资金费率差分摘要](../research/results/hourly-issues-20260928/funding-comparison.json)、[Java 原始响应](../research/results/hourly-issues-20260928/java-funding.json.gz)、[Rust 原始响应](../research/results/hourly-issues-20260928/rust-funding.json.gz)
- [修复前失败复现](../research/results/hourly-issues-20260928/repro-before.log)、[完整测试日志](../research/results/hourly-issues-20260928/workspace-tests.log)、[修改前源码指纹](../research/results/hourly-issues-20260928/baseline-manifest.json)
- [线上只读证据采集脚本](../research/hourly-issues-20260928/collect_inputs.py)

线上效果须结合部署后的 `stale_tail_examples`、补齐任务数和 ready 状态实测判断，不能仅以回归测试通过推断 503 已经消除。后续结果见[0.4.14 部署与整点验收](hourly-deployment-0414-20260928.md)。
