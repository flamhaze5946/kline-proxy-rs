> 历史报告：以下结论针对 0.3.0。六项缺陷已在 0.4.0 修复；当前结果和新增 review-loop 见 [后续验收报告](migration-review-loop-20260916.md)。原始复现数据保持不变。

Java → Rust 0.3.0 功能等价性复查（2026-09-16）

结论：**27 个 Java 业务 HTTP 操作都有对应实现，但尚未完整等价迁移。确认 6 项缺陷，其中 2 项影响数据正确性，应在替换生产服务前修复。** 上一轮“全部 HTTP 操作及配套后台功能均已完成”的结论过满，应改为“功能入口已覆盖，兼容性验收尚未通过”。

本次基于两个本地工程的代码对照，使用真实 Rust 模块、真实 Java Ticker/ConvertUtil 类和本机模拟上游复现。以下数值是可控测试数据，**不是实例上的线上事故观测**。业务代码和实例配置均未修改。

| 编号 | 级别 | 已确认的问题 | 影响 |
|---|---|---|---|
| R1 | P1 | ticker/price 将十进制字符串转为 f64，再按 Kline 规则格式化 | 改变返回价格；小数可能变成 0 |
| R2 | P1 | 周期补数没有复查最近历史窗口 | 较早的错误收盘数据、已标记 final 的合成补位不能自动纠正 |
| R3 | P2 | 全市场 ticker/price 改用 24hr 数据生成 | 价格来源和合约 time 字段语义与 Java 不一致 |
| R4 | P2 | 把收到部分 WS ticker 当成完整市场基线 | 启动或基线刷新失败时，以 HTTP 200 返回不完整列表 |
| R5 | P2 | Vision 将 404 当成整个月预热失败 | 一个标的缺归档，会丢弃同月其他标的已下载的历史 |
| R6 | P2 | Java 配置转换遗漏开关和参数 | 禁用的市场仍订阅，资金宽限丢失，默认历史深度变化 |

**R1 — 保留价格接口的十进制精度。** [ticker.rs:366](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/crates/kline-market/src/ticker.rs:366) 先解析 f64，再调用最多八位小数的 Kline 输出函数。Java 的 REST Ticker 实际使用 BigDecimal，与生产的 Kline `number.type=double` 无关，见 [BinanceFutureKlineServiceImpl.java:104](/Users/flamhaze5946/Workspace/Java/Personal/kline-proxy/src/main/java/com/zx/quant/klineproxy/service/impl/BinanceFutureKlineServiceImpl.java:104) 和 [ConvertUtil.java:117](/Users/flamhaze5946/Workspace/Java/Personal/kline-proxy/src/main/java/com/zx/quant/klineproxy/util/ConvertUtil.java:117)。现货同样受影响。本次直接调用 Java 实际类，确认如下差异：

| 上游价格 | Java 返回 | Rust 返回 |
|---|---|---|
| `100.50000000` | `100.50000000` | `100.5` |
| `0.000000001` | `0.000000001` | `0` |
| `123456789.123456789` | `123456789.123456789` | `123456789.12345679` |

建议复用市场模块现有的十进制字符串转换函数，不经过 f64；为价格接口单独建立 Java 差分用例。

**R2 — 恢复周期性历史复查。** [recovery.rs:84](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/crates/kline-runtime/src/recovery.rs:84) 在热恢复时从最新 final 开始，只有缺口或 nonfinal 才向前扩展。Java [AbstractKlineService.java:1786](/Users/flamhaze5946/Workspace/Java/Personal/kline-proxy/src/main/java/com/zx/quant/klineproxy/service/impl/AbstractKlineService.java:1786) 按 `rpcRefreshCount` 重查最近窗口，默认 99 根；`safeQueryKlines` 使用 `useSetCache=false`，已存在的 K 线也会重新读取。复现中有连续 11 根 1m 数据，第 6 根上游已从价格 100 修正为 200、成交笔数增至 2，Rust 周期同步只请求 `startTime=540000` 起的最后两根，第 6 根仍是 100。把该根换成 final 合成补位、成交笔数 0，也未纠正。持续正常运行且没有更早缺口时，该错误会一直留到被其他路径重新读取或淘汰。建议保留缺口快速修复，同时增加有界的历史复查窗口，并映射配置项。

**R3 — 全市场价格使用正确的数据源。** [ticker.rs:256](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/crates/kline-market/src/ticker.rs:256) 把 24hr 的 `lastPrice/closeTime` 转成 `price/time`。Java 的全市场价格快照由专用 `/ticker/price` 接口取得。复现上游价格接口为 `100.50000000/time=333`，24hr 为 `99.00000000/closeTime=222`；Rust 全市场请求仅访问 24hr，并返回 `99/time=222`。即使修复 R1，这个数据源差异仍存在。建议建立独立的价格快照与刷新路径。

**R4 — 区分 WS 增量与完整基线。** [ticker.rs:242](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/crates/kline-market/src/ticker.rs:242) 仅在缓存为空时加载基线。复现元数据和 REST 上游均有两个标的，先输入一个标的的 WS 消息，随后调用全市场 24hr：返回 HTTP 200、一个标的，而且没有请求 REST 基线。运行入口同时启动 WS 和 REST，存在这个启动时序；后台基线请求失败也会延长影响。Java 的全市场快照与 WS 增量缓存分开，空快照会加载全市场数据。建议维护独立的基线完成状态，完成全量合并后才发布全市场响应。

**R5 — Vision 单独处理 404。** [vision.rs:65](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/crates/kline-market/src/vision.rs:65) 把所有下载错误加入 `failed`，然后跳过该月全部分片。Java [BinanceVisionFundingHistoryLoader.java:168](/Users/flamhaze5946/Workspace/Java/Personal/kline-proxy/src/main/java/com/zx/quant/klineproxy/service/impl/BinanceVisionFundingHistoryLoader.java:168) 对 404 直接跳过该标的，不标记月份失败。复现两个有效 ZIP 可预热一个小时；仅将其中一个 ZIP 改成 404，Rust 预热结果降为零。这会使新上市等缺少历史归档的标的拖累整月预热，增加后续 REST 补查。其他真实下载/解析错误使月份失败是 Java 原有策略，应保留。

**R6 — 补齐配置映射。** [convert_java_config.py:31](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/scripts/convert_java_config.py:31) 无条件遍历市场配置；Java [AbstractKlineService.java:1178](/Users/flamhaze5946/Workspace/Java/Personal/kline-proxy/src/main/java/com/zx/quant/klineproxy/service/impl/AbstractKlineService.java:1178) 在 `enabled=false` 时不会启动后台服务。可控输入与输出确认：

| Java 输入 | 应保留的语义 | 转换后的实际结果 |
|---|---|---|
| `kline.binance.future.enabled=false`，保留 1h 配置 | 不启动合约订阅 | 仍生成合约 1h 订阅 |
| `funding.publicationGraceMs=500` | 500ms 发布宽限 | 未输出字段，Rust 回退为 50ms |
| 未指定 `minMaintainCount` | Java 默认 365 | 变为 1000 |
| `rpcRefreshCount=123` | 复查窗口为 123 | 字段被丢弃，对应能力见 R2 |

这些是支持配置的兼容性问题，不代表当前实例一定设置了上述自定义值。建议完整列出映射表，对尚不支持的关键字段明确报错，避免静默使用新默认值。

**已有验证能证明什么。** 本次重新运行原有 `all_java_http_operations_are_wired_with_expected_shapes`，仍然通过。它证明路由与部分响应结构可用；其中 [contracts.rs:262](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/crates/kline-market/tests/contracts.rs:262) 恰好把错误的 `100.5` 当成预期，所以不能作为接口等价的依据。之前 69,991 个数值差分针对 Kline double 输出，5,000 个提交差分针对提交规则，40 组统计差分针对计算夹具；这些通过结果仍有效，但没有覆盖以上数据源、调度范围和配置场景。此前真实全市场检查每序列只保留 2 根，并关闭资金后台、Vision、统计后台，不能验收完整生产配置。

除缺陷外，仍有明确范围差异：Java 的非 double 存储模式未移植；PNG 布局和 JVM 专属监控指标未按原样复制；Rust 全局恢复状态会使 bulk 返回 503，属于可用性语义变化。完整历史容量、至少两个整点以及 Nginx/TLS/Lambda 实际并发验收尚未完成。因此本次建议暂缓替换 Java 生产服务，先修复上述 6 项，再执行行为差分和实例影子验收。

本次证据保存在 [审查汇总](../research/results/migration-review-summary-20260916.json)、[Rust 复现输出](../research/results/migration-review-rust-20260916.json)、[Java 价格输出](../research/results/migration-review-java-ticker-20260916.json) 和 [配置转换输出](../research/results/migration-review-config-20260916.json)。复现程序在 [research/review](../research/review/)，仅使用本机模拟 HTTP 上游，不下载市场历史、不连接实例：

```sh
python3 research/review/run.py \
  --java-root /Users/flamhaze5946/Workspace/Java/Personal/kline-proxy \
  --java-build /tmp/kline-rust-evaluation-20260915-java \
  --jdk /Users/flamhaze5946/Library/Java/JavaVirtualMachines/azul-21.0.8/Contents/Home
```

该脚本专门复现 0.3.0 缺陷并校验实现文件指纹；修复后应更新验收断言，不能把“缺陷仍可复现”当作修复成功。
