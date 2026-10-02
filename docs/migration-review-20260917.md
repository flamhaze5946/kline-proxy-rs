复核对象：Rust 0.4.10 与当前 Java 工作区；2026-09-17 UTC。

2026-09-18 修复跟进：本报告保留 0.4.10 的原始发现。0.4.11 修复和验收结果见 [兼容性修复记录](compat-fix-20260918.md)。

**结论：Java 的 27 个业务 HTTP 操作在 Rust 中均有入口，主要业务模块均已实现；目前不能认定为全部行为等价。** 本轮确认一项配置功能遗漏、一组 HTTP 兼容性问题，以及一项 bulk 完成状态语义变化。下面的结论来自当前源码和新建离线对照实验，并非沿用 0.4.0 的迁移验收结论。

**1. P2：持久化周期白名单没有迁移。**

触发条件：订阅 `1h`、`1d`，开启持久化，但仅在 Java 的 `kline.persistence.future.intervalConfigs` 中列出 `1h`。

| 同一配置意图 | Java | Rust |
|---|---|---|
| 实际执行持久化的周期 | `1h` | `1h`、`1d` |
| 不在持久化白名单中的周期 | 保留查询/订阅，跳过写盘 | 仍写盘 |

Java 的 [isPersistenceEnabledFor](/Users/flamhaze5946/Workspace/Java/Personal/kline-proxy/src/main/java/com/zx/quant/klineproxy/service/impl/AbstractKlineService.java:2161) 明确检查周期是否在白名单内。Rust [配置转换器](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/scripts/convert_java_config.py:55) 把所有订阅周期都加入 retention；[Store::dump](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/crates/kline-runtime/src/storage.rs:155) 也没有周期启用判断，retention 只限制条数。

新实验调用实际 Java 服务的关闭落盘流程，以记录写入周期的文件系统替身观测调用；Rust 则调用实际 Store 并生成两个快照文件。结果见 [原始对照](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/parity-review-20260917/summary.json)。已有部署配置副本也只为 `1h` 启用持久化；本轮未重新读取实例配置，不能把离线写盘数量作为实例实测。

影响是无法保留“仅某些周期落盘”的配置意图，可能增加磁盘写入、占用和恢复读取。这里没有测量增加了多少 CPU 或耗时，也不据此归因此前的长尾。建议为 restore/dump 增加共同的周期选择策略，并让转换器显式保留 Java 白名单；只删除多余 retention 条目还不够，因为没有规则时仍会使用默认条数写盘。

**2. P2：Java 接受的部分请求在 Rust 中失败，且部分错误响应变成纯文本。**

以当前 Java 控制器、实际 Spring 参数绑定和 Rust Router 对照，复现以下差异：

| 请求 | Java | Rust 0.4.10 |
|---|---|---|
| Kline bulk GET，`interval=1h&limit=` | 200，使用默认值 | 400，纯文本 |
| Kline bulk GET，`interval=1h&closed_only=1` | 200，按 true 解析 | 400，纯文本 |
| Kline bulk POST，`{"interval":"1h","limit":"1"}` | 200，接受字符串数字 | 422，纯文本 |
| Kline bulk POST，JSON `null` | 400，JSON，code=-1102 | 422，纯文本 |
| 普通 Kline GET，其他参数有效、`limit=` | 200，使用默认值 | 400，JSON |
| Funding bulk POST，JSON `null` 前后有空格/换行 | 200，与无参数调用相同 | 400，JSON |

普通 `interval=1h` bulk GET 和不带空白的 funding JSON `null` 是对照项，两边均返回 200。上表关注参数接受情况和错误格式，不把 bulk 返回 200 当成完成标记相同的证明。

原因分别位于 [bulk 类型化提取器](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/crates/kline-service/src/http.rs:144)、[普通参数解析](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/crates/kline-market/src/http.rs:142) 和 [funding 的原始字节 null 判断](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/crates/kline-market/src/http.rs:223)。提取器失败发生在业务处理函数之前，因此不会进入现有的 JSON 错误封装。Java 的对应入口见 [BinanceFutureController](/Users/flamhaze5946/Workspace/Java/Personal/kline-proxy/src/main/java/com/zx/quant/klineproxy/controller/BinanceFutureController.java:153)。

影响是旧调用端如果发送空可选参数、兼容布尔值或字符串数字，迁移后会失败；依赖 `{code,msg}` 的错误处理也可能无法解析。带空白的 funding `null` 是同组中的低优先级边界问题。建议统一 HTTP 兼容层：处理可选空值和 Java 已接受的类型转换、解析 `Option<Query>`，并统一提取失败的响应格式。

**3. P2 兼容差异：bulk 的 `finalized` / `pending` 已不再保持 Java 原语义。**

使用同一固定时钟、相同 K 线记录、40ms 等待上限调用两边实际服务：

| 数据状态 | Java | Rust |
|---|---|---|
| 有更早历史，但刚收盘的那根完全缺失 | `finalized=true`、`pending=[]`、等待 0ms | `finalized=false`、该 symbol 进入 pending、实测等待 40ms |
| 刚收盘的那根已 final，返回窗口中更早一根未 final | `finalized=true`、`pending=[]` | `finalized=false`、该 symbol 进入 pending；等待 0ms |

Java 的 [hasNonFinalBar](/Users/flamhaze5946/Workspace/Java/Personal/kline-proxy/src/main/java/com/zx/quant/klineproxy/service/impl/AbstractKlineService.java:594) 只检查刚收盘且已经存在的那根。Rust 的 [needs_final](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/crates/kline-core/src/series.rs:200) 还把已有历史之后缺失的目标根视作待补齐；[响应构建](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/crates/kline-service/src/bulk.rs:257) 又把返回窗口中的任意未 final 记录纳入 pending。

Rust 的判断更保守，有助于发现缺失数据；这不是遗漏行情接口，也不建议单纯为对齐而降低数据保护。但调用端判断“本次整点是否完成”的含义和等待时间已经变化。默认 8 秒配置下，缺失目标根可能消耗整点后的剩余等待预算；这是代码路径推论，本轮实测预算为 40ms。现有 [架构文档](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/docs/architecture.md:40) 仍称缺失记录不进入 pending，与实现不符。建议将“本次收盘完成”和“返回历史窗口完整”分开定义，并补充迁移契约和跨语言测试。

**覆盖情况。**

| Java 功能范围 | 本轮核对结果 |
|---|---|
| 合约 11 个 HTTP 操作 | exchangeInfo/time、funding 单查与 bulk GET/POST、premiumIndex、ticker/price、ticker/24hr、Kline 单查与 bulk GET/POST 均有实现；上述边界差异仍存在 |
| 现货 5 个 HTTP 操作 | exchangeInfo/time、两种 ticker、普通 Kline 均有实现；symbols、MINI、symbolStatus、timeZone/月线转发有实现及现有测试 |
| CMS 2 个操作 | 文章和目录查询、缓存、输出字段裁剪有实现 |
| 统计 7 个操作 | AltCoin、Yama01/02/聚合及三个 PNG 接口均有实现 |
| Hello 2 个操作 | 文本和 IP/转发头查询均有实现 |
| WS 与行情状态 | 普通/continuous Kline、动态目录、订阅、重连、Ping/Pong、乱序和 final 优先规则有实现及测试 |
| REST 恢复 | 冷启动、历史补齐、周期修订、最新 final 补齐、限流和退避有实现及测试 |
| 持久化 | 快照、校验、四种数值模式恢复、Java 旧快照导入、关闭落盘有实现及测试；周期选择遗漏见第 1 项 |
| 资金后台 | 小时缓存、整点预热、:05 重试、Vision 历史预热有实现；funding POST 解析差异见第 2 项 |
| ATR 与数值模式 | ATR 后台统计及 double/float/string/bigDecimal 的完整处理链存在；本轮重跑现有测试，未重跑此前全部数值随机差分样本 |
| 运维接口 | 当前 Java 配置公开的 health/info/prometheus 路径有对应实现；健康判定及指标名称不完全相同，JVM 指标不会出现在 Rust 中 |

业务操作按“方法 + 路径”计数，共 27 个；[逐接口清单](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/parity-review-20260917/route-inventory.json) 已保存。Rust 额外提供的现货 bulk 不计入 Java 覆盖数。

严格历史容量、Rust 正则支持范围、资源上限、PNG 排版、JVM/Spring 专属配置和指标属于已有文档声明的实现差异，不在本轮作为新遗漏重复列出。没有发现整块业务模块尚未实现；接口入口存在不代表所有边界行为等价。

**验证与证据范围。**

本轮重新编译当前 Java 的 130 个源文件，而非使用旧版应用 class 文件；只复用已有依赖 JAR。8 个 HTTP 对照用例使用真实 Java 控制器及 Spring MockMvc 参数绑定、真实 Rust Router；funding 上游结果使用本地替身，因此该组实验验证的是 HTTP 接受/错误行为。2 个 bulk 状态用例调用两边实际服务；持久化选择另做对照。Rust 模拟上游全部位于 loopback，没有访问 Binance，也没有修改或部署线上程序。

重新执行 `cargo test --workspace --offline --locked`：**99 passed、0 failed、0 ignored**；配置转换测试：**7 passed**。这些测试通过，但新增对照用例仍揭示差异，现有测试尚不足以支持“完全迁移”的结论。

代码、输入、原始结果及日志：

- [可重复执行的复核脚本](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/parity-review-20260917/run.py)
- [Java 原始结果](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/parity-review-20260917/java.json)、[Rust 原始结果](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/parity-review-20260917/rust.json)
- [输入配置](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/parity-review-20260917/java-config.json)、[转换结果](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/parity-review-20260917/converted-config.json)
- [本轮测试结果](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/parity-review-20260917/validation.json)、[测试日志](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/parity-review-20260917/workspace-tests.log)
- [审查源码 SHA-256 清单](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/parity-review-20260917/source-manifest.json)；复核结束再次比对，无业务源码变化。

本轮没有覆盖所有 Binance 异常响应、真实新币/停牌事件或每种故障时序，也没有重新做整点性能压测。上述结论是当前功能覆盖和已复现兼容性差异的审查结果。建议先修复持久化白名单与 HTTP 兼容层，再明确 bulk 完成标记的契约并补上相应对照测试。
