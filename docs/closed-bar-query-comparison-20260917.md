**Java / Rust closed bar 入内存与查询对照 — 2026-09-17**

本次直接核对运行实例：Java 1.8.1-low-risk（market.feng.dog），Rust 0.4.6（market-mirror.feng.dog）。两边进程及二进制均未更换或重启。独立请求客户端为东京测试机 192.0.2.10。

本轮 Rust 的主要优势在 bulk 响应。13:00 UTC 合约与现货全部闭合数据可读分别领先约 10.5ms、16.8ms；全合约 bulk 完整响应提前约 95.2ms。12:00 UTC 的全部可读时间基本持平，不能据两轮认为 Rust 每小时固定领先。

**整点 closed bar 可读时间**

“入库”在此指写入内存缓存并发布 final/通知，不是磁盘持久化。Java 采用 CLOSED_BAR_LATENCY.ready_offset_ms_max；Rust 采用 detailed_latency_enabled 下的 closed_bars.max_ms。两者都不等待延后诊断日志写完。各实例使用自己的交易所校正时钟，数毫秒差距不应过度解读。

| 整点 UTC | 市场 | 完整币种数 | Java：整点后 ms | Rust：整点后 ms | Java − Rust ms |
|---|---|---:|---:|---:|---:|
| 12:00 | 合约 | 718 | 163.051 | 164.012 | -0.961 |
| 12:00 | 现货 | 493 | 174.253 | 174.006 | +0.247 |
| 13:00 | 合约 | 718 | 159.533 | 149.007 | +10.526 |
| 13:00 | 现货 | 493 | 136.784 | 120.005 | +16.779 |

两轮两市场均全部收齐，没有未到币种或收齐超时。Java 的最后接收时刻取 Netty 帧回调；Rust 取 ingest 入口。接收埋点不同，因此不将它们相减解释为同口径网络或队列耗时。

**单条缓存提交与处理排队**

| 13:00 UTC 市场 | Java cache p50 ms | Java cache p90 ms | Java cache 最大 ms | Rust commit p99 ms |
|---|---:|---:|---:|---:|
| 合约 | 0.005 | 0.007 | 2.545 | 0.007806 |
| 现货 | 0.005 | 0.008 | 0.143 | 0.007534 |

Java 当前只输出 p50/p90/最大值；Rust 输出 p99，且 Rust commit 含发布通知等工作、Java finalize 单列。不能据此计算严格的写入加速倍率。Java 13:00 合约队列 p90=19.257ms、最大 27.548ms，现货队列 p90=1.918ms、最大 11.836ms。Rust 没有同口径的独立入站队列统计，不将未统计的调度等待当作零。

**13:00 整点全合约 bulk 探针**

718 币 × 最近 10 根，closed_only=true，HTTP/1.1 长连接提前建立，两边在 +10ms 发起；每版只有一条首发探针。

| 指标 ms | Java | Rust |
|---|---:|---:|
| 请求发起至完整响应 | 252.912 | 157.461 |
| 整点至完整响应 | 263.018 | 167.802 |
| 全部可读至客户端完整响应，跨时钟近似值 | 103.485 | 18.795 |
| +2 秒复查请求耗时 | 72.134 | 28.913 |

首发与复查两次，两边 718 币 × 10 根 K 线业务值逐项相同，均 finalized=true、pending=[]，全部包含刚闭合的 12:00 开盘小时线。可读到响应的差值包含唤醒、快照、序列化、HTTP、Nginx、TLS、网络与客户端调度，并有跨时钟误差，不是纯内存查询函数耗时。

Nginx 首发 upstream_response_time 为 Java 250ms、Rust 153ms；+2 秒复查为 69ms、16ms，支持差异并非全部来自测试机到实例的网络。Java 仍承接生产业务，两边总负载不同。四个市场/服务探针同时运行，Rust 另有一条成功现货 bulk；这不是严格相同总负载或 Lambda 洪峰对照。

**13:02–13:04 UTC 近期闭合数据查询**

每个接口/路径/版本 500 个计时样本，另有 20 轮预热。HTTP/1.1 连接复用，禁压缩，交替请求顺序，固定种子 180–300ms 间隔。完整响应体收完即停表，JSON 解析在计时之外。p50/p99 使用 nearest-rank。普通 K 线使用 endTime=13:00 边界−1ms，bulk 使用 closed_only=true。

| 路径 | 查询 | Java p50 / p99 ms | Rust p50 / p99 ms | Rust p99 变化 |
|---|---|---:|---:|---:|
| 本机 HTTP 直连 | 合约 32 币 × 10 根 bulk | 0.929 / 3.837 | 0.389 / 1.193 | -68.9% |
| 本机 HTTP 直连 | BTC 合约最近 10 根 | 0.573 / 0.981 | 0.252 / 0.479 | -51.1% |
| 本机 HTTP 直连 | BTC 现货最近 10 根 | 0.702 / 1.265 | 0.336 / 0.731 | -42.2% |
| 东京 HTTPS | 合约 32 币 × 10 根 bulk | 1.927 / 5.034 | 1.417 / 2.455 | -51.2% |
| 东京 HTTPS | BTC 合约最近 10 根 | 1.274 / 1.964 | 1.135 / 2.060 | +4.9% |
| 东京 HTTPS | BTC 现货最近 10 根 | 1.399 / 2.218 | 1.187 / 1.692 | -23.7% |

本机直连绕过 Nginx、TLS 与跨主机网络，仍包含 HTTP、调度和 JSON 编码；它不是单独缓存读取函数耗时。本机采样运行于服务所在实例，客户端 CPU 时间分别不足采样时长的 1%；东京客户端约 1.1%。两实例直连窗口相差约 12 秒，东京两目标使用同一轮探针交替测量。

共同接口合计 6000 次计时请求全部 HTTP 200、无解析失败；全部请求的根数、最新闭合时间检查通过，bulk 均 finalized 且 pending 为空。各组预热末条及计时末条共有 12 组跨版本业务值对照，均完全一致。500 样本的 p99 接近第六慢样本，仍是短窗口估计。合约单币东京 p99 本轮 Rust 为 2.060ms，略高于 Java 1.964ms，不支持所有端到端指标都更快。

**排除项与实例状态**

最初探针还包含 POST /api/v3/klines/bulk。Java 当前没有该路由，响应 HTTP 500 / No static resource api/v3/klines/bulk；本地控制器路由核对一致。初轮 8000 条计时样本中 1000 条为这一路由错误，初轮整体排除，随后仅针对共同支持接口完整重测。原始错误记录保留，未把错误响应当作性能优势。13:00 的 Java 现货 bulk 首发及复查同样返回 500，故不列现货 bulk 跨版本延迟比较。

13:00 后观察到 Rust 全局 readiness 为 2421/2422、一条序列恢复中；最新闭合小时线收齐及本次查询完整性均单独验证通过。13:06:45 UTC 再查已恢复 2422/2422，14/14 流连接且有数据。此次没有重启、部署或修改应用配置。

**证据与复算**

- [统计与完整分位数](../research/results/closed-bar-query-20260917/summary.json)
- [Java 12:00 分段日志](../research/results/closed-bar-query-20260917/java-1200.log)、[13:00 分段日志](../research/results/closed-bar-query-20260917/java-1300.log)
- [Rust 两轮 closed bar 与 HTTP 诊断](../research/results/closed-bar-query-20260917/rust-performance-after.json)
- [东京共同接口原始请求](../research/results/closed-bar-query-20260917/tokyo/tokyo-supported-layers.jsonl)
- [Java 本机原始请求](../research/results/closed-bar-query-20260917/java/java-supported-layers.jsonl)、[Rust 本机原始请求](../research/results/closed-bar-query-20260917/rust/rust-supported-layers.jsonl)
- [Java Nginx 整点记录](../research/results/closed-bar-query-20260917/java-hour-nginx.log)、[Rust Nginx 整点记录与 readiness](../research/results/closed-bar-query-20260917/rust-hour-nginx-and-ready.txt)
- [Rust 最终健康回读](../research/results/closed-bar-query-20260917/rust-post-check.txt)
- [查询探针](../research/work/closed-bar-query-20260917/probe.py)、[整点探针](../research/work/closed-bar-query-20260917/hour.py)、[复算脚本](../research/work/closed-bar-query-20260917/analyze.py)

在 Rust 工程根目录运行 `python3 research/work/closed-bar-query-20260917/analyze.py` 可重新核对样本、分位数、最新闭合时间及保留响应的业务一致性。
