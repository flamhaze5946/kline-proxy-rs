# Java → Rust 0.4.0 移植清单

27 个 Java 业务 HTTP 操作和配套后台功能已迁移。0.3.0 审查确认的 6 项缺陷均已修复；本轮新增四种数值模式、类型保真的持久化、WS 非最终补位及 Java 可用性配置。经过修复、差分验证和重新审查，当前没有已知未解决的迁移阻断项，详见 [review-loop 报告](migration-review-loop-20260916.md)。线上完整容量与实际负载验收仍独立进行。

## HTTP 对照

GET/POST 按操作分别计数；表内路径相对于各组前缀。

| Java 控制器 / 前缀 | 操作 | 数量 | Rust 实现 |
|---|---|---:|---|
| BinanceFutureController / `/fapi/v1` | GET exchangeInfo、time、fundingRate、premiumIndex、ticker/24hr、ticker/price、klines；GET/POST fundingRate/bulk、klines/bulk | 11 | market/http、funding、ticker、klines；service/http、bulk |
| BinanceSpotController / `/api/v3` | GET exchangeInfo、time、ticker/24hr、ticker/price、klines | 5 | market/http、metadata、ticker、klines |
| BinanceCompositeController / `/bapi/composite` | GET v1/public/cms/article/catalog/list/query、v1/public/cms/article/list/query | 2 | market/http、metadata_shape |
| StatisticController / `/statistic` | GET getAltCoinIndex、getYama01AltCoinIndex、getYama02AltCoinIndex、getYamaAggAltCoinIndex，以及后三项对应的 pic/ 路径 | 7 | market/statistics、http |
| HelloController / `/hello` | GET helloWorld、whatsMyIp | 2 | market/http |

Rust 另提供现货 klines/bulk GET/POST、`/health/live`、`/health/ready`、`/health/diagnostics`、`/health/performance`、`/health/market`、`/metrics`，保留 `/actuator/health`、`/actuator/info`、`/actuator/prometheus` 采集入口。

## 后台与兼容功能

- [x] 动态交易目录：元数据刷新、交易状态、正则筛选、周期及标的容量覆盖、上新与下架清理。
- [x] WS：合约 continuous/普通 Kline、现货 Kline、自动分组、稳定 ID、重连补洞、单主题停滞重订阅。低成交量标的不会触发整组恢复重置。
- [x] 资金：单条与 bulk、范围/去重/限额、异步同键加载合并、小时切片缓存、发布宽限、整点预热、:05 重试、Vision 月度 ZIP 历史。
- [x] 普通 Kline：缓存范围查询、Java 无显式范围的回退窗口、spot timeZone/月线 REST 转发。
- [x] 市场数据：exchangeInfo、同步 time、premiumIndex、ticker/price/24hr、现货 MINI/symbols/symbolStatus、WS 与 REST 基线合并。现货使用当前逐标的 @ticker 订阅。
- [x] 统计：AltCoin、Yama01/02/聚合、ATR 定时记录；保留 BigDecimal 截断和 float 累加规则；三张 PNG 使用内置 OFL 字体。
- [x] 持久化：内存与磁盘独立保留、每周期/标的覆盖、原子校验快照、只读导入 Java 日分片、定时落盘、整点避让、优雅退出及下架清理。
- [x] 诊断：收盘到齐、最晚标的/消息、解析/提交/接收时延、逐小时 bulk p99、HTTP histogram、流/时钟/恢复就绪状态。
- [x] CMS、hello/IP；异常与参数边界；配置验证和 Java YAML 转换。
- [x] 并发隔离：辅助 CPU 工作池、限流与有界队列、bulk 独立编码限制、异步缓存加载、取消与关闭处理。
- [x] Linux x86 构建、版本化发布包、独立 systemd 单元、只读影子对照脚本和回滚步骤。

## 验证与实际边界

本轮验证包括 69 项 Rust 测试、7 项配置测试、格式、Clippy 全目标及 release 构建。Java 真实类差分检查：69,991 个 double 数值用例、5,000 个提交用例、8,853 组四模式 REST/WS/补位/提交用例，以及 160 组/20,800 个统计点，差异均为零。路由夹具覆盖全部 27 个 Java 操作；资金并发测试验证 192 个同键请求合并为一次加载，并保持 Kline 可响应。

0.3 的真实全市场和 Linux 实例隔离检查保留在 [旧验证清单](../research/results/full-port-validation.json)，不能代替 0.4 的验证。当前构建和检查范围见 [本轮验证清单](../research/results/migration-validation.json)。全市场检查使用小容量历史，包含只有形成中日线的新上市标的：此类序列可以在 REST 覆盖确认后就绪，但没有已收盘数据时不会伪造快照。验证逐条比对实际可持久化的序列，并核对文件身份与 SHA-256。

明确保留的实现差异：

- 四种数值模式均已移植；double/float 的常见解析与输出保留栈内数值路径，string/BigDecimal 使用共享不可变精确字段。数字长度和指数有限制，异常极端值会明确拒绝。
- 订阅使用 Rust 正则语法；Java 特有的回溯引用、环视等语法会在配置验证时报错。当前生产的 `.*?USDT` 兼容。JVM 线程与入口队列参数不逐项照搬，见 [配置映射](config-migration.md)。
- Rust 严格保留配置容量；Java 的额外 50 条裁剪缓冲不属于保证的历史深度。开启 Java 持久化时的有效容量会由转换器保留。
- PNG 的数据、标题、尺寸兼容，绘图布局不做逐像素复制；Prometheus 使用实际 Rust 指标，旧 JVM 专属面板需调整。
- 非预期上游故障采用结构化 502；部分错误消息文案不保证逐字相同。周/月固定周期规则保留当前 Java 算法。

完整历史容量、至少两个整点、Nginx/TLS 与 Lambda 并发的生产验收尚未执行，本轮未切换生产流量。0.1 性能基线不作为 0.4 的线上收益承诺。部署和回退见 [deployment.md](deployment.md)。
