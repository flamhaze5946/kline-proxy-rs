# Java → Rust 兼容性补齐与复核（2026-09-28）

本轮发现的实现问题已修复，并完成修复后的独立 review-loop。27 个 Java 业务操作的 719 个主参数用例，加上状态、真实元数据回放和异常上游用例，共 **801 次业务执行全部一致**；未使用上一轮的差异放行名单。工作区 **162 个测试通过**，Clippy `-D warnings` 和格式检查通过。

Java 基线代码未修改，Rust 修复仍在本地，**本轮未部署到实例**。这里的“一致”按下文公开的比较口径判定；容量保护、运行时指标和绘图像素等边界保留明确说明，不代表任意输入下逐字相同。

## 修复范围

- 多周期价格回退使用每个市场的首个配置周期，配置顺序在 Catalog 排序前传入。有效空数组触发回退；上游根 null 返回 Java 的 502/-1000。
- 统计排序补齐 Java HashMap 的冲突、树化、扩容拆分、UTF-16 键比较和重复赋值行为，以隔离的排列索引模块实现。
- 参数绑定支持 Java 接受的 BMP Unicode 数字、原始 JSON 数字转字符串、负零 Boolean、重复参数/重复 key、尾随 JSON 和数字长度限制。
- 普通 K 线非正 limit、倒序窗口和极端有符号运算对齐 Java；只遍历已保留数据，不按恶意 limit 分配内存。
- 多币响应按对应全市场 REST 快照的原始顺序返回，已有效的 WebSocket 数据继续优先使用；不为每个组合新增 REST 请求。
- Actuator discovery/info/health、Prometheus/OpenMetrics 协商与 includedNames、HEAD/OPTIONS/错误响应补齐。磁盘健康由 Rust 实测；业务恢复状态继续使用 `/health/ready`。
- 上游 HTTP、空 body、root null、JSON/DTO 解析异常采用实际 `ClientUtil` / Retrofit / Jackson 调用链行为。CMS 字段模型与 premium 标量转换同时核对。

## 验证口径

主业务比较包含状态码、错误码、数组顺序和所有业务数据字段。忽略 JSON 对象键序、HTTP 错误的文字描述、观察时间 ts_ms/waited_ms；成功响应中的 msg/message、数字类型、价格字符串与数组顺序都比较。Rust 额外 data_status 单独校验。PNG 检查格式和尺寸，不要求不同绘图库逐像素相同。HTTP Content-Type 规范化大小写与空格；捕获器的缺失值和 null 仅在“未返回该响应头”这一层视为相同，不对业务字段作此处理。

管理协议的 host、timestamp、HTML 生成时间和运行时指标值由各自进程生成，不把不同运行时伪装成相同数据。

测试重编译当前 130 个 Java 源文件，实际 controller/router、异常处理器和业务服务参与执行，上游响应与时钟采用固定输入。真实元数据回放使用已抓取的 912 个合约 symbol。三个 Yama 计算结果另有非空断言，避免空数据假通过。

本轮使用 `--strict`，历史 116 项差异名单不再用于放行。每个用例的请求方法、完整参数、body、Java/Rust 状态与结论见 [801 次执行明细](../research/results/api-parity-completion-20260928/parameter-results.json)；各组目录另保存完整原始响应。

| 验证组 | 执行数 | 通过 | 放行或未解释差异 |
|---|---:|---:|---:|
| [27 个业务操作的主参数矩阵](../research/results/api-parity-completion-20260928/final-parameters/summary.json) | 719 | 719 | 0 |
| closed 未齐、停止交易、缺最新、占位缺口、整点前，共 5 组 × 12 | 60 | 60 | 0 |
| [912 个真实合约元数据完整回放](../research/results/api-parity-completion-20260928/final-live-metadata/summary.json) | 1 | 1 | 0 |
| [上游 null 场景](../research/results/api-parity-completion-20260928/final-null-upstreams/summary.json) | 17 | 17 | 0 |
| [CMS/premium DTO 标量转换](../research/results/api-parity-completion-20260928/final-dto-coercions/summary.json) | 4 | 4 | 0 |
| **业务执行合计** | **801** | **801** | **0** |

管理与框架探针单独统计，不混入业务接口用例数：Actuator/HTTP **166/166**、真实 Spring Boot 媒体协商 **177/177**、独立 HEAD 复核 **6/6**，详见 [管理协议报告](../research/results/api-parity-completion-20260928/management/report.md)。其中 36 个成功 Prometheus 响应只比较 HTTP 与格式协议，JVM/Rust 指标名称及样本值分别保留。

独立 Java oracle 还覆盖 HashMap 排列 177 个场景（25,886 次 put、66 个实际树桶场景）、统计公式 15 个场景，以及 Jackson 绑定 149 个场景。这些是补充证据，部分封装在工作区的单个回归测试中，不与 162 个 Rust 测试简单相加。

## 接口与参数覆盖

完整接口说明、参数类型、默认值和绑定规则见 [Java 接口清单](../research/api-parity-20260928/inventory.md)。以下为本轮主参数矩阵；GET 与 POST 分别计为操作，合计 27 个操作、25 条路径。

| 操作 | 主矩阵用例数 | 结果 |
|---|---:|---|
| `GET /fapi/v1/exchangeInfo` | 2 | 全部一致 |
| `GET /fapi/v1/time` | 7 | 全部一致 |
| `GET /fapi/v1/ticker/price` | 19 | 全部一致 |
| `GET /fapi/v1/ticker/24hr` | 24 | 全部一致 |
| `GET /fapi/v1/klines` | 82 | 全部一致 |
| `GET /api/v3/exchangeInfo` | 2 | 全部一致 |
| `GET /api/v3/time` | 2 | 全部一致 |
| `GET /api/v3/ticker/price` | 53 | 全部一致 |
| `GET /api/v3/ticker/24hr` | 80 | 全部一致 |
| `GET /api/v3/klines` | 85 | 全部一致 |
| `GET /fapi/v1/klines/bulk` | 55 | 全部一致 |
| `POST /fapi/v1/klines/bulk` | 45 | 全部一致 |
| `GET /fapi/v1/fundingRate` | 52 | 全部一致 |
| `GET /fapi/v1/premiumIndex` | 12 | 全部一致 |
| `GET /fapi/v1/fundingRate/bulk` | 58 | 全部一致 |
| `POST /fapi/v1/fundingRate/bulk` | 33 | 全部一致 |
| `GET /bapi/composite/v1/public/cms/article/catalog/list/query` | 30 | 全部一致 |
| `GET /bapi/composite/v1/public/cms/article/list/query` | 39 | 全部一致 |
| `GET /statistic/getAltCoinIndex` | 2 | 全部一致 |
| `GET /statistic/getYama01AltCoinIndex` | 2 | 全部一致 |
| `GET /statistic/pic/getYama01AltCoinIndex` | 2 | 格式及尺寸一致 |
| `GET /statistic/getYama02AltCoinIndex` | 2 | 全部一致 |
| `GET /statistic/pic/getYama02AltCoinIndex` | 2 | 格式及尺寸一致 |
| `GET /statistic/getYamaAggAltCoinIndex` | 2 | 全部一致 |
| `GET /statistic/pic/getYamaAggAltCoinIndex` | 2 | 格式及尺寸一致 |
| `GET /hello/helloWorld` | 7 | 全部一致 |
| `GET /hello/whatsMyIp` | 18 | 全部一致 |

## 独立复核与证据

复核找到了 Jackson record 构造完成后重复字段仍被接受、重复字段反复复制 symbols 导致 CPU 放大，以及 HEAD 自动补 `Content-Length: 0` 三项问题；已逐项修复并由发现问题的 agent 复测。记录见 [绑定及热路径复核](../research/api-parity-completion-20260928/review/final-readonly-review.md)、[市场接口复核](../research/api-parity-completion-20260928/market-completion-findings.md)、[最终独立证据审计](../research/results/api-parity-completion-20260928/final-independent-audit.md)。本轮没有留下已发现而未修复的实现阻断项。

最终归档重新比较全部 801 对原始响应，并核对 9 组执行的源码哈希、文件集合和依赖；213 个产品/测试/Cargo 源文件集合无遗漏，Java 的 130 个源文件与上一阶段基线一致。Rust 的状态扩展按固定输入另作 [60 条状态断言](../research/results/api-parity-completion-20260928/bulk-extension-assertions.json)；独立审计还从 fixture 重建主矩阵和状态矩阵中的全部 108 个 data_status，均匹配，并检查 closed_only=false 时不输出该扩展。

- [最终审计摘要](../research/results/api-parity-completion-20260928/evidence-audit.json)
- [工作区测试日志：162 passed / 0 failed](../research/results/api-parity-completion-20260928/rust-tests.log)、[Clippy](../research/results/api-parity-completion-20260928/clippy-final.log)、[格式复核](../research/results/api-parity-completion-20260928/fmt-audit.log)
- [当前源码与 golden fixture 哈希](../research/results/api-parity-completion-20260928/final-workspace-manifest.json)
- [本轮源码补丁](../research/results/api-parity-completion-20260928/changes.patch)、[变化清单](../research/results/api-parity-completion-20260928/changed-files.json)

补丁记录 35 个 `.rs` 或根 Cargo 文件变化。本轮开始的快照未包含 crate 自己的 Cargo.toml 和 golden JSON，因此这些不伪造修改前 diff；新增依赖可直接审阅 [service Cargo.toml](../crates/kline-service/Cargo.toml)，全部当前 manifest 与 golden 已纳入最终哈希。

## 复核中的性能检查

- 200 个客户端各请求 5 个分散 symbol：冷请求共享全市场快照加载，price/24hr 各一次 REST；已有有效 WS 缓存时，400 个两类热请求均无额外 REST，后台 REST 被刻意卡住也不阻塞这些请求。
- 5-symbol、109B POST 参数绑定，release 下 7×100,000 次：旧版中位数 0.507µs，本轮 0.718µs，增量 0.210µs。这只是绑定微基准，不能替代线上响应延迟或 p99。
- 62KB 重复字段反例曾需 182ms；修复为逐字段校验后单样本约 0.426ms，消除了重复复制大型 symbols 列表的放大。
- 736 个实际币种的统计排序阶段微基准中位数约 57.2µs → 34.1µs；这不是整个统计接口耗时。
- 本轮 `bulk.rs` 与基线相同，新增请求绑定路径没有网络或阻塞锁。

性能证据见 [多客户端与配置顺序验证](../research/api-parity-completion-20260928/market-completion-findings.md)、[绑定复核及微基准](../research/api-parity-completion-20260928/review/final-readonly-review.md)、[统计排序验证](../research/api-parity-completion-20260928/statistics/statistics-completion-findings.md)。本轮没有做部署后的 Java/Rust CPU、内存或端点 p99 比较。

## 实际 Java 行为更正

Java Service 中部分 `null ? emptyList` 逻辑在当前客户端下不可达：`ClientUtil.getResponseBody` 先拒绝 null。已撤回上一轮“null→空列表/缓存回退”的判断，保留合法空数组回退。

当前 Java 的 `BinanceErrorResponse` 缺少 Jackson 可用的构造器，因此上游非2xx JSON错误并未被解成内部code/msg，而是以HTTP状态作为code、原始body作为msg返回。本轮按实际执行结果兼容；没有修改Java使基线迁就Rust。

## 边界与保护

- 资金费率无symbol的全市场分块窗口新增1000小时预算，超出返回400/-1130并要求缩小窗口或指定symbol，避免极端负时间导致数万亿次逐小时扫描。这是明确保留的容量保护；Java没有这项保护。
- Rust保留原有请求容量、body大小、上游响应大小及资金费率分页截断保护。
- 实时价格、serverTime、运行时指标及PNG像素不能据此宣称跨实例逐字相同；相同冻结输入的业务计算与协议才是本次对齐目标。
- 验证是有限场景的证据，不等于穷举任意字符串、任意上游数据与所有并发调度。固定日K线fixture以后重跑需更新日期；脚本会拒绝空Yama的假通过。

## 复现

在 Rust 工程根目录执行；需要现有 JDK 21 与 `research/api-parity-20260928/java-dependencies` 中的本地依赖。使用新的输出目录，脚本不会覆盖旧证据。

```sh
cargo test --workspace --offline
cargo clippy --workspace --all-targets --offline -- -D warnings
cargo fmt --all --check
python3 research/api-parity-20260928/parity.py \
  research/results/api-parity-completion-rerun \
  --strict --cases research/api-parity-completion-20260928/cases.json
```

其余 8 个业务组按各自 `source-manifest.json` 中的 fixture/cases 传入 `--fixture` 和 `--cases`；上表链接及 [证据目录说明](../research/results/api-parity-completion-20260928/README.md) 标明对应组。只重算本轮已归档证据，不请求实例：

```sh
python3 research/api-parity-completion-20260928/audit_final_evidence.py
python3 research/api-parity-completion-20260928/management-probe/compare.py
```
