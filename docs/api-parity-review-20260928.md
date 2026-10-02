# kline-proxy Java → Rust 接口与数据一致性核验（2026-09-28）

> 本页保留上一轮的历史结果，当前结论以 [补齐与复核报告](api-parity-completion-20260928.md) 为准。特别更正：上一轮将上游根 `null` 视为空列表是不正确的；真实 Java `ClientUtil` 会先抛出 `502/-1000`。本轮已按实际调用链纠正。

已按当前 Java 源码梳理 **27 个业务 HTTP 操作、25 个路径**，并单列 Actuator 和框架行为。完成 **641 个参数场景**：525 个状态码、响应结构与业务字段一致；116 个保留差异已逐例登记，0 个未解释差异。**这些数字不代表所有可能输入都完全等价**。

本轮已修复 Rust 代码中的实际缺陷，涉及元数据类型、周/月 K 线窗口、bulk 标的解析、HTTP 参数绑定、空行情回退、funding、premium 和 CMS；131 项 Rust 测试通过，Clippy `-D warnings` 通过。**代码修复尚未部署；本轮线上观察的是 Rust 0.4.12 与 Java 1.8.1。** Java 源码及实例配置未修改。

完整参数清单（含默认值、上下界、CSV/JSON 类型、缺参/空白/null、重复参数和异常）见 [接口与参数逐项说明](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/api-parity-20260928/inventory.md)。每一条请求、两边状态码和差异原因见 [641 条测试明细 CSV](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/api-parity-20260928/parameter-results.csv)；原始 JSON 响应见 [Java](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/api-parity-20260928/final-parameters/java.json)、[Rust](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/api-parity-20260928/final-parameters/rust.json)。

## 验证结果与口径

| 验证层次 | 覆盖 | 结果 |
|---|---|---|
| 当前源码编译 + 两边真实 controller/router 与业务服务 | 27 操作、641 参数场景 | 525 一致；116 已解释；0 未解释 |
| bulk 数据状态 | 未 final、停止交易、缺最新根、流式缺口占位、整点前 1ms；各 12 请求 | 60/60 共用字段与 K 线内容一致；Rust `data_status` 另行检查 |
| 线上元数据回放 | 采集的 912 个合约，进入两边当前元数据转换路径 | 完整输出一致，`multiplierDecimal` 全部恢复为整数 |
| 日期无关统计 oracle | 7 组普通、稀疏、缺 BTC、并列、空数据等输入 | Java/Rust 统计数值精确一致；另有 1 组合成 Hash 冲突差异，见后文 |
| 实例只读检查 | 27 操作 × Java/Rust = 54 请求 | Rust 27 个 200；Java 24 个 200、3 个字体错误 500 |
| Rust 回归 | 131 tests + fmt + Clippy | 通过 |

业务差分重新编译当前全部 130 个 Java 主源码文件，使用本机现有 JDK 21 / Spring 6.1.15 / Jackson 2.15.4 依赖；Rust 调用当前实际 router。只将外部行情、时钟及限流节奏替换为固定输入；K 线两边均走实际 stream 写入入口，包含缺口占位处理。管理端点另由隔离的真实 Spring Boot context 验证，不冒充业务 MockMvc 已覆盖 Actuator。

一致性比较忽略 JSON 对象键序、响应 content-type 大小写/空白、观察时间 `ts_ms/waited_ms` 和 HTTP 错误的文字 `msg`。**不忽略成功响应的 `message/msg`、价格小数字符串精度、数组顺序、状态码或错误码。** 数组顺序不同必须逐币检查行内容后单列；Rust 额外 `data_status` 保留在结果中并独立校验。PNG 验证有效 PNG 和 1024×768 尺寸，不要求不同绘图库的像素一致。

本轮还修正了验证工具的两个伪差异来源：MockMvc URI 中 `+` 的 Servlet 解码，以及默认 Latin-1 解码 JSON 的问题。统计 fixture 使用当前日期的有效日 K 线，并强制断言三个 Yama 输出非空，避免“两个空结果相同”的假通过。

## 27 个操作的覆盖数量

| 方法与路径 | 场景数 | 一致 | 已解释差异 |
|---|---:|---:|---:|
| `GET /fapi/v1/exchangeInfo` | 2 | 2 | 0 |
| `GET /fapi/v1/time` | 2 | 2 | 0 |
| `GET /fapi/v1/ticker/price` | 19 | 19 | 0 |
| `GET /fapi/v1/ticker/24hr` | 19 | 19 | 0 |
| `GET /fapi/v1/klines` | 76 | 60 | 16 |
| `GET /api/v3/exchangeInfo` | 2 | 2 | 0 |
| `GET /api/v3/time` | 2 | 2 | 0 |
| `GET /api/v3/ticker/price` | 43 | 37 | 6 |
| `GET /api/v3/ticker/24hr` | 75 | 70 | 5 |
| `GET /api/v3/klines` | 85 | 69 | 16 |
| `GET /fapi/v1/klines/bulk` | 41 | 35 | 6 |
| `POST /fapi/v1/klines/bulk` | 34 | 24 | 10 |
| `GET /fapi/v1/fundingRate` | 52 | 40 | 12 |
| `GET /fapi/v1/premiumIndex` | 12 | 12 | 0 |
| `GET /fapi/v1/fundingRate/bulk` | 54 | 47 | 7 |
| `POST /fapi/v1/fundingRate/bulk` | 25 | 19 | 6 |
| `GET /bapi/composite/v1/public/cms/article/catalog/list/query` | 30 | 17 | 13 |
| `GET /bapi/composite/v1/public/cms/article/list/query` | 39 | 20 | 19 |
| `GET /statistic/getAltCoinIndex` | 2 | 2 | 0 |
| `GET /statistic/getYama01AltCoinIndex` | 2 | 2 | 0 |
| `GET /statistic/pic/getYama01AltCoinIndex` | 2 | 2 | 0 |
| `GET /statistic/getYama02AltCoinIndex` | 2 | 2 | 0 |
| `GET /statistic/pic/getYama02AltCoinIndex` | 2 | 2 | 0 |
| `GET /statistic/getYamaAggAltCoinIndex` | 2 | 2 | 0 |
| `GET /statistic/pic/getYamaAggAltCoinIndex` | 2 | 2 | 0 |
| `GET /hello/helloWorld` | 2 | 2 | 0 |
| `GET /hello/whatsMyIp` | 13 | 13 | 0 |

普通 K 线覆盖全部 16 个 interval、limit 缺省/空/1/0/负/极大/溢出、起止时间单边/双边/倒序/非整周期边界、未知币、重复参数，以及现货 timeZone/1M 的 REST 透传分支。两个 bulk 的 GET/POST 分开验证，另外覆盖 JSON 根类型、标量转换、重复 key、Boolean 的 query/body 不同语法。ticker 覆盖单币/多币/全市场、FULL/MINI、status 过滤和参数冲突。其余详细场景见参数清单与 CSV。

## 已修复的代码问题

| 问题 | 修复后的行为 |
|---|---|
| 合约元数据 `multiplierDecimal` 等 DTO 字段保留了 Binance 原始字符串类型 | 按 Java Integer/Long/Boolean/String/BigDecimal 模型转换；非法类型拒绝发布。线上 912 个合约都能复现的类型差异，回放后全部对齐。 |
| 周线/月线缓存回退重新按 Unix epoch 对齐窗口，可能漏掉最后一根 | 直接以实际最新 openTime 为终点计算回退窗口；新增周/月非 epoch 对齐回归。 |
| NBSP 等字符被 Rust `trim` 清除，bulk 从指定未知币变成全市场 | 使用 Java isBlank/trim 规则，未知币仍是未知币，不再扩大请求范围。 |
| 重复 query、特殊数值格式与 Java 绑定不同 | String 参数按逗号合并；数值/Boolean 取首值；支持 Java query 内部空白、hex、范围校验；JSON 数字另按 Jackson 语义处理。 |
| JSON 数组误绑定 DTO、重复 key 拒绝、部分 scalar String/Boolean/整数转换不符 | POST 根只允许对象或 null；重复 key 后值覆盖；补标量转 String、null 文本、浮点截断前范围验证。 |
| 普通 K 线空 symbol/interval 与 spot REST 透传预处理不同 | 允许显式空 String 进入相同业务分支；spot timeZone/1M 先透传，原始有符号 limit 不被本地 clamp。 |
| Spot ticker 校验优先级不同 | 先验证 type、symbolStatus，再处理 symbol/symbols 冲突与数组内容；明确业务错误码一致。 |
| price/24hr 合法 null/空响应未沿用 Java 回退 | price 可从最后一根内存 K 线取 close；24hr 回退其缓存，1 天过期；新 K 线不会被缓存的空响应冻结。真正解析/网络错误继续返回错误。 |
| funding 普通接口使用无符号 limit，null 上游失败 | 改为 Java Integer 范围并原样转发负值；null→[]；bulk JSON symbol 转换和空白处理对齐。 |
| premium null 被变成 {}；CMS 整数未验证、空 catalogId 被拒绝 | premium null→[]；CMS 按 i32 验证并规范化分页参数，空 catalogId 保留；null 列表项保持 null。 |
| Actuator discovery 缺失、Prometheus includedNames 未过滤 | 补 `/actuator` discovery 与指标名过滤，保留 Rust 运行时指标和 readiness 语义。 |

修改集中在 10 个产品源码文件，新增 4 个测试文件；没有改 Java 基线。可审查 [完整修改补丁](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/api-parity-20260928/changes.patch) 和 [变更文件列表](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/api-parity-20260928/changed-files.json)。

## 116 个明确保留的参数差异

| 类别 | 场景数 | 原因 |
|---|---:|---|
| client-error-status | 92 | Java的全局异常处理将参数/JSON绑定错误转成500/-1000；Rust返回400并保留参数错误码。 |
| nonpositive-kline-limit | 6 | Java普通K线的非正limit会报500，或在i32最小值时溢出后返回大窗口；Rust延续下限1保护。 |
| empty-kline-range | 4 | 起止时间倒置，或对齐后无可用开盘时刻：Java subMap抛500；Rust返回空数组。 |
| symbol-array-order | 5 | 多币查询行内容相同；Java使用上游顺序，Rust保持请求顺序。 |
| strict-json | 4 | Java忽略首个JSON数组后的剩余文本；Rust要求完整合法的JSON，重复symbols合并产生两个数组时同样拒绝。 |
| json-leading-whitespace | 2 | Java Serializer先检查首字符[，因此拒绝前导空格；Rust接受JSON标准允许的外围空白。 |
| funding-timestamp-guard | 3 | Rust拒绝负资金费率时间戳和接近i64上界的溢出风险；Java未验证这些范围。 |

上述放行名单逐用例固定，校验状态码/错误码；多币顺序差异必须满足排序后逐字段相同，limit 下限保护必须等于该市场 `limit=1` 控制请求，空窗口必须返回 `[]`。没有“所有不相同都忽略”的规则。名单和原始差异见 [已解释差异](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/api-parity-20260928/documented-differences.json)、[最终差分摘要](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/api-parity-20260928/final-parameters/summary.json)。

## 实例上是否相同

采样使用相同 URL/参数。已收盘 K 线限定同一结束时间；动态接口的两个实际请求并非同一市场快照。采样记录含开始/结束时间、原始 body 与 SHA-256；本轮不是性能压测。

| 数据 | 本轮线上观察 |
|---|---|
| 合约与现货普通 1h K 线 | BTC 各最后 10 根逐字段一致 |
| 合约 bulk GET/POST | BTC、ETH、SOL、ADA、DOGE 各 10 根，逐字段一致 |
| 普通资金费率 | BTC 固定最近 24h 窗口内 3 条完全一致 |
| 资金费率 bulk GET/POST | 去观察时间后内容一致 |
| 两个 CMS | 内容一致 |
| Yama01/Yama02/Agg | 各 31 点，共 93 点逐值完全一致 |
| AltCoin | 两边 {}；只能说明返回相同，不能证明上游图表可用 |
| Spot exchangeInfo | 去 serverTime 后所有 3713 个 symbol 与其他字段一致 |
| Future exchangeInfo | 912 个 symbol 存在 multiplierDecimal 字符串/整数差异；已修本地代码，回放全字段一致，线上尚未更新 |
| time、ticker/price、ticker/24hr、premiumIndex | 有不同时间快照的值差异。Rust可读有效WS缓存，Java显式symbol会先读REST；这不是同一冻结输入下的业务计算差异。premium上游time相差14,997ms。 |
| 三个统计 PNG | Java 500：Fontconfig head is null；Rust 200、1024×768。Java字体环境问题，不是Rust缺功能。 |
| helloWorld / whatsMyIp | hello相同；IP随实际代理/出口头不同；同一注入头的离线测试一致 |

原始实例证据：[54 个请求及状态](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/api-parity-20260928/live-before/results.json)。元数据修复回放：[912 合约对照](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/api-parity-20260928/final-live-metadata/summary.json)。

## bulk finality 的准确含义

`finalized=true` 延续 Java 语义，只说明“已存在且应等待的刚收盘根”没有 pending，**不能单凭它判断最新根齐全**。本轮同状态测试确认：

- 最新根存在但未 final：两边 pending 包含该 symbol；Rust `window_finalized=false`。
- 最新根完全缺失、没有后续新根：两边 legacy finalized 仍可能 true；Rust `missing_latest` 明确列出 symbol。
- 后续 WS 新根到达并跨过缺口：两边生成同样的零成交占位根，尚非 final；pending 不会被误清空。
- 停止交易的未 final 根：两边列入 `not_trading` 并不等待。
- 整点前 1ms：最后一根的 closeTime 已满足 closed_only 时间过滤，但未必收到 x=true；Rust额外状态区分这一点。

五组状态各 12 个 GET/POST 请求全部对齐。后台等待唤醒、连接恢复、429及持久化等状态机还由现有工作区回归覆盖；本轮没有重新执行数小时线上观察。

## 明确保留的边界与限制

1. **多 interval 且 REST 价格返回合法空值时**：Java取配置Map首个interval，Rust确定性选择最短 tracked interval，可能取到不同 close。单 interval 无此差异；本轮没有把配置Map遍历顺序耦合进价格服务。回退价格也不保证新鲜，合约 time 沿用Java当前时钟语义，不能当价格成交时间。
2. **统计并列排序的合成极端样本**：16个同Java String hash的symbol且成交量并列时，Java HashMap树化改变top N选择，Rust的稳定桶顺序可能给出不同Yama02。已保留复现输入和双方数值，不把这一例说成通过；线上93点和7组常规/稀疏样本相同不证明未来任意币种集合都相同。见 [合成冲突反例](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/api-parity-20260928/statistics-adversarial-case.json)。
3. **更少见的输入语法**：Java可接受部分Unicode数字文本；Rust普通整数文本限ASCII。JSON数字被转为String时可能丢原始写法（如1e3、-0），不是所有非标准标的输入都严格等价。本轮表中的已测试等价类完整登记，不宣称穷举任意字符串。
4. **运行时管理协议**：Rust info增加version；health检查标准、指标名/值、OpenMetrics协商、OPTIONS/404/405错误包络与Spring不同。Rust额外的现货bulk和health诊断接口不是Java迁移缺项；Java没有现货bulk。
5. 图片比较不要求逐像素相等；外部503、限流和任意上游异常组合不可能由有限fixture全部证明。当前固定统计fixture日期为9月28日，以后重新运行需更新日K线日期；脚本会拒绝将空Yama结果当通过。

## 复现与证据

在 Rust 工程根执行（输出目录须未存在）：

```sh
python3 research/api-parity-20260928/parity.py research/results/api-parity-rerun
cargo test --offline --workspace
cargo fmt --all --check
cargo clippy --offline --workspace --all-targets -- -D warnings
```

差分工具 [parity.py](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/api-parity-20260928/parity.py) 会重新编译当前Java/Rust，保存原始响应、上游Java调用记录和源码/依赖SHA-256，并拒绝验证期间源码变化。构造数据和逐用例规则与结果一起保留；最终业务参数、状态组和真实元数据回放均使用最终源码。

[Rust测试日志](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/api-parity-20260928/rust-tests.log) · [Clippy日志](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/results/api-parity-20260928/clippy-final.log) · [Java接口机器清单及探针](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/api-parity-20260928/inventory.json) · [市场细项](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/api-parity-20260928/market-findings.md) · [资金/统计细项](/Users/flamhaze5946/Workspace/Rust/Personal/kline-proxy-rs/research/api-parity-20260928/funding-stats-findings.md)
