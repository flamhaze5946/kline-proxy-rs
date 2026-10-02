Rust 0.4.12 补充验收报告，2026-09-18 UTC。五项补充工作已完成：实际持久化白名单部署、完整路由差分、组合故障测试、两轮整点及超过两小时运行观察、文档更新。新版稳定期 1580 个 Rust bulk 请求全部通过窗口、最终状态和数据检查；最终仍为 2422/2422 条序列就绪，14/14 条 Kline 连接正常，进程没有重启。

**本次线上性能是当前实例各自实际负载的观测，不是等负载的语言性能基准。** 07:00 这一分钟，Java 除本次 200 个测试请求外，还有 1040 个额外业务请求，其中 394 个在第一秒完成；Rust 没有对应额外流量。两端监听队列、TLS 证书链、运行时内存配置也不同。因此不能把响应或 CPU 差值直接归因于语言，或用它推导相同吞吐下的性能倍数。

| 项目 | 当前结果 | 可复核证据 |
|---|---|---|
| 部署与配置 | 镜像实例已升级 0.4.12；实际 Java 白名单为 future/spot 的 1h，已同步；保持现有容量和网络参数 | [部署记录](../research/results/completion-20260918/deployment-0412.json)、[白名单来源](../research/results/completion-20260918/whitelist-source.json) |
| HTTP 行为差分 | 当前 Java 的 130 个源码文件重新编译，137 个请求覆盖全部 27 个业务操作；132 个归一化一致、5 个明确差异、0 个未解释差异 | [差分结果](../research/results/completion-20260918/parity-final-v0412/summary.json)、[独立路由核对](../research/results/completion-20260918/route-coverage.json) |
| 组合故障 | 429 + 等待收盘 + WS 补齐；断线 + 429 + 慢 REST；写盘失败 + 重试 + 损坏快照拒载，均通过 | [完整检查日志](../research/results/completion-20260918/check.log) |
| 持续与整点验收 | 稳定期每端 1580 个 bulk 请求，包含两轮各 200 客户端；Rust 观察 2 小时 2 分 38 秒 | [最终复算](../research/results/completion-20260918/analysis-final.json)、[数据核验](../research/results/completion-20260918/live-final-summary.json) |
| 文档 | 已对齐实际 ticker 来源、缓存期限、bulk 专用线程、PNG 尺寸和白名单部署规则 | [运行](runtime.md)、[架构](architecture.md)、[部署](deployment.md) |

本次扩展差分额外发现并修复两处边界：统计 PNG 原来为 1200 × 600，现对齐 Java 的 1024 × 768；原始资金费率接口现在保留显式空白 `symbol` 参数，只对非空白值做本地标的校验。另为 `/health/ready` 增加未就绪原因计数，便于区分初始恢复、连接代次、历史缺口与尾部过期；没有改变就绪判定。

本地最终检查包括 107 项 Rust 测试、8 项 Python 配置转换测试、格式检查、Clippy warnings-as-errors、原生与 Linux release 构建。第三方依赖锁定项没有变化。[验收清单](../research/results/completion-20260918/validation-0412.json)、[源码差异](../research/results/completion-20260918/change-0412.diff)、[源码与依赖哈希](../research/results/completion-20260918/parity-final-v0412/source-manifest.json) 可追溯最终输入。

组合故障在可重复的隔离测试中执行。写盘测试通过让目录路径被普通文件占用触发失败，检查旧快照及脏状态保留、恢复路径后重试成功，以及截断快照被校验拒绝；线上采样则检查真实进程、连接、数据、文件和资源表现。

差分使用相同的离线上游夹具，运行实际控制器、缓存、资金与统计业务服务。比较状态码、响应类型与业务字段；仅归一化对象键顺序、观测计时、错误文案和 content-type 大小写/空格，Rust `data_status` 扩展单独留存。三种 PNG 验证签名和尺寸，不比较像素。

| 明确保留的差异 | Java | Rust | 对常用近期查询的影响 |
|---|---|---|---|
| 普通 Kline 的 limit 为 0 或负数，两个市场共 4 个用例 | 当前返回 500 | 钳制为 1 | limit=10 不受影响 |
| 现货多币 price 的一个顺序用例 | 保留上游行顺序 | 跟随请求 symbol 顺序 | 按 symbol 取值时一致 |

其他已确认的运行边界仍见 [配置迁移](config-migration.md) 与 [架构](architecture.md)：WS 优先行情、过期上限、严格容量、正则与错误边界、只读 Java 快照导入、非 JVM 监控、图像重新绘制等均有说明。有限夹具覆盖具体测试输入，不代表穷尽所有输入。

06:46 UTC 又对真实域名逐个抽查 27 个共同操作。Rust 27/27 返回 200，三种 PNG 均为 1024 × 768，已目视检查其中一张的标题、曲线和坐标。Java 24/27 返回 200，三种 PNG 均返回 `Fontconfig head is null, check your fonts or fonts configuration`；这是实例字体配置问题。对已收盘 Kline、资金、bulk 与统计数据共 11 组稳定业务结果比较，全部相等，三个 Yama 序列各 31 点。AltCoin 两端均为空对象，不能据 HTTP 200 宣称这项上游数据完整可用。[线上抽查与比较摘要](../research/results/completion-20260918/live-contracts/summary.json)、[逐请求结果](../research/results/completion-20260918/live-contracts/results.json)。

0.4.12 在 06:24:31.247 UTC 激活，06:30:10.488 UTC 全部就绪，恢复约 5 分 39 秒。2422 条序列、14 条行情连接，PID 36832，自动重启次数 0，监听 backlog 1024。配置 SHA-256 在 0.4.11→0.4.12 之间保持不变。旧二进制及其对应配置均已保留，部署脚本包含就绪失败时的回退。

负载来自东京实例 `192.0.2.10`；每个服务端 200 个 HTTP/1.1 客户端，每个客户端请求共同 718 个合约标的中的 5 个分散 symbol、最后 10 根已收盘小时线。请求保留完整响应并检查标的数、根数、最新 open time、finalized/pending，以及两端 Kline 内容哈希。持续阶段每分钟每端 10 个请求。此模型复现请求形态，不包含真实 AWS Lambda 容器启动。

客户端固定源站 IP，使用域名 SNI 并完整验证 TLS 证书；HTTP 使用 `Accept-Encoding: identity`。响应耗时从发出请求计到读完响应体，JSON 业务校验随后执行。它测量的是该请求形态下的服务表现，不包含 DNS、Lambda 启动或客户端 JSON 解析的完整调用链。

两台服务主机均为 2 vCPU、约 4 GB RAM、Skylake 型号。Java 服务监听队列实测 100，Rust 为 1024。因此这是当前部署的比较，不能把所有差异归因于语言。

首轮 06:00（Rust 0.4.11）冷 TLS 使用 Go 1.17.6 交叉编译客户端，客户端两核饱和，p99 约 6.6 秒。CPU 剖析约 90% 落在 TLS/证书验证；应用侧并未出现同等时长。升级客户端到 Go 1.27.1 后明显改善，但 200 条冷连接仍占满测试机两核，系统 OpenSSL 交叉测试也受集中客户端 CPU 限制。实际证书路径也不同：Java 发送 3 张证书，Rust 发送 4 张，Rust 验证路径含更多 P-384 验签工作。冷 TLS 样本和 CPU profile 均已保留；07:00、08:00 使用已建立连接评估突发请求，客户端计时段 CPU 分别为 0.072/1.033 秒和 0.061/0.313 秒，未再次占满两核。

06:14 校准中的 Java 约 1 秒尾部与同一秒的 12 次主机级 ListenOverflows 同时出现。对应 Nginx upstream connect 耗时提供了请求级证据；主机计数本身不能确定具体端口。

同批 200 个请求均已关联 Nginx：Java upstream connect p99 为 1012 ms、客户端 p99 为 1021 ms，主要延迟发生在连接上游阶段。该波次为 0.4.11 校准，不混入 0.4.12 的整点结果。[原始复算](../research/results/completion-20260918/analysis-interim.json)、[TLS 路径核对](../research/results/completion-20260918/client-interim/tls-certificates.json)。

两轮整点的耗时保留全部 200 个响应，包括数据核验失败的快速响应。p99 使用最近秩分位数；数据通过数另行核对。

| 指标 | 07:00 Java | 07:00 Rust | 08:00 Java | 08:00 Rust |
|---|---:|---:|---:|---:|
| bulk p50，ms | 291.99 | 139.95 | 264.54 | 125.50 |
| bulk p99，ms | 1027.66 | 149.29 | 308.40 | 137.65 |
| 全部响应完成，整点后 ms | 1033.48 | 150.15 | 336.80 | 164.80 |
| 目标窗口及最终值通过数 | 198/200 | 200/200 | 200/200 | 200/200 |
| Nginx 上游连接 p99，ms | 1008 | 5 | 26 | 5 |
| 上游连接 ≥500 ms 的请求数 | 34 | 0 | 0 | 0 |
| 同一分钟额外业务请求数 | 1040 | 0 | 1028 | 0 |

稳定期另有每端 1180 个常态 bulk 样本，全部通过：Java p50/p99/max 为 4.89/8.29/10.07 ms，Rust 为 1.82/4.01/5.36 ms。稳定期共 1579 对请求通过窗口初筛，其中 1578 对 Kline 内容相等；剩余一对的差异由下文的 Java 边界提前返回解释。再加上一个旧窗口响应，Java 最终数据核验为 1578/1580；Rust 额外的 `data_status` 最终状态逐个检查为 1580/1580。[数据汇总](../research/results/completion-20260918/live-final-summary.json)、[最终证据核验](../research/results/completion-20260918/evidence-audit-final.json)。

Java 的一个响应耗时 5.813 ms，但返回旧窗口。该请求从东京整点后 0.558 ms 启动，响应中的 `ts_ms=1789714799997` 对应 Java 校正时间的整点前 3 ms；按源码的整点截断规则，该请求没有进入新窗口等待。它保留在总样本中并列为完整性失败，不能把这 5.813 ms 计作成功获取新收盘的快响应。两端时钟有数毫秒差异；调用端可核对最后 open time，避免只依据 finalized 判断是否拿到目标小时。Java 本轮 Nginx upstream connect p99 为 1008 ms，说明约一秒尾部仍主要发生在连接上游时。

逐字段复核又检出第二个边界响应，客户端编号 1，耗时 7.897 ms、`ts_ms=整点−1ms`、finalized=true。它通过了窗口时间与根数初筛，但 3 个 symbol 的成交量等字段尚未最终更新，其中一个收盘价格也随后变化。Java 在同波次编号 144/145 请求中返回的最终值与 Rust 完全相等，证明不是 JSON 格式或两端长期数据差异。源码先按 `floor(now/interval)` 计算等待目标，再以另一次 now 筛选 `closeTime <= now` 的记录；在整点前最后 1 ms，可能纳入刚结束的记录，但等待目标仍是前一根。最终值核验因此将 Java 本轮通过数从初筛 199 修正为 198；所有 200 个响应仍保留在耗时分位数内。[逐字段差异与同波次后续 Java 确认](../research/results/completion-20260918/boundary-0700-value-difference.json)。

Rust 整点后 16 秒的 readiness 为 503，`stale_tail=78`，其他未就绪原因均为 0。逐序列读取与该计数精确对应：78 个合约 1h 序列的最新观测仍是刚刚完成的收盘杆，尚无下一根形成中记录；最新目标收盘缺失数为 0。31 秒时减少到 58 个，61 秒时为 9 个。现货和日线均未触发该过期条件。该健康规则比“最新已收盘可查询”更严格，当前实例 `strict_readiness=false` 允许继续查询保留数据。

08:00 第二轮两端均为 200/200 最新窗口完整，Rust bulk p50/p99 为 125.501/137.655 ms，Java 为 264.542/308.402 ms。测试进程实际从整点后约 24 ms 开始释放请求，不能把请求耗时直接当作“整点到返回”的耗时；Rust 整波最后完成约在整点后 164.798 ms，Java 336.801 ms。本轮 readiness 在 +16 秒有 44 条 stale tail，在 +121 秒恢复全部就绪；全部逐序列观测的最新目标收盘缺失数仍为 0。

08:00 Java 现货的原始 ready 最大值为 62.184 ms，但该条记录包含 −112 ms 的应用时钟校正。最后就绪样本 ZKCUSDT 的系统接收时间实际上是整点后 169 ms；去掉应用校正后，入库约在整点后 174.184 ms。Java 时钟实现使用收到 REST 响应时的本地时间与返回 serverTime 求差，并按小时刷新，因此网络返回延迟也可能进入修正量。这个机制与 Rust 的多次采样、选取较低 RTT、按往返中点估算的机制不同。以下按 `系统时钟估算 = 原始 ready 耗时 − 应用时钟修正` 统一口径；Java 使用最后就绪样本自身的修正值，Rust 使用整点前后相同的时钟采样值。三台机器使用同一 NTP 源且报告正常同步，毫秒级小差异仍应谨慎判断。[原始值、修正值与换算](../research/results/completion-20260918/clock-normalization.json)、[NTP 状态](../research/results/completion-20260918/ntp-tracking.json)。

| 整点 UTC | 全部 close 写入内存 | Java，整点后约 ms | Rust，整点后约 ms |
|---|---|---:|---:|
| 07:00 | 合约 718 个 | 152.8 | 134.0 |
| 07:00 | 现货 493 个 | 108.3 | 109.0 |
| 08:00 | 合约 718 个 | 148.9 | 142.0 |
| 08:00 | 现货 493 个 | 174.2 | 157.0 |

两端 Nginx 的主要上游连接参数均为 keepalive 64、空闲超时 15 秒、每连接 90 次请求，worker_connections 均为 768。本次客户端 TLS 提前约 35 秒建立，因此整点请求仍需新建已过期的上游连接。[配置核对](../research/results/completion-20260918/nginx-comparison-settings.json)。

资源采样每秒一次。Rust 窗口为 06:30:11–08:32:49，Java 为 06:30:10–08:33:15，均从新版全部就绪后开始；两端 PID 均保持不变，采样错误为 0。CPU 100% 表示一个逻辑核，两核上限约为 200%；峰值是约 1 秒采样区间内的平均值。

| 指标 | Java | Rust 0.4.12 |
|---|---:|---:|
| 应用平均 CPU | 9.08% | 8.69% |
| 应用 CPU p99 / 峰值 | 21.15% / 198.08% | 18.10% / 93.22% |
| 应用 RSS 起点 → 终点，MiB | 2051.7 → 2075.5 | 541.3 → 619.0 |
| 应用 RSS 峰值，MiB | 2080.1 | 619.0 |
| Nginx 平均 CPU / 峰值 | 0.363% / 20.74% | 0.034% / 21.64% |
| 主机 ListenOverflows / ListenDrops 增量 | 42 / 42 | 0 / 0 |
| 额外未标记请求数 | 77650 | 19（其他路径） |

两端在采集窗口内各有 2007 个带本次测试标识的请求，以及 1580 个未标记的 Go time 请求（数量与连接准备相符）；Java 另有 76305 个普通 Kline 请求等额外流量。Nginx 和应用 CPU 因此不能用于推导等吞吐效率。Java 使用 `-Xms1g -Xmx2g`；两端 number_type 均为 double，Java 容量配置 1000/2000 并带裁剪缓冲，Rust 为 1001/2001。RSS 是当前部署的实际占用，不是同堆预算的语言实验。Rust 内存期间出现多次回落，但终点较初始就绪时增加约 78 MiB；两小时观察不足以判定长期内存趋势。[运行配置](../research/results/completion-20260918/runtime-comparison-settings.json)、[Java 流量](../research/results/completion-20260918/final/java/traffic-summary.json)、[Rust 流量](../research/results/completion-20260918/final/rust/traffic-summary.json)。

每 30 秒健康采样共 246 次：Java actuator 全为 200；Rust readiness 有 7 次 503，均仅命中 stale_tail。结合整点后 12/16/31/61/121/181/241 秒的逐序列检查，07:00 在 +181 秒时已恢复，08:00 在 +121 秒时已恢复；全部 14 次检查均未发现最新小时收盘缺失。健康采样只能限定状态变化区间，不能据此计算精确故障秒数。[原因与逐序列证据](../research/results/completion-20260918/readiness-diagnosis.json)。

持久化验证通过：两次启动都只选择小时线恢复，0.4.12 恢复 1211 条序列、1185893 根记录，损坏数为 0。被白名单排除的 1211 个日线文件，其标识、大小、mtime 和 SHA-256 与中途清单完全一致；1211 个小时线文件的 mtime 全部推进，07:04、08:04 两次全量脏快照写入均为 1211 条、失败 0。[文件核验](../research/results/completion-20260918/persistence-final.json)、[实例日志](../research/results/completion-20260918/final/rust/app.log)。

日志也保留了上游可用性限制：0.4.12 启动阶段 Binance Vision 有 503，档案预载结果为 seeded=0、failed_months=2；这次没有验证历史资金档案预载成功。07:00、08:00 两端资金结算均出现空快照警告，并按现有策略安排 :05 重试。上述情况与 AltCoin 空对象、Java PNG 字体故障均不能隐藏为“所有上游数据始终可用”；近期资金等 11 组线上稳定业务结果相等的结论限定在实际抽查时刻。

最终发布包、线上二进制、配置指纹和测试输入已核对，197 个差分输入文件保持一致；855 个客户端证据文件逐一验证哈希。测试运行器和本次采样器均已正常结束。原始失败样本、旧客户端校准、完整响应、CPU profile、来源指纹与回滚文件全部保留。

| 复核入口 | 内容 |
|---|---|
| [当前验证清单](../research/results/completion-20260918/validation-0412.json) | 构建、测试、部署和最终数据索引 |
| [最终复算](../research/results/completion-20260918/analysis-final.json) | 分位数、资源、Nginx 对应关系和流量背景 |
| [客户端文件清单](../research/results/completion-20260918/client-final/client-final-manifest.json) | 每个请求、完整响应与客户端构建的哈希 |
| [证据核验](../research/results/completion-20260918/evidence-audit-final.json) | 最终版本、源文件、窗口状态和持续时长检查 |
| [证据索引](../research/results/completion-20260918/evidence-index.json) | 主要文件和归档校验值 |

在项目根目录可使用保存的数据重新计算，输出路径需尚不存在：

```sh
python3 research/acceptance-20260918/analyze.py \
  --servers research/results/completion-20260918/final \
  --clients research/results/completion-20260918/client-final \
  --deployment research/results/completion-20260918/deployment-0412.json \
  --out /tmp/kline-acceptance-recomputed.json
```
