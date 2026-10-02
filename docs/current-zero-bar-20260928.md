# 当前周期零成交量占位 bar

2026-09-28 已随 Rust 0.4.15 部署到 market-mirror.feng.dog；Java 1.8.2 同步部署到 market.feng.dog。线上验证范围见 [部署记录](current-zero-deployment-20260928.md)。Rust 与 Java 现货、合约采用相同条件：**只有本进程收到并接受紧邻上一根 WebSocket `x=true`，才允许在缺少当前真实 bar 时构造当前周期的临时 bar。**

REST 已收盘记录、snapshot 恢复、历史补洞、`x=false` 或其他周期的 `x=true` 不能替代这个条件。每条 Series 仅保存最近接受的流式收盘 openTime，记录不持久化，clear 时清除；查询时还必须与最新记录的 openTime 相等。

OHLC 全部取上一根 close；volume、quoteVolume、tradeNum、主动买入量/额全为 0。openTime 为前一根 closeTime + 1，closeTime 为下一周期结束前 1 ms；月线按日历月计算，周线沿实际上一根时间。占位只在紧邻的当前周期有效，不连续外推历史。

## 行为

- 单币缓存查询和 `closed_only=false` bulk 在读取时构造，计入 limit，保留显式历史范围限制。现货非空 timeZone / 1M 的普通 HTTP 查询仍透传官方 REST。
- 占位不进入 Series，不带 final 状态，不进入 closed-only 或 snapshot，不改变真实序列的 freshness、历史覆盖和恢复进度。
- 真实 WS/REST 当前 bar 直接取代占位，包括真实 0 笔成交的 bar。bulk 用窗口代次验证响应；相同数值的上一根 `x=true` 到达也会推进代次，使旧响应立即失效。月/周实际周期到期会使占位响应失效，即使旧固定时长 cache key 尚未变更。
- 查询路径不新增 I/O 或扫描。真实形成中 bar 的后续价格更新仍沿用既有短缓存策略。
- 初始化、历史缺口、连接代次、时钟等就绪条件保留。若这些条件都通过，有有效占位可以满足尾部可查询条件；实际数据是否新鲜仍单独统计。

## K 线 REST 调度约束

收到 `x=true`、构造占位、缺少当前 bar、最终帧迟到、真实尾部过期、静默主题重订阅及 recheck 提示，都不触发即时 REST。缺口提示也不会提前定时订正。K 线数据由 WebSocket 更新，等待配置的 `reconcile_seconds`（默认 300 秒）订正；订正读取实际缓存，失败仍按原有退避规则重试。

启动/新序列的历史加载、容量扩展初始化和实际连接重连恢复暂保留。非 K 线接口及现货 timeZone/月线显式透传路径沿用原有行为。旧配置 `latest_repair_after_ms` / `latest_repair_workers` 保留解析兼容，但不再安排即时尾部任务；健康详情的两个 repair_jobs 字段为 0，`tail_rest_policy=scheduled_reconciliation_only`。

健康详情新增 `provisional_current`（满足占位条件的交易中序列数量）、`observed_stale_tail`（真实尾部超过宽限的序列数量）。`unready_reasons.stale_tail` 表示真实尾部不可用且没有符合条件的临时占位。旧的未确认历史 bar 仍可能阻止就绪；占位不能掩盖断线或缺口。

## 验证

- `kline-market/tests/current_zero_bar.rs`：双市场 × 四种数值模式 × 两种真实更新来源，x=true 严格门槛、恢复隔离、不同周期确认隔离、bulk 即时替换、limit/范围、月/周边界。
- `kline-runtime/tests/recovery.rs`：缺少当前 bar、最终帧迟到及静默 recheck 不新增 REST；到定时订正期限才发请求，订正阻塞时可查询临时 bar 且保持就绪；仍验证实际重连恢复、占位不持久化。
- `cargo test --workspace --offline`
- `cargo clippy --workspace --all-targets --offline -- -D warnings`
- `cargo fmt --all --check`

最终验证：工作区 174 项测试全部通过；Clippy（warnings 作为错误）和格式检查均通过。测试 mock 记录验证了即时补全请求为 0，以及到期后的定时订正仍能取得真实当前 bar。

Java 对应说明位于 Java 工程 `docs/current-zero-bar-20260928.md`。这是本地测试，不是新版本的线上整点验收。
