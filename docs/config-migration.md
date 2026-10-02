# Java 配置迁移到 Rust 0.4

转换器支持嵌套与点分 YAML 键。输入应为实际生效的 Java 配置；Spring profile、环境变量、命令行覆盖需要先合并。Rust JSON 对未知字段和非法值拒绝启动，转换后使用 `kline-proxy --check-config config.json` 检查。

| Java 配置 | Rust 对应与语义 |
|---|---|
| `number.type` | `number_type`，支持 double、float、string、bigDecimal；转换缺省值为 Java 的 bigDecimal；手写 Rust 配置缺省仍为 double |
| `kline.binance.{market}.enabled` | false 不产生该市场 Kline 订阅；不关闭独立的资金/统计接口 |
| `intervalSyncConfigs` | 逐市场/周期生成 `subscriptions`，保留 continuous 选择与 symbol_patterns |
| `minMaintainCount` | 缺省 365；启用持久化后保留 Java 的有效维护容量规则 |
| `listenSymbolPatterns` | 必须显式提供列表；空列表不订阅，避免意外扩大数据范围；使用 Rust 正则语法并在启动校验 |
| `rpcRefreshCount` | 分市场映射 `rest.future_refresh_count` / `spot_refresh_count`，缺省 99；null 使用保留容量 |
| `kline.bulk.finalWaitEnabled/finalWaitMaxMs` | `final_wait_ms`，禁用为 0，按 Java 规则限制到 0–30000ms |
| `kline.rpcSync.hourBoundaryGuardBeforeMs/AfterMs` | 独立整点保护，Java 默认 150000 / 30000ms；Rust 另保留可配的周期边界保护 |
| `kline.diagnostics.closedBarLatencyEnabled` | 细分计时开关；关闭后仍提供收盘到达摘要并明确标记 |
| `funding.publicationGraceMs` | `market_api.funding.publication_grace_ms`，缺省 50，负值归零 |
| `client.*.api.rootUrl` | 两个 REST 根地址、CMS 根地址、AltCoin 页面地址 |
| `ws.client.*.url` | 两个 WebSocket 根地址；`/ws` 转为组合流 `/stream` |
| `statistic.*` | ATR 标的/周期、AltCoin 起始日期、Yama 天数/成交量窗口/排名 |
| `kline.persistence.*` | load/dump 开关、周期、避让窗口、每市场/周期/标的保留；周期白名单映射 enabled_intervals；Java rootDir 转为只读 legacy_directory |
| Java 服务监听地址 | CLI `--listen` 显式给出，缺省独立端口 127.0.0.1:1889 |
| Rust `listen_backlog` | 缺省 1024，控制等待应用接收的连接队列；Linux 实际值受 `net.core.somaxconn` 限制，与 HTTP 请求并发上限独立 |
| Java 恢复时可查询已留存数据 | 转换器输出 `strict_readiness=false`；健康接口仍报告真实恢复状态 |

持久化 `maxStoreCount=null` 使用 Java 的两倍最低容量；symbol 覆盖为 null 则回退周期规则。所有有效容量至少保持 minMaintainCount。Rust 的数据目录由 `--data-dir` 指定，与 Java 快照目录分开；源 Java 文件不会改写。

0.4.11 起，转换器将 Java 明确列入 `kline.persistence.{market}.intervalConfigs` 的已订阅周期写入 `persistence.enabled_intervals`，例如 `[{"market":"future","interval":"1h"}]`。该选择同时控制恢复、Java 快照导入、写盘和脏数据重试；未选择周期的已有文件保留。空数组表示全部跳过。原生 Rust 配置省略该字段或设为 null 时继续允许所有周期，retention 仅决定条数，不作为白名单。已经转换过的配置需要从实际生效的 Java 配置补入该字段；仅升级二进制不会自动推断白名单，也不应覆盖后来调优的内存容量、网络和恢复参数。

`kline.ingress` 的 Java 工作线程、合并队列和 closed-key 队列属于 JVM 入口实现，Rust 不复制这些队列。Rust 在 WS 任务内直接解析/提交，定期让出执行权，不丢弃形成中帧；I/O 工作线程由 `KLINE_IO_WORKERS` 控制，REST、辅助 CPU 和 bulk 编码各有独立并发界限。JVM 启动参数、GC/JIT、Spring 日志/Actuator 配置不映射为虚假的 Rust 指标。

边界差异明确保留：Rust 正则不支持 Java 的环视和回溯引用，遇到这些配置会报错；当前生产 `.*?USDT` 兼容。Rust 对历史容量、数字长度/指数、响应长度和并发设置显式上限。非法值或超出这些边界的配置需要调整，不能静默截断为另一个订阅范围。图表保留数据与标题，但不复制 Java 图像的每个像素。

转换器的 8 项回归测试覆盖四模式与禁用市场的真实 Rust 配置解析、默认值、嵌套/点分键、资金宽限、历史刷新、持久化覆盖/空值、周期白名单及参数裁剪。


### 0.4.9 最新收盘补齐与缓存优化

`rest.latest_repair_after_ms` 默认 500（范围 100–30000），
`rest.latest_repair_workers` 默认 2（范围 1–4）。正常收盘先等待 WebSocket；
已完成初始加载的序列仍缺最新 final 时，后台只请求最新 2 根，并与普通历史回填
共用每市场 REST 配额和 Retry-After。WebSocket 提前补齐会取消该请求等待。
正常历史回填继续遵守整点保护窗口，紧急补齐无需等 30 秒保护窗口结束。
该补齐不会单独将整段历史标为 ready；历史数量、final 规则及健康检查保持原约束。

闭合行在响应层按每序列版本缓存；新条目、close time 变化、final、修正和裁剪失效，
普通未收盘价格更新不触发重编码。后台预编码默认最近 10 根，按查询需求最多 100 根；
历史存储仍保留配置的完整根数。`/health/performance` 的 `work` 包含诊断入队、
后台诊断、等待 final、构建排队及编码耗时；p99_upper_ms 是直方图桶上界。

历史恢复/冷启动会按验证后的实际条数加最多 32 根余量一次预留空间，空序列不分配历史缓冲；保留数量上限不变。
