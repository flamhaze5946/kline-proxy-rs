# kline-proxy-rs

Java `kline-proxy` 的 Rust 移植，面向整点突发请求。当前 0.4.15 覆盖全部 27 个 Java HTTP 操作、配套后台功能和 double/float/string/bigDecimal 四种数值模式。最新上线情况见 [Java / Rust 部署记录](docs/current-zero-deployment-20260928.md)；此前扩展差分与持续运行结果见 [补充验收报告](docs/acceptance-completion-20260918.md)；初始移植与 review-loop 的历史证据见 [0.4.0 验收报告](docs/migration-review-loop-20260916.md)。行为边界及观测限制在报告中单独列出。

## 运行

需要 Rust 1.92；依赖锁定在 `Cargo.lock`。PNG 字体内置，无需安装主机字体。

```sh
cargo build --release --locked
./target/release/kline-proxy --check-config examples/full-market.json
./target/release/kline-proxy examples/live.json
```

`examples/live.json` 维护 BTCUSDT 合约/现货的 1h、1d，便于小规模检查。`examples/full-market.json` 动态维护全部匹配 `.*?USDT` 的交易中标的，1h=9000 根、1d=1000 根；`examples/full-market-persistent.json` 示范 Java 持久化配置和只读快照导入。完整市场冷加载需要分页历史数据，不能用小规模启动时间估计。

```sh
curl http://127.0.0.1:8081/health/ready
curl 'http://127.0.0.1:8081/fapi/v1/klines/bulk?interval=1h&limit=1&symbols=BTCUSDT'
curl 'http://127.0.0.1:8081/fapi/v1/fundingRate/bulk?symbols=BTCUSDT&limit=1'
curl http://127.0.0.1:8081/health/performance
curl http://127.0.0.1:8081/health/market
curl http://127.0.0.1:8081/actuator/prometheus
```

`examples/local.json` 使用合成种子数据，启动时不连接交易所。配置 `rest` 且 `strict_readiness=true` 时，在目录、历史、连接和时钟检查通过前，bulk 返回 503。Java 配置转换器设置 `strict_readiness=false`，恢复期间允许查询已有数据；健康检查仍如实报告未就绪。SIGINT/SIGTERM 停止后台写入、排空 HTTP，然后保存最终快照。

## 功能

| 范围 | 已实现 |
|---|---|
| Kline | 合约/现货普通查询、bulk GET/POST、最终帧等待、数值与状态兼容、spot timeZone/月线转发 |
| 目录与流 | exchangeInfo 定期刷新、状态/上下架、正则订阅、稳定序列 ID、增删订阅、单主题停滞恢复、断线补洞 |
| 资金费率 | 单条/批量 GET/POST、范围与限额、异步请求合并、小时缓存、发布宽限、整点预热与 :05 重试、Vision ZIP 历史 |
| 市场接口 | time、exchangeInfo、premiumIndex、ticker price/24hr，现货 MINI、symbols、symbolStatus |
| 统计 | AltCoin、Yama01/02/聚合、ATR 日志、三种 PNG，保留 Java 的十进制运算和 float 累加规则 |
| 其他 | CMS 文章/目录缓存、hello、IP |
| 持久化 | 周期/标的内存与磁盘保留、原子校验快照、Java JSON 只读导入、退出保存、下架清理 |
| 观测 | 完整收盘集合、最晚消息、接收/解析/提交耗时、按小时 bulk p99、Prometheus、健康与市场后台状态 |

完整接口和差异见 [移植清单](docs/migration-work.md)，运行细节见 [运行与恢复](docs/runtime.md)。Java YAML 转换、Linux 构建、systemd、只读对照和回退步骤见 [部署说明](docs/deployment.md)。

## 模块和性能

| crate | 职责 |
|---|---|
| `kline-core` | 有界历史、数值和提交规则，不依赖网络/运行时 |
| `binance-wire` | WS/REST 解析与 Java 数值输出 |
| `kline-service` | 动态目录、提交、bulk 等待与共享响应字节、收盘诊断 |
| `kline-market` | 资金费率、ticker、元数据、统计、CMS；独立缓存与辅助 CPU 工作线程 |
| `kline-runtime` | 配置、发现与订阅管理、恢复、持久化、时钟、进程生命周期 |

Kline WS 直接提交，不创建每帧任务；锁只覆盖单条序列，网络/磁盘操作不持有锁。相同 bulk 共享等待与编码字节。资金加载全程异步，大 JSON、ZIP 和图像计算使用独立的有界工作线程，不占用 bulk 编码线程。exchangeInfo 的大 JSON 预先编码，每次只添加当前 serverTime；现货请求关闭 Java 不输出的 permissionSets 内容。

自有 Rust 禁止 unsafe，没有降低浮点精度的编译选项。`KLINE_IO_WORKERS` 默认 2；线程/队列有界，但后台和请求仍共享主机 CPU 与网络。详见 [架构与兼容性](docs/architecture.md)。

## 验证

```sh
./scripts/check.sh
python3 research/offline_modes_smoke.py
python3 research/live_smoke.py --output migration-live-double.json
python3 research/live_smoke.py --number-type bigDecimal --output migration-live-bigdecimal.json
```

测试覆盖全部 Java 路由、真实本地 WS、192 个资金请求合并与 Kline 独立响应、数据恢复/取消/目录变化、长范围截断、Java 快照、浮点和图像。真实数据脚本执行冷启动、查询、落盘、重启恢复；全市场检查只保留每序列 2 根，验证覆盖与生命周期，不代表完整历史容量或生产负载。

0.4.12 的补充验收证据位于 `research/results/completion-20260918/`，版本、测试与发布指纹见 [当前验证清单](research/results/completion-20260918/validation-0412.json)。`migration-*` 与 [初始验证清单](research/results/migration-validation.json) 保留为 0.4.0 证据，`full-port-*` 保留为 0.3 历史证据。线上测量的 TLS、额外业务流量与时钟限制见 [补充验收报告](docs/acceptance-completion-20260918.md)；真实 Lambda 全链路切换仍应按实际流量验收。
