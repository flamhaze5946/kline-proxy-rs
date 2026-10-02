# Rust 0.4.0 迁移与 review-loop

本轮补齐了已知业务迁移缺口，修复 0.3.0 审查中的 R1–R6，并完成四种数值模式、恢复行为与持久化链路。最终结论以 [验证清单](../research/results/migration-validation.json) 为准。生产流量没有切换，不能将代码验收结果解释为线上 p99 改善。

## 原六项问题的闭环

| 编号 | 完成的修复 | 验证 |
|---|---|---|
| R1 | ticker/price 使用原始十进制精度，保留 scale 和九位小数 | 实际 Java Ticker/ConvertUtil 与 Rust 路由差分 |
| R2 | 周期恢复重读可配置的历史窗口，修正较早真实 final 和合成 final | 两种历史修订场景均变为上游价格 200、成交数 2 |
| R3 | 全市场价格使用独立 `/ticker/price`，后台维护共享字节快照 | 验证来源为 price 端点，time=333，未用 24hr 的 time=222 |
| R4 | 完整 REST 基线与 WS 增量分开标记 | 提前到达单个 WS 标的不再发布部分市场，16 个并发冷请求合并加载 |
| R5 | Vision 404 跳过单个缺档，真实下载错误保留整月失败语义 | 正常/404 均预热 1 小时；500 场景不发布该月 |
| R6 | 补齐市场开关、365 默认容量、资金宽限与 RPC 复查数量 | 禁用市场为 0 订阅，宽限 500ms、默认 365、复查数量 123 |

证据：[修复验收](../research/results/migration-fixed-summary.json)、[原始 Rust 响应](../research/results/migration-fixed-rust.json)、[Java 价格](../research/results/migration-fixed-java-ticker.json)、[配置结果](../research/results/migration-fixed-config.json)。旧报告及 `migration-review-*-20260916.json` 保留为历史复现，未覆盖。

## review-loop 过程

| 轮次 | 检查范围 | 发现与处理 |
|---|---|---|
| 第一轮 | 路由之外的实际数据来源、定时任务和配置 | 修复 R1–R6；补齐 float/string/bigDecimal，贯通 REST、WS、HTTP、统计、持久化 |
| 第二轮 | 实际 Java 类的数值与状态差分 | 发现 `0E+5` 被输出为 `000000`；共用十进制层改为 Java 的 `0`。补齐 WS 缺口的非最终占位，不伪造 final |
| 第三轮 | 异常输入、恢复、关闭、配置覆盖 | 修复坏 E 导致整帧丢弃、坏 CSV 行导致整月丢弃、坏 Java 快照行导致整日丢弃；补齐 null 默认与边界裁剪；验证后台刷新不会阻塞热请求且可取消 |
| 第四轮 | 重新检查最终代码、依赖边界、回归与可执行程序 | 复查 mode 传播、原子恢复、流/REST 优先级、只读导入、等待通知、路由/配置覆盖；去除缓存命中的元数据扫描和不必要 Arc 克隆；没有新增未解决的迁移阻断项 |

每次发现问题后修改实现并加入针对性验证，再重新检查相关路径。审查为本地代码对照与自动化验证，没有将子代理独立评审或生产观察冒充为已执行。

## 已执行验证

| 范围 | 结果与证据 |
|---|---|
| 自动化回归 | 69 项 Rust 测试、7 项 Python 配置测试；覆盖 27 个 Java HTTP 操作、恢复、持久化、并发与真实本地 WS |
| double 输出与提交规则 | 69,991 个数值用例 + 5,000 个提交用例，0 差异；[结果](../research/results/migration-contracts.json) |
| 四模式 | 8,853 组 REST/WS/补位/提交用例，0 差异；[结果](../research/results/migration-numeric-modes.json) |
| 四模式统计 | 160 组、20,800 个结果点，0 差异；[结果](../research/results/migration-statistics.json) |
| Java 基线身份 | 使用实际 Java 类，相关 6 份 Java 源文件与当前工程一致；[校验](../research/results/migration-java-oracle-sources.json) |
| 构建 | fmt、Clippy 全目标零警告、macOS release、Linux x86_64/glibc 2.28 release |
| 四模式 release 进程 | 每模式冷启动与落盘重启，各轮 192 个 bulk 请求，共 1,536 个；普通查询与 bulk 一致、快照校验通过；[结果](../research/results/migration-offline-modes-smoke.json) |
| 真实公开数据 | double、BigDecimal 各 4 条 BTCUSDT 序列，每序列容量 32，验证 REST/WS、查询、快照与重启；资金/Vision/统计后台关闭；[double](../research/results/migration-live-double.json)、[BigDecimal](../research/results/migration-live-bigdecimal.json) |

复现核心检查：

```sh
./scripts/check.sh
python3 research/offline_modes_smoke.py
python3 research/review/verify_fixes.py --java-root /path/to/java --java-build /path/to/java-oracle-build --jdk /path/to/jdk21
python3 research/verify_contracts.py --java-snapshot /path/to/java-oracle-build --java-home /path/to/jdk21 --output migration-contracts.json
python3 research/verify_numeric_modes.py --java-snapshot /path/to/java-oracle-build --java-home /path/to/jdk21
python3 research/verify_statistics.py --java-snapshot /path/to/java-oracle-build --java-home /path/to/jdk21
```

小规模公开数据检查中，double 冷启动/恢复就绪为 7.606s / 5.333s，BigDecimal 为 5.325s / 4.282s。两次检查时刻不同，不能据此比较模式性能；这也不是整点 bar-close 或 bulk p99。Linux 0.4 本轮完成交叉构建和发布包校验，未在实例运行。

## 保留的边界与部署验收

四种数值模式均实现。默认 double/float 使用内联数值，精确模式按需使用不可变共享数据；领域规则、协议格式、应用服务和外部 I/O 仍分属五个 crate。原生 double v1 快照继续可读写，其他模式使用带类型和 scale 的 v2；Java 日分片保持只读。

明确保留的实现差异包括严格容量上限、Rust 正则语法及显式资源限制、不同 PNG 排版、Rust 指标替代 JVM 指标、非预期上游错误返回结构化 502；详见 [配置迁移](config-migration.md) 与 [架构](architecture.md)。月线恢复要求真实日历记录，不按固定 30 天制造缺失月线；bulk 周/月边界仍沿用当前 Java 固定周期算法。

完整生产历史容量、至少两个整点以及 Nginx/TLS/Lambda 并发验收尚未执行。0.3 的 2,422 序列检查只保留每序列两根，不能作为 0.4 满容量或实际整点负载验收。下一部署阶段应在隔离端口完成容量与流量对照，再评估切流；本轮没有改变实例上的 Java 服务或 Nginx。
