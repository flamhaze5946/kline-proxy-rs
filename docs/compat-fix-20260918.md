Rust **0.4.11** 已修复 [上轮复核](migration-review-20260917.md) 中的三组问题。最终源码通过跨语言对照、完整回归、实际进程恢复验证和 Linux 发布构建。本轮完成的是本地修复与发布包，尚未部署到实例。

后续部署与 0.4.12 扩展验收见 [补充验收记录](acceptance-completion-20260918.md)。本页保留 0.4.11 本地验收时的状态与原始证据。

| 问题 | 修复后的行为 | 验证 |
|---|---|---|
| 持久化周期白名单丢失 | Java 明确列出的已订阅周期转换为 `persistence.enabled_intervals`，统一限制恢复、Java 快照导入、周期落盘及关闭落盘 | 同时订阅 `1h/1d`、只允许持久化 `1h`：两端均仅选择 `1h`；实际 Rust 进程重启只恢复 `1h` |
| HTTP 参数及空 body 不兼容 | 统一输入层，接受空可选数字、字符串数字、Java 支持的布尔表示、JSON null；提取错误返回 `{code,msg}` | 13 个真实 Java 控制器/Rust Router 对照用例的状态码、响应类型及既有业务字段一致 |
| bulk 完成标记语义变化 | `finalized/pending` 恢复 Java 的刚收盘目标根语义；独立 `data_status` 报告返回窗口未 final 和最新根缺失 | 最新根缺失、较早根未 final 两个案例的既有字段与 Java 一致，同时保留数据诊断；修复提交会失效旧缓存 |

**持久化配置。** 周期是否落盘与保留条数分离。以下配置仅让合约 `1h` 执行持久化；其他订阅仍正常更新和查询：

```json
"enabled_intervals": [{"market": "future", "interval": "1h"}]
```

该字段位于 `persistence` 内。空数组表示全部跳过，省略或 null 保留原生 Rust 配置的“允许所有周期”行为；`retention` 仅决定条数。排除周期的已有快照保留，不读取、不覆盖，也不会因其更新不断触发落盘重试。转换器继续保留 Java 启用持久化时的有效内存容量规则。

**升级旧配置时必须补入白名单。** 已转换的线上配置不会因升级二进制自动获得该字段；应根据实际生效的 Java `intervalConfigs` 补入，保留后来调优的内存、网络和恢复参数，再执行 `--check-config`。详见 [配置迁移说明](config-migration.md)。本轮没有修改线上配置。

**HTTP 与 bulk 契约。** bulk 的空 body/null 返回 JSON `400/-1102`，funding bulk 的空 body/null 使用默认查询。空 `limit=` 使用默认值，`closed_only=1/off` 等按 Java 查询参数规则转换；JSON body 的数字和布尔转换单独处理。格式错误统一 JSON `400/-1100`，超长 body 为 413，媒体类型错误为 415。Java 对部分非法绑定返回 500 的行为未复制；错误文案不承诺逐字相同。

`closed_only=true` 的响应新增：

```json
"data_status": {
  "window_finalized": true,
  "nonfinal_symbols": [],
  "missing_latest": []
}
```

`window_finalized` 仅说明实际返回的记录是否全部 final；`nonfinal_symbols` 列出仍有未 final 记录的标的；`missing_latest` 列出已有历史之后缺少目标收盘根的标的。它们不保证返回数量达到请求 limit，也不检测所有内部历史缺口。空序列、首次上市前的根以及无法按固定周/月边界可靠推断的缺口不纳入 `missing_latest`。非 closed-only 响应省略该字段。

旧字段只等待“刚收盘且已经存在、尚未 final、仍在交易”的目标根。因此最新根完全缺失时，可以出现 `finalized=true` 与非空 `data_status.missing_latest`；这与 Java 既有完成语义一致。底层 `needs_final`、WebSocket 更新和后台 REST 最新根补齐逻辑保留。返回窗口存在非 final 记录时不会复用该 payload；数据补齐或修订仍触发缓存失效。

**验收结果。**

| 检查 | 结果与证据 |
|---|---|
| Rust 工作区回归 | **104 passed，0 failed，0 ignored**；[完整日志](../research/results/compat-fix-20260918/check-3.log) |
| Python 配置转换回归 | **8 passed**；同上 |
| 格式与静态检查 | `cargo fmt --all --check`、`cargo clippy --workspace --all-targets --locked -- -D warnings` 通过 |
| Java/Rust 最终差分 | 重新编译当前 Java 的 **130 个源文件**；13 个 HTTP 用例、2 个 bulk 边界案例、持久化选择均通过；[汇总](../research/results/compat-fix-20260918/differential-final/summary.json) |
| 实际发布进程验证 | 本机 0.4.11 启动、种子查询、SIGTERM 落盘、无种子重启恢复、再次关闭均通过；仅生成 1 个允许周期的快照；[结果](../research/results/compat-fix-20260918/native-smoke/result.json) |
| Release 构建 | 本机和 `x86_64-unknown-linux-gnu` 构建通过；[Linux 日志](../research/results/compat-fix-20260918/linux-build.log) |
| 依赖与源码 | 第三方 Cargo.lock 项无变化；只提升 5 个本工程包至 0.4.11；[验收清单](../research/results/compat-fix-20260918/validation.json)、[本轮源码差异](../research/results/compat-fix-20260918/change.diff) |

跨语言实验调用实际 Java 服务及 Spring MockMvc 参数绑定、实际 Rust Router/Store；资金上游使用本地替身，Java 落盘周期通过记录写入调用验证，Rust 实际生成快照。没有访问 Binance，全部依赖离线使用。时间戳、实际等待耗时和错误文案不作为 HTTP 字段相等的断言；两个固定时钟 bulk 边界案例的全部既有字段另做相等断言。

可在保留历史结果的前提下复跑对照：

```sh
python3 research/parity-review-20260917/run.py --output research/results/new-parity-run --expect-fixed
```

Linux 发布包：[kline-proxy-rs-0.4.11-x86_64-unknown-linux-gnu.tar.gz](../dist/kline-proxy-rs-0.4.11-x86_64-unknown-linux-gnu.tar.gz)。[发布清单](../research/results/compat-fix-20260918/release-manifest.json) 与验收清单记录文件哈希。

本轮结论是上轮已复现的三组问题均已修复。没有重新进行实例整点压测，也不据此声称所有异常输入与 Java 完全相同或线上延迟已经改善。
