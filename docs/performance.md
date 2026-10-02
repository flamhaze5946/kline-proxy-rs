# Rust 核心链路评估 · 2026-09-15

> 版本范围：本文数字对应 **0.1.0 核心原型**。工程现已进入 0.2.0，增加了 REST、持久化、时钟与健康协调；本轮尚未重跑相同资源条件下的完整性能对比。历史基准对应的源码保存在 [source-20260915.tar.gz](../research/results/source-20260915.tar.gz)，可用 [原源码指纹](../research/results/rust-source-manifest.json) 校验。

Rust 原型显示出明显的 CPU 和内存收益，值得继续补齐生产能力。正常整点的整体就绪时间改善较小，因为相同回放输入的最后一批收盘帧本身要在整点后约 131ms 到达。

当前完成的是核心路径原型与验证，尚未替换线上 Java 服务。以下是本地 Linux 实验结果，不是线上 SLA。

## 1. 先确认 Java 热点

基线来自本轮对当前 Java 工作区的隔离源码快照，包含已有未提交修改。使用 JDK 21，normal / bulk96 / flood 各 60 轮预热、40 轮测量，并在测量窗口内分析 JFR。

普通场景的 366 个消息工作线程 CPU 样本中，跳表 `doGet` / `findLast` / `doPut` / `put` 顶层叶子合计 183 个。收盘统计分类占 122 个样本；JSON 解析占 47 个。bulk96 的 191 个样本中，收盘统计占 96 个。样本数量有限，这些比例用于定位热点，不是完整 CPU 时间占比。

消息线程的分配采样中，`LinkedHashMap.Entry`、HashMap 桶、字符串、字节数组、Jackson 节点和装箱数值较突出。这支持优先减少 DOM 解析、临时集合和多容器索引。分配数据是 JFR 采样权重，不应解读为精确分配量。

此前实例上 GC(802) 的 Remark 暂停为 **428.623ms**，对应 `User=0.07s, Sys=0.06s, Real=0.43s`，安全点约 429.8ms。暂停确实存在，但 CPU 时间明显小于墙钟时间；记录不足以把整段时间都归因于回收计算。系统调度或等待仍需单独分析。

证据：[JFR 分析](../research/results/java-profile-jfr-analysis.json)、[源码指纹](../research/results/java-source-manifest.json)、[录制文件指纹](../research/results/java-jfr-manifest.json)。

## 2. 同输入、同两核核心回放

环境为同一台 Docker Linux ARM64 虚拟机，Java 21.0.12 / Rust 1.92。每个测试进程及其线程绑定 CPU 6、7，顺序运行；两次重复交换 Java/Rust 先后顺序。没有启动生产实例压测，也没有使用手机热点下载镜像。

每次运行先预热 60 轮，再测量 40 轮。每种实现、每个场景共有 80 轮测量；以下 p99 合并原始样本计算，未平均不同运行的 p99。

- 输入统一由已有 Java 回放器导出；718 个合约、490 个现货，1h 与 1d 各 1000 条初始历史，总计 2,416,000 条。
- normal 和 bulk96 每轮 3624 帧；flood 每轮 49,528 帧，最终帧均为 1208 条。
- bulk96 在整点后约 5ms 启动 96 个请求，每个查询 6 个合约、1 条 K 线，并完成 JSON 编码。此处包含整点等待，不包含 TCP/TLS/Nginx。

| 指标 | 当前 Java | Rust 原型 |
|---|---:|---:|
| normal：每轮 CPU 时间均值 | 82.1ms | **14.8ms** |
| normal：全部最终记录就绪偏移中位数 | +134.7ms | **+131.2ms** |
| bulk96：每轮 CPU 时间均值 | 103.0ms | **25.2ms** |
| bulk96：接收至处理完成 p99 | 7.302ms | **0.042ms** |
| bulk96：请求至 JSON 完成 p99 | 137.4ms | **126.0ms** |
| bulk96：全部最终记录就绪偏移中位数 | +135.9ms | **+131.7ms** |
| bulk96：进程峰值 RSS，两次运行范围 | 1554–1681MiB | **约 289MiB** |
| flood：每轮 CPU 时间均值 | 117.6ms | **23.0ms** |
| flood：从开始注入到全部最终记录就绪，中位数 | 64.7ms | **23.3ms** |

bulk96 CPU 时间下降约 **75.6%**，请求 p99 下降约 **8.3%**。洪峰回放从模拟整点前 20ms 开始注入，因此该行已把原始相对整点偏移加回 20ms。

全部 **1200 轮回放**（包含预热）通过最终记录数、最终值、形成中最新值和响应最终状态检查；Java 的丢弃/失败/队列未排空检查均为零。最终 Rust 保留 2,416,000 条，Java 因 50 条裁剪缓冲保留 2,475,192 条，相差约 2.45%。

RSS 包括运行时、框架、历史状态、临时对象和基准装置。Java 使用 1GiB 初始堆、2GiB 最大堆；这不是两种完整生产服务在同等功能下的内存对比。Java 的完整收盘统计、持久化 dirty-key 和部分诊断尚未在原型中等价移植；收益也来自算法、对象布局和调度结构，不能全部归因于 Rust 语言。

本机 Docker 新建容器启动异常，实验使用已有 Linux 容器内的独立目录。原有长时间阻塞的构建进程 CPU 为零，未停止它们。采用进程 CPU 亲和性限制，没有独立 CFS 配额或内存上限；仍可能有共享虚拟机噪声。Java 进程 CPU 计时约为 10ms 粒度，因此报告多轮均值。生产实例是 x86，最终切换前需要同实例验证。

证据：[合并指标](../research/results/pooled-comparison.json)、[每次运行与环境](../research/results/comparison.json)、[共享输入指纹](../research/results/comparison-input-manifest.json)。原始样本压缩保存在 `research/results/{java,rust}-{scenario}-{repeat}.json.gz`。

### 补充：最后一批消息与最后一个 bulk 的时间差

2026-09-16 从上述原始数据复算，每种实现 80 轮测量。共同夹具中最后一批收盘消息的**计划到达时刻**为整点后 131.000ms；`bulk_done_max_ms` 是每轮最后一个 bulk 完成 JSON 编码相对整点的偏移。逐轮计算后汇总如下：

| 指标 | Java | Rust |
|---|---:|---:|
| 最后一个 bulk 完成偏移，中位数 | +134.233ms | +132.086ms |
| 最后一个 bulk 完成 − 最晚计划到达，中位数 | 3.233ms | 1.086ms |
| 上述差值，最坏一轮 | 21.334ms | 1.681ms |

原始结果没有保留每条消息的实际接收时间与每个请求的对应关系，因此这些差值包含回放注入的调度延迟，不能称为实际收包后的纯处理耗时。也不能用原表的“请求耗时 p99”减“相对整点的消息到达偏移”，两者起点不同。本次实际到达时间的补测尝试被本机 Docker 执行阻塞中断，尚未产生新的测量结果。[复算证据](../research/results/scheduled-arrival-bulk-gap.json)。

## 3. 真实 HTTP 请求验证

另做了 96 条持久连接的 HTTP/1.1 loopback 测试，每轮同时请求，每个请求完整读取并校验 JSON。历史数据已最终确认，没有上游等待。每种实现预热 19,200 个请求，再测量 19,200 个请求。

Java 使用 JDK HttpServer + 当前真实 bulk service/Jackson，Rust 使用原型实际 Axum 路由。服务端仍各绑定同两个 CPU，客户端使用另外 6 个 CPU。两边都启用 TCP_NODELAY。

| 已就绪数据的 HTTP 指标 | Java 测试适配器 | Rust 原型 |
|---|---:|---:|
| 请求 p50 | 2.291ms | **0.726ms** |
| 请求 p99 | 5.325ms | **1.101ms** |

这验证了真实套接字、响应编码与复用的可行性。Java 适配器不是生产 Tomcat，测试也没有 TLS、HTTP/2、Nginx 或 Lambda，因此这张表不能代替整点线上请求延迟。

首轮未统一 TCP_NODELAY 时，Java 适配器出现约 50ms 的 p99；统一后降到约 5ms。保留了该原始记录用于审计，正式比较使用统一后的结果，避免把传输配置差异算作语言收益。

证据：[HTTP 指标](../research/results/http-comparison-summary.json)、[原始样本](../research/results/http-comparison.json.gz)。

## 4. 正确性与下一阶段

**69,991 个浮点数值样本和 5,000 组乱序提交与 Java 原实现一致**。22 项 Rust 测试全部通过，覆盖 96 路共享等待、取消后重试、收盘通知竞态、首次发布的缓存失效、超时、非交易状态、最终修订、容量保留、真实 WebSocket/Ping 和 HTTP 参数校验。格式检查、Clippy、release 构建及真实程序的本地 HTTP 冒烟检查通过。上述最终回放和 HTTP 比较均使用包含首次发布修正的版本。[差分结果](../research/results/contracts.json)、[验证汇总](../research/results/validation.json)、[源码指纹](../research/results/rust-source-manifest.json)、[Linux 二进制指纹](../research/results/benchmark-binary-manifest.json)。

下一阶段优先补齐 REST 初始化与重连补洞、持久化/恢复、动态目录和新鲜度检查，然后处理资金费率的独立加载及辅助接口。完成这些后，在同一 x86 实例上运行影子流量并比较真实整点窗口，再决定完整切换。

Binance 最后一批收盘消息决定数据最早全部就绪的时间。Lambda 连接复用以及 Nginx TLS/HTTP2 突发负载仍需在调用链中单独优化。
