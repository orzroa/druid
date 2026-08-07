# Druid Prometheus 1.5 期：高基数明细指标每日清理

## 背景与目标

一期 Prometheus 指标已经上线，定义见 [一期设计](README-Prometheus-Metrics-Design.md)。部分模块的 SQL 模板数量和变化速度较高，长期运行后即使一期已有 identity 上限和近似 LRU，仍会持续发生 Meter 创建、注销及相关对象积累，现场观察到 FGC 次数明显增加。

1.5 期仅解决一期高基数 SQL/URI 明细 Meter 的生命周期问题：每个业务日期清理一次 Starter 持有的一期明细 Meter 和 identity/LRU 引用，使其生命周期由整个 JVM 运行期收敛到一个业务日。该改造不引入二期低基数指标或结构化日志；二期设计继续见 [二期设计](README-Prometheus-Metrics-Design-Phase2.md)。

## 范围与边界

清理范围严格限定为 Starter 自己注册的一期 SQL/URI 明细 Timer 和 DistributionSummary，以及与这些 Meter 对应的 SQL/URI identity 表、近似 LRU 元数据、注册句柄和 PENDING 状态。

以下内容不清理：

- Druid 原生 SQL/URI Stat；
- 应用或其他组件注册到 `MeterRegistry` 的 Meter；
- SQL mapping 文件及其异步写入线程；
- Prometheus 已经抓取并保存的历史数据；
- 后续二期的 `druid.agg.*` 聚合 Meter 和结构化事件日志。

实现不得调用 Druid 的 `reset-all`、`getValueAndReset()` 或其他修改 Druid 原生 Stat 的 API，也不得按指标名称前缀扫描并删除 registry 中的 Meter。清理必须依据 Starter 保存的准确注册句柄执行，避免误删其他组件的指标。

## 触发模型

本方案不创建定时任务、`TaskScheduler`、后台线程或轮询任务。清理由一期 listener 收到 SQL/URI 事件时顺带判断并触发：

1. listener 在 JVM 内存中记录最近一次成功清理完成时间 `lastDetailMeterCleanupAt`，类型为 `Instant`，初始值为 `null`。
2. 每次事件到达时，将当前时间和 `lastDetailMeterCleanupAt` 按配置的业务时区转换为 `LocalDate`。
3. `lastDetailMeterCleanupAt == null` 时，表示当前从未成功清理，当前事件先执行一次清理。
4. 已有清理时间但其业务日期与当前业务日期不同时，由当前业务日期的第一个事件执行一次清理。
5. 清理时间与当前时间属于同一业务日期时直接处理事件，不重复清理。

因此“每日清理”准确含义是“每个业务日期的第一个事件触发清理”，不保证在 `00:00:00` 准时发生。应用整日没有 SQL/URI 事件时不会执行无意义清理；下一次事件到来时补做当日一次清理，但不会按错过的日期逐日补跑。

`lastDetailMeterCleanupAt` 不持久化到磁盘或外部存储。应用重启后，旧 JVM 的 Meter 和引用已经随进程释放；新 JVM 的第一条事件仍执行一次幂等清理，满足“当前没有清理过就清理一次”的统一语义。每个应用实例独立维护该时间，不需要分布式锁。

## 时区语义

日期边界必须使用显式配置的 `ZoneId`，不得直接依赖容器或 JVM 默认时区。默认业务时区为 `Asia/Shanghai`：

```properties
# 是否启用一期高基数明细 Meter 的事件触发式每日清理
spring.datasource.druid.prometheus.events.cleanup.enabled=true
# 用于判断业务日期的时区
spring.datasource.druid.prometheus.events.cleanup.zone=Asia/Shanghai
```

`events.cleanup.zone` 必须能被 `ZoneId.of(...)` 解析。启动配置非法时关闭清理并输出一次 `WARN`，不能影响 SQL/HTTP 主流程；Apollo 等动态刷新提供非法时区时拒绝新值、继续使用上一个有效时区并输出 `WARN`。启用状态和合法时区动态即时生效。

时区改变后不重写 `lastDetailMeterCleanupAt`。下一条事件分别用新时区转换“最后清理时间”和“当前时间”：若属于同一日期则不清理，若已跨日则立即清理一次。`events.cleanup.enabled` 从 `false` 变为 `true` 后，下一条事件按相同规则判断；若从未清理过则立即清理一次。

## 并发与清理流程

清理判断、一期 Meter 生命周期和事件更新共用一把读写锁：

1. 普通事件在查找、创建和更新一期明细 Meter 的整个临界段持有读锁。
2. 事件初步发现需要清理时释放读锁并申请写锁；取得写锁后必须重新读取当前时间和 `lastDetailMeterCleanupAt`，防止多个并发事件重复清理。
3. 确认需要清理后，按 Starter 保存的注册句柄逐个从 `MeterRegistry` 注销一期 SQL/URI Meter。
4. 清空 SQL/URI identity 表、近似 LRU 元数据、注册句柄和 PENDING 状态，使旧对象不再被 Starter 强引用。
5. 本地状态释放完成后，将当前 `Instant` 写入 `lastDetailMeterCleanupAt`，然后释放写锁。
6. 触发清理的当前事件重新进入一期正常记录流程，按需注册新 Meter，并成为新周期的第一条观测。

清理期间到达的其他事件只等待写锁，不允许并发更新即将注销的 Meter。单个 Meter 注销失败时记录限频 `WARN` 并继续清理其他 Meter；无论 registry 注销结果如何，都必须释放 Starter 的本地引用并记录本次清理完成时间，避免每条后续事件反复触发清理或继续持有陈旧对象。下一次事件可以按正常流程重新注册对应 Meter。

清理完成后输出一条组件业务日志，至少包含业务日期、时区、清理开始/完成时间、SQL identity 数、URI identity 数、成功注销 Meter 数、失败数和耗时。该日志不得写入结构化事件 logger，也不得包含 SQL、SQL MD5 或 URI 明细。

## Prometheus 时间序列语义

清理后，一期 Timer 的 `_count`、`_sum`，DistributionSummary 的 `_count`、`_sum` 以及窗口 `_max` 会在相同 label 的 Meter 被重新注册后从新周期开始。Prometheus 应将其视为一次受控的 counter reset；已抓取的历史样本不会被删除。

一期看板和告警必须使用 `rate()` / `increase()` 处理 counter reset，范围向量至少覆盖两次 scrape，避免直接对单点累计值设置绝对阈值。跨日趋势由 Prometheus 历史数据或 recording rule 提供，不能依赖 JVM 内一期 Meter 的进程累计值。清理和重新注册之间可能恰好发生一次 scrape，届时相关 series 会短暂缺失；告警应设置与 scrape 周期匹配的 `for` 或缺失容忍，避免清理窗口误报。

## 开关关系

`events.cleanup.enabled` 只控制 1.5 期清理逻辑，不改变一期其他配置语义：

- `prometheus.enabled=false` 时不处理 Prometheus 事件，也不触发清理；重新启用后的第一条事件按最近清理时间判断。
- `events.enabled=false` 时一期明细 Meter 不再创建或更新；若 registry 中仍有此前创建的明细 Meter，下一条进入 listener 的事件仍允许执行一次清理以释放存量引用，随后不重新创建一期 Meter。
- `events.max-sql-identities`、`events.max-uri-identities` 和近似 LRU 继续有效；每日清理不是对容量上限的替代。
- `events.max-window` 仍只控制一期 max 的统计窗口，不控制清理周期。

如业务必须保留一期明细 Meter 的完整进程生命周期累计值，可以关闭每日清理，但必须接受长期运行的内存和 FGC 风险。

## 开发与验收

1. 验证 `lastDetailMeterCleanupAt` 初始为 `null` 时首条事件只清理一次，同一业务日期的后续事件不重复清理，跨日后的首条事件再次清理。
2. 使用可注入的 `Clock` 测试 `Asia/Shanghai` 日期边界、UTC 日期不同但业务日期相同、业务日期跨日及夏令时区场景；生产代码不得通过散落的 `System.currentTimeMillis()` 判断日期。
3. 验证只注销 Starter 保存句柄对应的一期 SQL/URI Meter，并释放 identity、LRU、注册句柄和 PENDING 引用；不得影响 Druid 原生 Stat、SQL mapping、其他组件 Meter 或二期能力。
4. 验证触发清理的当前事件在清理后正常注册并记录，清理前的事件不会在旧 Meter 上发生并发更新。
5. 并发压测多个 SQL/URI 事件同时跨日到达的场景，确保只清理一次，不发生漏清、双重注册、死锁或并发修改异常。
6. 验证单个 Meter 注销失败时继续清理、释放所有本地引用、记录限频 `WARN`，且同日后续事件不会因失败反复执行整批清理。
7. 验证开关和时区动态刷新：关闭后不清理，重新开启后按已有时间判断；合法时区立即生效，非法时区保留旧值且不影响业务事件。
8. 验证一期 counter reset 后 Prometheus `rate()` / `increase()` 查询保持合理，清理窗口的 series 短暂缺失不会触发误报。
9. 断言实现不创建 `TaskScheduler`、定时任务、后台线程或轮询任务。

## 已接受的影响

1. 清理发生时间取决于当日首个 SQL/URI 事件，低流量应用不会严格在零点清理。
2. 每日清理会改变一期累计指标的进程生命周期语义，并产生可识别的 counter reset。
3. 写锁会在清理期间短暂阻塞事件记录；identity 上限默认 1000，清理工作有界，不得在写锁内执行 SQL mapping 文件 I/O 或其他慢操作。
4. 清理只能释放 Starter 仍持有的明细 Meter 和索引对象；若 FGC 主要由 Druid 原生 Stat、应用 SQL 缓存或其他组件引起，需要另行定位，1.5 期不能保证完全消除 FGC。
