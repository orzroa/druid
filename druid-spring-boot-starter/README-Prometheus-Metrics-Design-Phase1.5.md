# Druid Prometheus 1.5 期：高基数明细 Meter 定期清理

## 背景与目标

一期 Prometheus 指标已经上线，定义见 [一期设计](README-Prometheus-Metrics-Design.md)。部分模块的 SQL 模板数量和变化速度较高，长期运行后即使一期已有 identity 上限和近似 LRU，仍会持续发生 Meter 创建、注销及相关对象积累，现场观察到 FGC 次数明显增加。

1.5 期仅解决一期高基数 SQL/URI 明细 Meter 的生命周期问题：从每天 00:00 开始，每经过配置的 `n` 小时清理一次 Starter 持有的一期明细 Meter 和 identity/LRU 引用，使其生命周期由整个 JVM 运行期收敛到 `n` 小时。该改造不引入二期低基数指标或结构化日志；二期设计继续见 [二期设计](README-Prometheus-Metrics-Design-Phase2.md)。

## 范围与边界

清理范围严格限定为 Starter 自己注册的一期 SQL/URI 明细 Timer 和 DistributionSummary，以及与这些 Meter 对应的 SQL/URI identity 表、近似 LRU 元数据、注册句柄和 PENDING 状态。

以下内容不清理：

- Druid 原生 SQL/URI Stat；
- 应用或其他组件注册到 `MeterRegistry` 的非 Druid 明细 Meter；
- SQL mapping 文件及其异步写入线程；
- Prometheus 已经抓取并保存的历史数据；
- 后续二期的 `druid.agg.*` 聚合 Meter 和结构化事件日志。

实现不得调用 Druid 的 `reset-all`、`getValueAndReset()` 或其他修改 Druid 原生 Stat 的 API。清理按一期定义的 7 个精确 Meter 名称扫描 registry，不使用宽泛名称前缀；这些名称属于本模块保留的明细指标命名空间。不能只依赖 Listener 的本地 identity/ownership 索引，否则索引淘汰或注销异常后会留下无法再次发现的孤儿 Meter。

## 触发模型

本方案不创建定时任务、`TaskScheduler`、后台线程或轮询任务。清理由一期 listener 收到 SQL/URI 事件时顺带判断并触发。清理周期由配置项 `events.cleanup.interval-hours` 决定，记为整数 `n`：

1. 当 `n <= 0` 时，清理完全关闭，不执行任何清理判断或操作。
2. 当 `n >= 24` 时，退化为每日清理一次：仅在跨入新自然日的首条事件触发清理，当日内不再清理。
3. 当 `0 < n < 24` 时，清理生效。基准时间点为业务时区下的 `00:00:00`，此后每经过 `n` 小时为一个清理点：`00:00`、`n:00`、`2n:00`……，直到当日 `23:59:59` 为止；下一个自然日 `00:00` 重新开始计数。
4. 独立清理组件 `DruidPrometheusMeterCleanup` 在 JVM 内存中以 epoch millis 记录最近一次清理基准时间，初始状态为未初始化。进程刚启动时由首条事件将其初始化为当前时间，不算清理；同时按系统默认时区预先算出下一个固定清理点。
5. 普通事件到达时，清理组件只调用 `Clock.millis()`，并与缓存的下一个固定清理点做整数比较；未到截止时间直接返回，不创建 `Instant`、`ZonedDateTime`、lambda 或其他清理判断临时对象。时区换算仅发生在首次初始化、确认跨过清理点、动态刷新周期和输出清理日志等低频路径。
6. 判定规则（必须按顺序求值）：
   - 若 `n <= 0`：不执行任何清理判断。
   - 若清理基准尚未初始化：进程刚启动，**不执行清理**，仅初始化当前基准及下一个固定清理点；事件按正常流程处理。
   - 否则若当前 epoch millis 尚未到缓存的下一个固定清理点：不清理。
   - 否则若 `n >= 24`：当前事件执行每日清理。
   - 否则（`0 < n < 24`）：设当前时间的当地小时字段为 `h`（`0 <= h < 24`），当前区间序号为 `slot = floor(h / n)`，对应的固定清理点小时为 `boundaryHour = slot * n`。
     - 当前事件立即清理；若最近基准时间与当前时间属于不同自然日，原因为 `cross-day`，否则为 `new-interval`。
   - 清理完成后，以当前时间推进最近基准，并重新计算当前时间所在区间之后的下一个固定清理点；不会按错过的区间逐个补跑。
7. 同一固定清理点内的后续事件不重复清理。

因此“每 `n` 小时清理一次”的准确含义是：清理点是按业务时区当日 `00:00` 起每 `n` 整点固定的（`00:00`、`n:00`、`2n:00`……），与上次清理在区间内的具体时刻无关；事件到达时若发现当前已到达缓存的下一个固定清理点，即触发一次清理。某区间内一直没有 SQL/URI 事件时不会执行无意义清理；下一次事件到来时按规则补做一次，但不会按错过的区间逐个补跑。

清理基准和下一个清理点不持久化到磁盘或外部存储。应用重启后，旧 JVM 的 Meter 和引用已经随进程释放，新 JVM 的清理基准为未初始化，第一条事件按规则不立即清理，而是建立当前基准和下一个固定清理点；避免新进程刚启动就做无意义的空清理。每个应用实例独立维护该时间，不需要分布式锁。

## 时间语义

日期/小时边界使用 JVM 系统默认时区（`ZoneId.systemDefault()`），不提供单独的时区配置项。应用容器时区应与业务时区一致：

```properties
# 清理周期（整数小时），默认 6，自当日 00:00 起每 n 小时清理一次
# n<=0 时关闭清理；n>=24 时退化为每日 0 点清理一次
spring.datasource.druid.prometheus.events.cleanup.interval-hours=6
```

`interval-hours` 默认值为 `6`；动态即时生效。`interval-hours` 是清理功能开关，不再使用单独的 `cleanup.enabled`；清理实际执行还要求 `prometheus.enabled=true`、`events.enabled=true`。

`interval-hours` 的动态刷新立即作用于下一条事件：若新值落入 `1..23`，按新 `n` 和已有基准重新计算下一个固定清理点；若新值落到 `<=0`，清理立即关闭，此后事件不再清理，也不重置已有基准；若新值落到 `>=24`，按已有基准重新计算次日 0 点。重新恢复到合法值后，下一条事件与重新计算的固定清理点比较，已经跨过该点则立即清理一次。

`interval-hours` 从有效值变到 `<=0` 时清理立即关闭，此后事件不再清理，也不重置已有基准；从 `<=0` 恢复到 `1..23` 或 `>=24` 后，若基准已经建立则立即按新周期重算固定清理点，若基准尚未建立则由下一条事件初始化且不立即清理。

## 并发与清理流程

清理判断、一期 Meter 生命周期和事件更新共用一把读写锁：

1. 普通事件在查找、创建和更新一期明细 Meter 的整个临界段持有读锁。
2. 事件初步发现当前时间已到缓存清理点时申请写锁；取得写锁后必须重新读取当前时间、当前周期和缓存清理点，防止动态刷新混用新旧配置或多个并发事件重复清理。
3. 确认需要清理后，扫描 `MeterRegistry` 并按 7 个精确名称逐个注销一期 SQL/URI 明细 Meter。
4. 清空 SQL/URI identity 表、近似 LRU 元数据和 PENDING 状态，使旧对象不再被 Starter 强引用。注销失败的 Meter 仅保留准确句柄供下一个清理周期重试，不保留其 identity/LRU 状态。
5. 本地状态释放完成后，更新最近清理基准和下一个固定清理点，然后释放写锁。
6. 触发清理的当前事件重新进入一期正常记录流程，按需注册新 Meter，并成为新区间的第一条观测。

清理期间到达的其他事件只等待写锁，不允许并发更新即将注销的 Meter。单个 Meter 注销失败时记录限频 `WARN` 并继续清理其他 Meter；无论 registry 注销结果如何，都必须记录本次清理完成时间，避免每条后续事件反复触发整批清理。注销失败的 Meter 保留准确句柄，并在下一个清理周期重试。下一次事件仍按正常流程获取或注册对应 Meter。

清理完成后输出一条组件业务日志，用于事后检查清理是否按预期执行以及清理了多少存量对象。触发来源解析、日志格式化和日志输出均在释放清理写锁后执行，避免延长事件阻塞时间。日志至少包含以下字段：

- 业务日期（`yyyy-MM-dd`，按系统默认时区）
- 时区 ID
- 触发原因：`cross-day`（跨自然日）/ `new-interval`（同日进入新区间）
- 固定清理点（`floor(h / n) * n:00`，按系统默认时区）
- 区间序号（当日第几个 `n` 小时段，0-based）
- `interval-hours` 当前值
- 清理开始时间、完成时间（ISO 8601 含时区）
- 耗时毫秒
- 清理前 SQL identity 数、清理前 URI identity 数
- 成功注销 Meter 数、失败注销 Meter 数
- 触发事件类型（`sql`/`uri`）和触发事件的数据源名（SQL 事件时）或已解析的 URI 模板（URI 事件时）

该日志写入独立 logger（不得写入结构化事件 logger，也不得写入 Prometheus 抓取通道），默认级别 `INFO`；单个 Meter 注销失败的限频 `WARN` 与本日志分离输出。日志不得包含 SQL 或 SQL MD5。

细粒度排障使用独立的 `druid.prometheus.detail` 业务 logger，默认关闭；将其设置为 `TRACE` 后，会记录每次 identity 查询命中/未命中、Meter 注册或复用、数值 record、LRU 淘汰、注册失败回滚、注销与失败重试、Registry 清理扫描，以及清理调度判断。TRACE 日志仅包含 Meter ID/tags、SQL MD5、数据源名、URI 模板和记录值，不包含 SQL 原文。Spring Boot 示例：

```properties
logging.level.druid.prometheus.detail=TRACE
```

TRACE 仅用于短时故障定位；它会为每次指标操作产生业务日志，不建议在高流量生产环境长期启用。Prometheus/Actuator 对 Registry 的抓取由 Micrometer 执行，不经过 Listener，因此这里的“查询”指 Listener identity 查询和清理时的 Registry 扫描，不包含每次 scrape 对 Meter 的读取。

## Prometheus 时间序列语义

成功注销后，一期 Timer 的 `_count`、`_sum`，DistributionSummary 的 `_count`、`_sum` 以及窗口 `_max` 会在相同 label 的 Meter 被重新注册后从新周期开始。Prometheus 应将其视为一次受控的 counter reset；已抓取的历史样本不会被删除。注销失败的 Meter 会继续复用原有实例并保留累计值，不会在本次清理中 reset；待后续清理周期成功注销并重新注册后才发生 reset。

一期看板和告警必须使用 `rate()` / `increase()` 处理 counter reset，范围向量至少覆盖两次 scrape，避免直接对单点累计值设置绝对阈值。跨区间趋势由 Prometheus 历史数据或 recording rule 提供，不能依赖 JVM 内一期 Meter 的进程累计值。清理和重新注册之间可能恰好发生一次 scrape，届时相关 series 会短暂缺失；告警应设置与 scrape 周期匹配的 `for` 或缺失容忍，避免清理窗口误报。

## 开关关系

`events.cleanup.interval-hours` 是清理功能开关；实际执行还受 Prometheus 总开关和事件开关约束，不改变一期其他配置语义：

- `prometheus.enabled=false` 时不处理 Prometheus 事件，也不触发清理；重新启用后的第一条事件按最近清理时间判断。
- `events.enabled=false` 时不再处理一期明细 Meter 事件，也不触发清理；registry 中此前创建的明细 Meter 和 Starter 本地引用继续保留。重新启用后，下一条有效事件按最近清理时间判断是否需要清理。
- `events.max-sql-identities`、`events.max-uri-identities` 和近似 LRU 继续有效；定期清理不是对容量上限的替代。
- `events.max-window` 仍只控制一期 max 的统计窗口，不控制清理周期；清理周期仅由 `interval-hours` 决定。
- `interval-hours <= 0` 时清理整体关闭，但不会重置已有清理基准；`interval-hours >= 24` 时退化为每日 0 点清理一次，同日不重复清理。

如业务必须保留一期明细 Meter 的完整进程生命周期累计值，可以关闭定期清理，但必须接受长期运行的内存和 FGC 风险。

## 开发与验收

1. 验证清理基准初始未建立时，进程刚启动的首条事件不立即清理，仅初始化当前基准和下一个固定清理点；之后进入下一固定清理点后的首条事件才执行清理；跨自然日后首条事件也立即清理。同一固定清理点内的后续事件不重复清理。
2. 使用可注入的 `Clock` 测试系统默认时区下的区间边界（如 `n=6` 时 06:00:00 的边界事件归属、11:59 → 12:00 跨区间）及跨日场景；生产代码不得通过散落的 `System.currentTimeMillis()` 判断时间。
3. 验证 `n` 的取值边界：`n <= 0` 时事件路径不执行任何清理判断或操作，仅在启动读取配置或动态刷新为关闭状态时输出一次日志说明清理已关闭；`n >= 24` 时退化为每日清理一次，当日首条跨日事件清理后同日不再清理。
4. 验证按 7 个精确名称注销 registry 中全部一期 SQL/URI 明细 Meter，并释放 identity、LRU、PENDING 引用；不得影响 Druid 原生 Stat、SQL mapping、其他名称的组件 Meter 或二期能力。
5. 验证触发清理的当前事件在清理后正常注册并记录，清理前的事件不会在旧 Meter 上发生并发更新。
6. 并发压测多个 SQL/URI 事件同时跨区间到达的场景，确保只清理一次，不发生漏清、双重注册、死锁或并发修改异常。
7. 验证单个 Meter 注销失败时继续清理其他 Meter、保留失败 Meter 的准确句柄供重试、记录限频 `WARN`，且同区间后续事件不会因失败反复执行整批清理；下一个清理周期会重试该 Meter。
8. 验证开关和 `interval-hours` 动态刷新：`prometheus.enabled=false` 或 `events.enabled=false` 时既不记录事件也不触发清理，重新启用后下一条有效事件按已有固定清理点判断；`interval-hours` 从合法变到 `<=0` 时立即停止清理，从 `<=0` 恢复到 `1..23` 或 `>=24` 时按已有基准重新计算固定清理点，若基准尚未建立则由下一条事件初始化且不立即清理；`n` 在 `1..23` 与 `>=24` 之间切换时，按新语义立即作用于下一条事件。
9. 验证一期 counter reset 后 Prometheus `rate()` / `increase()` 查询保持合理，清理窗口的 series 短暂缺失不会触发误报。
10. 验证清理完成后输出一条组件业务日志，包含业务日期、时区、触发原因（`cross-day`/`new-interval`）、固定清理点、区间序号、`interval-hours`、开始/完成时间、耗时、清理前 SQL/URI identity 数、成功/失败注销数、触发事件类型，以及 SQL 事件的数据源名或 URI 事件的已解析 URI 模板；日志不得写入结构化事件 logger，也不得包含 SQL 或 SQL MD5。事后可通过该日志核对每次清理是否按时触发、清理了多少存量对象。

## 已接受的影响

1. 清理发生时间取决于进入新区间的首个 SQL/URI 事件，低流量应用不会严格在整点清理。
2. 定期清理会改变一期累计指标的进程生命周期语义，并产生可识别的 counter reset。
3. 写锁会在清理期间短暂阻塞事件记录；identity 上限默认 1000，清理工作有界，不得在写锁内执行 SQL mapping 文件 I/O 或其他慢操作。
4. 清理只能释放 Starter 仍持有的明细 Meter 和索引对象；若 FGC 主要由 Druid 原生 Stat、应用 SQL 缓存或其他组件引起，需要另行定位，1.5 期不能保证完全消除 FGC。
5. `n` 越小，counter reset 越频繁；`rate()`/`increase()` 的范围向量窗口仍应主要根据 scrape 周期、样本数量和查询稳定性确定。过小的 `n`（如 1）会在高 QPS 模块上产生明显 reset 次数，需结合 scrape 周期权衡。
