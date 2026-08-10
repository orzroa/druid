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

同样不得调用 `CollectorRegistry.clear()`，也不得遍历清空应用的 `MeterRegistry`。Starter 使用的是 Spring 容器中已有的应用级 `MeterRegistry`，并非 Druid 私有 Registry；其中可能同时包含 JVM、Spring、HTTP 和业务指标。Prometheus 的一个 metric family 还会包含多个不同标签组合，注销整个 family 会误删未被 LRU 淘汰的其他 SQL/URI 序列。

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
3. 确认需要清理后，先固定解析一期 7 个 SQL/URI metric family 的 `children`；全部解析成功后按 family 执行 `clear()`。该步骤不依赖当前 `MeterRegistry` 快照，因此能清理已经从 Micrometer 删除、却仍被 Prometheus 暴露的孤儿 child。
4. 扫描 `MeterRegistry` 并按 7 个精确逻辑名称注销 Micrometer Meter；若注入的是 `CompositeMeterRegistry`，再通过公开 API 单次扫描各子 Registry 并同步注销实际 Meter。随后清空 SQL/URI identity 表、近似 LRU 元数据和 PENDING 状态。
5. 本地状态释放完成后，更新最近清理基准和下一个固定清理点，然后释放写锁。
6. 触发清理的当前事件重新进入一期正常记录流程，按需注册新 Meter，并成为新区间的第一条观测。

清理期间到达的其他事件只等待写锁，不允许并发更新即将注销的 Meter。周期清理若无法解析任一目标 family，会输出 `ERROR`、中止本周期并保持 Meter 与本地状态不变；禁止退化为逐 Meter collector 清理。无论结果如何都记录本次清理完成时间，避免每条后续事件反复触发整批清理。实现不再保存 `failedRemovals` 或执行逐 Meter 重试；下一周期仍固定通杀 7 个 family，自然覆盖此前遗留的孤儿 child。

清理完成后输出一条组件业务日志，用于事后检查清理是否按预期执行以及清理了多少存量对象。触发来源解析、日志格式化和日志输出均在释放清理写锁后执行，避免延长事件阻塞时间。日志至少包含以下字段：

- 业务日期（`yyyy-MM-dd`，按系统默认时区）
- 时区 ID
- 触发原因：`cross-day`（跨自然日）/ `new-interval`（同日进入新区间）
- 固定清理点（`floor(h / n) * n:00`，按系统默认时区）
- 区间序号（当日第几个 `n` 小时段，0-based）
- `interval-hours` 当前值
- 清理开始时间、完成时间（ISO 8601 含时区）
- 耗时毫秒
- Prometheus family 清理结果、Composite Registry 树清理结果
- 清理前 SQL/URI identity 数与 SQL/URI Meter 数（单位分开）
- SQL/URI 成功注销 Meter 数、失败注销 Meter 数
- 触发事件类型（`sql`/`uri`）和触发事件的数据源名（SQL 事件时）或已解析的 URI 模板（URI 事件时）

若 `familySuccess=false`，表示 Meter 注销阶段没有执行，此时成功数和失败数均为 0；不得把
“未尝试”计入 `sqlMeterFailed` / `uriMeterFailed`。

该日志写入独立 logger（不得写入结构化事件 logger，也不得写入 Prometheus 抓取通道），默认级别 `INFO`；单个 Meter 注销失败的限频 `WARN` 与本日志分离输出。日志不得包含 SQL 或 SQL MD5。

细粒度排障使用独立的 `druid.prometheus.detail` 业务 logger，默认关闭；将其设置为 `TRACE` 后，会记录每次 identity 查询命中/未命中、Meter 注册或复用、数值 record、LRU 淘汰、注册失败回滚、注销、Registry 清理扫描，以及清理调度判断。TRACE 日志仅包含 Meter ID/tags、SQL MD5、数据源名、URI 模板和记录值，不包含 SQL 原文。Spring Boot 示例：

```properties
logging.level.druid.prometheus.detail=TRACE
```

TRACE 仅用于短时故障定位；它会为每次指标操作产生业务日志，不建议在高流量生产环境长期启用。Prometheus/Actuator 对 Registry 的抓取由 Micrometer 执行，不经过 Listener，因此这里的“查询”指 Listener identity 查询和清理时的 Registry 扫描，不包含每次 scrape 对 Meter 的读取。

## Micrometer 1.1.0 与 simpleclient 暴露层清理

### 实际绑定关系

当前 Starter 不创建独立 Registry。`DruidPrometheusMetricsListener` 通过 `ObjectProvider<MeterRegistry>` 获取 Spring 容器中的应用级 Registry；它可能是 `PrometheusMeterRegistry`，也可能是包含 Prometheus 子 Registry 的 `CompositeMeterRegistry`。

Micrometer 1.1.0 和 `simpleclient_spring_boot` 0.5.0 的关系如下：

1. `PrometheusMeterRegistry(PrometheusConfig)` 会创建一个新的 `CollectorRegistry`。
2. `PrometheusMeterRegistry(PrometheusConfig, CollectorRegistry, Clock)` 使用调用方传入的 `CollectorRegistry`。
3. `simpleclient_spring_boot` 0.5.0 创建的 `PrometheusEndpoint` 固定暴露 `CollectorRegistry.defaultRegistry`。
4. 因此，只有 Micrometer 的 Prometheus Registry 显式绑定到 `CollectorRegistry.defaultRegistry` 时，Druid 写入的指标和 `/admin/prometheus` 才处于同一条暴露链；若应用注入的是 `CompositeMeterRegistry`，必须检查其中实际的 Prometheus 子 Registry。

初始化和每次清理完成后，`druid.prometheus.detail=TRACE` 会输出诊断日志，包含：

- `meterRegistryType`、`meterRegistryIdentity`；
- `collectorRegistryType`、`collectorRegistryIdentity`；
- `simpleclient` 默认 Registry 的 identity；
- `sharedWithSimpleclientDefault`；
- Micrometer 绑定 Registry 和默认 Registry 当前各自暴露的 Druid metric family。

`sharedWithSimpleclientDefault=true` 表示清理目标与 `/admin/prometheus` 是同一个 CollectorRegistry。若为 `false`，必须先检查应用的 Registry 装配：清理 Micrometer 所绑定的独立 CollectorRegistry，不会删除 `PrometheusMvcEndpoint` 所暴露的默认 Registry 中的旧数据。禁止用 `CollectorRegistry.clear()` 掩盖绑定错误。

### `MeterRegistry.remove()` 后 endpoint 仍有旧指标的原因

Micrometer 1.1.0 的 `MeterRegistry.remove(meter)` 只从 Micrometer 的 `meterMap` 删除 Meter。`PrometheusMeterRegistry` 还会按 metric family 把采样 child 保存在私有的 `collectorMap -> MicrometerCollector.children` 中；1.1.0 没有在 `remove()` 时同步删除这个 child，也没有提供公开的单 child 删除 API。

因此会出现以下现象：

- `meterRegistry.getMeters().size()` 已经下降，甚至只剩少量非 Druid Meter；
- Starter 的 `sqlStates.size()`、`uriMeters.size()` 已经下降或清零；
- `/admin/prometheus` 仍然输出旧的 `druid_sql_*`、`druid_uri_*` label set。

这些 size 只代表 Micrometer 逻辑层或 Starter 本地缓存，不代表 simpleclient CollectorRegistry 的最终暴露内容。该问题不是 Prometheus 抓取缓存，也不是 endpoint 转换组件重新创建指标，而是 Micrometer 1.1.0 的暴露层 child 没有随 Meter 删除。

### 精确清理实现

清理按以下顺序执行：

1. 使用公开 API `MeterRegistry.remove(meter)` 删除 Micrometer 逻辑层 Meter。
2. 若是 `CompositeMeterRegistry`，定位实际 Prometheus 子 Registry，并通过公开 API 删除子 Registry 中对应的实际 Meter。
3. 使用 Registry 当前配置的公开 `NamingConvention` 计算 collectorMap key，不能手写点号转下划线或 Timer `_seconds` 规则。
4. 由于 Micrometer 1.1.0 没有公开的 child 删除 API，版本适配器反射读取 `PrometheusMeterRegistry.collectorMap` 和 `MicrometerCollector.children`，按捕获的 Meter 实例或相同 `Meter.Id` 精确定位并删除一个 child。
5. 不注销仍有其他 child 的 metric family，避免误删其他 SQL/URI 标签组合。最后一个 child 删除后保留固定数量的空 `MicrometerCollector`；1.1.0 的空 collector `collect()` 返回空列表，不会出现在 scrape 中，后续同名 Meter 还能安全复用它。该对象数量最多等于一期固定的 7 个 metric family，不随 SQL/URI identity 数增长。
6. Prometheus child 精确清理失败会记录限频告警，但不保存失败句柄；下一次周期清理固定清空全部 7 个 family，可覆盖该孤儿 child。关闭周期清理时，告警是唯一自动诊断信号，需要人工处置。

上述顺序仅适用于 LRU 单 identity 淘汰和注册失败回滚。定期清理已确定要删除全部一期明细 Meter，因此固定解析 7 个已知 family，一次性清空这些 collector 的
`children`，再执行 `MeterRegistry.remove()`；这避免在 `CopyOnWriteArrayList` 上逐项删除
产生 O(N²) 数组复制。批量清理会先解析全部目标 collector，全部成功后才统一 `clear()`；
解析失败时输出 `ERROR` 并中止本周期，保持 Meter 和 identity 不变，禁止退化为逐项
collector 清理。批量清理只清空 child，不从 `CollectorRegistry` 注销 collector，保证下一
周期同名 Meter 可以重新暴露。即使孤儿 child 已经不在 Registry 快照中，固定 family 清理仍能覆盖。LRU 单 identity 淘汰仍使用上述逐 Meter 精确清理，避免误删同 family 的其他有效标签。

反射代码集中封装在 `DruidPrometheusCollectorCleanup`，生产代码仍只编译依赖可选的 `micrometer-core`，不强制应用引入 Prometheus 实现。`micrometer-registry-prometheus`、`simpleclient` 和 `simpleclient_spring_boot` 仅作为测试依赖用于锁定 1.1.0/0.5.0 行为。升级 Micrometer 后必须重新验证 `collectorMap`、`children` 和 child 捕获字段布局；布局不兼容时周期适配器记录带 `micrometerVersionRisk` 的 `ERROR` 并中止本周期，不得静默报告清理成功。

禁止采用以下替代方案：

- `CollectorRegistry.clear()`：会删除同一 Registry 中 JVM、Spring 和业务 Collector；
- 清空或遍历删除应用 `MeterRegistry` 的全部 Meter：Druid 不拥有该 Registry；
- 直接注销整个 Druid metric family：LRU 只淘汰一个 identity 时会同时删除其他有效 label set；
- 只执行 `sqlStates.clear()` / `uriMeters.clear()`：这只释放 Starter 本地索引，不会删除 Micrometer Meter 和 Prometheus child；
- 单独为 Druid `new PrometheusMeterRegistry` 但不改 endpoint：新 Registry 默认使用独立 CollectorRegistry，现有 `/admin/prometheus` 不会暴露其中的数据。若未来采用独立 Registry，必须同时设计独立 endpoint 或合并 scrape，并重新处理公共 tags、NamingConvention、MeterFilter 和 Composite 后端。

## Prometheus 时间序列语义

成功注销后，一期 Timer 的 `_count`、`_sum`，DistributionSummary 的 `_count`、`_sum` 以及窗口 `_max` 会在相同 label 的 Meter 被重新注册后从新周期开始。Prometheus 应将其视为一次受控的 counter reset；已抓取的历史样本不会被删除。LRU 精确清理若出现“Micrometer 删除成功、Prometheus child 删除失败”，旧 child 可能继续暴露到下一次固定 family 清理；该情况会记录限频告警，不能按成功 reset 解释。

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
7. 验证 LRU 单 Meter 精确注销失败时记录限频告警、不保存失败句柄；下一个周期通过固定 family 清理移除孤儿 child。同区间后续事件不会因失败反复执行整批清理。
8. 验证开关和 `interval-hours` 动态刷新：`prometheus.enabled=false` 或 `events.enabled=false` 时既不记录事件也不触发清理，重新启用后下一条有效事件按已有固定清理点判断；`interval-hours` 从合法变到 `<=0` 时立即停止清理，从 `<=0` 恢复到 `1..23` 或 `>=24` 时按已有基准重新计算固定清理点，若基准尚未建立则由下一条事件初始化且不立即清理；`n` 在 `1..23` 与 `>=24` 之间切换时，按新语义立即作用于下一条事件。
9. 验证一期 counter reset 后 Prometheus `rate()` / `increase()` 查询保持合理，清理窗口的 series 短暂缺失不会触发误报。
10. 验证清理完成后输出一条组件业务日志，包含业务日期、时区、触发原因（`cross-day`/`new-interval`）、固定清理点、区间序号、`interval-hours`、开始/完成时间、耗时、清理前 SQL/URI identity 数、成功/失败注销数、触发事件类型，以及 SQL 事件的数据源名或 URI 事件的已解析 URI 模板；日志不得写入结构化事件 logger，也不得包含 SQL 或 SQL MD5。事后可通过该日志核对每次清理是否按时触发、清理了多少存量对象。
11. 使用 Micrometer Prometheus 1.1.0 验证“创建 Druid Meter → scrape 可见 → 删除 Meter 和 collector child → scrape 不再可见”，并确认同一 Registry 中的非 Druid Collector 仍然存在。
12. 使用 `simpleclient_spring_boot` 0.5.0 的实际 `PrometheusMvcEndpoint` 和 `CollectorRegistry.defaultRegistry` 验证 `/admin/prometheus` 暴露链，而不只验证 `PrometheusMeterRegistry.scrape()`。
13. 验证 `CompositeMeterRegistry`、自定义 `NamingConvention`、同 family 多标签 LRU 淘汰及 family 清空后重新注册：只能删除被淘汰 label set，其他 SQL/URI 标签必须继续暴露。

## 已接受的影响

1. 清理发生时间取决于进入新区间的首个 SQL/URI 事件，低流量应用不会严格在整点清理。
2. 定期清理会改变一期累计指标的进程生命周期语义，并产生可识别的 counter reset。
3. 写锁会在清理期间短暂阻塞事件记录；identity 上限默认 1000，清理工作有界，不得在写锁内执行 SQL mapping 文件 I/O 或其他慢操作。
4. 清理只能释放 Starter 仍持有的明细 Meter 和索引对象；若 FGC 主要由 Druid 原生 Stat、应用 SQL 缓存或其他组件引起，需要另行定位，1.5 期不能保证完全消除 FGC。
5. `n` 越小，counter reset 越频繁；`rate()`/`increase()` 的范围向量窗口仍应主要根据 scrape 周期、样本数量和查询稳定性确定。过小的 `n`（如 1）会在高 QPS 模块上产生明显 reset 次数，需结合 scrape 周期权衡。

## auth tpdev 清理长链路实验记录

### 实验目标与执行方式

在 `tpdev` profile 下启动 `crs-auth-java`，验证完整链路：访问数据源触发 Druid SQL 指标、
`DruidPrometheusMeterCleanup` 清理、再访问 `/admin/prometheus`，确认被清理 SQL 的
`druid_sql_*` series 不再暴露，而触发清理后的新 SQL series 仍存在。

测试文件：
`/home/sc/Workspaces/saiscjava/crs-auth-java/src/test/java/nextdms/DruidPrometheusCleanupEndToEndTest.java`

执行命令：

```bash
mvn -Dtest=nextdms.DruidPrometheusCleanupEndToEndTest test
```

### 实验 1：直接 JDBC + SQL 原文 MD5

结果：失败（已废弃）。

现象：真实 `JdbcTemplate` 查询后，测试以 `MD5("SELECT 131415")` 在
`/admin/prometheus` 中寻找 series，未找到。

原因：测试将执行 SQL 的原文直接等同于 Listener 最终收到的 SQL label，缺少对 Listener
实际注册 Meter 的验证；同时查询没有经过 HTTP 访问链路，不符合端到端目标。

处理：改为执行 tpdev 只读 SQL，并从 Listener 已注册的 Meter 获取真实
`sql` / `datasource` 标签，再匹配 Prometheus 的单条样本。

### 实验 2：测试 Controller 重复注册

结果：失败（方案废弃）。

现象：Spring MVC 报测试路由 `Ambiguous mapping`。

原因：嵌套 `@RestController` 已被 `nextdms` 组件扫描，同时又被测试配置显式 `@Bean`
注册。

处理：移除测试 Controller 方案；该应用的 MVC Context 结构不适合用临时 Controller
作为 SQL 指标入口。

### 实验 3：注入了错误的 MeterRegistry

结果：失败，不能用于判断清理。

现象：HTTP 请求完成后，自动注入的 `MeterRegistry` 中没有新的
`druid.sql.execution.duration`。

原因：auth 应用中存在多个 `MeterRegistry`；测试字段注入的实例不保证是
`DruidPrometheusMetricsListener` 实际使用的实例。

处理：测试改为读取 Listener 的实际 `meterRegistry` 字段；该字段只用于测试观察对象，
生产代码不依赖此反射。

### 实验 4：HTTP 请求可能未进入 Controller

结果：失败，不能用于判断清理。

现象：原测试没有断言测试业务路由的响应状态，普通路径会进入应用默认安全链，可能在
Controller 前被拦截。

原因：测试没有证明用于产生指标的 SQL 已真正执行。

处理：不再依赖测试路由；测试通过 tpdev 的 `primaryJdbcTemplate` 执行只读 SQL，并调用
Listener 的公开事件入口来覆盖正式的清理调度与 Meter 注册逻辑。

### 实验 5：tpdev 数据源连接失败

结果：启动失败，未执行测试断言。

现象：本次执行在初始化 tpdev MySQL 数据源时出现 `CommunicationsException`，连接未建立；
Maven 因测试上下文启动失败退出。

原因：外部 tpdev 数据库连接瞬时不可用，与 Druid Meter 清理逻辑无关。

处理：连接恢复后继续执行下列实验。

### 实验 6：确认 endpoint 与 Listener 绑定

结果：通过。

现象：直接调用 `PrometheusMvcEndpoint` 与通过 MockMvc 访问 `/admin/prometheus` 都能看到
新建的 `druid_sql_*` 样本。

结论：验收对象确实是 simpleclient 暴露的 endpoint，而非只验证 Micrometer 的内存 Meter 数。

### 实验 7：修复后首轮长链路仍报失败

结果：测试失败；该结果后来确认是 SQL identity 干扰造成的误判，不能用于否定清理修复。

现象：调度日志显示已移除 6 个 SQL Meter；但用相同 `sql` 标签匹配 scrape 时仍能看到 9 条
`druid_sql_*` 样本。

背景：原生产问题的根因仍是 Micrometer 1.1.0 的 `MeterRegistry.remove()` 不会自动移除
Prometheus `MicrometerCollector.children`。正式实现已经在删除 Meter 时同步删除其对应
child，且不调用 `CollectorRegistry.clear()`；本实验验证的是修复后的长链路。

### 实验 8：Collector child 清理诊断

结果：通过诊断，排除“child 未清空”。

现象：清理 trace 显示周期扫描移除了 6 个 SQL Meter；实验性 family 兜底执行时 child 已为 0。

当时结论：本次样本中的相同标签由 SQL identity 归并造成，不能用来判断旧 child 是否仍然
留存。该诊断只解释了当次测试误判，不代表逐 Meter 重试是必要设计；后续整体复审已将周期
路径收敛为固定 family 清理。

### 实验 9：SQL identity 归并误判

结果：发现测试问题并修正。

现象：两条仅常量不同的 `SELECT` 可能被 Druid 参数化/归并为相同 SQL identity。清理后第二条
SQL 会在新周期重新创建同一标签；测试把它误判成旧指标未清理。

处理：改用别名不同的两条 SQL（`cleanup_probe`、`cleanup_trigger`），并在测试中断言两次
得到的 `sql` 标签不同。

### 实验 10：最终 tpdev 长链路验证

结果：通过；2026-08-10 再次验证耗时约 68 秒。

链路与断言：

1. 使用 `tpdev` profile 启动 auth；
2. 执行第一条只读 SQL，确认 `/admin/prometheus` 输出该 `druid_sql_*` 标签；
3. 强制下一次周期清理到期，执行第二条、不同 identity 的只读 SQL；
4. 确认第一条 SQL 的全部 `druid_sql_*` 样本不再输出，第二条 SQL 的样本存在。

Maven 结果：`Tests run: 2, Failures: 0, Errors: 0, Skipped: 0`。

结论：本次新建的 `druid_sql_*`（同样适用于受同一 family 清理逻辑处理的
`druid_uri_*`）可以在周期清理后从 `/admin/prometheus` 实际输出中移除；不会清空 JVM、
Spring 或其他业务指标。

### 实验 11：干扰项代码复审与收敛

结果：删除实验性方案，并保留安全的性能优化。

删除内容及原因：

1. 删除“全 JVM Listener 静态实例表”。该推测没有被实验支持，而且诊断代码创建的临时
   cleanup 对象会进入静态表，存在生命周期泄漏；
2. 删除从 `PrometheusMvcEndpoint` 私有字段反查 registry 的代码。Druid 只应清理注入给
   Listener 的 MeterRegistry 暴露层，不应耦合具体 endpoint 实现；
3. 删除直接修改 simpleclient `namesToCollectors` / `collectorsToNames` 并注销 collector 的
   代码。这样会留下仍缓存于 Micrometer `collectorMap`、却已从 endpoint 注销的 collector，
   导致后续同名指标无法重新暴露；
4. 删除递归扫描 lambda 对象图和调用 child `samples()` 匹配标签的回退。Micrometer 1.1.0
   的 Druid Timer/Summary child 直接捕获对应 Meter，现有精确引用匹配已经被单测和长链路验证；
5. 删除 auth UT 中仅为排查同 identity 误判加入的 endpoint 私有字段、collector child 数量
   反射和 TRACE 日志。

保留并新增的优化：周期清理不依赖 Registry 快照推导 family，而是固定解析一期 7 个 Druid
family 并批量 `clear()` collector child，再删除 Registry 中的 Meter。这样既能覆盖已经脱离
Micrometer Registry 的孤儿 child，也不注销 collector、不影响其他指标；同时
避免最多约 7000 个 Meter 在 `CopyOnWriteArrayList` 上逐项删除造成 O(N²) 数组复制。若批量
反射解析失败，则输出 `ERROR` 并中止本周期；不会进入可能急剧影响性能的逐 Meter fallback。
因此删除 `failedRemovals`、逐 Meter 重试和相关计数；下一周期再次固定清理全部 family 即可。

### 实验 12：清理代码整体复审

结果：通过，清理模型完成收敛。

1. 周期清理固定解析并清空一期全部 7 个 family，不再从 Registry 快照推导目标，因此
   `MeterRegistry.remove()` 后已经脱离快照的孤儿 child 也能被清理；
2. 删除 `failedRemovals`、逐 Meter 重试及 retry 计数。family 解析失败时输出 `ERROR` 并中止
   本周期，下一周期仍固定通杀，不进入 O(N²) 的逐 child fallback；
3. LRU 和注册失败回滚仍精确删除单个 child，避免误删同 family 的其他有效标签；直接
   Prometheus Registry 不再在每次 LRU 删除后额外 O(N) 扫描 Meter；
4. `CompositeMeterRegistry` 的周期路径在 family 清空后使用 Micrometer 公开 API 单次扫描子
   Registry 并注销实际 Meter，避免新注册复用一个已清空 child 的旧 Meter；
5. 清理日志拆分 identity 数和 Meter 数，并增加 `familySuccess`、`registryTreeSuccess`，避免
   把不同单位混在同一组 success/before 字段中。

验证结果：Starter 全量 `70/70`、清理定向 `26/26`、7 个实际 family 批量清理 `11/11`；
auth `tpdev` 长链路 `2/2`，真实 `/admin/prometheus` 中旧 SQL series 消失且新周期 series 存在。
