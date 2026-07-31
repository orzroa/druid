# Druid Micrometer / Prometheus 指标改造设计

## 1. 背景与目标

`druid2prom` 是进程外 exporter：它在 Prometheus 抓取时访问 Druid 的
`/druid/sql.json`、`/druid/weburi.json`，并默认每四次抓取调用一次
`/druid/reset-all.json`。合入 Druid Spring Boot Starter 后，指标应写入业务
已有的 Micrometer `MeterRegistry`，由业务自身唯一的
`/actuator/prometheus` 暴露。

本设计的目标是：

- 只保留业务 Actuator 的一个 Prometheus Endpoint；Druid 不注册 Servlet。
- 删除 JSON 管理协议在 JVM 内的序列化、字符串传输和解析开销。
- 不保留旧 `druid2prom` 指标；仅提供旧到新指标的迁移关系表。
- 新增符合 Micrometer/Prometheus 语义的实时指标，尤其是可过期的 max。
- 不因监控采集改变或清空 Druid 业务统计。
- 兼顾 SQL、URI 标签的基数控制及多数据源场景。

非目标：本期不把 Druid core 直接依赖到 Micrometer，也不兼容旧指标名称、类型或
`reset-all` 窗口语义；本期不覆盖 Servlet 异步请求的完整生命周期统计。

## 2. 当前状态

当前 starter 已直接将 SQL 与 Web 事件注册进已有的 `MeterRegistry`，不注册
`/actuator/prometheus` Servlet，也不再读取 Druid 的 JSON 管理端点或启动周期刷新线程。
当前仅提供本文定义的 7 个 Meter；近期 max 由 Micrometer 的滑动统计窗口计算。

## 3. 目标架构

### 3.1 单层新指标

```text
SQL / URI completed event -> Micrometer adapter -> native meters
                         (Timer / Summary / Counter)
                                             |
                                             v
                            existing MeterRegistry -> /actuator/prometheus
```

新指标在 SQL 执行、HTTP 请求完成时记录原始观测值，由 Micrometer 维护累计量、
分布和时间窗口 max。旧名称不注册到 `MeterRegistry`。

### 3.2 不使用管理 JSON 协议

事件模型不读取 `/sql.json`、`/weburi.json`，也不需要周期性快照任务。事件从
SQL 执行和 URI 请求完成的现场取得，直接写入新 Meter。不得使用
`getValueAndReset()`；它和 `reset-all` 一样会修改被观测对象。

## 4. reset 与时间语义

### 4.1 原 exporter 的问题

原 `druid2prom` 默认按抓取次数 reset，而不是可靠的固定时间窗口。这使
Prometheus 抓取频率、手工访问、多个采集端和采集失败都会改变统计窗口；同时
它还会清空 Druid 管理页及其他消费者正在读取的统计数据。

### 4.2 拟定原则

- D2 已确认：Druid 指标采集永不执行全局或逐项 reset；不提供任何 destructive
  reset 配置。
- D3 已确认：近期 max 使用约 2 分钟时间窗口（`expiry=1m`、`bufferLength=2`）；窗口由一个全局配置项统一覆盖，
  不支持按单个 Meter 分别配置。
- 累计次数、总耗时和总行数使用 Counter 或 Timer/Summary 的 count/sum；近期速率
  由 PromQL `rate()` / `increase()` 计算。
- 平均值由 `sum / count` 在 PromQL 或 recording rule 中计算，不作为新的
  原始 Gauge。
- max/peak 使用 Micrometer `Timer` 或 `DistributionSummary` 的
  time-window max；窗口由 `distributionStatisticExpiry` 等配置决定。
- 旧 max 指标仍仅表示“自 Druid 启动或外部 reset 后的高水位”，文档必须
  明示该限制。

仅靠累计快照无法重建近期窗口的真实最大值：历史慢请求已经抬高 Druid max
后，后续较快请求不会改变该值。因此“近期 max”必须在事件发生处记录。

## 5. 新指标模型（提案）

下表的名称是工作提案，均采用秒作为耗时单位。最终 Prometheus 后缀由
Micrometer registry 生成。

| 观测事件 | 建议 Meter | 建议名称 | 标签 | 典型 Prometheus 输出 |
| --- | --- | --- | --- | --- |
| URI 请求（次数与耗时） | `Timer` | `druid_uri_request_duration` | `uri` | `_seconds_count`、`_seconds_sum`、`_seconds_max` |
| SQL 执行（次数与耗时） | `Timer` | `druid_sql_execution_duration` | `sql`, `datasource` | `_seconds_count`、`_seconds_sum`、`_seconds_max` |
| SQL 影响行数 | `DistributionSummary` | `druid_sql_affected_rows` | `sql`, `datasource` | `_count`、`_sum`、`_max` |
| SQL 返回行数 | `DistributionSummary` | `druid_sql_fetched_rows` | `sql`, `datasource` | `_count`、`_sum`、`_max` |
| 单个 URI 请求中的 JDBC 执行次数 | `DistributionSummary` | `druid_uri_jdbc_executions` | `uri` | `_count`、`_sum`、`_max` |
| 单个 URI 请求中的 JDBC 返回行数 | `DistributionSummary` | `druid_uri_jdbc_fetched_rows` | `uri` | `_count`、`_sum`、`_max` |
| 单个 URI 请求中的 JDBC 影响行数 | `DistributionSummary` | `druid_uri_jdbc_affected_rows` | `uri` | `_count`、`_sum`、`_max` |

示例查询：

```promql
# 最近 5 分钟每个 SQL 的平均执行耗时
rate(druid_sql_execution_duration_seconds_sum[5m])
/
rate(druid_sql_execution_duration_seconds_count[5m])
```

D8 已确认不启用 Histogram，因此不配置 SLA bucket；耗时异常通过 `_max` 与
`_sum / _count` 的平均值观察。

### 5.1 旧到新指标的迁移关系

新指标不是旧 19 项的逐一改名。它们记录的是每个 SQL 执行或每个 URI 请求的
原始观测值，再由 Micrometer 聚合为 count、sum、max；因此是更适合
计算的聚合基础，但不是可回放的逐事件日志。

以下映射只用于迁移现有看板、告警和 recording rule；旧指标不会继续输出。
在完成所需的标签聚合或拆分后，完整的关系如下。新 SQL 指标统一使用 `sql` 与
`datasource` 两个标签；旧 SQL 指标只有 `sql`，因此迁移时必须明确按数据源分别看，
不能把不同数据源直接合并为同一序列。
`_seconds_*` 为 Prometheus registry 对 Timer 的输出名称。`_seconds_count` 是
Timer 记录的请求/执行次数；它不是秒数，`seconds` 描述的是同一 Timer 家族的
耗时单位：

#### 5.1.1 URI 请求耗时：`Timer druid_uri_request_duration`

| 旧指标完整名称 | 旧标签 | druid2prom 类型 | 新指标类型与迁移计算 |
| --- | --- | --- | --- |
| `druid_uri_request_count_sum` | `uri` | Counter | Timer 的 counter-like：`druid_uri_request_duration_seconds_count` |
| `druid_uri_request_time_sum` | `uri` | Counter | Timer 的 counter-like：`_seconds_sum * 1000` |
| `druid_uri_request_time_max` | `uri` | Gauge | Timer 的 Gauge：`_seconds_max * 1000`；窗口语义不同 |
| `druid_uri_request_time_avg` | `uri` | Gauge | 派生 Gauge：`_seconds_sum / _seconds_count * 1000` |
| `druid_uri_request_time_histogram` | `uri`, `max` | Counter，旧式区间桶 | 新指标不输出 Histogram；需改用 count/sum/max 或重新定义规则 |

#### 5.1.2 URI JDBC 执行次数：`DistributionSummary druid_uri_jdbc_executions`

| 旧指标完整名称 | 旧标签 | druid2prom 类型 | 新指标类型与迁移计算 |
| --- | --- | --- | --- |
| `druid_uri_jdbc_execute_time_peak` | `uri` | Gauge | DistributionSummary 的 Gauge：`druid_uri_jdbc_executions_max`；窗口语义不同 |

#### 5.1.3 URI JDBC 返回行数：`DistributionSummary druid_uri_jdbc_fetched_rows`

| 旧指标完整名称 | 旧标签 | druid2prom 类型 | 新指标类型与迁移计算 |
| --- | --- | --- | --- |
| `druid_uri_jdbc_fetch_row_peak` | `uri` | Gauge | DistributionSummary 的 Gauge：`druid_uri_jdbc_fetched_rows_max`；窗口语义不同 |

#### 5.1.4 URI JDBC 影响行数：`DistributionSummary druid_uri_jdbc_affected_rows`

| 旧指标完整名称 | 旧标签 | druid2prom 类型 | 新指标类型与迁移计算 |
| --- | --- | --- | --- |
| `druid_uri_jdbc_effect_row_peak` | `uri` | Gauge | DistributionSummary 的 Gauge：`druid_uri_jdbc_affected_rows_max`；窗口语义不同 |

#### 5.1.5 SQL 执行耗时：`Timer druid_sql_execution_duration`

| 旧指标完整名称 | 旧标签 | druid2prom 类型 | 新指标类型与迁移计算 |
| --- | --- | --- | --- |
| `druid_sql_execute_count_sum` | `sql` | Counter | Timer 的 counter-like：`druid_sql_execution_duration_seconds_count` |
| `druid_sql_execute_time_sum` | `sql` | Counter | Timer 的 counter-like：`_seconds_sum * 1000` |
| `druid_sql_execute_time_max` | `sql` | Gauge | Timer 的 Gauge：`_seconds_max * 1000`；窗口语义不同 |
| `druid_sql_execute_time_avg` | `sql` | Gauge | 派生 Gauge：`_seconds_sum / _seconds_count * 1000` |
| `druid_sql_execute_time_histogram` | `sql`, `max` | Counter，旧式区间桶 | 新指标不输出 Histogram；需改用 count/sum/max 或重新定义规则 |

#### 5.1.6 SQL 影响行数：`DistributionSummary druid_sql_affected_rows`

| 旧指标完整名称 | 旧标签 | druid2prom 类型 | 新指标类型与迁移计算 |
| --- | --- | --- | --- |
| `druid_sql_effect_row_sum` | `sql` | Counter | DistributionSummary 的 counter-like `_sum` |
| `druid_sql_effect_row_max` | `sql` | Gauge | DistributionSummary 的 Gauge：`druid_sql_affected_rows_max`；窗口语义不同 |
| `druid_sql_effect_row_histogram` | `sql`, `max` | Counter，旧式区间桶 | 新指标不输出 Histogram；需改用 count/sum/max 或重新定义规则 |

#### 5.1.7 SQL 返回行数：`DistributionSummary druid_sql_fetched_rows`

| 旧指标完整名称 | 旧标签 | druid2prom 类型 | 新指标类型与迁移计算 |
| --- | --- | --- | --- |
| `druid_sql_fetch_row_sum` | `sql` | Counter | DistributionSummary 的 counter-like `_sum` |
| `druid_sql_fetch_row_max` | `sql` | Gauge | DistributionSummary 的 Gauge：`druid_sql_fetched_rows_max`；窗口语义不同 |
| `druid_sql_fetch_row_histogram` | `sql`, `max` | Counter，旧式区间桶 | 新指标不输出 Histogram；需改用 count/sum/max 或重新定义规则 |

新 Timer / DistributionSummary 的 `_count` 和 `_sum` 是累计、counter-like 的
分量；D8 确认不输出 bucket，`_max` 则为会按窗口过期的 Gauge。

这不是无条件的严格等价，原因如下：

- 旧 Druid histogram 没有新指标对应的 bucket；相关看板需要重新设计。
- 新耗时单位是秒，旧指标为毫秒。
- 新 `Timer` / `DistributionSummary` 的 `_max` 是配置化的近期窗口 max；旧
  `*_max` / `*_peak` 是 Druid 自上次 reset 以来的高水位。两者刻意具有不同的
  时间语义。
- 新 SQL 指标排除失败 SQL，而旧 SQL 快照统计的失败口径可能不同；新 URI 标签还
  可能由原始路径变为路由模板。这两类看板迁移均需核对聚合口径。
- 原 `druid2prom` 的“每第 N 次 scrape reset”依赖抓取行为，无法从新指标可靠
  重建，也不应继续重建。
- Prometheus 保存的是聚合时间序列，不保存每一次 SQL/HTTP 事件；无法凭借新
  指标恢复原始事件顺序或精确的分位点。

新看板必须使用新指标；平均值和速率由 PromQL 或 recording rule 计算。旧的
直方图看板没有直接替代指标，需要重新设计。

## 6. 迁移策略

迁移策略：

1. 删除当前 19 个旧指标、快照 exporter 和其周期刷新线程。
2. 发布文档中的完整映射表；升级时同步迁移 dashboard、告警和 recording rule。
3. 新指标成为唯一输出；不提供 `legacy.enabled` 或任何旧指标开关。

## 7. 上游事件接入设计

D1 已确认：Druid core 定义轻量、无第三方依赖的统计事件 SPI，由 starter 提供
Micrometer 实现；Druid core 不引入 Micrometer 依赖。

事件草案必须按真实数据完成时机拆分，不能用一个 SQL 完成事件同时携带所有值：

```java
onSqlExecutionCompleted(sqlIdentity, dataSourceIdentity, durationNanos, error)
onSqlUpdateCount(sqlIdentity, dataSourceIdentity, updateCount)
onSqlResultSetClosed(sqlIdentity, dataSourceIdentity, fetchedRows)
onWebRequestCompleted(request, uri, durationNanos, jdbcExecuteCount,
                      jdbcAffectedRows, jdbcFetchedRows, error)
```

原因是 SQL 耗时和 update count 在 statement execute 返回时确定，而 fetched rows
只有在 ResultSet 关闭时才最终确定；batch 还可能一次返回多个 update count。成功
执行记录 Timer；每个有效 update count 分别记录 affected-rows Summary；每个关闭的
ResultSet 记录一次 fetched-rows Summary。`sqlIdentity` 必须取 Druid StatFilter 最终
使用的合并 SQL，而不是未经 mergeSql 处理的原始 SQL。

事件应该在 Druid 已完成自身统计更新、且业务操作已经结束后触发。监听器为空时应是
低成本分支；监听器异常必须隔离、记录限频日志，不能影响 SQL 或 Web 请求。
Micrometer 适配器对 `error != null` 的 SQL 事件直接忽略，不写入 SQL 三个业务
Meter；HTTP 事件即使为 4xx/5xx 也写入 URI Timer。

备选方案是 starter 分别安装 Druid JDBC Filter 与 Servlet Filter。它不扩展 core
API，但会面临 Filter 顺序、异常路径、异步请求和与 Druid StatFilter 统计不一致
的问题，因此不作为首选。

## 8. 标签基数与资源保护

事件侧注册 Meter 会使 SQL hash 和 URI 长期留在 registry；这比快照导出更需要
资源边界。必须具备：

- SQL 与 URI 分别设置 Meter 上限，默认值均为 1000。
- 超限后的策略：拒绝新序列、聚合到 `other`，按 TTL 删除，或按 LRU 淘汰；本实现
  使用近似 LRU，以保留当前活跃的序列且不引入额外的聚合标签或 dropped 指标。正常命中
  只更新时间戳，序列创建或淘汰时才加锁，因此并发下访问顺序和上限可能短暂存在轻微偏差。
  新序列会先完成 Meter 注册，再淘汰旧序列，避免注册失败损失已有观测。
- SQL 标签不使用原文：D4 已确认，严格沿用 druid2prom，取 Druid 最终统计 SQL
  文本的 UTF-8 MD5，输出 32 位小写十六进制。每个新 hash 首次出现时，将
  `hash -> SQL` 写入可配置的本地映射目录；每个 SQL 一个文件，文件名就是 32 位
  小写 MD5。写线程先判断目标文件是否存在；不存在时使用临时文件加原子移动和
  并发保护，不覆盖已有文件。映射文件属于
  诊断数据，必须有目录、权限、容量和敏感 SQL 风险说明。
- URI 的模板化不能依靠猜测数字、UUID 等路径段；这种猜测会把语义不同的 URI
  错误合并。
- D5 已确认其中的 404 回退语义：没有匹配模板、且 Druid 尚未存在该 URI 统计项
  的 404，继续使用 Druid 现有的 `<contextPath>error_404` 聚合键；不改为
  `NOT_FOUND`，也不按原始随机路径创建序列。
- D6 已确认：SQL 指标增加稳定的 `datasource` 标签；URI 指标不增加该标签。
  数据源标签优先从 JDBC URL 解析库名，其次取 Druid 名称或稳定的 Spring Bean 名称；
  SQL 与 URI 分别计算各自的 Meter 上限。
- D5 已确认：URI 模板优先级为“应用自定义 URI 模板 SPI → Spring MVC 已匹配模板
  → Druid 原始 URI”；非 Spring MVC 且无自定义模板时允许回退原始 URI。模板标签
  去掉 context path；无稳定数据源名称时使用固定值 `unknown`。
- SQL 与 URI 分别设置上限 1000；达到上限时按近似 LRU 淘汰最久未使用的序列，
  并从 MeterRegistry 注销其 Meter。上限调整会在后续事件访问时生效。
- SQL identity 进入 PENDING 状态时即占用 `max-sql-identities` 配额；它不能借由
  等待异步映射写盘而绕过上限。
- 禁用采集时停止后续事件记录；已有 Meter 保留，直到后续 LRU 淘汰需要释放容量。

## 9. 配置草案

配置均有默认值。Prometheus 指标默认启用；可通过
`spring.datasource.druid.prometheus.enabled=false` 关闭。默认在启动时加载，接入刷新
SPI 后支持运行时替换配置快照：

```properties
spring.datasource.druid.prometheus.enabled=true
spring.datasource.druid.prometheus.events.enabled=true
spring.datasource.druid.prometheus.events.max-sql-identities=1000
spring.datasource.druid.prometheus.events.max-uri-identities=1000
spring.datasource.druid.prometheus.events.max-window=2m
spring.datasource.druid.prometheus.sql-mapping.enabled=true
spring.datasource.druid.prometheus.sql-mapping.directory=./logs/druid/sql-mapping
spring.datasource.druid.prometheus.sql-mapping.queue-size=1000
spring.datasource.druid.prometheus.uri-template.include-context-path=false
```

不提供 `reset-all` 或任何 destructive reset 配置。

配置语义：`max-sql-identities` 限制不同的 `(sql, datasource)` 标签组合，
`max-uri-identities` 限制不同的 `uri` 标签值；两个上限均按近似 LRU 缓存执行，达到
上限时淘汰最久未使用的身份；`max-window` 只影响近期
`_max`，不影响累计 `_count`/`_sum`。实现固定 `bufferLength=2`，并将任意
`max-window` 均分为两个 `expiry` 时间片；默认 2 分钟即 `expiry=1m`。
SQL 映射目录按 MD5 文件名保存单条 SQL，待写状态用于避免重复提交，
`queue-size` 限制单写线程的待写任务数；
URI 模板固定采用“应用 SPI → Spring MVC 模板 → 原始 URI”的优先级。
Histogram 不提供配置项，按 D8 永久关闭。

运行时需要替换配置时，应用可取得 starter 暴露的
`DruidPrometheusMetricsRefresher` 并调用 `refresh(Prometheus)`；替换对事件路径
原子生效；identity 上限缩小时会在后续事件中按近似 LRU 淘汰已有 Meter，
`max-window` 仅作用于之后新建的 Meter。
`enabled`、`events.enabled`、两个 identity 上限、SQL 映射开关与目录、
以及 URI context-path 开关，对后续事件立即生效；`max-window` 仅对新建 Meter
生效；`sql-mapping.queue-size` 仅在映射写线程创建时读取，修改后需重启才能生效。

## 10. 测试与验收

- 单元测试：SQL/URI 到新 Meter 的值、单位、标签和窗口 max；确认不注册 bucket。
- SQL 生命周期测试：execute 成功记录 Timer、batch 的每个 update count 分别记录、
  ResultSet.close 后才记录 fetched rows；失败 SQL 不产生 SQL Meter 观测。
- 并发测试：listener 慢或抛异常时，不影响 SQL 与 Web 请求。
- 映射写盘测试：并发相同 identity 只提交一次；已有 MD5 文件不重写；PENDING 占用
  上限；队列满、写失败和应用重启后的重试不阻塞 SQL。
- 基数测试：达到上限时淘汰最久未使用的 Meter，并保持实际 Meter 数不超过上限。
- Web 测试：4xx/5xx 记录 URI Timer，`isAsyncStarted()` 请求不记录 URI 事件。
- 多数据源测试：全局数据源去重、SQL 标识的处理符合决定的语义。
- 删除测试：确认不再注册旧 19 项、不再访问 JSON URL、也不启动周期刷新线程。
- Prometheus scrape 集成测试：只有业务 Actuator Endpoint，且只包含新指标。
- 性能基准：比较旧 JSON 快照、内部快照、事件记录三种路径的分配率和吞吐影响。

## 11. 决策清单

### 11.1 已确定

| ID | 决策 | 结论 |
| --- | --- | --- |
| F1 | Endpoint 与 Registry | Druid 只注册到业务已有 `MeterRegistry`；只由业务的 `/actuator/prometheus` 暴露。 |
| F2 | 旧指标 | 不保留、不 deprecated、不输出旧 19 项；旧到新表仅用于迁移看板、告警和 recording rule。 |
| F3 | 采集方式 | 不再调用 `/sql.json`、`/weburi.json`，也不保留周期快照 exporter。 |
| F4 | 新 Metric 集合 | 定义 7 个 Meter：2 个 Timer（URI 请求、SQL 执行）和 5 个 DistributionSummary（3 个 URI JDBC 值、2 个 SQL 行数值）。 |
| D1 | 事件接入位置 | core 定义无第三方依赖事件 SPI；starter 实现 Micrometer 适配器。 |
| D2 | reset 策略 | 永不 reset；不提供自动、显式或逐项 destructive reset。 |
| D3 | 近期 max 窗口 | 默认约 2 分钟（`expiry=1m`、`bufferLength=2`）；由一个全局配置项统一覆盖，不支持按 Meter 覆盖。 |
| D4 | SQL 标签及查证 | 严格沿用 druid2prom：Druid 最终统计 SQL 的 UTF-8 MD5（32 位小写），每个 SQL 一个文件，文件名为该 hash。 |
| D5 | URI 标签 | 自定义 SPI → Spring MVC 模板 → 原始 URI；模板去掉 context path；无模板 404 保持 Druid `<contextPath>error_404`。 |
| D6 | 多数据源语义 | SQL 使用稳定 `datasource` 标签，名称取显式配置名 → Bean 名 → `unknown`；URI 不带该标签；上限按 `sql × datasource` 计算。 |
| D7 | 标签基数与资源保护 | SQL/URI 分别设置上限 1000；达到上限时按近似 LRU 淘汰最久未使用的序列，并从 Registry 注销对应 Meter。 |
| D8 | Histogram | 不启用 Histogram | 只保留 count、sum、max；平均值通过 sum/count 计算，不产生 bucket 或 percentile 状态。 |
| D9 | 异常语义 | SQL 失败不记录；HTTP 全部记录 | SQL 语法、连接、超时、锁、约束等失败不进入 SQL 三个 Meter；HTTP 请求无论成功、4xx 或 5xx 均记录 URI Timer；失败 SQL 的行数不记录。 |
| D10 | 配置与默认值 | 配置快照原子替换；可选刷新适配器 | 配置有默认值；支持通过刷新 SPI 动态替换快照。已有 Meter 不因配置刷新删除或重建。 |
| D12 | 运行时配置刷新 | Spring Cloud Refresh；应用自定义刷新 SPI；仅启动加载 | 已确认：starter 提供轻量刷新 SPI，不强制依赖配置中心；`max-window` 变更只对新创建的 Meter 生效，已有 Meter 保持原窗口。 |
| D11 | max 窗口精确定义 | `expiry=1m`、`bufferLength=2` | 采用 1 分钟时间片、2 个环形缓冲，近期 max 的有效覆盖约 2 分钟；通过可控时钟测试验证轮转和过期。 |
| D13 | URI 模板 SPI 生命周期 | 仅同步请求；异步请求后续增强 | 已确认：本期只统计同步 Servlet 请求；检测到 `request.isAsyncStarted()` 时跳过 URI 事件，禁止按 Filter 提前返回时间记录残缺数据。 |
| D14 | SQL MD5 冲突 | 忽略碰撞；检测告警；增加冲突后缀 | 已确认：严格沿用 MD5 标签算法，忽略碰撞，不增加后缀、不改变标签；映射文件按原 hash 文件名写入。 |
| D15 | SQL 映射文件安全 | 目录落盘；关闭落盘 | 一个 SQL 一个文件，文件名为 MD5；写入失败只限频 WARN，不影响 SQL；目标存在即跳过，否则使用临时文件加原子移动，目录和权限由部署环境负责。 |
| D16 | 辅助保护指标 | 纳入 7 个业务 Meter；独立辅助指标 | 不提供 dropped 辅助指标。 |
| D20 | URI 模板模式 | 固定 auto；允许多模式配置 | 已确认：删除 `uri-template.mode` 配置项，固定采用应用 SPI → Spring MVC 模板 → 原始 URI。 |
| D17 | 上限的计数单位 | 标签身份组合 | SQL 上限按不同 `(sql, datasource)` 组合计数，URI 上限按不同 `uri` 计数；配置名使用 `max-sql-identities` / `max-uri-identities`。 |
| D18 | SQL 映射写盘线程 | 有界异步单写线程 | 新 SQL Meter 同步创建并加入近似 LRU，SQL 映射写线程异步落盘。写线程先判断 MD5 文件是否存在，存在则跳过，不存在才写临时文件并原子移动；队列满或写失败不阻塞 SQL，仅跳过本次落盘。 |
| D19 | SQL hash 计算 | 每次计算；内容缓存 | 已确认：每次执行都按 D4 重新计算 UTF-8 MD5，不缓存 SQL 原文到 hash；仅保留待写状态避免重复提交文件任务。 |

### 11.2 待逐项讨论

| ID | 决策点 | 已发现的问题 | 需要确定的结论 |
目前没有待决策项。

## 12. 推荐决策顺序

没有剩余决策顺序；D1-D20 均已确定，进入实现、测试和验收阶段。
