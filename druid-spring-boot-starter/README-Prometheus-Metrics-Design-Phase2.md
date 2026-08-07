# Druid Prometheus 二期：低基数监控叠加方案

## 目标与边界

SQL /URI 文本是高基数数据；它们不得作为 Prometheus label，也不得为每个值创建 Meter。
一期高基数指标继续保留，定义见 [一期设计](README-Prometheus-Metrics-Design.md)。二期是在一期实现上叠加一套独立命名的低基数聚合指标和结构化事件日志，不替换、不迁移、也不删除一期 Meter；对一期辅助 SQL mapping 的定向改动以本文件“对一期的定向改动”章节为准，一期设计文档本身保持历史版本不更新。

```
SQL / URI 事件（同一事件流）
  ├─ 一期明细 Meter（SQL/URI label）── events.enabled 控制
  ├─ 二期聚合 Meter（无 SQL/URI label）── 总开关打开即启用
  └─ 二期结构化日志 ── logging.enabled 控制
                         |
                         v
              应用已有 MeterRegistry / 独立日志文件
```

这不是 exporter：starter 不增加 HTTP 端点、不创建独立 registry；一期和二期都写入应用已有的 Micrometer `MeterRegistry`，继续由同一个 `/actuator/prometheus` 暴露。

## 一期与二期开关模型

沿用一期已有的 `events.enabled` 作为一期明细指标总开关。二期不再增加单独的指标开关：只要 `prometheus.enabled=true`，二期低基数指标就始终更新。

| `prometheus.enabled` | `events.enabled` | `logging.enabled` | 一期 SQL/URI 明细指标 | 二期低基数指标 | 二期结构化日志 | SQL mapping 文件 |
| --- | --- | --- | --- | --- | --- | --- |
| `false` | 任意 | 任意 | 关闭 | 关闭 | 关闭 | 关闭 |
| `true` | `true` | `true` | 开启 | 开启 | 开启 | 开启 |
| `true` | `true` | `false` | 开启 | 开启 | 关闭 | 由一期 `sql-mapping.enabled` 控制 |
| `true` | `false` | `true` | 关闭 | 开启 | 开启 | 开启 |
| `true` | `false` | `false` | 关闭 | 开启 | 关闭 | 关闭 |

开关关闭只停止后续更新，不从 `MeterRegistry` 删除已经注册的 Meter，避免计数器重建、时间序列抖动和误删其他组件注册的 Meter。Apollo 重新打开后，从下一次事件继续累计。

一期的 `max-sql-identities`、`max-uri-identities`、`max-window` 和 URI template 配置继续只作用于一期明细 Meter。二期不读取这些配置，也不为 SQL/URI 创建任何 Meter。`sql-mapping.enabled` 是一期已有开关，不是二期新增开关：仅在只启用一期明细 Meter 时决定是否输出 mapping；只要二期 `logging.enabled=true`，mapping 就是 SQL 日志查证的必需数据，不再受该开关限制。mapping 的最终生效条件为 `prometheus.enabled && (logging.enabled || (events.enabled && sql-mapping.enabled))`。

一期原有的 `sql-mapping` 本地文件落盘机制继续保留，二期不移除其实现或测试。映射文件与结构化事件日志共用 `logging.directory`，并统一命名为 `sql_mapping_<md5>.log`，文件内容保持一期的规整 SQL 模板文本。末端日志处理按文件名中的 MD5 汇总为 `sql_md5 → sql_template` 映射。Fluent 输入必须覆盖 `logging.directory/sql_mapping_*.log`，并将源文件路径保留为记录字段或 tag，供末端提取 MD5；仅采集文件内容不足以恢复 MD5。SQL 模板可能含换行，采集端如何保持一个文件内容的完整性由 Fluent/末端方案负责，本设计不擅自改变 mapping 文件内容格式。文件仍通过临时文件加原子移动创建，避免采集到写入中的文件。K8s 容器文件系统仍是短暂介质，Fluent 必须在 Pod 存活期间完成采集；Pod 异常退出前尚未采集的文件仍可能丢失。

## 对一期的定向改动

一期设计文档作为历史基线不修改；二期实现仅对一期的 SQL mapping 辅助能力作如下定向调整，其余一期 Meter 名称、标签、LRU、URI 模板、窗口和配置语义均保持不变：

1. 映射文件由原来的 `<md5>.sql` 改为 `sql_mapping_<md5>.log`，并与结构化事件日志共用 `logging.directory`；SQL 内容格式、MD5 算法、原子写入、目标存在即跳过和有界异步写入语义不变。
2. `sql-mapping.enabled`、`sql-mapping.queue-size` 配置和写盘线程保留，不能被二期事件日志替代或删除；`sql-mapping.directory` 在二期不再生效，统一使用 `logging.directory`。
3. 当 `events.enabled=false`、`logging.enabled=true` 时，mapping 仍为实际输出的 SQL 业务日志生成文件，确保二期日志中的 `sql_md5` 可在末端查到模板；这使 SQL mapping 成为一期与二期日志共用的辅助能力，此时不读取一期 `sql-mapping.enabled`。
4. Fluent 采集规则由部署侧配置：采集 `${spring.datasource.druid.prometheus.logging.directory}/sql_mapping_*.log`，保留源文件路径，并将文件内容传给末端；末端从路径提取 MD5 后汇总映射。

## Prometheus 指标

### 标签定义

应用内注册的 Meter 只有一个低基数 `datasource` 标签（仅 SQL Meter）；URI Meter 不带标签。每个 SQL Meter 在一个实例内最多为每个稳定数据源产生一条序列：

| 标签来源 | 允许的标签 | 说明 |
| --- | --- | --- |
| Druid/Micrometer SQL Meter | `datasource` | 仅使用稳定、有限的数据源名称；不注册 SQL、SQL 模板、参数、异常或状态标签 |
| Druid/Micrometer URI Meter | 无（`{}`） | 不注册 URI、数据源、参数、异常或状态标签 |
| Prometheus scrape 配置 | `job`、`hostname` | 由 Prometheus 自动或静态配置添加，用于区分采集目标 |
| 部署平台（可选） | `service`、`environment`、`cluster`、`region` | 由服务发现或 relabel 注入，值域必须有限且稳定 |

禁止将 `SQL 原文` / `sql_template` / `SQL 参数` / `URI` / `URI 参数` / 异常消息 / 线程名 / 用户 ID / 请求 ID / 数据源连接地址作为标签。`datasource` 只能用于 SQL 指标，值必须是稳定且有限的逻辑名称，不能使用连接地址、动态租户名或对象 identity；无法解析时统一使用 `unknown`。

`datasource` 名称沿用现有实现的解析顺序：优先使用 JDBC URL 中的数据库名，其次使用 Druid `DataSourceProxy#getName()`，再尝试 Spring 容器中的稳定 Bean 名称，最终回退为 `unknown`。名称解析结果需要缓存，但缓存只保存 DataSource 实例到稳定名称的映射，不保存 SQL/URI 明细。

二期 Meter 统一使用 `druid.agg.*` 名称前缀，不能复用一期 Meter 名称。一期 SQL Timer 的标签集合是 `{sql,datasource}`，二期是 `{datasource}`；若使用相同名称，Prometheus registry 会因同一 metric family 的 label key 不一致而注册失败。

| 观测项 | Micrometer Meter | 标签 | Prometheus 主要序列 | 含义 |
| --- | --- | --- | --- | --- |
| SQL 执行数及耗时 | `druid.agg.sql.execution.duration` Timer | `{datasource}` | `druid_agg_sql_execution_duration_seconds_count`、`_sum`、`_max` | 每一次 SQL execute，包括执行失败；`_count` 即 SQL 执行数 |
| 慢 SQL 数 | `druid.agg.sql.slow` Counter | `{datasource}` | `druid_agg_sql_slow_total` | 耗时达到 `slow-sql-millis` |
| 大读取 SQL 数 | `druid.agg.sql.large.read` Counter | `{datasource}` | `druid_agg_sql_large_read_total` | 每次 ResultSet 关闭时，读取行数达到 `large-sql-read-rows` 的 ResultSet 次数 |
| 大写入 SQL 数 | `druid.agg.sql.large.write` Counter | `{datasource}` | `druid_agg_sql_large_write_total` | 每个有效 update count 达到 `large-sql-write-rows` 的结果次数；batch 可产生多次 |
| URI 访问数及耗时 | `druid.agg.uri.request.duration` Timer | `{}` | `druid_agg_uri_request_duration_seconds_count`、`_sum`、`_max` | 每一个完成的同步 HTTP 请求；`_count` 即 URI 访问数 |
| 慢 URI 数 | `druid.agg.uri.slow` Counter | `{}` | `druid_agg_uri_slow_total` | 耗时达到 `slow-uri-millis` |
| 大读取 URI 数 | `druid.agg.uri.large.read` Counter | `{}` | `druid_agg_uri_large_read_total` | 请求累计读取行数达到 `large-uri-read-rows` |
| 大写入 URI 数 | `druid.agg.uri.large.write` Counter | `{}` | `druid_agg_uri_large_write_total` | 请求累计写入行数达到 `large-uri-write-rows` |
| 大量执行 SQL 的 URI 数 | `druid.agg.uri.large.sql.executions` Counter | `{}` | `druid_agg_uri_large_sql_executions_total` | 请求 SQL 次数达到 `large-uri-sql-executions` |

Timer 的 `_count`、`_sum` 和 Counter 都是进程累计值；应用重启后 Prometheus 通过 target 的重启识别新的计数器。Timer 的 `_max` 使用 MeterRegistry 默认的时间窗口，不承诺固定窗口或进程生命周期最大值，也不作为告警契约。告警使用慢事件 Counter，以及 Timer 的 `rate(_count)`、`rate(_sum)` 和两者计算出的平均耗时。

对于 32 个实例，设每个实例有 `D` 个稳定数据源，则 SQL 侧 series 为 `D × (3 × 1 + 1 × 3) = 6D`，URI 侧为 `1 × 3 + 4 = 7`，合计每实例 `6D + 7` 条、全量 `32 × (6D + 7)` 条（未计入 Prometheus 的 target/job 等公共标签）。series 数量随数据源数量线性增长，但不随 SQL/URI 数量增长。Prometheus Meter 本身不使用 LRU：淘汰并重建不同 SQL/URI 的 Meter 仍会向 TSDB 制造新的 series，不能解决根因。

## 应用内存边界

二期新增部分不维护 SQL/URI 的长期明细缓存，也不新增 LRU；一期原有的明细 Meter LRU 保持不变，并继续受 `events.enabled` 和两个 identity 上限控制。二期事件发生时只更新固定的聚合 Meter；日志异步队列最多保留 `queue-size` 个待写事件，实际内存取决于事件文本大小。正常事件依据采样率写日志，异常事件在正常运行条件下全量写日志；队列彻底满或日志系统故障等紧急情况下允许整条丢弃，但任何已输出事件都不截断 SQL/URI 内容。

若启用了 Druid 原生 SQL/URI Stat，仍必须保留并核对其自身的容量限制。当前 `JdbcDataSourceStat.maxSqlSize` 与 `WebAppStat.maxStatUriCount` 默认均为 1000；本方案只保证新增的 Prometheus 与日志适配器不持有高基数状态，不能替代或取消已有的 Druid 原生统计表容量保护。

## 阈值与动态刷新

下面是一份可直接合并到应用配置中心的完整配置，按【公共】、【一期】、【二期】标记；未标注的业务含义和默认值沿用对应阶段设计。

```properties
# 【公共】是否处理 Druid Prometheus 事件；listener 保持注册，false 时停止一期、二期指标和日志，可由 Apollo 动态恢复
spring.datasource.druid.prometheus.enabled=true
# 【一期】SQL/URI 高基数明细 Meter 开关；false 仅关闭一期 Meter，二期聚合指标继续更新
spring.datasource.druid.prometheus.events.enabled=true
# 【一期】单实例 SQL 明细 identity 上限；保持一期原有 LRU 容量语义
spring.datasource.druid.prometheus.events.max-sql-identities=1000
# 【一期】单实例 URI 明细 identity 上限；保持一期原有 LRU 容量语义
spring.datasource.druid.prometheus.events.max-uri-identities=1000
# 【一期】一期明细事件统计窗口；仅影响一期 Meter 的窗口统计
spring.datasource.druid.prometheus.events.max-window=2m
# 【一期】一期 SQL mapping 开关；二期 logging.enabled=true 时为保证日志可查证，mapping 强制启用且不读取此开关
spring.datasource.druid.prometheus.sql-mapping.enabled=true
# 【一期】SQL 映射异步写入队列容量
spring.datasource.druid.prometheus.sql-mapping.queue-size=1000
# 【一期】URI 模板是否包含 context-path
spring.datasource.druid.prometheus.uri-template.include-context-path=false

# 注：SQL mapping 与结构化日志共用 logging.directory；Fluent 采集 sql_mapping_*.log 时保留源文件路径，供末端提取 md5。

# 以下 thresholds 和 logging 配置全部属于【二期】，不改变一期明细 Meter 的原有阈值和配置。

# 【二期】SQL 慢执行阈值，单位毫秒；执行耗时 >= 此值时 slow SQL Counter +1，并强制记录异常事件日志
spring.datasource.druid.prometheus.thresholds.slow-sql-millis=200
# 【二期】单次 SQL ResultSet 读取行数阈值；读取行数 >= 此值时 large read Counter +1，并强制记录日志
spring.datasource.druid.prometheus.thresholds.large-sql-read-rows=100
# 【二期】单次 SQL update/insert/delete 影响行数阈值；影响行数 >= 此值时 large write Counter +1，并强制记录日志
spring.datasource.druid.prometheus.thresholds.large-sql-write-rows=10
# 【二期】URI 请求慢阈值，单位毫秒；请求耗时 >= 此值时 slow URI Counter +1，并强制记录日志
spring.datasource.druid.prometheus.thresholds.slow-uri-millis=1000
# 【二期】单个 URI 请求累计读取行数阈值；读取行数 >= 此值时 large read URI Counter +1，并强制记录日志
spring.datasource.druid.prometheus.thresholds.large-uri-read-rows=300
# 【二期】单个 URI 请求累计写入行数阈值；写入行数 >= 此值时 large write URI Counter +1，并强制记录日志
spring.datasource.druid.prometheus.thresholds.large-uri-write-rows=30
# 【二期】单个 URI 请求触发 JDBC/SQL 次数阈值；SQL 次数 >= 此值时 large SQL URI Counter +1，并强制记录日志
spring.datasource.druid.prometheus.thresholds.large-uri-sql-executions=20

# 【二期】是否输出 SQL/URI 结构化事件日志；false 时完全关闭事件日志，两期 Prometheus 指标仍正常更新
spring.datasource.druid.prometheus.logging.enabled=true
# 【二期】是否由 Starter 自动配置专用日志 appender；true 时无需应用提供日志配置，false 时由应用接管 druid.metrics.event logger
spring.datasource.druid.prometheus.logging.auto-configure=true
# 【二期】正常事件日志采样比例，范围 0~1；0 表示不记录正常事件，1 表示全部记录
spring.datasource.druid.prometheus.logging.normal-sample-rate=0.01
# 【二期】事件日志目录；目录不存在时自动创建
spring.datasource.druid.prometheus.logging.directory=./logs
# 【二期】内置事件日志文件名
spring.datasource.druid.prometheus.logging.file-name=druid-metrics-events.log
# 【二期】异步日志队列容量；队列满时正常采样事件丢弃，异常事件优先处理
spring.datasource.druid.prometheus.logging.queue-size=4096
# 【二期】单个日志文件最大大小，超过后轮转
spring.datasource.druid.prometheus.logging.max-file-size=10MB
# 【二期】历史日志保留天数
spring.datasource.druid.prometheus.logging.max-history-days=7
# 【二期】当前文件与全部历史归档文件的总容量上限，超过后优先删除最旧归档
spring.datasource.druid.prometheus.logging.total-size-cap=1GB
# 【二期】应用关闭时等待异步日志队列刷盘的最长时间
spring.datasource.druid.prometheus.logging.shutdown-flush-timeout=3s
```

### 配置生效方式

| 配置项 | 阶段 | 生效方式 | 变更业务日志 | 说明 |
| --- | --- | --- | --- | --- |
| `prometheus.enabled` | 公共 | 动态即时 | `INFO`：已应用的新值 | 每次 SQL/URI 事件读取；下一次事件生效。 |
| `events.enabled` | 一期 | 动态即时 | `INFO`：已应用的新值 | 每次 SQL/URI 事件读取；仅控制一期明细 Meter。 |
| `events.max-sql-identities`、`events.max-uri-identities` | 一期 | 动态即时 | `INFO`：已应用的新值 | 下一次命中或创建明细 Meter 时按新上限执行 LRU 淘汰。 |
| `events.max-window` | 一期 | 部分动态 | `INFO`：已应用，且仅影响新建 Meter | 只影响变更后新建的一期 Meter；已有 Meter 不重建。 |
| `sql-mapping.enabled` | 一期遗留 | 动态即时 | `INFO`：已应用的新值 | 下一次一期 SQL mapping 判断生效；`logging.enabled=true` 时 mapping 强制输出，不读取此值。 |
| `sql-mapping.queue-size` | 一期/二期共享 | 需重启 | `INFO`：收到新值，重启后生效 | 仅在 mapping 写入线程创建时读取，不动态重建该线程。 |
| `uri-template.include-context-path` | 一期 | 动态即时 | `INFO`：已应用的新值 | 下一次 URI 事件解析时生效。 |
| `thresholds.*` | 二期 | 动态即时 | `INFO`：已应用的新值 | 每次对应 SQL/URI 事件读取；阈值变更不清零既有 Counter。 |
| `logging.enabled`、`logging.normal-sample-rate` | 二期 | 动态即时 | `INFO`：已应用的新值 | 每次事件读取；下一次日志判定生效。 |
| `logging.auto-configure` | 二期 | 动态切换日志接管方式 | `INFO`：Starter 自动配置已启用/已关闭 | `true → false` 时 detach 并关闭 Starter appender；`false → true` 时按串行交接流程创建内置 appender。 |
| `logging.directory` | 二期及 mapping 共享 | 动态切换目录 | `INFO`：事件 appender 与 mapping writer 的切换结果 | Mapping writer 始终切换到新目录；`auto-configure=true` 时同时串行重建事件 appender，`false` 时不处理应用自有 appender。旧目录文件不迁移。 |
| `logging.file-name`、`logging.queue-size`、`logging.max-file-size`、`logging.max-history-days`、`logging.total-size-cap` | 二期 | 动态重建 appender | `INFO`：新 appender 已切换；失败则 `WARN` 并恢复旧 appender | `auto-configure=true` 时执行串行交接，不允许两个 RollingFileAppender 同时写同一文件；`false` 时仅记录“应用接管、Starter 未处理”。 |
| `logging.shutdown-flush-timeout` | 二期 | 动态生效于后续关闭/重建 | `INFO`：已应用的新值 | 用于下一次 appender 重建或应用关闭时的最长 flush 等待时间。 |

每个发生变化的配置项都必须通过组件自身的 SLF4J 业务 logger 输出一条结果日志，不能写入 `druid.metrics.event`。日志至少包含配置项名、旧值、新值和生效方式，例如 `property=thresholds.slow-sql-millis, old=200, new=500, effect=next-event`；同一次 Apollo 发布修改三个配置项时可以输出三条日志，不要求合并为一条。合法更新使用 `INFO`；非法值、配置绑定失败、appender 创建/切换/flush 失败使用 `WARN`，并继续使用最后一个有效配置或旧 appender。若一次通知同时修改多个 appender 配置，实现可以合并为一次 appender 重建，但仍逐项输出变更结果。业务事件路径不重复打印配置日志。

listener 始终注册，`prometheus.enabled=false`（公共配置）时停止一期、二期指标更新和事件日志输出；Apollo 改为 `true` 后，下一次事件立即恢复。`events.enabled`（一期配置）只包围一期明细 Meter 的创建、更新和 LRU，不能作为整个 listener 的提前返回条件。二期固定 URI Meter 启动时注册，SQL Meter 按稳定的 `datasource` 懒注册；只要总开关开启就更新，不读取 `events.enabled`。

事件处理路径每次重新读取公共总开关、一期明细开关、二期全部阈值和二期 `normal-sample-rate`，不能在初始化时复制为不可变的普通字段。通过 `DruidPrometheusMetricsRefresher.refresh(...)` 或 Apollo/Spring 的 bean 刷新替换配置对象后，无须注销或重建已有 Meter。二期 `normal-sample-rate` 必须在 `[0, 1]`；配置绑定时会截断到该范围。

所有日志配置均有上述内置默认值，并可被 application properties、环境变量或 Apollo 覆盖。采样率在下一次事件立即生效；目录、文件名、队列容量和轮转参数属于 appender 生命周期配置，Apollo 更新后由 Starter 在配置变更回调中原子重建专用 appender，旧 appender 最多等待 `shutdown-flush-timeout` 完成 flush 后关闭，不在业务事件线程中重建文件。

二期阈值的比较语义为“达到即计入”（`>=`）。二期阈值为 `0` 时关闭对应的异常判断；负数属于非法配置，动态更新时拒绝该值并继续使用上一个有效值。正数按正常阈值处理，避免错误配置导致日志风暴。一期阈值和 `max-window` 语义以一期文档为准，不受本段规则影响。

## 结构化事件日志

### 默认自动输出

日志使用独立的 SLF4J logger：`druid.metrics.event`。Starter 本期仅自动配置 Logback，内置专用异步 RollingFileAppender 的默认配置并自动加载，因此使用 Logback 的应用无须增加日志配置即可写入：

```text
./logs/druid-metrics-events.log
```

内置配置只作用于 `druid.metrics.event`，关闭 additivity，不修改 root logger，也不与普通业务日志混写。`auto-configure=true` 且检测到非 Logback 实现时，Starter 不安装 appender 并关闭事件日志，只输出一次提示，防止事件沿 root logger 混入业务日志；应用需要设置 `auto-configure=false` 并自行配置 `druid.metrics.event` 后才能接管事件日志。应用可以通过属性覆盖目录、文件名、启用状态、采样率、队列容量和轮转策略：

部署约定为每个服务实例运行在独立容器文件系统中，因此各实例的 `./logs/druid-metrics-events.log` 不会被多个 JVM 同时写入。

```properties
spring.datasource.druid.prometheus.logging.enabled=true
spring.datasource.druid.prometheus.logging.auto-configure=true
spring.datasource.druid.prometheus.logging.directory=./logs
spring.datasource.druid.prometheus.logging.file-name=druid-metrics-events.log
spring.datasource.druid.prometheus.logging.queue-size=4096
spring.datasource.druid.prometheus.logging.max-file-size=10MB
spring.datasource.druid.prometheus.logging.max-history-days=7
spring.datasource.druid.prometheus.logging.total-size-cap=1GB
spring.datasource.druid.prometheus.logging.shutdown-flush-timeout=3s
```

实现要求：

- 业务事件线程只构造 JSON 并调用 `druid.metrics.event` logger，文件 I/O 由日志框架的异步 appender 执行；
- 专用 Logback AsyncAppender 使用 `queueSize=queue-size`、`neverBlock=true` 和 Logback 默认 `discardingThreshold`；专用 RollingFileAppender 顺序写入，保证每条事件一行且不会交错；
- 正常采样事件使用 `INFO`，执行失败、慢事件和大结果事件使用 `WARN`；达到 Logback 默认丢弃阈值后，`INFO` 可被优先丢弃，`WARN` 仅在队列彻底满等紧急情况下整条丢弃；
- 队列彻底满时 `WARN` 仍可能整条丢失，日志处理不能阻塞业务请求；因此“异常全量记录”指日志系统及进程正常运行条件下的全量记录，不承诺在队列耗尽、磁盘故障或进程崩溃时零丢失；
- 文件由日志框架按大小和日期轮转，应用退出时执行有限时间 flush；
- 目录自动创建，路径不合法或不可写时只记录一次启动错误并关闭专用 appender，不影响 SQL/HTTP 主流程；
- SQL 模板按 JSON 规则转义，不写入绑定参数和 SQL 原文；
- 调用 logger 时只传入完整 JSON 字符串，不把 `Throwable` 作为日志参数传递，防止 Logback 在 JSON 后追加多行堆栈；
- SQL、URI 和其他事件字段不截断，确保已写出的日志可用于完整查证；紧急情况下丢弃整条日志，不输出残缺内容。

轮转采用“当前文件 + 日期/序号归档”模型。当前文件固定为 `druid-metrics-events.log`。写入后达到 `max-file-size` 或日期变化时关闭当前文件，并归档为：

```text
druid-metrics-events.2026-08-05.0.log
druid-metrics-events.2026-08-05.1.log
```

序号按同一天的轮转次数递增，随后重新创建当前文件。日志框架在启动及每日轮转时删除早于 `max-history-days` 的归档文件，并在总容量超过 `total-size-cap` 时优先删除最旧归档；当前文件不参与按历史天数删除。轮转和删除只匹配专用 appender 的归档文件名，不处理目录中的其他文件。`max-file-size` 是软上限：单条完整事件写入后才判断轮转，因此文件最多可能超过一条事件的大小。

应用如需完全使用自己的日志配置，可设置 `logging.auto-configure=false`，再自行配置 `druid.metrics.event` logger；Starter 不再安装内置 appender，避免重复输出。`logging.enabled=false` 则完全停止事件日志。

生命周期顺序固定为：启动时先创建并启动专用 appender，再注册 SQL/URI listener；关闭时先注销 listener，再等待异步队列最多 `shutdown-flush-timeout` 完成 flush，最后关闭 appender。Apollo 重建时只移除带有 Starter 专用名称的自有 appender，不修改或移除应用挂载到同一 logger 的其他 appender。

Appender 动态重建采用串行交接，禁止新旧 RollingFileAppender 同时持有同一当前文件：

1. 在配置线程校验完整的新配置并构造尚未启动的新 appender；校验失败时保留旧 appender，记录 `WARN` 后结束。
2. 将事件输出入口原子切换到临时有界 handoff 队列。业务线程只做非阻塞 `offer`，不直接写文件；队列满时整条丢弃，并在重建结果业务日志中记录丢弃数量。
3. 从 `druid.metrics.event` detach 旧的 Starter appender，再等待其异步队列最多 `shutdown-flush-timeout` 完成 flush 并关闭，使旧 RollingFileAppender 释放文件。
4. 启动并 attach 新 appender，然后将输出入口切回 `druid.metrics.event`，最后把 handoff 队列中的完整 JSON 事件送入新 appender。切换期间不承诺严格落盘顺序，事件发生时间仍以 JSON `timestamp` 为准。
5. 新 appender 启动失败时尝试重新启动并挂回旧 appender，再回放 handoff 队列；无法恢复时关闭结构化事件日志并记录 `WARN`，不能影响 SQL/HTTP 主流程。

`logging.auto-configure=false` 时 Starter detach 并关闭自己的事件 appender，后续文件名、事件队列及轮转配置变化只输出“应用接管、Starter 未处理”的 SLF4J 业务日志，不执行 appender 重建；从 `false` 改为 `true` 时才按上述流程创建内置 appender。`logging.directory` 仍由 Starter 的 mapping writer 使用，因此目录变化始终切换 mapping 输出位置，只是不处理应用自有事件 appender。

`logging.directory` 动态变化时，事件 appender 与 mapping writer 使用同一份原子配置快照切换到新目录；切换前已提交的 mapping 任务继续写旧目录，切换后提交的任务写新目录。Starter 不迁移或删除旧目录文件，部署侧必须确保 Fluent 在变更期间同时采集旧、新目录，直至旧任务和旧文件处理完成。

每条日志为一行 JSON（JSON Lines），方便 Logstash、Vector 或 Fluent Bit 直接提取；输出实现必须正确转义 SQL 和 URI，不能用字符串拼接形成无效 JSON。

SQL 事件按 Druid 回调时机拆成三种。三种事件携带 `sql_md5`（规整 SQL 的 UTF-8 MD5，32 位小写十六进制），不携带 `sql_template`。`sql_md5 → sql_template` 映射由 `logging.directory` 中的 `sql_mapping_<md5>.log` 文件提供，并经 Fluent 传输到末端汇总，避免在事件日志中维护声明状态或依赖事件日志轮转保留映射：

```json
{"timestamp":"2026-08-05T17:30:12.123+08:00","event":"sql_execute","datasource":"order","sql_md5":"<md5>","duration_ms":230,"error":false}
{"timestamp":"2026-08-05T17:30:12.456+08:00","event":"sql_read","datasource":"order","sql_md5":"<md5>","rows":120}
{"timestamp":"2026-08-05T17:30:12.789+08:00","event":"sql_write","datasource":"order","sql_md5":"<md5>","rows":15}
```

URI 事件包括：

```json
{"timestamp":"2026-08-05T17:30:13.123+08:00","event":"uri","uri":"<uri>","duration_ms":<n>,"sql_count":<n>,"write_rows":<n>,"read_rows":<n>,"error":true}
```

所有事件包含 ISO-8601 `timestamp`，时间在 SQL/URI 事件发生时生成，不使用异步 appender 的实际落盘时间。正常事件按 `normal-sample-rate` 抽样；慢 SQL/URI、大行数/大量 SQL 事件，以及执行失败的事件在正常运行条件下全量记录。URI 异常仅指 Web 事件回调携带 `Throwable`，不根据 HTTP status 判断。日志保留周期、脱敏规则和外部索引由应用日志平台配置。`sql_template` 仅写入 `sql_mapping_<md5>.log`，保持 StatFilter 现有的 merge/parameterize 结果，不改变规整算法；SQL 业务事件（execute/read/write）不携带 `sql_template`；不记录 SQL 原文或异常类名。

事件主路径分两种 mapping 触发方式：一期 `events.enabled=true` 时，沿用一期首次创建 SQL identity 时提交 mapping；一期关闭而二期 `logging.enabled=true` 时，先更新低基数 Meter，再判断异常条件和正常采样，只有确定实际输出 SQL 业务日志后才计算 MD5、提交对应 `sql_mapping_<md5>.log`，随后序列化并调用 logger。未采样的正常事件不计算 MD5、不提交 mapping，也不承担事件日志构造开销。mapping 文件和业务事件日志经不同异步路径传输，不承诺到达末端的先后顺序，末端按 `sql_md5` 做最终一致的关联。

SQL 指标和日志依赖 Druid `StatFilter`，URI 指标和日志依赖 `WebStatFilter`。Starter 启动时检查对应 Filter；缺失时输出一次明确提示，相应事件不会产生指标观测或日志。

## 开发与验收

1. 保留一期现有 Meter 名称、标签、LRU、URI template、max window、`sql-mapping.enabled`、`sql-mapping.queue-size` 配置及映射写盘能力；原有一期测试不得删除或降低断言。SQL 映射文件的唯一命名规则调整为 `sql_mapping_<md5>.log`，并统一写入 `logging.directory`；SQL 内容格式及其余原子写入、目标存在即跳过和有界异步写入语义保持不变。
2. 复用一个 listener 和同一批 SQL execute、update count、ResultSet close、Web 请求事件；一期分支由 `events.enabled` 控制，二期聚合分支由总开关控制，日志分支由 `logging.enabled` 控制。同一事件不得重复向同一期分支记录。
3. 二期 URI Meter 启动时固定注册，二期 SQL Meter 按稳定的 `datasource` 懒注册。共新增 9 种 `druid.agg.*` Meter，不按 SQL/URI 创建 Meter，也不与一期名称重叠。
4. SQL MD5、规整 SQL、URI 和原始 SQL 都不得用于二期 Prometheus 标签或进程内明细缓存；SQL MD5 作为三类 SQL 业务日志字段使用，规整 SQL 只写入 `sql_mapping_<md5>.log`。SQL 业务事件（execute/read/write）不得携带 `sql_template`，也不维护其他 SQL 声明去重集合。
5. 开关矩阵测试必须覆盖总开关、一期开关、日志开关的全部组合；关闭一期时二期仍递增，关闭日志时两期指标仍递增，关闭总开关时三条分支均不更新。开关重新打开后复用已有 Meter 继续累计。
6. 二期指标测试应验证不同 SQL 在同一 datasource 下命中相同 Meter、不同稳定 datasource 分别聚合、不同 URI 命中同一无标签 Meter、配置刷新在下一次事件生效，以及二期 Meter 数量不随 SQL/URI 数量增加。
7. 集成验收时同时开启两期，确认一期序列仍含 `sql`/`uri` 标签，二期 `druid_agg_*` 序列不含 `sql`/`uri`，二期 SQL 指标的业务标签仅为 `datasource`；检查两期 metric family 无名称或 label-key 冲突。
8. Logback 自动配置测试应覆盖默认值、应用属性覆盖、Apollo 变更后原子重建、非 Logback 回退提示、INFO/WARN 队列丢弃策略，以及停机最多等待 `shutdown-flush-timeout`。
9. Starter 增加 optional 的 `logback-classic` 依赖，Logback 自动配置使用 `@ConditionalOnClass`；不得强制使用其他日志实现的应用引入 Logback。
10. 为支持 `prometheus.enabled` 从 `false` 动态恢复，自动配置不得使用该属性作为创建 listener Bean 的 `@ConditionalOnProperty`；listener 始终存在并在事件路径读取当前开关。
11. SQL mapping 测试应覆盖：文件名严格为 `sql_mapping_<md5>.log` 且写入 `logging.directory`；SQL 内容格式保持一期语义；同一 MD5 目标文件存在时不重写；临时文件原子移动后 Fluent 不会读取到写入中的文件；当 `events.enabled=false`、`logging.enabled=true` 时只为实际输出的 SQL 日志建立映射文件；SQL 业务事件日志中不含 `sql_template` 字段。集成验收应验证 Fluent 保留 mapping 源文件路径，末端从路径提取 MD5 后汇总映射。
12. Appender 动态重建测试应验证 handoff 队列、新旧 appender 不同时持有同一文件、成功切换、失败回退、切换期丢弃计数，以及 `auto-configure=false` 时 Starter 不处理应用自有 appender。`logging.directory` 变更测试应验证切换前后 mapping 任务分别写入旧、新目录且不迁移旧文件。

## 评审发现的问题

已确定的处理：一期 `sql-mapping` 保留，不增加单独的 SQL 声明事件；映射文件统一为 `sql_mapping_<md5>.log`，由 Fluent 采集并在末端按文件路径中的 MD5 汇总。该方案避免事件日志轮转导致映射先失效，也不引入 SQL 声明去重集合。

以下评审结论已确认并纳入设计或验收边界：

1. **已接受风险（P1）：映射文件不能承诺零丢失。** 映射写入队列满、磁盘错误、Pod 异常终止或 Fluent 尚未来得及 tail 时，`sql_mapping_<md5>.log` 仍可能缺失。末端汇总必须允许业务事件暂时没有映射，并在后续同 MD5 再次出现时补齐；实现不得因“已尝试过”永久放弃失败映射的重试机会。
2. **已接受范围（P1）：日志只支持 SQL 模板级查证。** 日志不记录参数、SQL 原文、请求 ID、trace ID 或 SQL execution ID；并发执行相同模板时，`sql_execute`、`sql_read` 和 `sql_write` 无法互相关联，也无法还原某次执行的实际参数。需要单次执行级查证时，由业务 trace 或数据库审计日志承担，敏感参数不直接写入本组件日志。
3. **已确定实现方式（P1）：Apollo 动态刷新分级生效并输出业务日志。** 开关、阈值和采样率在下一次事件读取；目录、文件名、队列和轮转策略更新后通过 handoff 队列串行重建专用 appender；mapping 队列容量需重启。每个发生变化的配置项由组件自身 SLF4J 业务 logger 单独记录实际生效结果，不要求一次 Apollo 发布只生成一条合并日志。完整配置项口径见“配置生效方式”表。
4. **已接受口径（P1）：动态阈值改变后续 Counter 的判断条件。** Counter 按事件发生时的当前阈值判断，配置变更不清零；配置中心统一发布后，告警在刷新完成后的完整观察窗口内按新口径解读。
5. **已接受前置条件（P1）：`datasource` 必须稳定且有限。** 动态租户库、运行时创建数据源或不断变化的库名会持续创建 `druid_agg_*` series，二期不以 LRU 兜底。部署必须使用稳定别名，禁止将动态租户名作为 `datasource`；集成验收覆盖多数据源基数。
6. **已接受口径（P1）：大读取/大写入 Counter 按 Druid 回调结果计数。** `large.read` 按 ResultSet close 次数计数，`large.write` 按有效 update count 结果计数，batch 一次执行可递增多次。看板与告警按此口径命名和解释，不增加执行级关联。
7. **部署侧处理（P2）：结构化日志的 schema 版本和实例来源。** `schema_version`、`service`、`hostname` 等来源字段由日志采集/处理链路自动补充，Starter 不增加对应日志字段或配置项。
8. **已接受风险（P2）：`./logs` 与不截断单条日志的资源边界。** `./logs` 相对于 JVM `user.dir`，容器重启后的保留和采集器挂载由部署保证；SQL 模板不截断，异步队列实际内存不能仅由 `queue-size` 推算。部署需提供工作目录、volume/采集路径和磁盘配额，并接受超长 SQL 带来的内存峰值风险。
9. **已接受风险（P2）：mapping 文件数量不由事件日志轮转参数控制。** `sql_mapping_*.log` 是待 Fluent 采集的独立映射文件，不参与 `max-history-days` 或 `total-size-cap` 清理，Pod 生命周期内文件数随不同 SQL 模板增长。Starter 不自动删除 mapping，部署通过 Pod 生命周期、磁盘配额和 Fluent 及时采集控制风险。
