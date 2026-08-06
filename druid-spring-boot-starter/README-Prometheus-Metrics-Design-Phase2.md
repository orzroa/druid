# Druid Prometheus 二期：低基数监控叠加方案

## 目标与边界

SQL /URI 文本是高基数数据；它们不得作为 Prometheus label，也不得为每个值创建 Meter。
一期高基数指标继续保留且行为不变，定义见 [一期设计](README-Prometheus-Metrics-Design.md)。二期是在一期实现上叠加一套独立命名的低基数聚合指标和结构化事件日志，不替换、不迁移、也不删除一期 Meter。

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

| `prometheus.enabled` | `events.enabled` | `logging.enabled` | 一期 SQL/URI 明细指标 | 二期低基数指标 | 二期结构化日志 |
| --- | --- | --- | --- | --- | --- |
| `false` | 任意 | 任意 | 关闭 | 关闭 | 关闭 |
| `true` | `true` | `true` | 开启 | 开启 | 开启 |
| `true` | `true` | `false` | 开启 | 开启 | 关闭 |
| `true` | `false` | `true` | 关闭 | 开启 | 开启 |
| `true` | `false` | `false` | 关闭 | 开启 | 关闭 |

开关关闭只停止后续更新，不从 `MeterRegistry` 删除已经注册的 Meter，避免计数器重建、时间序列抖动和误删其他组件注册的 Meter。Apollo 重新打开后，从下一次事件继续累计。

一期的 `max-sql-identities`、`max-uri-identities`、`max-window`、SQL mapping 和 URI template 配置继续只作用于一期明细 Meter。二期不读取这些配置，也不为 SQL/URI 创建任何 Meter。

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
# 【一期】是否启用 SQL 模板到原始 SQL 的映射记录
spring.datasource.druid.prometheus.sql-mapping.enabled=true
# 【一期】SQL 映射文件目录
spring.datasource.druid.prometheus.sql-mapping.directory=./logs/druid/sql-mapping
# 【一期】SQL 映射异步写入队列容量
spring.datasource.druid.prometheus.sql-mapping.queue-size=1000
# 【一期】URI 模板是否包含 context-path
spring.datasource.druid.prometheus.uri-template.include-context-path=false

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

listener 始终注册，`prometheus.enabled=false`（公共配置）时停止一期、二期指标更新和事件日志输出；Apollo 改为 `true` 后，下一次事件立即恢复。`events.enabled`（一期配置）只包围一期明细 Meter 的创建、更新、LRU 和 SQL mapping，不能作为整个 listener 的提前返回条件。二期固定 URI Meter 启动时注册，SQL Meter 按稳定的 `datasource` 懒注册；只要总开关开启就更新，不读取 `events.enabled`。

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
- 正常采样事件使用 `INFO`，执行失败、慢事件和大结果事件使用 `WARN`；Logback 异步 appender 优先丢弃低级别事件；
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

每条日志为一行 JSON（JSON Lines），方便 Logstash、Vector 或 Fluent Bit 直接提取；输出实现必须正确转义 SQL 和 URI，不能用字符串拼接形成无效 JSON。

SQL 事件按 Druid 回调时机拆成三种。三种事件都携带 Druid 现有算法产生的 SQL MD5（规整 SQL 的 UTF-8 MD5，32 位小写十六进制）和规整后的 `sql_template`，避免 execute 日志未被采样时无法查证大读/大写 SQL：

```json
{"timestamp":"2026-08-05T17:30:12.123+08:00","event":"sql_execute","datasource":"order","sql_md5":"<md5>","sql_template":"<normalized-sql>","duration_ms":230,"error":false}
{"timestamp":"2026-08-05T17:30:12.456+08:00","event":"sql_read","datasource":"order","sql_md5":"<md5>","sql_template":"<normalized-sql>","rows":120}
{"timestamp":"2026-08-05T17:30:12.789+08:00","event":"sql_write","datasource":"order","sql_md5":"<md5>","sql_template":"<normalized-sql>","rows":15}
```

URI 事件包括：

```json
{"timestamp":"2026-08-05T17:30:13.123+08:00","event":"uri","uri":"<uri>","duration_ms":<n>,"sql_count":<n>,"write_rows":<n>,"read_rows":<n>,"error":true}
```

所有事件包含 ISO-8601 `timestamp`，时间在 SQL/URI 事件发生时生成，不使用异步 appender 的实际落盘时间。正常事件按 `normal-sample-rate` 抽样；慢 SQL/URI、大行数/大量 SQL 事件，以及执行失败的事件在正常运行条件下全量记录。URI 异常仅指 Web 事件回调携带 `Throwable`，不根据 HTTP status 判断。日志保留周期、脱敏规则和外部索引由应用日志平台配置。`sql_template` 保持 StatFilter 现有的 merge/parameterize 结果，不改变规整算法；不记录 SQL 原文或异常类名。

事件主路径的处理顺序固定为：先更新低基数 Meter，再判断阈值和正常采样；只有异常事件或采样命中的正常事件才生成 timestamp、计算 SQL MD5、序列化 JSON 并调用 logger。未采样的正常事件不承担日志构造开销。

SQL 指标和日志依赖 Druid `StatFilter`，URI 指标和日志依赖 `WebStatFilter`。Starter 启动时检查对应 Filter；缺失时输出一次明确提示，相应事件不会产生指标观测或日志。

## 开发与验收

1. 保留一期现有 Meter 名称、标签、LRU、SQL mapping、URI template、max window 和默认值，原有一期测试不得删除或降低断言。
2. 复用一个 listener 和同一批 SQL execute、update count、ResultSet close、Web 请求事件；一期分支由 `events.enabled` 控制，二期聚合分支由总开关控制，日志分支由 `logging.enabled` 控制。同一事件不得重复向同一期分支记录。
3. 二期 URI Meter 启动时固定注册，二期 SQL Meter 按稳定的 `datasource` 懒注册。共新增 9 种 `druid.agg.*` Meter，不按 SQL/URI 创建 Meter，也不与一期名称重叠。
4. SQL MD5、规整 SQL、URI 和原始 SQL 都不得用于二期 Prometheus 标签或进程内明细缓存；SQL MD5 和规整 SQL 只作为三类 SQL 日志字段使用。
5. 开关矩阵测试必须覆盖总开关、一期开关、日志开关的全部组合；关闭一期时二期仍递增，关闭日志时两期指标仍递增，关闭总开关时三条分支均不更新。开关重新打开后复用已有 Meter 继续累计。
6. 二期指标测试应验证不同 SQL 在同一 datasource 下命中相同 Meter、不同稳定 datasource 分别聚合、不同 URI 命中同一无标签 Meter、配置刷新在下一次事件生效，以及二期 Meter 数量不随 SQL/URI 数量增加。
7. 集成验收时同时开启两期，确认一期序列仍含 `sql`/`uri` 标签，二期 `druid_agg_*` 序列不含 `sql`/`uri`，二期 SQL 指标的业务标签仅为 `datasource`；检查两期 metric family 无名称或 label-key 冲突。
8. Logback 自动配置测试应覆盖默认值、应用属性覆盖、Apollo 变更后原子重建、非 Logback 回退提示、INFO/WARN 队列丢弃策略，以及停机最多等待 `shutdown-flush-timeout`。
9. Starter 增加 optional 的 `logback-classic` 依赖，Logback 自动配置使用 `@ConditionalOnClass`；不得强制使用其他日志实现的应用引入 Logback。
10. 为支持 `prometheus.enabled` 从 `false` 动态恢复，自动配置不得使用该属性作为创建 listener Bean 的 `@ConditionalOnProperty`；listener 始终存在并在事件路径读取当前开关。
